"""Health supervision, heartbeats, safe mode and self-healing.

The governing idea of this module is the distinction the whole system is built
around: **market risk versus software stupidity.**  Losing money because a
judged trade went against us is acceptable.  Losing money because MT5 quietly
died, the feed went stale, a stop vanished or a process hung is an engineering
failure, and this module exists to make those failures impossible to miss and,
wherever possible, self-correcting.

Two principles:

* A component may not certify its own health.  Every worker writes a
  *heartbeat*; the supervisor - and, one level up, an independent watchdog
  process - decides whether that heartbeat is acceptable.
* "Always trading" means always *operating and looking*.  It never means
  sending market orders while the data is stale or the connection is broken.
  That situation is SAFE MODE: keep recovering, keep positions protected, keep
  reconciling, resume automatically once healthy.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import shutil
import socket
import threading
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Callable, Optional, Sequence

from ..clock import UTC, is_weekend_gap, to_utc, utcnow
from ..config import Config

log = logging.getLogger("mintel.health")


class Severity(str, Enum):
    OK = "OK"
    WARN = "WARN"
    FAIL = "FAIL"


@dataclass
class Check:
    name: str
    severity: Severity
    message: str
    detail: dict = field(default_factory=dict)
    # A check that blocks entries but not position management.
    blocks_entries: bool = False
    # A check whose failure means we cannot trust anything: SAFE MODE.
    critical: bool = False

    @property
    def ok(self) -> bool:
        return self.severity is Severity.OK


@dataclass
class HealthReport:
    as_of: dt.datetime
    checks: tuple[Check, ...]
    safe_mode: bool
    entries_allowed: bool
    summary: str

    @property
    def failures(self) -> tuple[Check, ...]:
        return tuple(c for c in self.checks if c.severity is Severity.FAIL)

    @property
    def warnings(self) -> tuple[Check, ...]:
        return tuple(c for c in self.checks if c.severity is Severity.WARN)

    def plain_english(self) -> list[str]:
        out = []
        for c in self.checks:
            if c.ok:
                continue
            prefix = "PROBLEM" if c.severity is Severity.FAIL else "WARNING"
            out.append(f"{prefix}: {c.message}")
        return out

    def to_dict(self) -> dict:
        return {"as_of": self.as_of.isoformat(), "safe_mode": self.safe_mode,
                "entries_allowed": self.entries_allowed,
                "summary": self.summary,
                "checks": [{"name": c.name, "severity": c.severity.value,
                            "message": c.message, "detail": c.detail}
                           for c in self.checks]}


# ------------------------------------------------------------- heartbeats -----

class Heartbeat:
    """A file-backed liveness signal.

    Deliberately a file rather than in-process state: the watchdog runs in a
    *separate process* and must be able to see it even when the trader has
    hung, and it must survive the trader being killed outright.
    """

    def __init__(self, directory: str | Path, name: str,
                 clock: Callable[[], dt.datetime] = utcnow):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.name = name
        self.clock = clock
        self.path = self.dir / f"{name}.heartbeat.json"
        self._lock = threading.Lock()
        self.counter = 0

    def beat(self, detail: Optional[dict] = None) -> None:
        self.counter += 1
        payload = {"name": self.name, "pid": os.getpid(),
                   "ts_utc": to_utc(self.clock()).isoformat(),
                   "counter": self.counter,
                   "host": socket.gethostname(), "detail": detail or {}}
        tmp = self.path.with_suffix(".tmp")
        with self._lock:
            try:
                tmp.write_text(json.dumps(payload))
                tmp.replace(self.path)      # atomic: never a half-written beat
            except OSError as exc:
                log.warning("heartbeat write failed for %s: %s", self.name, exc)

    def read(self) -> Optional[dict]:
        try:
            return json.loads(self.path.read_text())
        except Exception:
            return None

    def age_seconds(self) -> Optional[float]:
        data = self.read()
        if not data:
            return None
        try:
            ts = dt.datetime.fromisoformat(data["ts_utc"])
        except Exception:
            return None
        return (to_utc(self.clock()) - to_utc(ts)).total_seconds()


def read_all_heartbeats(directory: str | Path,
                        now: Optional[dt.datetime] = None) -> dict[str, dict]:
    out: dict[str, dict] = {}
    d = Path(directory)
    if not d.exists():
        return out
    for f in d.glob("*.heartbeat.json"):
        try:
            data = json.loads(f.read_text())
            ts = dt.datetime.fromisoformat(data["ts_utc"])
            ref = to_utc(now) if now is not None else utcnow()
            data["age_seconds"] = round((ref - to_utc(ts)).total_seconds(), 1)
            out[data.get("name", f.stem)] = data
        except Exception:
            continue
    return out


# -------------------------------------------------------------- supervisor ----

@dataclass
class Healer:
    """One recoverable fault and how to fix it, with a rate limit."""
    name: str
    attempt: Callable[[], bool]
    max_per_hour: int = 6
    _attempts: list[dt.datetime] = field(default_factory=list)

    def allowed(self, now: dt.datetime) -> bool:
        cutoff = now - dt.timedelta(hours=1)
        self._attempts = [t for t in self._attempts if t >= cutoff]
        return len(self._attempts) < self.max_per_hour

    def run(self, now: dt.datetime) -> tuple[bool, str]:
        if not self.allowed(now):
            return False, (f"{self.name}: giving up for now - already tried "
                           f"{len(self._attempts)} times this hour")
        self._attempts.append(now)
        try:
            ok = bool(self.attempt())
            return ok, (f"{self.name}: recovered" if ok
                        else f"{self.name}: recovery attempt failed")
        except Exception as exc:
            return False, f"{self.name}: recovery raised {exc}"


class HealthSupervisor:
    def __init__(self, cfg: Config, broker=None, journal=None, news=None,
                 executor=None, expected_login: int = 0,
                 expected_server: str = "",
                 clock: Callable[[], dt.datetime] = utcnow,
                 own_heartbeats: Sequence[str] = ()):
        # The clock is injected so the supervisor works unchanged in a
        # backtest, where "now" is the replay timestamp rather than wall time.
        self.clock = clock
        self.cfg = cfg
        self.broker = broker
        self.journal = journal
        self.news = news
        self.executor = executor
        self.expected_login = expected_login or cfg.account_login
        self.expected_server = expected_server or cfg.account_server
        # Heartbeats written by THIS process are excluded from its own
        # liveness check: a hung process cannot notice its own hang, so
        # checking its own beats would be self-certification.  The independent
        # watchdog judges this process; this process judges the watchdog.  The
        # two watch each other and neither vouches for itself.
        self.own_heartbeats = set(own_heartbeats)
        self.heartbeat_dir = Path(cfg.ops.data_dir) / "heartbeats"
        self.safe_mode = False
        self.safe_mode_reason = ""
        self.safe_mode_since: Optional[dt.datetime] = None
        self.last_report: Optional[HealthReport] = None
        self.healers: dict[str, Healer] = {}
        self.recovery_log: list[str] = []
        self._register_default_healers()

    # -------------------------------------------------------------- healers --
    def _register_default_healers(self) -> None:
        if self.broker is not None:
            # Deliberately a SINGLE attempt with no sleeping.  Healing runs
            # inside the trading cycle; a retry loop here would stall position
            # management and freeze the dashboard while the operator is trying
            # to find out what went wrong.  The cycle repeats, and the healer's
            # own hourly limit supplies the backoff.
            self.healers["BROKER_CONNECTION"] = Healer(
                "MT5 connection", self._reconnect_broker_once,
                max_per_hour=self.cfg.ops.max_restarts_per_hour)
        if self.journal is not None:
            self.healers["DATABASE"] = Healer(
                "trade database", lambda: bool(self.journal.reconnect()))
        if self.news is not None:
            self.healers["NEWS"] = Healer(
                "news engine", lambda: bool(self.news.refresh()))

    def _reconnect_broker_once(self) -> bool:
        reconnect = getattr(self.broker, "reconnect", None)
        if reconnect is None:
            return bool(self.broker.connect())
        try:
            return bool(reconnect(attempts=1, base_delay=0.0))
        except TypeError:
            return bool(reconnect())

    def register_healer(self, check_name: str, healer: Healer) -> None:
        self.healers[check_name] = healer

    # --------------------------------------------------------------- checks --
    def run_checks(self, *, positions: Sequence = (), risk_snapshot=None,
                   last_scan: Optional[dt.datetime] = None,
                   symbols_expected: int = 0,
                   scan_stale_limit: Optional[float] = None) -> HealthReport:
        now = to_utc(self.clock())
        checks: list[Check] = []
        add = checks.append

        # --- process / OS ----------------------------------------------------
        add(self._check_disk())
        add(self._check_memory())
        add(self._check_clock())

        # --- broker ----------------------------------------------------------
        account = None
        if self.broker is None:
            add(Check("BROKER", Severity.FAIL, "no broker is configured",
                      critical=True))
        else:
            connected = False
            try:
                connected = bool(self.broker.is_connected())
            except Exception as exc:
                add(Check("BROKER_CONNECTION", Severity.FAIL,
                          f"the trading platform could not be reached: {exc}",
                          critical=True))
            if not connected:
                add(Check("BROKER_CONNECTION", Severity.FAIL,
                          "MetaTrader 5 is not connected to the broker",
                          critical=True))
            else:
                add(Check("BROKER_CONNECTION", Severity.OK,
                          "MetaTrader 5 is connected"))
                stale = ""
                try:
                    stale = str(getattr(self.broker, "outdated", lambda: "")() or "")
                except Exception:
                    stale = ""
                if stale:
                    add(Check("BRIDGE_VERSION", Severity.FAIL, stale, critical=True))
                try:
                    account = self.broker.account()
                except Exception as exc:
                    add(Check("ACCOUNT", Severity.FAIL,
                              f"account details could not be read: {exc}",
                              critical=True))
            if account is not None:
                add(self._check_account(account))
                add(self._check_permission(account))
                add(self._check_margin(account, risk_snapshot))
            if connected:
                add(self._check_ticks(symbols_expected))

        # --- news ------------------------------------------------------------
        add(self._check_news())

        # --- database --------------------------------------------------------
        if self.journal is not None:
            ok = self.journal.healthy()
            add(Check("DATABASE",
                      Severity.OK if ok else Severity.FAIL,
                      "trade database is fine" if ok
                      else "the trade database is not responding",
                      blocks_entries=not ok))

        # --- heartbeats ------------------------------------------------------
        add(self._check_heartbeats())
        if last_scan is not None:
            age = (now - to_utc(last_scan)).total_seconds()
            # The caller can supply a limit derived from its OBSERVED cycle
            # cadence, which is what makes this check meaningful both live
            # (seconds per cycle) and in replay (where a cycle can advance the
            # clock by minutes).
            limit = scan_stale_limit or max(
                60.0, self.cfg.scan.scan_interval_seconds * 12)
            stale = age > limit
            add(Check("SCAN_FRESHNESS",
                      Severity.FAIL if stale else Severity.OK,
                      f"the last market scan was {age:.0f}s ago"
                      if stale else f"markets scanned {age:.0f}s ago",
                      {"age_seconds": round(age, 1)}, blocks_entries=stale))

        # --- positions -------------------------------------------------------
        add(self._check_position_protection(positions))

        critical = [c for c in checks if c.critical and not c.ok]
        if critical:
            self._enter_safe_mode("; ".join(c.message for c in critical))
        elif self.safe_mode:
            self._leave_safe_mode()

        blocking = [c for c in checks
                    if not c.ok and (c.blocks_entries or c.critical)]
        entries_allowed = not blocking and not self.safe_mode
        fails = sum(1 for c in checks if c.severity is Severity.FAIL)
        warns = sum(1 for c in checks if c.severity is Severity.WARN)
        if self.safe_mode:
            summary = f"SAFE MODE - {self.safe_mode_reason}"
        elif fails:
            summary = f"{fails} problem(s) found; new trades are paused"
        elif warns:
            summary = f"running with {warns} warning(s)"
        else:
            summary = "everything is healthy"

        report = HealthReport(now, tuple(checks), self.safe_mode,
                              entries_allowed, summary)
        self.last_report = report
        return report

    # ------------------------------------------------------ individual checks -
    def _check_disk(self) -> Check:
        try:
            usage = shutil.disk_usage(Path(self.cfg.ops.data_dir).resolve().anchor
                                      or ".")
            free_mb = usage.free / (1024 * 1024)
        except Exception as exc:
            return Check("DISK", Severity.WARN,
                         f"disk space could not be checked: {exc}")
        low = free_mb < self.cfg.ops.min_free_disk_mb
        return Check("DISK", Severity.FAIL if low else Severity.OK,
                     f"only {free_mb:.0f} MB of disk space left" if low
                     else f"{free_mb:.0f} MB of disk space free",
                     {"free_mb": round(free_mb, 1)}, blocks_entries=low)

    def _check_memory(self) -> Check:
        pct = None
        try:  # psutil is optional; absence must not be a failure
            import psutil
            pct = psutil.virtual_memory().percent
        except Exception:
            try:
                with open("/proc/meminfo") as fh:
                    info = {}
                    for line in fh:
                        k, v = line.split(":", 1)
                        info[k] = float(v.strip().split()[0])
                total, avail = info.get("MemTotal", 0), info.get("MemAvailable", 0)
                if total:
                    pct = (1.0 - avail / total) * 100.0
            except Exception:
                pct = None
        if pct is None:
            return Check("MEMORY", Severity.OK, "memory usage not measurable "
                                                "on this platform")
        high = pct > self.cfg.ops.max_memory_pct
        return Check("MEMORY", Severity.WARN if high else Severity.OK,
                     f"memory is {pct:.0f}% used" if high
                     else f"memory {pct:.0f}% used",
                     {"percent": round(pct, 1)})

    def _check_clock(self) -> Check:
        """Compare the OS clock with the broker's own clock.

        A wrong machine clock silently corrupts session windows, news timing and
        every staleness test, so this is checked explicitly rather than assumed.
        """
        if self.broker is None:
            return Check("CLOCK", Severity.OK, "clock not checked")
        try:
            srv = self.broker.server_time()
            clock = getattr(self.broker, "clock", None)
            if clock is not None:
                implied_utc = clock.server_to_utc(srv)
            else:
                implied_utc = to_utc(srv) if srv.tzinfo else None
            if implied_utc is None:
                return Check("CLOCK", Severity.OK,
                             "clock offset not measurable")
            skew = abs((to_utc(self.clock()) - implied_utc).total_seconds())
        except Exception as exc:
            return Check("CLOCK", Severity.WARN,
                         f"clock comparison failed: {exc}")
        bad = skew > self.cfg.ops.max_clock_skew_seconds
        return Check("CLOCK", Severity.FAIL if bad else Severity.OK,
                     f"this machine's clock is {skew:.0f}s out of step with "
                     f"the broker - session and news timing cannot be trusted"
                     if bad else f"clock agrees with the broker "
                                 f"(within {skew:.0f}s)",
                     {"skew_seconds": round(skew, 1)}, blocks_entries=bad)

    def _check_account(self, account) -> Check:
        """Right account, right server, right environment.

        Connecting to the wrong login - or finding a LIVE account while
        configured for DEMO - stops trading immediately.  This is the check that
        prevents a config mishap from trading real money by accident.
        """
        problems = []
        if self.expected_login and int(account.login) != int(self.expected_login):
            problems.append(f"connected to account {account.login} but the "
                            f"configuration expects {self.expected_login}")
        if self.expected_server and account.server and \
                account.server.strip().lower() != self.expected_server.strip().lower():
            problems.append(f"connected to server '{account.server}' but the "
                            f"configuration expects '{self.expected_server}'")
        want_live = self.cfg.is_live
        if want_live and account.is_demo:
            problems.append("configured for LIVE but this is a demo account")
        if not want_live and not account.is_demo:
            problems.append("this is a LIVE account but the configuration is "
                            "DEMO - refusing to trade")
        if problems:
            return Check("ACCOUNT", Severity.FAIL, "; ".join(problems),
                         {"login": account.login, "server": account.server,
                          "is_demo": account.is_demo}, critical=True)
        return Check("ACCOUNT", Severity.OK,
                     f"{'DEMO' if account.is_demo else 'LIVE'} account "
                     f"{account.login} on {account.server}",
                     {"login": account.login, "server": account.server,
                      "is_demo": account.is_demo})

    def _check_permission(self, account) -> Check:
        ok = bool(account.trade_allowed)
        return Check("TRADE_PERMISSION",
                     Severity.OK if ok else Severity.FAIL,
                     "trading is enabled" if ok
                     else "trading is disabled in MetaTrader 5 - switch on "
                          "'Algo Trading' (the button in the toolbar)",
                     blocks_entries=not ok)

    def _check_margin(self, account, risk_snapshot) -> Check:
        level = account.margin_level
        if account.margin <= 0:
            return Check("MARGIN", Severity.OK, "no margin in use")
        low = level < self.cfg.risk.min_margin_level_pct
        return Check("MARGIN", Severity.FAIL if low else Severity.OK,
                     f"margin level is {level:.0f}% (minimum "
                     f"{self.cfg.risk.min_margin_level_pct:.0f}%)" if low
                     else f"margin level {level:.0f}%",
                     {"margin_level": level}, blocks_entries=low)

    def _check_ticks(self, symbols_expected: int) -> Check:
        """Are prices actually arriving?

        Checked against a handful of liquid symbols rather than all of them, and
        relaxed over the weekend so a closed market does not look like a fault.
        """
        try:
            symbols = list(self.broker.symbols())[:60]
        except Exception as exc:
            return Check("MARKET_DATA", Severity.FAIL,
                         f"the symbol list could not be read: {exc}",
                         critical=True)
        if not symbols:
            return Check("MARKET_DATA", Severity.FAIL,
                         "no symbols are available in Market Watch",
                         critical=True)
        if symbols_expected and len(symbols) < symbols_expected * 0.5:
            return Check("MARKET_DATA", Severity.WARN,
                         f"only {len(symbols)} symbols available, expected "
                         f"about {symbols_expected}")
        now = to_utc(self.clock())
        weekend = is_weekend_gap(now)
        limit = (self.cfg.scan.max_tick_age_seconds_weekend if weekend
                 else self.cfg.scan.max_tick_age_seconds)
        ages: list[float] = []
        for sym in symbols[:8]:
            try:
                t = self.broker.tick(sym)
            except Exception:
                continue
            if t is not None:
                ages.append((now - to_utc(t.time)).total_seconds())
        if not ages:
            return Check("MARKET_DATA", Severity.FAIL,
                         "no live prices are arriving from the broker",
                         critical=True)
        freshest = min(ages)
        stale = freshest > limit
        return Check("MARKET_DATA",
                     Severity.WARN if (stale and weekend) else
                     (Severity.FAIL if stale else Severity.OK),
                     (f"price data is stale - the freshest price is "
                      f"{freshest:.0f}s old" + (" (weekend, markets closed)"
                                                if weekend else ""))
                     if stale else f"live prices arriving "
                                   f"({freshest:.0f}s old)",
                     {"freshest_age_seconds": round(freshest, 1),
                      "weekend": weekend},
                     blocks_entries=stale, critical=stale and not weekend)

    def _check_news(self) -> Check:
        if self.news is None or not self.cfg.news.enabled:
            return Check("NEWS", Severity.OK, "news engine is switched off")
        h = self.news.health
        if not h.healthy:
            return Check("NEWS", Severity.FAIL,
                         f"the news engine is unhealthy: "
                         f"{h.last_error or h.degraded_reason or 'unknown'}",
                         asdict(h) if hasattr(h, "__dataclass_fields__") else {},
                         blocks_entries=False)
        if h.stale(self.cfg.news.max_staleness_seconds, to_utc(self.clock())):
            return Check("NEWS", Severity.WARN,
                         "the economic calendar has not refreshed recently; "
                         "trading continues on charts and the stored calendar",
                         {"events_known": h.events_known})
        if h.degraded_reason:
            return Check("NEWS", Severity.WARN, h.degraded_reason,
                         {"events_known": h.events_known})
        return Check("NEWS", Severity.OK,
                     f"calendar live ({h.events_known} events known)",
                     {"events_known": h.events_known})

    def _check_heartbeats(self) -> Check:
        now = to_utc(self.clock())
        beats = read_all_heartbeats(self.heartbeat_dir, now)
        external = {n: b for n, b in beats.items()
                    if n not in self.own_heartbeats}
        # Scaled with the cycle cadence: a slow configured interval must not
        # make a perfectly healthy component look dead.
        limit = max(self.cfg.ops.heartbeat_stale_seconds,
                    4.0 * self.cfg.scan.scan_interval_seconds)
        if not external:
            return Check("HEARTBEATS", Severity.WARN,
                         "the watchdog has not reported in yet - this process "
                         "is running unsupervised",
                         {"known": list(beats)})
        stale = {n: b["age_seconds"] for n, b in external.items()
                 if b.get("age_seconds", 1e9) > limit}
        if stale:
            names = ", ".join(f"{n} ({a:.0f}s)" for n, a in stale.items())
            # A dead watchdog is a serious warning, but it is NOT a reason to
            # stop trading: the trader is demonstrably still working.
            blocking = any(n != "watchdog" for n in stale)
            return Check("HEARTBEATS",
                         Severity.FAIL if blocking else Severity.WARN,
                         f"these parts of the system have stopped reporting: "
                         f"{names}", {"stale": stale}, blocks_entries=blocking)
        return Check("HEARTBEATS", Severity.OK,
                     f"{len(external)} supervising component(s) reporting",
                     {"components": {n: b["age_seconds"]
                                     for n, b in external.items()}})

    def _check_position_protection(self, positions: Sequence) -> Check:
        naked = [p for p in positions if not getattr(p, "sl", 0)]
        if naked:
            return Check("POSITION_PROTECTION", Severity.FAIL,
                         f"{len(naked)} open position(s) have no stop loss at "
                         f"the broker",
                         {"tickets": [p.ticket for p in naked]},
                         blocks_entries=True)
        return Check("POSITION_PROTECTION", Severity.OK,
                     f"all {len(positions)} open position(s) protected at the "
                     f"broker")

    # ------------------------------------------------------------ safe mode --
    def _enter_safe_mode(self, reason: str) -> None:
        if not self.safe_mode:
            log.critical("ENTERING SAFE MODE: %s", reason)
            self.safe_mode_since = to_utc(self.clock())
            if self.journal is not None:
                try:
                    self.journal.log_event("SAFE_MODE", reason, "CRITICAL")
                except Exception:
                    pass
        self.safe_mode = True
        self.safe_mode_reason = reason

    def _leave_safe_mode(self) -> None:
        log.warning("leaving SAFE MODE - conditions recovered")
        if self.journal is not None:
            try:
                self.journal.log_event("SAFE_MODE_END",
                                       "conditions recovered", "WARNING")
            except Exception:
                pass
        self.safe_mode = False
        self.safe_mode_reason = ""
        self.safe_mode_since = None

    # ------------------------------------------------------------- self-heal --
    def heal(self, report: Optional[HealthReport] = None) -> list[str]:
        """Attempt recovery for every failing check that has a healer.

        Rate-limited per healer so a genuinely broken environment produces a
        clear alert instead of an infinite restart loop.
        """
        report = report or self.last_report
        if report is None:
            return []
        now = to_utc(self.clock())
        actions: list[str] = []
        for check in report.failures:
            healer = self.healers.get(check.name)
            if healer is None:
                continue
            ok, msg = healer.run(now)
            actions.append(msg)
            log.warning("self-heal: %s", msg)
            if self.journal is not None:
                try:
                    self.journal.log_event(
                        "SELF_HEAL", msg, "WARNING" if ok else "ERROR",
                        {"check": check.name})
                except Exception:
                    pass
            if ok and check.name == "BROKER_CONNECTION" and self.executor:
                # Reconnection invalidates local state: the broker is the
                # source of truth, so rebuild from it before doing anything.
                try:
                    result = self.executor.reconcile()
                    actions.append(f"reconciled after reconnect: {result}")
                except Exception as exc:
                    actions.append(f"reconciliation after reconnect failed: {exc}")
        self.recovery_log = (self.recovery_log + actions)[-100:]
        return actions
