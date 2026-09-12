"""External news adapters.

Modular by design, and ordered by a strict source hierarchy.  The bot's default
configuration uses **no** external source at all: MT5's own calendar is enough
to trade, and every external dependency is another thing that can freeze at 3am.
Adapters are opt-in per deployment.

Source tiers (lower number wins on conflict, and only tier <= 2 may contribute
to an entry decision on its own):

1. Official central-bank / statistical-office releases
2. Reputable low-latency structured providers
3. High-quality financial news wires
4. Secondary aggregators
5. Social / unstructured  - never sufficient on its own, and disabled unless
   ``NewsConfig.allow_social_sources`` is explicitly set

Every adapter must be non-fatal: a failure marks the adapter unhealthy and the
engine keeps trading on charts plus the MT5 calendar.
"""
from __future__ import annotations

import datetime as dt
import json
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Optional, Protocol, Sequence

from ..broker.base import CalendarEvent
from ..clock import UTC, utcnow


@dataclass
class Headline:
    """An unscheduled item.  Deliberately minimal - we act on price, not prose."""
    time_utc: dt.datetime
    source: str
    tier: int
    currencies: tuple[str, ...]
    title: str
    url: str = ""
    sentiment: float = 0.0        # -1..1 if the provider supplies one
    confirmations: int = 1

    @property
    def actionable_alone(self) -> bool:
        """Only tier 1-2 with corroboration may influence an entry."""
        return self.tier <= 2 and self.confirmations >= 1


@dataclass
class AdapterHealth:
    name: str
    tier: int
    last_success_utc: Optional[dt.datetime] = None
    last_error: str = ""
    consecutive_failures: int = 0
    calls: int = 0
    enabled: bool = True
    # True when the last answer came from cache because the live call failed.
    # Serving stale beats serving nothing, but it must never be invisible.
    serving_stale: bool = False

    def stale(self, max_age_seconds: float, now: Optional[dt.datetime] = None) -> bool:
        if self.last_success_utc is None:
            return True
        now = now or utcnow()
        return (now - self.last_success_utc).total_seconds() > max_age_seconds

    @property
    def healthy(self) -> bool:
        return self.enabled and self.consecutive_failures < 3


class NewsAdapter(Protocol):
    name: str
    tier: int

    def fetch_calendar(self, start: dt.datetime,
                       end: dt.datetime) -> list[CalendarEvent]: ...
    def fetch_headlines(self, since: dt.datetime) -> list[Headline]: ...


class BaseAdapter:
    name = "base"
    tier = 4

    def __init__(self, *, api_key: str = "", timeout: float = 8.0,
                 cache_seconds: float = 60.0,
                 opener: Optional[Callable[[str, float], bytes]] = None):
        self.api_key = api_key
        self.timeout = timeout
        self.cache_seconds = cache_seconds
        self.health = AdapterHealth(self.name, self.tier)
        self._cache: dict[str, tuple[float, object]] = {}
        self._lock = threading.RLock()
        self._opener = opener or _http_get

    # Caching exists so a flapping provider cannot be hammered, and so a brief
    # outage is served from the last good response rather than looking like
    # "no news in the world".
    def _cached(self, key: str, producer: Callable[[], object]) -> object:
        with self._lock:
            hit = self._cache.get(key)
            if hit and time.time() - hit[0] < self.cache_seconds:
                return hit[1]
        try:
            value = producer()
        except Exception as exc:
            self.health.consecutive_failures += 1
            self.health.last_error = f"{type(exc).__name__}: {exc}"
            with self._lock:
                hit = self._cache.get(key)
            if hit:
                # Serving stale beats serving nothing, but the health record
                # says so explicitly so the dashboard can report it.
                self.health.serving_stale = True
                return hit[1]
            raise
        self.health.consecutive_failures = 0
        self.health.last_error = ""
        self.health.serving_stale = False
        self.health.last_success_utc = utcnow()
        self.health.calls += 1
        with self._lock:
            self._cache[key] = (time.time(), value)
        return value

    def clear_cache(self) -> None:
        """Drop cached responses so the next call really hits the source."""
        with self._lock:
            self._cache.clear()

    def get_json(self, url: str) -> object:
        return json.loads(self._opener(url, self.timeout).decode("utf-8"))

    def fetch_calendar(self, start: dt.datetime,
                       end: dt.datetime) -> list[CalendarEvent]:
        return []

    def fetch_headlines(self, since: dt.datetime) -> list[Headline]:
        return []


def _http_get(url: str, timeout: float) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "mintel/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


class Mt5CalendarAdapter(BaseAdapter):
    """The default and only always-on source: MT5's native calendar (tier 2)."""
    name = "mt5_calendar"
    tier = 2

    def __init__(self, broker, **kw):
        super().__init__(**kw)
        self.broker = broker

    def fetch_calendar(self, start: dt.datetime,
                       end: dt.datetime) -> list[CalendarEvent]:
        key = f"cal:{start.isoformat()}:{end.isoformat()}"
        return list(self._cached(key, lambda: self.broker.calendar(start, end)))


class StoreBackedAdapter(BaseAdapter):
    """Reads the local archive.  This is the *only* source used in backtests,
    which is what makes a news-aware backtest honest."""
    name = "calendar_store"
    tier = 2

    def __init__(self, store, **kw):
        super().__init__(**kw)
        self.store = store

    def fetch_calendar(self, start: dt.datetime,
                       end: dt.datetime) -> list[CalendarEvent]:
        return self.store.query(start, end)


class JsonCalendarAdapter(BaseAdapter):
    """Generic adapter for a structured JSON calendar endpoint.

    Configured with a URL template and a field map so a new provider is a config
    change, not a code change.  Left unconfigured by default.
    """
    name = "json_calendar"
    tier = 3

    def __init__(self, url_template: str, field_map: Optional[dict] = None,
                 *, name: str = "", tier: int = 3, **kw):
        super().__init__(**kw)
        if name:
            self.name = name
        self.tier = tier
        self.health = AdapterHealth(self.name, self.tier)
        self.url_template = url_template
        self.field_map = field_map or {
            "id": "id", "time": "date", "currency": "country",
            "name": "title", "importance": "impact", "actual": "actual",
            "forecast": "estimate", "previous": "previous",
        }

    def fetch_calendar(self, start: dt.datetime,
                       end: dt.datetime) -> list[CalendarEvent]:
        url = (self.url_template
               .replace("{start}", start.date().isoformat())
               .replace("{end}", end.date().isoformat())
               .replace("{key}", self.api_key))
        raw = self._cached(f"json:{url}", lambda: self.get_json(url))
        items = raw if isinstance(raw, list) else raw.get("events", [])
        fm = self.field_map
        out: list[CalendarEvent] = []
        for it in items:
            try:
                ts = _parse_time(it.get(fm["time"]))
                if ts is None:
                    continue
                out.append(CalendarEvent(
                    event_id=str(it.get(fm["id"], "")) or f"{self.name}:{ts.isoformat()}",
                    time_utc=ts,
                    currency=str(it.get(fm["currency"], "")).upper()[:3],
                    country=str(it.get("country", "")),
                    name=str(it.get(fm["name"], "")),
                    importance=_importance(it.get(fm["importance"])),
                    actual=_num(it.get(fm["actual"])),
                    forecast=_num(it.get(fm["forecast"])),
                    previous=_num(it.get(fm["previous"])),
                    source=self.name))
            except Exception:
                continue
        return out


def _parse_time(v) -> Optional[dt.datetime]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return dt.datetime.fromtimestamp(float(v), tz=UTC)
    s = str(v).replace("Z", "+00:00")
    try:
        d = dt.datetime.fromisoformat(s)
    except ValueError:
        return None
    return d.replace(tzinfo=UTC) if d.tzinfo is None else d.astimezone(UTC)


def _importance(v) -> int:
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return max(0, min(3, int(v)))
    s = str(v).strip().lower()
    return {"high": 3, "medium": 2, "moderate": 2, "low": 1, "none": 0,
            "holiday": 0}.get(s, 0)


def _num(v) -> Optional[float]:
    if v in (None, "", "-"):
        return None
    s = str(v).strip().replace(",", "").replace("%", "")
    mult = 1.0
    if s and s[-1] in "KMB":
        mult = {"K": 1e3, "M": 1e6, "B": 1e9}[s[-1]]
        s = s[:-1]
    try:
        return float(s) * mult
    except ValueError:
        return None


class AdapterRegistry:
    """Holds adapters in tier order and merges their output.

    Conflict rule: for the same event at the same time, the lowest tier wins,
    and a missing field is filled from the next tier down.
    """

    def __init__(self, adapters: Sequence[BaseAdapter] = ()):
        self.adapters: list[BaseAdapter] = sorted(adapters, key=lambda a: a.tier)

    def add(self, adapter: BaseAdapter) -> None:
        self.adapters.append(adapter)
        self.adapters.sort(key=lambda a: a.tier)

    def health(self) -> list[AdapterHealth]:
        return [a.health for a in self.adapters]

    @property
    def any_healthy(self) -> bool:
        return any(a.health.healthy for a in self.adapters)

    @property
    def serving_stale(self) -> tuple[str, ...]:
        """Adapters currently answering from cache after a live failure."""
        return tuple(a.name for a in self.adapters if a.health.serving_stale)

    def clear_caches(self) -> None:
        for ad in self.adapters:
            try:
                ad.clear_cache()
            except Exception:
                pass

    def fetch_calendar(self, start: dt.datetime, end: dt.datetime, *,
                       force: bool = False) -> tuple[list[CalendarEvent],
                                                     list[str]]:
        if force:
            # A forced refresh must see the world as it is now, not as it was
            # when the cache was filled - otherwise an event that has just
            # appeared stays invisible until the TTL expires.
            self.clear_caches()
        merged: dict[tuple[str, str], CalendarEvent] = {}
        errors: list[str] = []
        for ad in self.adapters:
            if not ad.health.enabled:
                continue
            try:
                events = ad.fetch_calendar(start, end)
            except Exception as exc:
                errors.append(f"{ad.name}: {exc}")
                continue
            for e in events:
                key = (e.name.lower().strip(), e.time_utc.isoformat())
                cur = merged.get(key)
                if cur is None:
                    merged[key] = e
                    continue
                merged[key] = CalendarEvent(
                    event_id=cur.event_id, time_utc=cur.time_utc,
                    currency=cur.currency or e.currency,
                    country=cur.country or e.country,
                    name=cur.name or e.name,
                    importance=max(cur.importance, e.importance),
                    actual=cur.actual if cur.actual is not None else e.actual,
                    forecast=cur.forecast if cur.forecast is not None else e.forecast,
                    previous=cur.previous if cur.previous is not None else e.previous,
                    revised_previous=(cur.revised_previous
                                      if cur.revised_previous is not None
                                      else e.revised_previous),
                    unit=cur.unit or e.unit, source=cur.source)
        return sorted(merged.values(), key=lambda e: e.time_utc), errors

    def fetch_headlines(self, since: dt.datetime, *,
                        allow_social: bool = False) -> list[Headline]:
        """Merge headlines, corroborating across sources.

        Two independent reputable sources carrying the same story raise
        ``confirmations``; a lone tier-5 social post never becomes actionable.
        """
        out: list[Headline] = []
        for ad in self.adapters:
            if not ad.health.enabled:
                continue
            if ad.tier >= 5 and not allow_social:
                continue
            try:
                out.extend(ad.fetch_headlines(since))
            except Exception as exc:
                ad.health.last_error = str(exc)
        by_title: dict[str, Headline] = {}
        for h in out:
            key = " ".join(sorted(h.title.lower().split()))[:120]
            cur = by_title.get(key)
            if cur is None:
                by_title[key] = h
            else:
                best = cur if cur.tier <= h.tier else h
                by_title[key] = Headline(
                    time_utc=min(cur.time_utc, h.time_utc), source=best.source,
                    tier=best.tier, currencies=tuple(set(cur.currencies)
                                                     | set(h.currencies)),
                    title=best.title, url=best.url,
                    sentiment=(cur.sentiment + h.sentiment) / 2,
                    confirmations=cur.confirmations + 1)
        return sorted(by_title.values(), key=lambda h: h.time_utc)
