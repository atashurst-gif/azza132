"""The bot publishes its own daily record, so nobody has to paste screenshots."""
from __future__ import annotations

import base64
import datetime as dt
import json
from pathlib import Path

import pytest

from mintel.broker.sim import SimBroker
from mintel.config import Config
from mintel.engine.journal import Journal
from mintel.ops import report_upload as ru

NOW = dt.datetime(2026, 9, 17, 21, 30, tzinfo=dt.timezone.utc)
DAY = NOW.replace(hour=0, minute=0)


def _cfg(workdir) -> Config:
    cfg = Config()
    cfg.ops.data_dir = str(workdir)
    cfg.ops.log_dir = str(workdir)
    cfg.tracking_start_utc = "2026-09-15T12:59:00+00:00"
    return cfg


def _journal_with_trades(workdir):
    j = Journal(workdir / "journal.sqlite")
    j._exec("INSERT INTO trades (ticket, symbol, side, opened_utc, closed_utc, pnl_money, tactic, regime, "
            "opportunity, tier, exit_reason, realised_r, mfe_r, mae_r, final_flow_state) VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (7, "XAUJPY", "BUY", "2026-09-17T11:54:00+00:00", "2026-09-17T11:58:00+00:00", 16.06,
             "BREAKOUT_RETEST", "TREND", 71.2, "STRONG", "[tp 678567]", 3.1, 3.2, 0.1, "STRONG_FLOW"))
    j.close()


class FakeBroker(SimBroker):
    def deals_since(self, since, magic=0, closing_only=True):
        t0 = dt.datetime(2026, 9, 17, 11, 54, tzinfo=dt.timezone.utc)
        return [{"position": 7, "symbol": "XAUJPY", "volume": 0.02, "profit": 0.0, "commission": -0.06,
                 "is_entry": True, "time": t0},
                {"position": 7, "symbol": "XAUJPY", "volume": 0.02, "profit": 16.0, "commission": -0.06,
                 "is_entry": False, "time": t0 + dt.timedelta(minutes=4)}]


class TestBundle:
    def test_bundle_has_review_trades_status_rules_and_latest(self, workdir):
        cfg = _cfg(workdir)
        _journal_with_trades(workdir)
        broker = FakeBroker(("XAUJPY",), start=NOW); broker.connect()
        files = ru.build_bundle(cfg, broker, DAY, fetch=lambda url: json.dumps({"status": {"today_pnl": 16.0}}))
        assert set(files) >= {"reports/2026-09-17/day_review.txt", "reports/2026-09-17/trades.csv",
                              "reports/2026-09-17/status.json", "reports/2026-09-17/rules.json",
                              "reports/latest/day_review.txt", "reports/latest/DATE"}
        assert "REAL RESULT" in files["reports/2026-09-17/day_review.txt"]
        csv_text = files["reports/2026-09-17/trades.csv"]
        assert "BREAKOUT_RETEST" in csv_text and "[tp 678567]" in csv_text and "15.94" in csv_text
        rules = json.loads(files["reports/2026-09-17/rules.json"])
        assert rules["scan"]["min_reward_risk"] == cfg.scan.min_reward_risk
        assert rules["build"] and rules["tracking_start_utc"] == cfg.tracking_start_utc
        assert json.loads(files["reports/2026-09-17/status.json"])["status"]["today_pnl"] == 16.0
        assert files["reports/latest/DATE"].strip() == "2026-09-17"
        assert files["reports/latest/trades.csv"] == csv_text

    def test_status_unavailable_is_noted_not_fatal(self, workdir):
        cfg = _cfg(workdir)
        def boom(url): raise OSError("connection refused")
        assert "unavailable" in json.loads(ru.status_json(cfg, fetch=boom))


class FakeGitHub:
    """Enough of the contents API to prove the writes are right."""
    def __init__(self, branch_exists=False):
        self.files: dict = {}
        self.branches = {"main"} | ({"reports"} if branch_exists else set())
        self.calls = []

    def __call__(self, method, url, body):
        self.calls.append((method, url))
        path = url.replace(ru.API, "")
        if method == "GET" and "/branches/" in path:
            return (200, {}) if path.rsplit("/", 1)[1] in self.branches else (404, {})
        if method == "GET" and path == "/repos/o/r":
            return 200, {"default_branch": "main"}
        if method == "GET" and "/git/ref/heads/" in path:
            return 200, {"object": {"sha": "abc123"}}
        if method == "POST" and path.endswith("/git/refs"):
            self.branches.add(body["ref"].split("/")[-1]); return 201, {}
        if method == "GET" and "/contents/" in path:
            p = path.split("/contents/")[1].split("?")[0]
            if p in self.files:
                return 200, {"sha": "s" + p, "content": base64.b64encode(self.files[p].encode()).decode()}
            return 404, {}
        if method == "PUT" and "/contents/" in path:
            p = path.split("/contents/")[1]
            assert body["branch"] == "reports"
            if p in self.files:
                assert body.get("sha") == "s" + p, "an existing file must be updated with its sha"
            self.files[p] = base64.b64decode(body["content"]).decode()
            return 201, {}
        return 500, {"message": f"unexpected {method} {path}"}


class TestUpload:
    def test_creates_the_branch_and_writes_every_file(self):
        gh = FakeGitHub(branch_exists=False)
        n = ru.upload({"reports/2026-09-17/day_review.txt": "hello\n",
                       "reports/latest/DATE": "2026-09-17\n"}, "o/r", "reports", "tok", opener=gh, day="2026-09-17")
        assert n == 2 and "reports" in gh.branches
        assert gh.files["reports/2026-09-17/day_review.txt"] == "hello\n"

    def test_rewrites_changed_files_and_skips_unchanged(self):
        gh = FakeGitHub(branch_exists=True)
        gh.files["reports/latest/DATE"] = "2026-09-16\n"
        gh.files["reports/latest/day_review.txt"] = "same\n"
        ru.upload({"reports/latest/DATE": "2026-09-17\n", "reports/latest/day_review.txt": "same\n"},
                  "o/r", "reports", "tok", opener=gh)
        assert gh.files["reports/latest/DATE"] == "2026-09-17\n"
        puts = [u for m, u in gh.calls if m == "PUT"]
        assert len(puts) == 1 and puts[0].endswith("reports/latest/DATE")

    def test_bad_token_is_a_clear_error(self):
        def denied(method, url, body): return 401, {"message": "Bad credentials"}
        with pytest.raises(RuntimeError, match="check the token"):
            ru.upload({"a": "b"}, "o/r", "reports", "bad", opener=denied)


class TestSecrets:
    def test_token_is_stored_outside_config_and_survives_config_save(self, workdir):
        cfg_path = workdir / "config.json"
        cfg = Config(); cfg.account_password = "pw"
        cfg.save(cfg_path)
        ru.store_token(str(cfg_path), "ghp_secret\n")
        assert ru.read_token(str(cfg_path)) == "ghp_secret"
        assert "ghp_secret" not in cfg_path.read_text()
        cfg.save(cfg_path)                          # a later save must not drop it
        assert ru.read_token(str(cfg_path)) == "ghp_secret"
        sec = json.loads((workdir / "secrets.json").read_text())
        assert sec["account_password"] == "pw"

    def test_no_token_means_a_helpful_exit(self, workdir, capsys):
        cfg_path = workdir / "config.json"
        Config().save(cfg_path)
        assert ru.main(["--config", str(cfg_path)]) == 2
        assert "--set-token" in capsys.readouterr().out


class TestInstallerWiring:
    def test_nightly_agent_and_icon_are_installed(self):
        root = Path(__file__).parent.parent / "deploy" / "mac"
        body = (root / "mintel_mac.sh").read_text()
        assert "com.mintel.report" in body and "mintel.ops.report_upload" in body
        assert "<key>Hour</key><integer>22</integer>" in body
        assert '"Send Report"' in body
        icon = (root / "Send Report.command").read_text()
        assert "report_upload" in icon and "--set-token" in icon


# ------------------------------------------------------------- 9 Oct --
class ConflictingGitHub(FakeGitHub):
    """The 8 Oct collision: something else writes to the branch between our
    read and our write, so the first PUTs of a file come back 409 (or 422)
    and the file's sha has moved on."""

    def __init__(self, conflicts: dict, code=409, branch_exists=True):
        super().__init__(branch_exists=branch_exists)
        self.conflicts = dict(conflicts)          # path -> how many PUTs to refuse
        self.code = code
        self.version: dict = {}

    def __call__(self, method, url, body):
        path = url.replace(ru.API, "")
        if method == "GET" and "/contents/" in path:
            p = path.split("/contents/")[1].split("?")[0]
            if p in self.files:
                self.calls.append((method, url))
                return 200, {"sha": f"s{p}-{self.version.get(p, 0)}",
                             "content": base64.b64encode(self.files[p].encode()).decode()}
        if method == "PUT" and "/contents/" in path:
            p = path.split("/contents/")[1]
            self.calls.append((method, url))
            if self.conflicts.get(p, 0) > 0:
                self.conflicts[p] -= 1
                self.files[p] = f"written by someone else {self.conflicts[p]}\n"     # the branch moved on
                self.version[p] = self.version.get(p, 0) + 1
                return self.code, {"message": "reports/x does not match"}
            if p in self.files:
                assert body.get("sha") == f"s{p}-{self.version.get(p, 0)}", "a retry must carry the fresh sha"
            self.files[p] = base64.b64decode(body["content"]).decode()
            return 201, {}
        return super().__call__(method, url, body)


class TestTheUploadSurvivesACollision:
    def test_a_conflict_is_retried_with_a_fresh_sha(self):
        for code in (409, 422):
            gh = ConflictingGitHub({"reports/latest/DATE": 2}, code=code)
            gh.files["reports/latest/DATE"] = "2026-10-07\n"
            waits: list = []
            n = ru.upload({"reports/latest/DATE": "2026-10-08\n"}, "o/r", "reports", "tok", opener=gh, sleep=waits.append)
            assert n == 1 and gh.files["reports/latest/DATE"] == "2026-10-08\n"
            assert [m for m, _ in gh.calls].count("PUT") == 3                 # two refused, the third written
            assert waits == [0.5, 1.0]                                      # a short, growing pause

    def test_a_file_that_still_fails_does_not_stop_the_others(self):
        gh = ConflictingGitHub({"reports/2026-10-08/status.json": 99})
        waits: list = []
        files = {"reports/2026-10-08/day_review.txt": "review\n", "reports/2026-10-08/status.json": "{}",
                 "reports/2026-10-08/rider.json": "{}", "reports/latest/DATE": "2026-10-08\n"}
        with pytest.raises(ru.UploadIncomplete) as exc:
            ru.upload(files, "o/r", "reports", "tok", opener=gh, sleep=waits.append)
        assert exc.value.written == 3
        assert [p for p, _ in exc.value.failed] == ["reports/2026-10-08/status.json"]
        assert "after 5 tries" in exc.value.failed[0][1] and "status.json" in str(exc.value)
        for p in ("reports/2026-10-08/day_review.txt", "reports/2026-10-08/rider.json", "reports/latest/DATE"):
            assert p in gh.files, p                                         # written after the failure too
        assert len(waits) == 4                                               # five tries, four pauses
        assert isinstance(exc.value, RuntimeError)                          # the pulse and potential still catch it

    def test_a_refusal_that_is_not_a_conflict_is_not_retried(self):
        gh = FakeGitHub(branch_exists=True)
        def forbidden(method, url, body):
            if method == "PUT":
                gh.calls.append((method, url))
                return 403, {"message": "Resource not accessible"}
            return gh(method, url, body)
        with pytest.raises(ru.UploadIncomplete, match=r"\(403\): Resource not accessible"):
            ru.upload({"a.txt": "x"}, "o/r", "reports", "tok", opener=forbidden, sleep=lambda s: None)
        assert [m for m, _ in gh.calls].count("PUT") == 1

    def test_the_nightly_run_names_what_failed_and_exits_non_zero(self, workdir, monkeypatch, capsys):
        import mintel.run as run_mod
        cfg_path = workdir / "config.json"
        cfg = Config(); cfg.ops.data_dir = str(workdir); cfg.save(cfg_path)
        ru.store_token(str(cfg_path), "tok")

        class B:
            def connect(self): return True
            def disconnect(self): pass
        monkeypatch.setattr(run_mod, "build_broker", lambda c: B())
        monkeypatch.setattr(ru, "build_bundle", lambda c, b, d: {"reports/2026-10-08/a.txt": "a",
                                                                 "reports/2026-10-08/b.txt": "b"})
        gh = ConflictingGitHub({"reports/2026-10-08/b.txt": 99})
        monkeypatch.setattr(ru.GitHub, "_default_open", lambda self, m, u, b: gh(m, u, b))
        monkeypatch.setattr(ru.time, "sleep", lambda s: None)
        assert ru.main(["--config", str(cfg_path), "--date", "2026-10-08"]) == 1
        out = capsys.readouterr().out
        assert "Published 1 of 2 files" in out and "reports/2026-10-08/b.txt" in out
        assert gh.files["reports/2026-10-08/a.txt"] == "a"
        gh.conflicts.clear()
        assert ru.main(["--config", str(cfg_path), "--date", "2026-10-08"]) == 0      # all through: exit 0
        assert "Published 2 files" in capsys.readouterr().out


class TestTheNewBotsDayFiles:
    def _rider_db(self, workdir):
        from mintel.rider.journal import RiderJournal
        j = RiderJournal(workdir / "rider.sqlite")
        j.insert_trade({"ticket": 900001, "mode": "LIVE", "symbol": "GBPJPY", "side": "BUY", "volume": 0.07,
                        "opened_utc": "2026-09-17T09:00:00+00:00", "closed_utc": "2026-09-17T09:04:00+00:00",
                        "realised_pips": 6.2, "net_money": 6.05, "money_source": "broker",
                        "explanation": "GBPJPY ran up; the bot got on early and stepped off when it slowed."})
        j.insert_trade({"ticket": 900002, "mode": "PAPER", "symbol": "EURUSD", "side": "SELL", "volume": 0.1,
                        "opened_utc": "2026-09-16T09:00:00+00:00", "closed_utc": "2026-09-16T09:04:00+00:00",
                        "realised_pips": -3.0, "net_money": -3.0, "money_source": "paper"})
        j.log_event("START", "mode LIVE", now=NOW)
        j.close()
        (workdir / "rider-status.json").write_text(json.dumps({"mode": "LIVE", "updated_utc": NOW.isoformat()}))

    def test_rider_and_ian_files_carry_the_days_trades_with_their_mode(self, workdir):
        from mintel.ian.journal import Journal as IanJournal
        cfg = _cfg(workdir)
        self._rider_db(workdir)
        IanJournal(workdir / "ian.sqlite").close()
        (workdir / "ian-status.json").write_text(json.dumps({"mode": "PAPER", "feed": {"state": "NOT CONFIGURED"}}))
        broker = FakeBroker(("XAUJPY",), start=NOW); broker.connect()
        files = ru.build_bundle(cfg, broker, DAY, fetch=lambda url: "{}")
        rider = json.loads(files["reports/2026-09-17/rider.json"])
        assert [(t["ticket"], t["mode"], t["net_money"]) for t in rider["trades"]] == [(900001, "LIVE", 6.05)]
        assert rider["status"]["mode"] == "LIVE" and rider["events"][0]["kind"] == "START"
        ian = json.loads(files["reports/2026-09-17/ian.json"])
        assert ian["trades"] == [] and ian["status"]["feed"]["state"] == "NOT CONFIGURED" and ian["signals"] == []
        assert files["reports/latest/rider.json"] == files["reports/2026-09-17/rider.json"]

    def test_no_files_no_day_file(self, workdir):
        cfg = _cfg(workdir)
        broker = FakeBroker(("XAUJPY",), start=NOW); broker.connect()
        files = ru.build_bundle(cfg, broker, DAY, fetch=lambda url: "{}")
        assert not any(k.endswith(("/rider.json", "/ian.json")) for k in files)
