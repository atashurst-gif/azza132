"""Real-money approval: prepared, DISABLED (Aaron, 14 Oct 2026). The ids, logins and phrases here are MADE UP."""
from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import pytest

from mintel.ops import readiness as RD
from mintel.ops import realmoney as RM

PHRASE = "a made-up passphrase 14 Oct"
ROOT = Path(__file__).resolve().parents[1] / "mintel"
FAST = 1000                                         # PBKDF2 iterations in tests (the real default is 310,000)


@pytest.fixture
def data(tmp_path):
    RM.set_passphrase(tmp_path, PHRASE, iterations=FAST)
    bots = [{"id": "lab:abc123", "version": "v1", "stage": RD.QUALIFIED, "fingerprint": "f1"},
            {"id": "bot:runner", "version": "current rules", "stage": RD.APPROACHING, "fingerprint": "f2"},
            {"id": "lab:def456", "version": "v2", "stage": RD.QUALIFIED, "fingerprint": "f3"},
            {"id": "lab:ghi789", "version": "v1", "stage": RD.QUALIFIED, "fingerprint": "f4"}]
    (tmp_path / RD.FILE).write_text(json.dumps({"stage": RD.QUALIFIED, "bots": bots}))
    (tmp_path / "config.json").write_text(json.dumps({"account_login": 53020675, "mode": "DEMO"}))
    return tmp_path


def approve(data, sid="lab:abc123", version="v1", login=11223344, risk=0.5, typed=None, phrase=PHRASE):
    typed = typed if typed is not None else RM.confirmation_phrase(RM.APPROVE, sid, version, risk, login)
    return RM.decide(data, RM.APPROVE, sid, version, typed=typed, passphrase=phrase, login=login, risk_pct=risk)


class TestTheSwitchIsOff:
    def test_the_constant_and_the_guard(self):
        assert RM.REAL_TRADING_ENABLED is False
        assert RM.real_trading_allowed() is False and RM.real_connected() is False
        with pytest.raises(RM.RealTradingDisabled):
            RM.guard_real_order({"symbol": "EURUSD", "volume": 0.01})

    def test_an_approval_enables_nothing(self, data):
        cfg_before = (data / "config.json").read_bytes()
        rec = approve(data)
        assert rec["real_trading_enabled"] is False
        assert RM.real_trading_allowed() is False
        with pytest.raises(RM.RealTradingDisabled):
            RM.guard_real_order()
        assert (data / "config.json").read_bytes() == cfg_before                  # no mode, no config changed
        # the head file is new (rv14 #17: it anchors the record's end); still no real_account.json or anything else
        assert sorted(p.name for p in (data / "realmoney").iterdir()) == ["approvals.head.json", "approvals.jsonl",
                                                                          "passphrase.json"]

    def test_no_order_path_knows_the_real_money_module(self):
        """Nothing that can send an order imports realmoney or reads the future real account's file; and
        realmoney imports no broker, engine or bot module."""
        allowed = {"ops/realmoney.py", "ops/leagues.py", "ops/squad_pages.py", "ops/readiness.py"}   # pages, records
        for path in ROOT.rglob("*.py"):
            rel = path.relative_to(ROOT).as_posix()
            src = path.read_text(encoding="utf-8", errors="replace")
            if rel in allowed:
                continue
            assert "realmoney" not in src, rel
            assert "real_account.json" not in src, rel
        tree = ast.parse((ROOT / "ops" / "realmoney.py").read_text())
        mods = set()
        for n in ast.walk(tree):
            if isinstance(n, ast.ImportFrom):
                mods.add(("." * n.level) + (n.module or ""))
            elif isinstance(n, ast.Import):
                mods.update(a.name for a in n.names)
        for m in mods:
            assert not re.search(r"broker|engine|botexec|trader|mt5|bridge|modes|weekly", m), m
        src = (ROOT / "ops" / "realmoney.py").read_text()
        assert "order_send" not in src and "MetaTrader5" not in src
        assert re.search(r"^REAL_TRADING_ENABLED = False\b", src, re.M)


class TestThePassphrase:
    def test_stored_only_as_a_salted_hash(self, data):
        raw = (data / "realmoney" / "passphrase.json").read_text()
        assert PHRASE not in raw and json.loads(raw)["algo"] == "pbkdf2_sha256"
        assert RM.check_passphrase(data, PHRASE) and not RM.check_passphrase(data, PHRASE + "x")
        a, b = RM.hash_passphrase(PHRASE, iterations=FAST), RM.hash_passphrase(PHRASE, iterations=FAST)
        assert a["salt"] != b["salt"] and a["hash"] != b["hash"]
        assert (data / "realmoney" / "passphrase.json").stat().st_mode & 0o077 == 0

    def test_set_once_changed_only_with_the_current_one(self, data):
        with pytest.raises(RM.ApprovalRefused):
            RM.set_passphrase(data, "another long phrase", iterations=FAST)
        RM.set_passphrase(data, "another long phrase", iterations=FAST, current=PHRASE)
        assert RM.check_passphrase(data, "another long phrase")
        with pytest.raises(RM.ApprovalRefused):
            RM.set_passphrase(data / "x", "short", iterations=FAST)

    def test_the_default_strength(self):
        assert RM.PBKDF2_ITERATIONS >= 300_000


class TestApproval:
    def test_the_record_holds_everything_it_was_tied_to(self, data):
        rec = approve(data)
        assert rec["decision"] == "approve" and rec["id"] == "lab:abc123" and rec["version"] == "v1"
        assert rec["real_login"] == 11223344 and rec["risk_pct"] == 0.5 and rec["tier"] == 1
        assert rec["confirmation"] == "APPROVE REAL MONEY lab:abc123 v1 0.5% ON 11223344"
        assert rec["readiness_stage"] == RD.QUALIFIED and rec["readiness_fingerprint"] == "f1"
        assert PHRASE not in (data / "realmoney" / "approvals.jsonl").read_text()
        assert RM.chain_ok(data) and RM.records(data)[0]["prev"] == ""

    @pytest.mark.parametrize("kw,why", [
        ({"typed": "yes"}, "confirmation did not match"),
        ({"phrase": "wrong passphrase"}, "passphrase is not right"),
        ({"login": 53020675}, "is the DEMO account"),
        ({"login": "abc", "typed": "x"}, "must be a number"),
        ({"login": float("inf"), "typed": "x"}, "must be a number"),       # rv14 R6: refused, not a traceback
        ({"risk": 0.7}, "one of the tiers"),
        ({"risk": 2.0}, "one of the tiers"),
        ({"risk": 1.0}, "approve 0.5% first"),
        ({"sid": "bot:runner", "version": "current rules"}, "not qualified"),
        ({"version": "v2"}, "not qualified"),
    ])
    def test_refused_and_nothing_recorded(self, data, kw, why):
        with pytest.raises(RM.ApprovalRefused, match=why):
            approve(data, **kw)
        assert RM.records(data) == []

    def test_higher_tiers_one_at_a_time_each_approved_separately(self, data):
        approve(data, risk=0.5)
        with pytest.raises(RM.ApprovalRefused, match="already approved"):
            approve(data, risk=0.5)
        with pytest.raises(RM.ApprovalRefused, match="approve 1% first"):
            approve(data, risk=1.5)
        assert approve(data, risk=1.0)["tier"] == 2
        assert RM.approved_tiers(data, "lab:abc123", "v1") == [0.5, 1.0]

    def test_the_combined_cap(self, data):
        for r in (0.5, 1.0, 1.5):
            approve(data, "lab:abc123", "v1", risk=r)                  # its highest tier counts: 1.5%
        approve(data, "lab:def456", "v2", risk=0.5)                    # 2.0% in all: allowed
        with pytest.raises(RM.ApprovalRefused, match="combined planned risk"):
            approve(data, "lab:ghi789", "v1", risk=0.5)                # 2.5% would pass the 2% cap

    def test_keep_on_demo_and_reject_are_recorded(self, data):
        for dec in (RM.KEEP_DEMO, RM.REJECT):
            typed = RM.confirmation_phrase(dec, "bot:runner", "current rules")
            rec = RM.decide(data, dec, "bot:runner", "current rules", typed=typed, passphrase=PHRASE, why="made up")
            assert rec["decision"] == dec and rec["why"] == "made up"
        assert [r["decision"] for r in RM.records(data)] == ["keep-demo", "reject"] and RM.chain_ok(data)
        assert RM.latest_decision(data, "bot:runner", "current rules")["decision"] == "reject"

    def test_a_changed_or_removed_line_shows(self, data):
        approve(data)
        approve(data, risk=1.0)
        p = data / "realmoney" / "approvals.jsonl"
        lines = p.read_text().splitlines()
        p.write_text(lines[1] + "\n")
        assert not RM.chain_ok(data)
        p.write_text(lines[0].replace("11223344", "99999999") + "\n" + lines[1] + "\n")
        assert not RM.chain_ok(data)

    def test_a_cut_off_end_or_an_unreadable_line_shows_and_blocks_new_decisions(self, data):
        """rv14 #17: cutting the last line off (the REJECT after an approval) or a final line that does not parse
        used to leave chain_ok True; and nothing new may be recorded on top (that would rewrite the head)."""
        assert RM.chain_ok(data)                                      # nothing recorded yet: no head, fine
        approve(data)
        typed = RM.confirmation_phrase(RM.REJECT, "lab:abc123", "v1")
        RM.decide(data, RM.REJECT, "lab:abc123", "v1", typed=typed, passphrase=PHRASE)
        assert RM.chain_ok(data)
        p = data / "realmoney" / "approvals.jsonl"
        full = p.read_text()
        lines = full.splitlines()
        p.write_text(lines[0] + "\n")                                 # the REJECT cut off the end
        assert not RM.chain_ok(data)
        with pytest.raises(RM.ApprovalRefused, match="does not check out"):
            approve(data, "lab:def456", "v2")
        with pytest.raises(RM.ApprovalRefused, match="does not check out"):
            RM.decide(data, RM.REJECT, "lab:abc123", "v1", typed=typed, passphrase=PHRASE)
        assert p.read_text() == lines[0] + "\n" and not RM.chain_ok(data)   # nothing added, nothing healed
        p.write_text(full + "{garbage\n")                             # a final line that does not parse
        assert not RM.chain_ok(data)
        p.write_text(full)
        assert RM.chain_ok(data)
        (data / "realmoney" / "approvals.head.json").unlink()        # a record with no head
        assert not RM.chain_ok(data)

    def test_a_reject_or_keep_demo_withdraws_the_approvals(self, data):
        """rv14 #16: a later keep-demo or reject used to leave the approved tiers standing - the ladder could
        go on to 1% with no 0.5% approval, 0.5% could never be approved again, and the cap stayed used up."""
        for dec in (RM.KEEP_DEMO, RM.REJECT):
            approve(data, risk=0.5)                                   # the 2nd pass is a re-approval after keep-demo
            RM.decide(data, dec, "lab:abc123", "v1", typed=RM.confirmation_phrase(dec, "lab:abc123", "v1"),
                      passphrase=PHRASE)
            assert RM.approved_tiers(data, "lab:abc123", "v1") == [] and RM.current_approvals(data) == {}
            with pytest.raises(RM.ApprovalRefused, match="approve 0.5% first"):
                approve(data, risk=1.0)
        assert approve(data, risk=0.5)["tier"] == 1                   # 0.5% can be approved again
        assert RM.approved_tiers(data, "lab:abc123", "v1") == [0.5]

    def test_a_withdrawn_approval_frees_the_combined_cap(self, data):
        for r in (0.5, 1.0, 1.5):
            approve(data, "lab:abc123", "v1", risk=r)
        approve(data, "lab:def456", "v2", risk=0.5)                    # 2.0% in all
        for sid, ver in (("lab:abc123", "v1"), ("lab:def456", "v2")):
            RM.decide(data, RM.REJECT, sid, ver, typed=RM.confirmation_phrase(RM.REJECT, sid, ver), passphrase=PHRASE)
        assert approve(data, "lab:ghi789", "v1", risk=0.5)["tier"] == 1   # nothing else stands: 0.5% in all

    def test_the_ladder_is_tied_to_the_real_login(self, data):
        """rv14 #16: 0.5% ON one login used to let 1% ON another through, and then refuse 0.5% on that one."""
        approve(data, login=11111111, risk=0.5)
        with pytest.raises(RM.ApprovalRefused, match="approve 0.5% first"):
            approve(data, login=22222222, risk=1.0)
        assert approve(data, login=22222222, risk=0.5)["real_login"] == 22222222
        assert RM.approved_tiers(data, "lab:abc123", "v1", 22222222) == [0.5]
        assert RM.approved_tiers(data, "lab:abc123", "v1", 11111111) == []
        assert approve(data, login=22222222, risk=1.0)["tier"] == 2

    def test_two_versions_of_one_id_both_count_toward_the_cap(self, data):
        """rv14 #16: the cap skipped every record of the same id, whatever the version: v1 and v2 of one id
        could both reach 1.5% (3% planned against the 2% cap)."""
        bots = [{"id": "lab:abc123", "version": v, "stage": RD.QUALIFIED, "fingerprint": v} for v in ("v1", "v2")]
        (data / RD.FILE).write_text(json.dumps({"stage": RD.QUALIFIED, "bots": bots}))
        for r in (0.5, 1.0, 1.5):
            approve(data, "lab:abc123", "v1", risk=r)
        approve(data, "lab:abc123", "v2", risk=0.5)                    # 2.0% in all: allowed
        with pytest.raises(RM.ApprovalRefused, match="combined planned risk"):
            approve(data, "lab:abc123", "v2", risk=1.0)                # 2.5% would pass the 2% cap

    def test_the_command_line(self, data, monkeypatch, capsys):
        typed = RM.confirmation_phrase(RM.KEEP_DEMO, "lab:abc123", "v1")
        monkeypatch.setattr("builtins.input", lambda *_: typed)
        monkeypatch.setattr(RM.getpass, "getpass", lambda *_: PHRASE)
        assert RM.main(["--data", str(data), "keep-demo", "--id", "lab:abc123", "--version", "v1"]) == 0
        assert "stays DISABLED" in capsys.readouterr().out
        monkeypatch.setattr("builtins.input", lambda *_: "no")
        assert RM.main(["--data", str(data), "approve", "--id", "lab:abc123", "--version", "v1", "--login",
                        "11223344", "--risk", "0.5"]) == 1
        assert [r["decision"] for r in RM.records(data)] == ["keep-demo"]


class TestAccounts:
    def test_demo_and_real_never_confused(self, data):
        assert RM.verify_accounts(53020675, 11223344, data) == (True, "")
        assert not RM.verify_accounts(53020675, 53020675, data)[0]
        assert not RM.verify_accounts(None, 53020675, data)[0]                 # the known demo, from the ledger
        assert not RM.verify_accounts(77777777, 77777777)[0]
        (data / "realmoney" / "real_account.json").write_text(json.dumps({"login": 11223344}))
        ok, why = RM.verify_accounts(11223344, 55555555, data)
        assert not ok and "never treated as demo" in why
        assert RM.real_account(data)["connected"] is False

    @pytest.mark.parametrize("ra,cfg_login,why", [
        ({"login": 53020675, "server": "ICMarketsSC-Demo"}, 53020675, "never treated as demo"),   # the IC demo
        ({"login": 44444444}, 44444444, "never treated as demo"),               # config.json's demo login
        ({"login": 66666666, "server": "ICMarketsSC-Demo"}, 53020675, "demo server"),
        ({"login": "not a number"}, 53020675, "must be a number"),
    ])
    def test_a_demo_real_account_json_is_refused_without_demo_login(self, data, monkeypatch, ra, cfg_login, why):
        """rv14 #18: real_account.json was only compared with ``demo_login``, which the command line never
        passes, so a real_account.json holding the demo login (or a demo server) went unflagged."""
        (data / "config.json").write_text(json.dumps({"account_login": cfg_login, "mode": "DEMO"}))
        (data / "realmoney" / "real_account.json").write_text(json.dumps(ra))
        ok, msg = RM.verify_accounts(None, 11223344, data)
        assert not ok and why in msg
        typed = RM.confirmation_phrase(RM.APPROVE, "lab:abc123", "v1", 0.5, 11223344)
        monkeypatch.setattr("builtins.input", lambda *_: typed)
        monkeypatch.setattr(RM.getpass, "getpass", lambda *_: PHRASE)
        assert RM.main(["--data", str(data), "approve", "--id", "lab:abc123", "--version", "v1", "--login",
                        "11223344", "--risk", "0.5"]) == 1
        assert RM.records(data) == []
        (data / "realmoney" / "real_account.json").unlink()            # the empty template: still fine
        assert RM.verify_accounts(None, 11223344, data) == (True, "")

    @pytest.mark.parametrize("login_text", ["Infinity", "-Infinity", "1e400"])
    def test_an_infinite_real_account_login_is_refused_not_a_traceback(self, data, monkeypatch, capsys, login_text):
        """rv14 R6: Python's json reads Infinity and 1e400 as float('inf'), and int() of that raises OverflowError,
        which went out of verify_accounts, decide() and main() as a traceback instead of the refusal."""
        (data / "realmoney" / "real_account.json").write_text('{"login": %s, "server": "ICMarketsSC-Live"}'
                                                             % login_text)
        assert RM.verify_accounts(None, 11223344, data) == (False, "real_account.json's login must be a number")
        assert RM.verify_accounts(float("inf"), 11223344, data) == (False, "real_account.json's login must be a number")
        assert RM.verify_accounts(None, float("inf"), data) == (False, "the real account login must be a number")
        typed = RM.confirmation_phrase(RM.APPROVE, "lab:abc123", "v1", 0.5, 11223344)
        monkeypatch.setattr("builtins.input", lambda *_: typed)
        monkeypatch.setattr(RM.getpass, "getpass", lambda *_: PHRASE)
        assert RM.main(["--data", str(data), "approve", "--id", "lab:abc123", "--version", "v1", "--login",
                        "11223344", "--risk", "0.5"]) == 1
        assert "Refused: real_account.json's login must be a number" in capsys.readouterr().out
        assert RM.records(data) == []

    def test_the_real_account_section_is_separate_and_unused(self, tmp_path):
        ra = RM.real_account(tmp_path)
        assert ra["login"] is None and ra["connected"] is False and "password" not in json.dumps(ra).lower().replace(
            "no password here", "")
        assert not (tmp_path / "realmoney").exists()                              # reading creates nothing


class TestThePageCannotApprove:
    def test_no_post_and_the_buttons_only_show_a_command(self, tmp_path):
        from mintel.ops import dashboard as D
        assert not hasattr(D._Handler, "do_POST") and not hasattr(D._Handler, "do_PUT")
        from mintel.ops import squad as SQ
        from mintel.ops import squad_pages as P
        b = SQ.board(None)
        x = {"readiness": {"stage": RD.QUALIFIED, "bots": [{
            "id": "lab:abc123", "version": "v1", "nick": "Made Up", "stage": RD.QUALIFIED,
            "demo": {"net": 120.0, "trades": 48, "days": 12, "up_days": 9, "max_dd": 30.0, "mean": 2.5},
            "paper": {"net": 200.0, "trades": 90, "days": 20, "up_days": 14, "max_dd": 40.0, "mean": 2.2},
            "promising": ["made up"], "needed": [], "justified": True, "justified_words": "Yes",
            "earliest": "now, once you approve", "sim_1500": {"max_dd_pct": 3.1}}]}}
        page = P.render_leagues_page(b, x)
        card = page.split('data-bot="lab:abc123"')[1]
        for label in ("APPROVE LIVE", "KEEP ON DEMO", "REJECT"):
            assert f"<summary>{label}</summary>" in card
        assert "python -m mintel.ops.realmoney --data data approve --id lab:abc123 --version &quot;v1&quot;" in card
        assert "<form" not in page and "method=" not in page.lower() and "fetch(" not in page.split("<script>")[0]

    def test_a_get_with_approve_in_it_records_nothing(self, tmp_path):
        from tests.test_page_figures import a_day, serve
        d = a_day(tmp_path, records=False)
        httpd, get = serve(d)
        try:
            page = get("/leagues?approve=lab%3Aabc123&version=v1&risk=1.5")
        finally:
            httpd.shutdown()
        assert "REAL MONEY LEAGUE" in page and "Not connected yet" in page
        assert not (Path(d.data) / "realmoney").exists()
