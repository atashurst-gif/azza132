"""10 Oct: Aaron's four answers for Formula 1 - "fine to go with your
suggestions", yes to all four questions put to him that morning:

1. only Formula 1's momentum continuation trades on minor pairs go live
   (--tnb-live-tactics MOMENTUM_CONTINUATION);
2. a flat 13 a trade, no top-opportunity size, until Formula 1 has 20 live
   trades (--top-size off);
3. no new Trend & Breakout entry in the 15 minutes before a high-importance
   release on either currency of the pair (--news-before-minutes 15);
4. the daily-loss stop counts real money only (--daily-loss-practice
   ignore, risk.daily_loss_counts_practice false); the 3% limit stays.

A. the modes tool's new switch for answer 4, and what its words promise
   against what the trader does with the setting;
B. the Mac installer's one-time step (apply_formula1_choices, marker
   .formula1-choices-2026-10-10), right after Formula 1's and only once it
   is set;
C. the same step on Windows (Set-Formula1ChoicesOnce);
D. docs/plan.json and the page saying the same as config.json.

EVERY FIGURE HERE IS MADE-UP TEST DATA (a fake broker, fake installs and
the tests' own numbers), except D's check that each of the plan's figures
is in the repo document it comes from: nothing here measures performance."""
from __future__ import annotations

import datetime as dt
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from mintel.config import Config, NewsConfig, RiskConfig
from mintel.engine.risk import RiskManager
from mintel.ops import modes
from mintel.ops import verdicts as V
from tests.test_formula1_core import a_config
from tests.test_formula1_switches import account, daily
from tests.test_integration_rider_ian import MAC, WIN, _home
from tests.test_lineup import F1_CALLS, F1_STEP, FORMULA1, LINE_UP, _bash_env, _calls, _fake_python_home

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 10, 9, 0, tzinfo=UTC)
MC = "MOMENTUM_CONTINUATION"
CHOICES = ".formula1-choices-2026-10-10"
CHOICES_CALL = "--tnb-live-tactics MOMENTUM_CONTINUATION --top-size off --news-before-minutes 15 --daily-loss-practice ignore"
REST_CALL = "--top-size off --news-before-minutes 15 --daily-loss-practice ignore"
CHOICES_STEP = 'apply_formula1_choices; echo "CODE=${CODE_CHANGED:-none}"'
IGNORE_LINE = ("Daily-loss stop: counts real money only - Trend & Breakout's real trades and the real trades of the "
               "Momentum Runner, Band Breaker and Crowd Fader, closed today and still open; Trend & Breakout's "
               "practice losses no longer count towards it")
LIVE_LINE = ("Daily-loss stop: Trend & Breakout is LIVE just now, so every trade of its is real and the stop counts "
             "its own trades only, closed today and still open, as before - the Momentum Runner's, Band Breaker's "
             "and Crowd Fader's trades are not in its figure, and this setting changes nothing until it is on PAPER.")


def _plain(text: str) -> str:
    """The Start window's words without its colours."""
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def _main(p: Path, *args: str) -> int:
    return modes.main(["--config", str(p), *args])


# ===================================================== A. the modes switch --
class TestTheDailyLossSwitch:
    def test_ignore_writes_a_json_false_and_nothing_else(self, tmp_path, capsys):
        p = a_config(tmp_path)
        before = json.loads(p.read_text())
        assert before["risk"]["daily_loss_counts_practice"] is True                 # the default: practice counts
        assert _main(p, "--daily-loss-practice", "ignore") == 0
        out = capsys.readouterr().out
        after = json.loads(p.read_text())
        assert after["risk"]["daily_loss_counts_practice"] is False
        assert '"daily_loss_counts_practice": false' in p.read_text()                 # a JSON false, nothing else
        before["risk"]["daily_loss_counts_practice"] = False
        assert after == before                                                       # the 3% limit included
        assert after["risk"]["max_daily_loss_pct"] == 3.0
        assert Config.load(p).risk.daily_loss_counts_practice is False
        assert IGNORE_LINE in out
        assert ("(they still count, as before, towards the wait after a loss and the losses-a-day limit on that "
                "market). When the broker's records cannot be read, it reads the whole account's change since its "
                "first look today, as before. The limit stays 3% (risk.max_daily_loss_pct) (config.json: "
                "risk.daily_loss_counts_practice false).") in out
        assert "Restart the bot for this to take effect" in out
        assert "no longer stop anything" not in out                                 # it would not be true

    def test_count_writes_a_json_true(self, tmp_path, capsys):
        p = a_config(tmp_path)
        modes.set_modes(p, {}, daily_loss_practice="ignore", now=NOW)
        assert _main(p, "--daily-loss-practice", "count") == 0
        out = capsys.readouterr().out
        assert json.loads(p.read_text())["risk"]["daily_loss_counts_practice"] is True
        assert ("Daily-loss stop: counts Trend & Breakout's practice trades as well as its real ones, closed today "
                "and still open; a practice profit never covers a real loss. The Momentum Runner's, Band Breaker's "
                "and Crowd Fader's trades are not in its figure.") in out
        assert "(config.json: risk.daily_loss_counts_practice true)" in out

    @pytest.mark.parametrize("word", ["Ignore", " IGNORE ", "ignore"])
    def test_the_word_in_any_case(self, tmp_path, word):
        p = a_config(tmp_path)
        assert _main(p, "--daily-loss-practice", word) == 0
        assert json.loads(p.read_text())["risk"]["daily_loss_counts_practice"] is False

    @pytest.mark.parametrize("bad", ["maybe", "false", "true", "off", "0", ""])
    def test_anything_else_is_refused_and_nothing_written(self, tmp_path, capsys, bad):
        p = a_config(tmp_path)
        stamp = p.read_text()
        assert _main(p, "--daily-loss-practice", bad) == 1
        assert capsys.readouterr().err.startswith("Nothing changed: --daily-loss-practice must be count or ignore")
        assert p.read_text() == stamp

    @pytest.mark.parametrize("bad", [0, 1, None, "no", 0.0])
    def test_the_function_refuses_anything_but_the_two_words_or_a_boolean(self, tmp_path, bad):
        p = a_config(tmp_path)
        stamp = p.read_text()
        if bad is None:
            modes.set_modes(p, {}, daily_loss_practice=None, now=NOW)               # not given: nothing to do
            assert json.loads(p.read_text()) == json.loads(stamp)
            return
        with pytest.raises(ValueError):
            modes.set_modes(p, {}, daily_loss_practice=bad, now=NOW)
        assert p.read_text() == stamp

    def test_every_file_is_checked_first(self, tmp_path, capsys):
        p = a_config(tmp_path)
        stamp = p.read_text()
        # a bad setting beside it: nothing at all is written
        assert _main(p, "--daily-loss-practice", "ignore", "--news-before-minutes", "0") == 1
        assert capsys.readouterr().err.startswith("Nothing changed: ")
        assert p.read_text() == stamp
        # no config file, or a broken one: nothing written, said plainly
        missing = tmp_path / "nowhere" / "config.json"
        assert _main(missing, "--daily-loss-practice", "ignore") == 1
        assert "Nothing changed: no config file" in capsys.readouterr().err and not missing.exists()
        p.write_text("{ not json")
        assert _main(p, "--daily-loss-practice", "ignore") == 1
        assert "Nothing changed: " in capsys.readouterr().err and p.read_text() == "{ not json"
        assert not (tmp_path / "config.json.tmp").exists()

    def test_show_says_so_only_when_it_is_false(self, tmp_path, capsys):
        p = a_config(tmp_path)
        assert _main(p, "--show") == 0
        assert "Daily-loss stop" not in capsys.readouterr().out                    # the default: nothing new
        modes.set_modes(p, {}, daily_loss_practice="ignore", now=NOW)
        stamp = p.read_text()
        assert _main(p, "--show") == 0
        out = capsys.readouterr().out
        assert IGNORE_LINE in out and p.read_text() == stamp                       # --show changes nothing
        # a value that is not a JSON false is read as true by the trader (Config.load): not "real money only"
        raw = json.loads(stamp)
        raw["risk"]["daily_loss_counts_practice"] = "no"
        p.write_text(json.dumps(raw))
        assert _main(p, "--show") == 0
        assert "Daily-loss stop" not in capsys.readouterr().out
        assert Config.load(p).risk.daily_loss_counts_practice is True

    @pytest.mark.parametrize("word", ("ignore", "count"))
    def test_on_live_the_line_opens_with_what_counts_now(self, tmp_path, capsys, word):
        # 10 Oct review: on LIVE the trader hands the stop Trend & Breakout's own trades only (no paper
        # wrapper, so no other bot's money: TestTheWordsAreWhatTheTraderDoes) - said first, never PAPER's words
        p = a_config(tmp_path, tnb_mode="LIVE")
        assert _main(p, "--daily-loss-practice", word) == 0
        line = next(l for l in capsys.readouterr().out.splitlines() if l.startswith("Daily-loss stop: "))
        assert line.startswith(LIVE_LINE), line
        assert not line.startswith("Daily-loss stop: counts")
        if word == "ignore":
            assert (" On PAPER it would count real money only: Trend & Breakout's real trades and the real trades "
                    "of the Momentum Runner, Band Breaker and Crowd Fader, not its practice losses. ") in line
            assert line.endswith("(config.json: risk.daily_loss_counts_practice false).")
        else:
            assert (" On PAPER it would count its practice trades as well as its real ones; a practice profit never "
                    "covers a real loss. ") in line
            assert line.endswith("(config.json: risk.daily_loss_counts_practice true).")
        assert "The limit stays 3% (risk.max_daily_loss_pct)" in line
        p2 = a_config(tmp_path / "paper")
        assert _main(p2, "--daily-loss-practice", word) == 0
        out = capsys.readouterr().out
        assert "LIVE just now" not in out and "On PAPER it would" not in out


class TestTheWordsAreWhatTheTraderDoes:
    """The line's promises, against RiskManager with the very file the switch wrote (MADE-UP figures)."""

    def _rm(self, tmp_path, word, real, practice, others=None):
        p = a_config(tmp_path)
        modes.set_modes(p, {}, daily_loss_practice=word, now=NOW)
        rm = RiskManager(Config.load(p))
        rm.realised_today = lambda: (real, practice)
        rm.is_practice = lambda ticket: False
        asked = []
        if others is not None:
            rm.others_real = lambda: asked.append(1) or others
        return rm, asked

    def test_ignore_practice_losses_never_trip_it(self, tmp_path):
        rm, _ = self._rm(tmp_path, "ignore", 0.0, -66.0, others=0.0)
        snap = rm.snapshot(account(1_900.0), [], NOW)
        assert daily(snap) is None and snap.entries_allowed

    def test_ignore_the_runners_real_loss_trips_it(self, tmp_path):
        rm, asked = self._rm(tmp_path, "ignore", 0.0, 0.0, others=-60.0)
        snap = rm.snapshot(account(1_840.0), [], NOW)
        assert asked and daily(snap) is not None and daily(snap).reason.startswith("real money down 3.16% today")
        assert "(limit 3.00%)" in daily(snap).reason                                # the limit never moved

    def test_ignore_trend_and_breakouts_real_loss_trips_it(self, tmp_path):
        rm, _ = self._rm(tmp_path, "ignore", -60.0, 40.0, others=0.0)
        assert daily(rm.snapshot(account(1_840.0), [], NOW)) is not None

    def test_count_practice_losses_trip_it_and_the_other_bots_are_not_asked(self, tmp_path):
        rm, asked = self._rm(tmp_path, "count", 0.0, -66.0, others=-1_000.0)
        snap = rm.snapshot(account(1_900.0), [], NOW)
        assert daily(snap) is not None and asked == []                              # not in its figure
        rm, _ = self._rm(tmp_path / "b", "count", -60.0, 40.0, others=0.0)
        assert daily(rm.snapshot(account(1_840.0), [], NOW)) is not None            # a practice profit never covers

    def test_unreadable_records_fall_back_to_the_account(self, tmp_path):
        rm, _ = self._rm(tmp_path, "ignore", 0.0, 0.0, others=0.0)
        rm.others_real = lambda: (_ for _ in ()).throw(RuntimeError("no history"))
        rm.snapshot(account(1_900.0), [], NOW)                                       # the day's first look
        snap = rm.snapshot(account(1_840.0), [], NOW + dt.timedelta(minutes=5))
        assert snap.daily_pnl == -60.0 and daily(snap) is not None

    def test_on_live_only_its_own_trades_count_whatever_the_setting(self, tmp_path):
        """10 Oct review: Trend & Breakout fully LIVE, practice not counted - a REAL Trader on a plain broker
        (no paper wrapper). A MADE-UP Momentum Runner real ride loses more than 3% of the account: the stop's
        figure is Trend & Breakout's own trades (none), so it does not trip - what the LIVE line says first."""
        from mintel.broker.base import OrderRequest, Side
        from tests.test_formula1_switches import RUNNER, TNB, _by_magic
        from tests.test_tnb_paper_wiring import move, setup, trader_on
        sim, cfg = setup(tmp_path, tnb="LIVE", symbols=("EURGBP", "US500"))
        cfg.risk.min_margin_level_pct = 0.0
        cfg.risk.daily_loss_counts_practice = False
        tr = trader_on(sim, sim, cfg)
        assert tr.risk.others_real is None                                          # never asked on LIVE
        start = sim.account().equity
        t = sim.tick("US500")
        ride = sim.send(OrderRequest(symbol="US500", side=Side.BUY, volume=15.0, sl=round(t.bid - 300, 1), tp=0.0,
                                     comment="RUNNER", magic=RUNNER, idempotency_key="runner-live-1"))
        assert ride.ok
        _by_magic(sim, {ride.ticket})
        move(sim, "US500", -28.0)
        sim.advance(30)
        assert sim.close(ride.ticket).ok
        assert (start - sim.account().equity) / start * 100.0 > 3.0                # real money down past 3%
        tr._realised_cache = {"at": None, "value": 0.0}
        snap = tr.risk.snapshot(sim.account(), sim.positions(TNB), sim.now)
        assert snap.daily_pnl == 0.0 and daily(snap) is None                        # its own trades only
        tr.journal.close()


class TestTheFourChoicesInOneCall:
    def test_one_call_sets_all_four_and_nothing_else(self, tmp_path, capsys):
        p = a_config(tmp_path)
        modes.set_modes(p, {}, tnb_live_groups=["FX_MINOR"], now=NOW)              # Formula 1, as on 9 Oct
        before = json.loads(p.read_text())
        assert _main(p, *CHOICES_CALL.split()) == 0
        out = capsys.readouterr().out
        after = json.loads(p.read_text())
        assert after["tnb"]["live_tactics"] == [MC] and after["tnb"]["live_groups"] == ["FX_MINOR"]
        assert after["risk"]["top_size_enabled"] is False
        assert after["news"]["blackout_before_seconds"] == 900.0
        assert after["risk"]["daily_loss_counts_practice"] is False
        since = dt.datetime.fromisoformat(after["tnb"]["live_since_utc"])
        assert since > NOW                                                           # a new trial: counted from now
        for k in ("live_tactics", "live_since_utc"):
            before["tnb"][k] = after["tnb"][k]
        before["risk"]["top_size_enabled"], before["risk"]["daily_loss_counts_practice"] = False, False
        before["news"]["blackout_before_seconds"] = 900.0
        assert after == before                                  # the 3% limit, 0.5 / 10 here and the rest unchanged
        for want in ("minor currency pairs LIVE for momentum continuation only (Formula 1)",
                     "The set of live trades changed, so the live trial starts again",
                     "Top-opportunity size: OFF (config.json: risk.top_size_enabled false).",
                     "News: Trend & Breakout takes no new trade - practice or real - in the 15 minutes before",
                     IGNORE_LINE):
            assert want in out, want
        # the same call again changes nothing that counts: the trial does not start again
        assert _main(p, *CHOICES_CALL.split()) == 0
        assert json.loads(p.read_text())["tnb"]["live_since_utc"] == after["tnb"]["live_since_utc"]

    def test_with_nothing_live_the_approach_is_refused_and_nothing_written(self, tmp_path, capsys):
        p = a_config(tmp_path)                                                       # no live markets
        stamp = p.read_text()
        assert _main(p, *CHOICES_CALL.split()) == 1
        assert capsys.readouterr().err.startswith("Nothing changed: --tnb-live-tactics chooses")
        assert p.read_text() == stamp
        assert _main(p, *REST_CALL.split()) == 0                                     # the other three alone: fine


# =================================================== B. the Mac installer --
def _f1_config(data: Path, tnb: dict) -> None:
    raw = json.loads((data / "config.json").read_text())
    raw["tnb"] = {**(raw.get("tnb") or {}), **tnb}
    (data / "config.json").write_text(json.dumps(raw))


F1_LIVE = {"mode": "PAPER", "live_groups": ["FX_MINOR"], "live_since_utc": "2026-10-09T18:45:00+00:00"}


class TestTheMacChoicesStep:
    def test_it_runs_once_right_after_formula1_with_one_call(self, tmp_path):
        f = _fake_python_home(tmp_path)
        _f1_config(f.data, F1_LIVE)
        r = _bash_env("apply_formula1; " + CHOICES_STEP, f.home)
        assert r.returncode == 0, r.stderr
        calls = _calls(f.log)
        assert calls[:3] == F1_CALLS and calls[3:] == [CHOICES_CALL]                # Formula 1, then the choices
        assert (f.data / FORMULA1).exists() and (f.data / CHOICES).exists()
        assert "CODE=yes" in r.stdout                                               # the trader restarts to read them
        assert r.stdout.index("Formula 1 live, the Rapid Momentum Rider off") < r.stdout.index(
            "Your choices for Formula 1")
        out = _plain(r.stdout)
        assert out.index("[ OK ] Your choices for Formula 1 are set:") < out.rindex("made-up modes output")
        assert "[WARN]" not in r.stdout
        # once only: a second run calls nothing and changes nothing
        again = _bash_env(CHOICES_STEP, f.home)
        assert again.returncode == 0 and again.stdout.strip() == "CODE=none" and len(_calls(f.log)) == 4

    def test_never_while_formula1_is_not_set(self, tmp_path):
        f = _fake_python_home(tmp_path)
        _f1_config(f.data, F1_LIVE)
        r = _bash_env(CHOICES_STEP, f.home)                                         # no Formula 1 marker
        assert r.stdout.strip() == "CODE=none" and _calls(f.log) == [] and not (f.data / CHOICES).exists()
        g = _fake_python_home(tmp_path / "part", fail="--risk-pct")                 # Formula 1 not fully set
        _f1_config(g.data, F1_LIVE)
        r = _bash_env("apply_formula1; " + CHOICES_STEP, g.home)
        assert _calls(g.log) == F1_CALLS and not (g.data / CHOICES).exists()
        assert "Your choices for Formula 1" not in r.stdout

    def test_the_marker_is_written_only_when_the_call_worked(self, tmp_path):
        f = _fake_python_home(tmp_path, fail="--daily-loss-practice")
        _f1_config(f.data, F1_LIVE)
        (f.data / FORMULA1).write_text("2026-10-09T19:45:00Z")
        r = _bash_env(CHOICES_STEP, f.home)
        assert r.returncode == 0, r.stderr
        assert _calls(f.log) == [CHOICES_CALL] and not (f.data / CHOICES).exists()
        want = (f"cd {f.home}/app && {f.home}/venv/bin/python -m mintel.ops.modes --config {f.data}/config.json "
                + CHOICES_CALL)
        assert "[WARN] Could not set your choices for Formula 1 (nothing was changed). Run: " + want in _plain(r.stdout)
        assert "Nothing changed: a made-up refusal" in r.stdout
        assert "CODE=none" in r.stdout and "are set" not in r.stdout                 # nothing changed: no restart
        # the next double-click tries again, and this time it is set
        g = _fake_python_home(tmp_path / "again")
        (g.data / "config.json").write_text((f.data / "config.json").read_text())
        (g.data / FORMULA1).write_text("2026-10-09T19:45:00Z")
        r = _bash_env(CHOICES_STEP, g.home)
        assert _calls(g.log) == [CHOICES_CALL] and (g.data / CHOICES).exists() and "CODE=yes" in r.stdout

    def test_an_existing_marker_and_no_settings_are_left_alone(self, tmp_path):
        f = _fake_python_home(tmp_path)
        (f.data / FORMULA1).write_text("x")
        (f.data / CHOICES).write_text("2026-10-10T09:00:00Z")
        r = _bash_env(CHOICES_STEP, f.home)
        assert r.stdout.strip() == "CODE=none" and _calls(f.log) == []
        assert (f.data / CHOICES).read_text() == "2026-10-10T09:00:00Z"
        g = _fake_python_home(tmp_path / "none")
        (g.data / FORMULA1).write_text("x")
        (g.data / "config.json").unlink()
        r = _bash_env(CHOICES_STEP, g.home)
        assert r.stdout.strip() == "CODE=none" and _calls(g.log) == [] and not (g.data / CHOICES).exists()

    @pytest.mark.parametrize("tnb", [
        {"mode": "PAPER", "live_groups": [], "live_since_utc": ""},                 # --tnb-live-groups none by hand
        {"mode": "LIVE", "live_groups": ["FX_MINOR"]},                               # --live tnb by hand
        {"mode": "PAPER", "live_groups": ["FX_MINOR"], "live_tactics": ["FOO"]},     # a list naming no approach
    ], ids=["cleared", "fully-live", "names-none"])
    def test_with_formula1_not_live_the_other_three_are_set_and_it_says_so(self, tmp_path, tnb):
        f = _fake_python_home(tmp_path)
        _f1_config(f.data, tnb)
        (f.data / FORMULA1).write_text("x")
        r = _bash_env(CHOICES_STEP, f.home)
        assert r.returncode == 0, r.stderr
        assert _calls(f.log) == [REST_CALL]                                         # never --tnb-live-tactics
        assert (f.data / CHOICES).exists() and "CODE=yes" in r.stdout              # done: never tried again
        assert "Your choices for Formula 1 are set:" in r.stdout
        out = _plain(r.stdout)
        if tnb["mode"] == "LIVE":
            # 10 Oct review: fully LIVE, every Trend & Breakout trade is real - never "no minor-pair trade goes real"
            assert "no minor-pair trade of Trend & Breakout goes to the real broker" not in out
            assert ("[WARN] Trend & Breakout is fully LIVE on this Mac just now (put there by hand with the modes "
                    "command): every trade of it, minor pairs included, is real money, so the first choice - only "
                    "its momentum continuation trades with real money - cannot apply until it is back on PAPER. "
                    "To put it back on PAPER with Formula 1 and that choice: ") in out
            assert (f"-m mintel.ops.modes --config {f.data}/config.json --paper tnb --tnb-live-groups FX_MINOR "
                    "--tnb-live-tactics MOMENTUM_CONTINUATION") in r.stdout
            return
        assert ("[WARN] Formula 1 is not live on this Mac just now (no minor-pair trade of Trend & Breakout goes to "
                "the real broker - changed by hand with the modes command), so the first choice - only its momentum "
                "continuation trades with real money - was not applied.") in out
        assert "fully LIVE" not in out
        assert (f"-m mintel.ops.modes --config {f.data}/config.json --tnb-live-groups FX_MINOR --tnb-live-tactics "
                "MOMENTUM_CONTINUATION") in r.stdout

    def test_a_restart_is_remembered(self, tmp_path):
        f = _fake_python_home(tmp_path)
        _f1_config(f.data, F1_LIVE)
        (f.data / FORMULA1).write_text("x")
        r = _bash_env("remember_restart; " + CHOICES_STEP + "; remember_restart", f.home)
        assert r.returncode == 0 and (f.data / ".restart-pending").exists()

    def test_it_comes_right_after_formula1_and_the_fast_path_knows_it(self):
        text = (MAC / "Start Trading Bot.command").read_text()
        body = text.split("write_config         ||")[1]
        assert (body.index("\napply_lineup") < body.index("\napply_ian_binance") < body.index("\napply_formula1 ")
                < body.index("\napply_formula1_choices") < body.index("\nremember_restart")
                < body.index("\ninstall_launch_agent") < body.index("\nstart_everything"))
        fast = text.split("# ---- Fast path")[1].split("# ---- Otherwise")[0]
        assert '[[ -f "$DATA_DIR/.formula1-choices-2026-10-10" ]]' in fast
        lib = (MAC / "mintel_mac.sh").read_text()
        assert 'FORMULA1_CHOICES_MARKER_NAME=".formula1-choices-2026-10-10"' in lib
        step = lib.split("apply_formula1_choices() {")[1].split("\n}\n")[0]
        assert step.index('[[ -f "$marker" ]] && return 0') < step.index("formula1_done || return 0") \
            < step.index("-m mintel.ops.modes") < step.index('date -u +%Y-%m-%dT%H:%M:%SZ > "$marker"')
        assert step.index("if (( code != 0 )); then") < step.index('> "$marker"')
        assert 'CODE_CHANGED="yes"' in step and 'good "Your choices for Formula 1 are set:"' in step
        # what it runs (every line but comments and the printed warnings)
        runs = "\n".join(l for l in step.splitlines() if not l.strip().startswith(("#", "warn ")))
        for never in ("max_daily_loss", "--risk-pct", "--risk-money", "--live", "--paper", "--top-size on",
                      "--tnb-live-groups"):
            assert never not in runs, never                                         # nothing Aaron did not decide
        hints = [l for l in step.splitlines() if l.strip().startswith("warn ")]
        assert any("--tnb-live-groups FX_MINOR --tnb-live-tactics MOMENTUM_CONTINUATION" in l for l in hints)
        assert any("--paper tnb --tnb-live-groups FX_MINOR --tnb-live-tactics MOMENTUM_CONTINUATION" in l
                   for l in hints)                                                  # only as hints, never run

    def test_still_bash_32(self):
        r = subprocess.run(["bash", "-n", str(MAC / "mintel_mac.sh")], capture_output=True, text=True)
        assert r.returncode == 0, r.stderr
        lib = (MAC / "mintel_mac.sh").read_text()
        step = lib.split("apply_formula1_choices() {")[1].split("\n}\n")[0]
        for bad in (r"\$\{\w+,,", r"\$\{\w+\^\^", r"\bmapfile\b", r"\breadarray\b", r"\bdeclare\s+-A\b", "&>>",
                    r"\blocal\s+-n\b"):
            assert not re.search(bad, step), bad


class TestTheMacChoicesStepForReal:
    """The same step with the real modes command (this checkout, this Python), on a MADE-UP fresh install."""

    def test_a_fresh_install_runs_line_up_formula1_choices_in_that_order(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        before = json.loads((data / "config.json").read_text())
        r = _bash_env(LINE_UP + "; " + F1_STEP + "; " + CHOICES_STEP, home)
        assert r.returncode == 0, r.stderr
        assert "[WARN]" not in r.stdout, r.stdout
        assert (r.stdout.index("The bots' line-up") < r.stdout.index("Formula 1 live, the Rapid Momentum Rider off")
                < r.stdout.index("Your choices for Formula 1 are set:"))
        raw = json.loads((data / "config.json").read_text())
        assert raw["tnb"]["mode"] == "PAPER" and raw["tnb"]["live_groups"] == ["FX_MINOR"]
        assert raw["tnb"]["live_tactics"] == [MC]
        assert raw["risk"]["top_size_enabled"] is False and raw["risk"]["daily_loss_counts_practice"] is False
        assert raw["news"]["blackout_before_seconds"] == 900.0
        assert raw["risk"]["base_risk_pct"] == 0.7 and raw["risk"]["max_risk_money"] == 13.0
        assert raw["risk"]["max_daily_loss_pct"] == before["risk"]["max_daily_loss_pct"] == 3.0   # never loosened
        for k, v in before["risk"].items():
            if k not in ("base_risk_pct", "max_risk_money", "top_size_enabled", "daily_loss_counts_practice"):
                assert raw["risk"][k] == v, k
        assert (data / CHOICES).exists() and "CODE=yes" in r.stdout
        tail = r.stdout.split("Your choices for Formula 1 are set:")[1]
        for want in ("minor currency pairs LIVE for momentum continuation only (Formula 1)",
                     "Top-opportunity size: OFF", "in the 15 minutes before a high-importance release", IGNORE_LINE):
            assert want in tail, want
        assert "Restart the bot" not in tail
        cfg = Config.load(data / "config.json")                                    # as the trader reads it
        assert cfg.tnb.live_tactics == (MC,) and cfg.risk.top_size_enabled is False
        assert cfg.news.blackout_before_seconds == 900.0 and cfg.risk.daily_loss_counts_practice is False

    def test_a_later_choice_made_by_hand_is_never_undone(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        assert _bash_env(LINE_UP + "; " + F1_STEP + "; " + CHOICES_STEP, home).returncode == 0
        later = _bash_env('cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.ops.modes --config "$CONFIG" '
                          "--top-size on --tnb-live-tactics all --news-before-minutes 5 --daily-loss-practice count "
                          ">/dev/null", home)
        assert later.returncode == 0, later.stderr
        again = _bash_env(LINE_UP + "; " + F1_STEP + "; " + CHOICES_STEP, home)
        assert "CODE=none" in again.stdout and "Your choices" not in again.stdout
        raw = json.loads((data / "config.json").read_text())
        assert raw["risk"]["top_size_enabled"] is True and raw["tnb"]["live_tactics"] == []
        assert raw["news"]["blackout_before_seconds"] == 300.0 and raw["risk"]["daily_loss_counts_practice"] is True

    def test_formula1_taken_off_live_by_hand_stays_off(self, tmp_path):
        home = _home(tmp_path)
        data = home / "data"
        assert _bash_env(LINE_UP + "; " + F1_STEP, home).returncode == 0
        off = _bash_env('cd "$APP_DIR" && "$VENV_DIR/bin/python" -m mintel.ops.modes --config "$CONFIG" '
                        "--tnb-live-groups none >/dev/null", home)
        assert off.returncode == 0, off.stderr
        r = _bash_env(CHOICES_STEP, home)
        assert r.returncode == 0, r.stderr
        raw = json.loads((data / "config.json").read_text())
        assert raw["tnb"]["live_groups"] == [] and raw["tnb"]["mode"] == "PAPER"   # nothing goes real
        assert raw["risk"]["top_size_enabled"] is False and raw["risk"]["daily_loss_counts_practice"] is False
        assert raw["news"]["blackout_before_seconds"] == 900.0
        assert (data / CHOICES).exists() and "Formula 1 is not live on this Mac just now" in r.stdout


# ================================================================ C. Windows --
class TestTheWindowsChoicesStep:
    """PowerShell 5.1 is not on this machine: these read the scripts, the way the line-up's tests do."""

    def _ps(self, name):
        return (WIN / name).read_text()

    def test_the_same_step_once_with_one_call_and_a_marker_only_on_success(self):
        text = self._ps("mintel_bots.ps1")
        step = text.split("function Set-Formula1ChoicesOnce")[1].split("\nfunction ")[0]
        assert '$marker = Join-Path $P.Data ".formula1-choices-2026-10-10"' in step
        assert (step.index("if (Test-Path $marker) { return $false }")
                < step.index('if (-not (Test-Path (Join-Path $P.Data ".formula1-2026-10-09"))) { return $false }')
                < step.index("Invoke-MintelPython"))
        assert '$rest = @("--top-size", "off", "--news-before-minutes", "15", "--daily-loss-practice", "ignore")' in step
        assert '$modeArgs = @("--tnb-live-tactics", "MOMENTUM_CONTINUATION") + $rest' in step
        assert '$tactics[0] -eq "NONE"' in step and "$groups.Count -gt 0" in step
        assert step.index("if ($r.Code -ne 0)") < step.index("return $false\n    }") < step.index(
            "Set-Content -Path $marker")
        assert "[WARN] Could not set your choices for Formula 1 (nothing was changed). Run it by hand" in step
        assert "[ OK ] Your choices for Formula 1 are set:" in step
        assert "Formula 1 is not live on this computer just now" in step
        assert "--tnb-live-groups FX_MINOR --tnb-live-tactics MOMENTUM_CONTINUATION" in step
        for never in ("max_daily_loss", '"--risk-pct"', '"--live"', '"--paper"', '"on"'):
            assert never not in step, never

    def test_setup_runs_it_right_after_formula1(self):
        text = self._ps("SETUP-AND-START.ps1")
        assert (text.index("Set-IanBinanceOnce $P") < text.index("Set-Formula1Once $P")
                < text.index("Set-Formula1ChoicesOnce $P") < text.index("Write-LineUp $P")
                < text.index("-FromSetup -Restart"))

    def test_start_bot_applies_it_once_on_its_own_after_formula1(self):
        text = self._ps("START-BOT.ps1")
        f1 = text.index("# ------------------------------------------- Formula 1, once (10 Oct) ----")
        mine = text.index("# ------------------------------- Formula 1's choices, once (10 Oct) ----")
        restart = text.index("# ---------------------------------------------------------------- restart ----")
        assert f1 < mine < restart
        block = text[mine:restart]
        assert ('if (-not $FromSetup -and (Test-Path $formula1Marker) -and -not (Test-Path $choicesMarker))'
                in block)
        assert block.index("if (Test-Path $choicesTried)") < block.index("Set-Formula1ChoicesOnce $P")
        assert "if (Set-Formula1ChoicesOnce $P) { $Restart = $true }" in block
        assert '$choicesTried = Join-Path $dataDir ".formula1-choices-2026-10-10-tried"' in block
        assert "they are not tried again on their own. Run SETUP-AND-START.cmd to try again" in block

    def test_ascii_only(self):
        for name in ("mintel_bots.ps1", "SETUP-AND-START.ps1", "START-BOT.ps1"):
            data = (WIN / name).read_bytes()
            assert all(b < 128 for b in data), name

    def test_fully_live_has_its_own_warning(self):
        step = self._ps("mintel_bots.ps1").split("function Set-Formula1ChoicesOnce")[1].split("\nfunction ")[0]
        assert '$allLive = ((Get-ConfigBotMode $P "tnb" "LIVE") -ne "PAPER")' in step
        assert step.index("if ($allLive) {") < step.index("} elseif (-not $liveNow) {")
        assert ("[WARN] Trend & Breakout is fully LIVE on this computer just now (put there by hand with the modes "
                "command): every trade of it, minor pairs included, is real money") in step
        assert "--paper tnb --tnb-live-groups FX_MINOR --tnb-live-tactics MOMENTUM_CONTINUATION" in step

    @pytest.mark.parametrize("which", ("live", "cleared", "fully-live"))
    def test_under_powershell(self, tmp_path, which):
        pwsh = shutil.which("pwsh")
        if pwsh is None:
            pytest.skip("PowerShell is not on this machine: the script is read above")
        live = which == "live"
        data = tmp_path / "data"
        data.mkdir()
        (data / ".formula1-2026-10-09").write_text("x")
        cfg = data / "config.json"
        cfg.write_text(json.dumps({"tnb": {"mode": "LIVE" if which == "fully-live" else "PAPER",
                                           "live_groups": [] if which == "cleared" else ["FX_MINOR"]}}))
        script = (f'. "{WIN / "mintel_bots.ps1"}"; '
                  "$P = @{ Data = '" + str(data) + "'; Config = '" + str(cfg) + "'; App = 'a'; Py = 'p' }; "
                  "$script:calls = @(); "
                  "function Test-MintelInstalled { param($P) return $true }; "
                  "function Invoke-MintelPython { param($P, [string[]] $Arguments) "
                  "$script:calls += ($Arguments -join ' '); return @{ Code = 0; Lines = @('made-up') } }; "
                  "$a = Set-Formula1ChoicesOnce $P; $b = Set-Formula1ChoicesOnce $P; "
                  'Write-Output "A=$a B=$b"; Write-Output ("CALLS=" + ($script:calls -join "|"))')
        r = subprocess.run([pwsh, "-NoProfile", "-NonInteractive", "-Command", script], capture_output=True,
                           text=True, timeout=120)
        assert r.returncode == 0, r.stderr
        assert "A=True B=False" in r.stdout                                          # once only
        call = CHOICES_CALL if live else REST_CALL
        assert f"CALLS=-m mintel.ops.modes --config {cfg} {call}" in r.stdout
        assert (data / CHOICES).exists()
        assert ("Formula 1 is not live on this computer just now" in r.stdout) is (which == "cleared")
        assert ("Trend & Breakout is fully LIVE on this computer just now" in r.stdout) is (which == "fully-live")
        if which == "fully-live":
            assert "no minor-pair trade of Trend & Breakout goes to the real broker" not in r.stdout
            assert "--paper tnb --tnb-live-groups FX_MINOR --tnb-live-tactics MOMENTUM_CONTINUATION" in r.stdout


# ================================== E. re-entering the settings keeps them --
# 10 Oct review: answering "y" to "Change them?" (Mac) or "Change your
# settings?" (Windows) rebuilds config.json's risk and news blocks from the
# answers, and carried over only the top-level blocks - so three of Aaron's
# four choices (top size off, practice not counted, 15 minutes before news)
# were quietly dropped, and the marker kept the one-time step from setting
# them again. These run each installer's OWN config-writing code with the
# default answers (MADE-UP account; no real setting is read).
CHOSEN = {"risk": {"base_risk_pct": 0.7, "max_risk_money": 13.0, "max_daily_loss_pct": 3.0,
                   "top_size_enabled": False, "daily_loss_counts_practice": False},
          "news": {"enabled": True, "blackout_before_seconds": 900.0},
          "tnb": {"mode": "PAPER", "live_groups": ["FX_MINOR"], "live_tactics": [MC]}}


def _mac_reenter(data: Path, old: dict, daily: str = "3.0") -> dict:
    """The Mac's write_config Python block, as answering "y" and pressing Enter runs it."""
    import os
    import sys
    lib = (MAC / "mintel_mac.sh").read_text()
    py = lib.split("write_config() {")[1].split("<<'PYEOF'\n")[1].split("\nPYEOF")[0]
    (data / "config.json").write_text(json.dumps(old))
    env = {**os.environ, "MINTEL_DATA": str(data), "MINTEL_LOGS": str(data / "logs"), "MINTEL_MODE": "DEMO",
           "MINTEL_BASE": "0.7", "MINTEL_MAX": "1.5", "MINTEL_DAILY": daily, "MINTEL_AGGR": "NORMAL",
           "MINTEL_PASSWORD": "made-up", "MINTEL_APP": str(Path(V.PLAN_FILE).parents[1])}
    r = subprocess.run([sys.executable, "-"], input=py, text=True, env=env, capture_output=True, timeout=60)
    assert r.returncode == 0, r.stderr
    return json.loads((data / "config.json").read_text())


def _win_reenter(data: Path, old: dict, pwsh: str, daily: str = "3.0") -> dict:
    """SETUP-AND-START.ps1's own $cfg block and carry-over, as answering "y" and pressing Enter runs them."""
    text = (WIN / "SETUP-AND-START.ps1").read_text()
    jsonf = text[text.index("function Write-JsonFile"):]
    jsonf = jsonf[:jsonf.index("\n}\n") + 3]
    end = "Write-JsonFile $configPath ($cfg | ConvertTo-Json -Depth 10)"
    block = text[text.index("    $cfg = [ordered]@{"):]
    block = block[:block.index(end) + len(end)]
    cfg = data / "config.json"
    cfg.write_text(json.dumps(old))
    script = ('$ErrorActionPreference = "Stop"\nfunction Warn { param($m) Write-Host "    [WARN] $m" }\n' + jsonf
              + f"$dataDir = '{data}'; $logDir = '{data / 'logs'}'; $configPath = '{cfg}'; $existing = $true\n"
              + "$mode = 'DEMO'; $marker = ''; $login = '0'; $server = ''; $baseRisk = '0.7'; $maxRisk = '1.5'\n"
              + f"$dailyMax = '{daily}'; $aggr = 'NORMAL'; $newsKey = ''\n" + block + "\n")
    (data / "reenter.ps1").write_text(script)
    r = subprocess.run([pwsh, "-NoProfile", "-NonInteractive", "-File", str(data / "reenter.ps1")],
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr
    assert "[WARN]" not in r.stdout, r.stdout
    return json.loads(cfg.read_text())


def _kept(after: dict, old: dict) -> None:
    for block, key in (("risk", "top_size_enabled"), ("risk", "daily_loss_counts_practice"),
                       ("news", "blackout_before_seconds")):
        if key in old[block]:
            assert after[block][key] == old[block][key], (block, key)
        else:
            assert key not in after[block], (block, key)                     # never added: the step's to set
    assert after["tnb"]["live_tactics"] == old["tnb"]["live_tactics"]


class TestReEnteringTheSettingsKeepsTheChoices:
    def test_mac_keeps_all_four_and_the_limit_is_as_typed(self, tmp_path):
        after = _mac_reenter(tmp_path, CHOSEN)
        _kept(after, CHOSEN)
        assert after["risk"]["max_daily_loss_pct"] == 3.0
        cfg = Config.load(tmp_path / "config.json")                                # as the trader reads it
        assert cfg.risk.top_size_enabled is False and cfg.risk.daily_loss_counts_practice is False
        assert cfg.news.blackout_before_seconds == 900.0 and cfg.tnb.live_tactics == (MC,)
        # the limit is never carried: it is what was typed (a tighter 2% typed stays 2%)
        assert _mac_reenter(tmp_path, CHOSEN, daily="2.0")["risk"]["max_daily_loss_pct"] == 2.0

    def test_mac_keeps_a_later_hand_choice_and_never_adds_one(self, tmp_path):
        later = json.loads(json.dumps(CHOSEN))
        later["risk"].update(top_size_enabled=True, daily_loss_counts_practice=True)
        later["news"]["blackout_before_seconds"] = 300.0
        _kept(_mac_reenter(tmp_path, later), later)
        before = json.loads(json.dumps(CHOSEN))                                    # the step not run yet
        for block, key in (("risk", "top_size_enabled"), ("risk", "daily_loss_counts_practice"),
                           ("news", "blackout_before_seconds")):
            del before[block][key]
        (tmp_path / "b").mkdir()
        _kept(_mac_reenter(tmp_path / "b", before), before)

    def test_windows_carries_the_same_three(self):
        text = (WIN / "SETUP-AND-START.ps1").read_text()
        carry = text.split("foreach ($key in @(")[1].split("Write-JsonFile $configPath")[0]
        for want in ('@("risk", "top_size_enabled")', '@("risk", "daily_loss_counts_practice")',
                     '@("news", "blackout_before_seconds")'):
            assert want in carry, want
        code = "\n".join(l for l in carry.splitlines() if not l.strip().startswith("#"))
        assert "max_daily_loss_pct" not in code                                     # never the limit
        assert all(b < 128 for b in (WIN / "SETUP-AND-START.ps1").read_bytes())

    @pytest.mark.parametrize("which", ("chosen", "later", "absent"))
    def test_windows_under_powershell(self, tmp_path, which):
        pwsh = shutil.which("pwsh")
        if pwsh is None:
            pytest.skip("PowerShell is not on this machine: the script is read above")
        old = json.loads(json.dumps(CHOSEN))
        if which == "later":
            old["risk"].update(top_size_enabled=True, daily_loss_counts_practice=True)
            old["news"]["blackout_before_seconds"] = 300.0
        elif which == "absent":
            for block, key in (("risk", "top_size_enabled"), ("risk", "daily_loss_counts_practice"),
                               ("news", "blackout_before_seconds")):
                del old[block][key]
        after = _win_reenter(tmp_path, old, pwsh)
        _kept(after, old)
        assert after["risk"]["max_daily_loss_pct"] == 3.0
        assert _win_reenter(tmp_path, old, pwsh, daily="2.0")["risk"]["max_daily_loss_pct"] == 2.0


# ======================================================= D. the plan and page --
DOC = Path(V.PLAN_FILE).parent / "strategies" / "v2-commission-first.md"


class TestThePlanSaysTheDecision:
    def test_the_focus_steps_and_changes(self):
        plan, err = V.load_plan()
        assert not err and plan["updated"] == "2026-10-10"
        f = plan["focus"]
        assert f["settings"] == {"live_tactics": [MC], "top_size_enabled": False, "news_before_minutes": 15,
                                 "daily_loss_counts_practice": False}
        assert f["judge_after"] == 20 and f["live_group"] == "FX_MINOR"
        for want in ("momentum continuation trades on minor currency pairs", "flat £13 a trade",
                     "0.7% of the balance", "no top-opportunity size until it has 20 live trades",
                     "15 minutes before a high-importance release on either currency",
                     "daily-loss stop counts real money only; its 3% limit is unchanged"):
            assert want in f["what"], want
        assert "counted from the moment these choices took effect" in f["caution"]
        first = plan["steps"][0]["text"]
        assert first.startswith("Formula 1 live - judge it after 20 live trades, counted from when the 10 Oct "
                                "choices took effect")
        assert "keep it and grow it (the top-opportunity size back on) if it is positive after all costs with more " \
               "days up than down; otherwise back to PAPER and test the next candidate." in first
        # 10 Oct review: the stop's real side is Trend & Breakout plus the bots it gates
        # (Trader.stop_gated_magics: runner, bandbreaker, crowd) - never "the other bots'", never Ian's
        why = next(c["why"] for c in plan["change"] if c["text"] == "The daily-loss stop counts real money only")
        assert ("Now it counts Trend & Breakout's real trades and the Momentum Runner's, Band Breaker's and Crowd "
                "Fader's real trades (not Financial Ian's)") in why
        assert "the other bots' real trades" not in why
        import inspect
        from mintel.engine.trader import Trader
        assert '("runner", "bandbreaker", "crowd")' in inspect.getsource(Trader.stop_gated_magics)
        changes = [c["text"] for c in plan["change"]]
        assert changes == ["Formula 1 is momentum continuation only",
                           "A flat £13 a trade - no top-opportunity size until 20 live trades",
                           "No new entry in the 15 minutes before a big release",
                           "The daily-loss stop counts real money only"]
        # everything else as it was
        steps = [s["text"] for s in plan["steps"]]
        assert steps[1].startswith("Rapid Momentum Rider switched OFF and binned")
        assert "about £10 a ride" in steps[2] and "30 trades" in steps[3] and "stay on PAPER" in steps[4]
        assert any(b["text"] == "Trend & Breakout's momentum continuation on FX major pairs" for b in plan["bin"])
        assert [k.get("bot") for k in plan["keep"]] == ["momentum_runner", "financial_ian", None, "band_breaker",
                                                        "crowd_fader"]
        assert not [c for c in plan["change"] if c.get("bot") or c.get("stance")]  # never a "Check:" of its own

    def test_every_new_figure_is_in_the_document_it_comes_from(self):
        doc = DOC.read_text()
        for fig in ("+156.31", "29.89", "81.10", "-32.47", "-3.04 R", "about 15 pips", "+10.65, -26.22, -23.91",
                    "21-29 at risk", "13:46-21:49 UK", "38", "8 min before"):
            assert fig in doc, fig
        assert "| **Momentum continuation, FX minor pairs** (fast + news markets) | **38** | **10** | **8 / 2** |" in doc
        assert "at least **20 trades over at least 4 trading days**" in doc
        # the only minor-pair segment that meets the sample size is momentum continuation
        segs = doc.split("Every segment that meets the sample size")[1].split("\n\n")[1]
        assert [l for l in segs.splitlines() if "minor" in l.lower()] == [
            l for l in segs.splitlines() if "Momentum continuation, FX minor pairs" in l]
        assert RiskConfig().max_daily_loss_pct == 3.0 and NewsConfig().blackout_before_seconds == 90.0


def _four_choices_day(tmp_path, **over):
    from tests.test_formula1_page import a_formula1_day
    d = a_formula1_day(tmp_path)
    raw = json.loads((d.data / "config.json").read_text())
    raw["tnb"]["live_tactics"] = over.get("tactics", [MC])
    raw["risk"] = {"top_size_enabled": over.get("top_size", False),
                   "daily_loss_counts_practice": over.get("counts", False)}
    raw["news"] = {"blackout_before_seconds": over.get("news", 900.0)}
    (d.data / "config.json").write_text(json.dumps(raw))
    return d


class TestThePageAgreesWithThePlan:
    def test_with_the_four_choices_in_force(self, tmp_path):
        from tests.test_formula1_page import _pages, _plan_box
        from tests.test_simple_dashboard import row_of
        d = _four_choices_day(tmp_path)
        home, detail = _pages(d, "/", "/?bot=market_intelligence")
        box = _plan_box(home)
        assert ("In force now (config.json): only its momentum continuation trades on minor pairs use real money "
                "(tnb.live_tactics); the top-opportunity size is OFF, so every trade risks the normal amount, never "
                "the bigger size (risk.top_size_enabled); no new Trend & Breakout entry in the 15 minutes before a "
                "high-importance release on either currency of the pair (news.blackout_before_seconds); the "
                "daily-loss stop counts real money only, its limit unchanged (risk.daily_loss_counts_practice).") in box
        assert "Not in config.json here yet" not in box and "none of the plan's choices" not in box
        assert "Check:" not in box                                                   # nothing disagrees
        plan, _ = V.load_plan()
        assert plan["focus"]["what"] in box and plan["steps"][0]["text"] in box
        row = row_of(home, "Trend &amp; Breakout")
        assert "LIVE: minor pairs (Formula 1, momentum continuation only) - rest PAPER" in row
        assert ("Formula 1 - its momentum continuation trades on minor pairs - is LIVE: judge it after 20 live "
                "trades") in row
        assert "Keep it and grow it only if it is positive after costs with more up days than down; otherwise " \
               "back to PAPER and test the next candidate." in row
        assert "Formula 1 LIVE - its momentum continuation trades on minor pairs with real money" in detail

    @pytest.mark.parametrize("over, missing", [
        ({"tactics": []}, "only its momentum continuation trades on minor pairs with real money (tnb.live_tactics)"),
        ({"top_size": True}, "the top-opportunity size OFF (risk.top_size_enabled)"),
        ({"news": 90.0}, "no new entry in the 15 minutes before a high-importance release "
                         "(news.blackout_before_seconds is 90 here)"),
        ({"counts": True}, "the daily-loss stop on real money only (risk.daily_loss_counts_practice)"),
    ], ids=["tactics", "top-size", "news", "daily-loss"])
    def test_a_choice_the_plan_names_but_config_lacks_is_said_plainly(self, tmp_path, over, missing):
        from tests.test_formula1_page import _pages, _plan_box
        d = _four_choices_day(tmp_path, **over)
        box = _plan_box(_pages(d, "/")[0])
        assert "In force now (config.json): " in box
        assert "Not in config.json here yet, though the plan says so: " + missing + "." in box
        assert "Check:" not in box

    def test_without_settings_in_the_plan_the_line_is_as_before(self, tmp_path):
        d = _four_choices_day(tmp_path, tactics=[], top_size=True, counts=True, news=90.0)
        assert V.formula1_in_force(str(d.data)) == ""                               # nothing new, nothing asked
        assert V.formula1_in_force(str(d.data), {"live_tactics": [MC]}).startswith("none of the plan's choices - ")
        assert V.formula1_in_force(str(tmp_path / "nowhere"), {"live_tactics": [MC]}) == ""   # never raises
