"""python -m mintel.ops.modes: switching the bots between OFF, PAPER and
LIVE edits only the mode keys, atomically, and the bots read the result."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from mintel.config import Config
from mintel.ops import modes
from mintel.ian.engine import IanConfig
from mintel.rider.config import RiderConfig
from mintel.scalper.config import ScalperConfig


def a_config(tmp_path: Path) -> Path:
    """A real config.json as the setup writes it, plus a hand edit the tool
    must not lose."""
    cfg = Config()
    cfg.ops.data_dir = str(tmp_path)
    cfg.runner.trail_r = 2.5
    cfg.risk.max_risk_pct = 1.25
    p = tmp_path / "config.json"
    cfg.save(p)
    raw = json.loads(p.read_text())
    raw["note_from_aaron"] = "keep me"
    p.write_text(json.dumps(raw, indent=2))
    return p


class TestWritingTheFiles:
    def test_every_bot_live_and_the_bots_read_it(self, tmp_path):
        p = a_config(tmp_path)
        (tmp_path / "scalper.json").write_text(json.dumps({"mode": "PAPER", "entry_hours_utc": [0], "keep_overrides": True}))
        out = modes.set_modes(p, modes.plan_changes(live=["all"]))
        # "all" is the five active bots; the retired scalper is never switched on
        assert out == {"rider": "LIVE", "runner": "LIVE", "bandbreaker": "LIVE", "crowd": "LIVE", "ian": "LIVE",
                       "scalper": "PAPER", "tnb": "LIVE"}             # Trend & Breakout: named on its own, LIVE by default
        cfg = Config.load(p)
        assert cfg.runner.mode == "LIVE" and cfg.bandbreaker.mode == "LIVE" and cfg.crowd.mode == "LIVE"
        assert cfg.runner.magic == 990_511 and cfg.bandbreaker.magic == 990_611 and cfg.crowd.magic == 990_711
        assert RiderConfig.load(tmp_path / "rider.json").mode == "LIVE"
        assert IanConfig.load(tmp_path / "ian.json").mode == "LIVE"
        s = ScalperConfig.load(tmp_path / "scalper.json")
        assert s.mode == "PAPER" and s.entry_hours_utc == (0,)
        assert json.loads((tmp_path / "scalper.json").read_text())["mode"] == "PAPER"     # not touched

    def test_everything_else_in_both_files_is_kept(self, tmp_path):
        p = a_config(tmp_path)
        (tmp_path / "scalper.json").write_text(json.dumps({"mode": "PAPER", "entry_hours_utc": [0], "keep_overrides": True}))
        before = json.loads(p.read_text())
        modes.set_modes(p, {"crowd": "LIVE", "scalper": "OFF"})
        after = json.loads(p.read_text())
        assert after["note_from_aaron"] == "keep me" and after["risk"]["max_risk_pct"] == 1.25
        assert after["runner"]["trail_r"] == 2.5 and after["runner"]["mode"] == "PAPER"
        assert after["crowd"]["mode"] == "LIVE"
        before["crowd"]["mode"] = "LIVE"
        assert after == before                                              # nothing but that one key moved
        s = json.loads((tmp_path / "scalper.json").read_text())
        assert s == {"mode": "OFF", "entry_hours_utc": [0], "keep_overrides": True}
        assert Config.load(p).crowd.mode == "LIVE" and Config.load(p).runner.mode == "PAPER"
        assert ScalperConfig.load(tmp_path / "scalper.json").mode == "OFF"

    def test_a_missing_block_or_bot_file_is_created(self, tmp_path):
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"mode": "DEMO", "risk": {"base_risk_pct": 0.5}}))
        out = modes.set_modes(p, {"bandbreaker": "LIVE", "rider": "LIVE", "scalper": "OFF"})
        assert out["bandbreaker"] == "LIVE" and out["rider"] == "LIVE" and out["scalper"] == "OFF"
        assert out["runner"] == "PAPER" and out["crowd"] == "PAPER" and out["ian"] == "PAPER"   # untouched: the default
        raw = json.loads(p.read_text())
        assert raw["bandbreaker"] == {"mode": "LIVE"} and raw["risk"] == {"base_risk_pct": 0.5} and raw["mode"] == "DEMO"
        assert json.loads((tmp_path / "scalper.json").read_text()) == {"mode": "OFF"}
        assert json.loads((tmp_path / "rider.json").read_text()) == {"mode": "LIVE"}
        assert not (tmp_path / "ian.json").exists()                          # not named: not written
        assert Config.load(p).bandbreaker.mode == "LIVE" and Config.load(p).runner.mode == "PAPER"
        assert ScalperConfig.load(tmp_path / "scalper.json").mode == "OFF"
        assert RiderConfig.load(tmp_path / "rider.json").mode == "LIVE"

    def test_the_write_is_atomic_and_leaves_no_temporary_file(self, tmp_path):
        p = a_config(tmp_path)
        modes.set_modes(p, {"runner": "LIVE", "scalper": "OFF", "rider": "PAPER", "ian": "PAPER"})
        for name in ("config.json", "scalper.json", "rider.json", "ian.json"):
            assert not (tmp_path / (name + ".tmp")).exists(), name
        assert json.loads(p.read_text())["runner"]["mode"] == "LIVE"

    def test_back_to_paper_and_off(self, tmp_path):
        p = a_config(tmp_path)
        modes.set_modes(p, modes.plan_changes(live=["all"]))
        out = modes.set_modes(p, modes.plan_changes(paper=["runner", "ian"], off=["crowd", "scalper"]))
        assert out == {"rider": "LIVE", "runner": "PAPER", "bandbreaker": "LIVE", "crowd": "OFF", "ian": "PAPER",
                       "scalper": "OFF", "tnb": "LIVE"}
        assert Config.load(p).crowd.mode == "OFF" and ScalperConfig.load(tmp_path / "scalper.json").mode == "OFF"
        assert IanConfig.load(tmp_path / "ian.json").mode == "PAPER"

    def test_no_config_file_means_nothing_is_written(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            modes.set_modes(tmp_path / "config.json", {"runner": "LIVE"})
        assert not (tmp_path / "config.json").exists()

    def test_a_broken_config_is_never_overwritten(self, tmp_path):
        p = tmp_path / "config.json"
        p.write_text("{not json")
        with pytest.raises(ValueError):
            modes.set_modes(p, {"runner": "LIVE"})
        assert p.read_text() == "{not json"


class TestRefusals:
    def test_unknown_bot_names_are_refused(self):
        with pytest.raises(ValueError, match="unknown bot"):
            modes.plan_changes(live=["turbo"])            # ("trend" is Trend & Breakout since 9 Oct)
        with pytest.raises(ValueError, match="unknown bot"):
            modes.set_modes("config.json", {"scalpr": "LIVE"})

    def test_a_bot_named_for_two_modes_is_refused(self):
        with pytest.raises(ValueError, match="say one"):
            modes.plan_changes(live=["all"], paper=["crowd"])

    def test_a_mode_must_be_a_mode(self, tmp_path):
        p = a_config(tmp_path)
        with pytest.raises(ValueError, match="OFF, PAPER, LIVE"):
            modes.set_modes(p, {"runner": "REAL"})
        assert json.loads(p.read_text())["runner"]["mode"] == "PAPER"


def mode_of(out: str, name: str) -> str:
    """The mode printed on the line for one bot."""
    for line in out.splitlines():
        if line.startswith(name + ":"):
            return line.split(":", 1)[1].split()[0]
    raise AssertionError(f"no line for {name} in:\n{out}")


class TestCommandLine:
    def test_live_all_prints_every_mode_and_the_restart_line(self, tmp_path, capsys):
        p = a_config(tmp_path)
        assert modes.main(["--config", str(p), "--live", "all"]) == 0
        out = capsys.readouterr().out
        for name in ("Rapid Momentum Rider", "Momentum Runner", "Band Breaker", "Crowd Fader", "Financial Ian"):
            assert mode_of(out, name) == "LIVE"
        assert mode_of(out, "Rapid Scalper (retired)") == "PAPER"            # its file says nothing: untouched
        assert "retired 9 Oct" in out
        assert "Restart the bot for this to take effect (Stop Trading Bot, then Start Trading Bot)" in out
        assert Config.load(p).runner.mode == "LIVE" and RiderConfig.load(tmp_path / "rider.json").mode == "LIVE"
        assert not (tmp_path / "scalper.json").exists()

    def test_show_changes_nothing(self, tmp_path, capsys):
        p = a_config(tmp_path)
        stamp = p.read_text()
        assert modes.main(["--config", str(p), "--show"]) == 0
        out = capsys.readouterr().out
        assert mode_of(out, "Momentum Runner") == "PAPER" and mode_of(out, "Rapid Scalper (retired)") == "PAPER"
        assert mode_of(out, "Rapid Momentum Rider") == "PAPER" and mode_of(out, "Financial Ian") == "PAPER"
        assert "Restart the bot" not in out and p.read_text() == stamp
        for name in ("scalper.json", "rider.json", "ian.json"):
            assert not (tmp_path / name).exists(), name

    def test_an_unknown_bot_on_the_command_line_is_refused(self, tmp_path, capsys):
        p = a_config(tmp_path)
        stamp = p.read_text()
        assert modes.main(["--config", str(p), "--live", "runner", "turbo"]) == 2
        assert "unknown bot 'turbo'" in capsys.readouterr().err
        assert p.read_text() == stamp                                        # refused as a whole: nothing written

    def test_runnable_as_a_module(self, tmp_path):
        import subprocess, sys
        p = a_config(tmp_path)
        r = subprocess.run([sys.executable, "-m", "mintel.ops.modes", "--config", str(p), "--paper", "crowd", "--live", "rider",
                            "--off", "scalper"],
                           capture_output=True, text=True, cwd=str(Path(__file__).resolve().parents[1]), timeout=60)
        assert r.returncode == 0, r.stderr
        assert mode_of(r.stdout, "Crowd Fader") == "PAPER" and mode_of(r.stdout, "Rapid Momentum Rider") == "LIVE"
        assert mode_of(r.stdout, "Rapid Scalper (retired)") == "OFF"
        assert "Restart the bot" in r.stdout
        assert ScalperConfig.load(tmp_path / "scalper.json").mode == "OFF"
        assert RiderConfig.load(tmp_path / "rider.json").mode == "LIVE"


class TestEveryFileIsCheckedBeforeAnyIsWritten:
    """Review, 8 Oct: the tool read config.json, wrote it, and only then read
    scalper.json, so a broken scalper.json left three bots switched while the
    window said 'Nothing changed'; and a scalper-only switch wrote scalper.json
    next to whatever path was typed, never where the scalper reads it."""

    def test_a_broken_scalper_file_means_nothing_is_written(self, tmp_path):
        p = a_config(tmp_path)
        (tmp_path / "scalper.json").write_text("{not json")
        with pytest.raises(ValueError, match="scalper.json"):
            modes.set_modes(p, modes.plan_changes(live=["runner"], off=["scalper"]))
        assert json.loads(p.read_text())["runner"]["mode"] == "PAPER"       # config.json untouched
        assert (tmp_path / "scalper.json").read_text() == "{not json"

    def test_a_broken_rider_or_ian_file_means_nothing_is_written(self, tmp_path):
        p = a_config(tmp_path)
        for name in ("rider.json", "ian.json"):
            (tmp_path / name).write_text("[1, 2")
            with pytest.raises(ValueError, match=name):
                modes.set_modes(p, modes.plan_changes(live=["all"]))
            assert json.loads(p.read_text())["runner"]["mode"] == "PAPER"
            assert (tmp_path / name).read_text() == "[1, 2"
            (tmp_path / name).unlink()
        assert not (tmp_path / "rider.json").exists() and not (tmp_path / "ian.json").exists()

    def test_scalper_alone_with_no_config_file_writes_nothing(self, tmp_path):
        typo = tmp_path / "dat" / "config.json"                             # ~/MarketBot/dat instead of data
        with pytest.raises(FileNotFoundError):
            modes.set_modes(typo, {"scalper": "OFF"})
        assert not (tmp_path / "dat").exists()                              # no folder, no scalper.json
        assert not (tmp_path / "scalper.json").exists()

    def test_scalper_json_lives_in_the_data_folder_the_config_names(self, tmp_path, capsys):
        data = tmp_path / "elsewhere"
        data.mkdir()
        cfg = Config()
        cfg.ops.data_dir = str(data)
        p = tmp_path / "config.json"
        cfg.save(p)
        assert modes.scalper_path(p) == data / "scalper.json"
        assert modes.bot_file(p, "rider") == data / "rider.json" and modes.bot_file(p, "ian") == data / "ian.json"
        out = modes.set_modes(p, {"scalper": "OFF", "rider": "LIVE", "ian": "PAPER"})
        assert out["scalper"] == "OFF" and out["rider"] == "LIVE" and out["ian"] == "PAPER"
        assert json.loads((data / "scalper.json").read_text()) == {"mode": "OFF"}
        for name in ("scalper.json", "rider.json", "ian.json"):
            assert not (tmp_path / name).exists(), name                     # not next to config.json
        assert modes.read_modes(p)["scalper"] == "OFF"
        assert ScalperConfig.load(ScalperConfig.default_path(cfg.ops.data_dir)).mode == "OFF"   # as the scalper reads it
        assert RiderConfig.load(RiderConfig.default_path(cfg.ops.data_dir)).mode == "LIVE"       # as the rider reads it
        assert IanConfig.load(IanConfig.default_path(cfg.ops.data_dir)).mode == "PAPER"
        assert modes.main(["--config", str(p), "--show"]) == 0
        shown = capsys.readouterr().out
        assert f"{data / 'scalper.json'}: mode)" in shown and f"{data / 'rider.json'}: mode)" in shown   # the window shows the file

    def test_a_config_naming_no_data_folder_uses_its_own(self, tmp_path):
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"mode": "DEMO"}))
        assert modes.scalper_path(p) == tmp_path / "scalper.json"

    def test_a_write_that_stops_part_way_says_which_file_holds_the_change(self, tmp_path, monkeypatch, capsys):
        p = a_config(tmp_path)
        real = modes._write_atomic
        def flaky(path, obj):
            if path.name == "rider.json":
                raise OSError("disk full")
            real(path, obj)
        monkeypatch.setattr(modes, "_write_atomic", flaky)
        with pytest.raises(modes.PartialWrite, match="config.json.*already holds the change.*rider.json.*does not"):
            modes.set_modes(p, {"runner": "LIVE", "rider": "LIVE"})
        assert Config.load(p).runner.mode == "LIVE"                           # true: it was written
        assert modes.main(["--config", str(p), "--paper", "runner", "--live", "rider"]) == 1
        err = capsys.readouterr().err
        assert err.startswith("Stopped part way:") and "Nothing changed" not in err


class TestMagicNumbers:
    """Review, 8 Oct: the magic number is the only thing that keeps one bot
    from seeing or touching another's positions, and config.json carries
    every bot's number as an editable field. A bot whose number is 0 or
    another bot's is refused on the way to LIVE; nothing is written."""

    def _config(self, tmp_path, **edits) -> Path:
        p = a_config(tmp_path)
        raw = json.loads(p.read_text())
        for block, magic in edits.items():
            raw.setdefault(block, {})["magic"] = magic
        p.write_text(json.dumps(raw, indent=2))
        return p

    def test_every_bots_number_is_read_from_the_files(self, tmp_path):
        p = a_config(tmp_path)
        (tmp_path / "scalper.json").write_text(json.dumps({"mode": "PAPER", "magic": 990_412, "keep_overrides": True}))
        assert modes.magics(p) == {"tnb": 990_311, "scalper": 990_412, "runner": 990_511,
                                   "bandbreaker": 990_611, "crowd": 990_711, "rider": 990_811, "ian": 990_911}
        assert modes.magic_problems(modes.magics(p), modes.BOTS) == []

    def test_a_bot_carrying_trend_and_breakouts_number_is_refused(self, tmp_path):
        p = self._config(tmp_path, crowd=990_311)
        stamp = p.read_text()
        with pytest.raises(ValueError, match="crowd's magic 990311 is Trend & Breakout's; a LIVE bot must carry its own magic number"):
            modes.set_modes(p, {"crowd": "LIVE"})
        assert p.read_text() == stamp and Config.load(p).crowd.mode == "PAPER"
        with pytest.raises(ValueError, match="crowd's magic 990311"):
            modes.set_modes(p, modes.plan_changes(live=["all"]))             # refused as a whole
        assert p.read_text() == stamp and not (tmp_path / "scalper.json").exists()
        # the check is only on the way to LIVE: PAPER and OFF still work
        assert modes.set_modes(p, {"crowd": "OFF"})["crowd"] == "OFF"

    def test_zero_and_a_shared_number_are_refused(self, tmp_path):
        p = self._config(tmp_path, runner=0, bandbreaker=990_711)
        with pytest.raises(ValueError, match="runner's magic number is 0; a LIVE bot must carry its own positive magic number"):
            modes.set_modes(p, {"runner": "LIVE"})
        with pytest.raises(ValueError, match="bandbreaker's magic 990711 is Crowd Fader's"):
            modes.set_modes(p, {"bandbreaker": "LIVE"})
        with pytest.raises(ValueError, match="crowd's magic 990711 is Band Breaker's"):
            modes.set_modes(p, {"crowd": "LIVE"})
        assert Config.load(p).runner.mode == "PAPER" and Config.load(p).crowd.mode == "PAPER"

    def test_the_scalpers_number_counts_too(self, tmp_path):
        p = a_config(tmp_path)
        (tmp_path / "scalper.json").write_text(json.dumps({"mode": "PAPER", "magic": 990_511, "keep_overrides": True}))
        with pytest.raises(ValueError, match="retired"):
            modes.set_modes(p, {"scalper": "LIVE"})                       # retired: never LIVE at all
        with pytest.raises(ValueError, match="runner's magic 990511 is Rapid Scalper's"):
            modes.set_modes(p, {"runner": "LIVE"})
        assert ScalperConfig.load(tmp_path / "scalper.json").mode == "PAPER"
        assert modes.set_modes(p, {"bandbreaker": "LIVE"})["bandbreaker"] == "LIVE"   # its own number: fine

    def test_the_command_refuses_and_prints_each_bots_number(self, tmp_path, capsys):
        p = self._config(tmp_path, crowd=990_411)
        assert modes.main(["--config", str(p), "--live", "crowd"]) == 1
        assert "Nothing changed: crowd's magic 990411 is Rapid Scalper's" in capsys.readouterr().err
        assert modes.main(["--config", str(p), "--live", "runner"]) == 0
        out = capsys.readouterr().out
        assert "magic 990511; config.json: runner.mode" in out and "magic 990411; config.json: crowd.mode" in out
        assert "magic 990411; " in [l for l in out.splitlines() if l.startswith("Rapid Scalper (retired):")][0]
        assert "magic 990811; " in [l for l in out.splitlines() if l.startswith("Rapid Momentum Rider:")][0]
        assert "magic 990911; " in [l for l in out.splitlines() if l.startswith("Financial Ian:")][0]


class TestMoreCommandLine:
    def test_show_refuses_a_missing_or_broken_config(self, tmp_path, capsys):
        assert modes.main(["--config", str(tmp_path / "config.json"), "--show"]) == 1
        assert "Nothing changed: no config file at" in capsys.readouterr().err
        p = tmp_path / "config.json"
        p.write_text("{not json")
        assert modes.main(["--config", str(p), "--show"]) == 1
        assert "config.json is not valid JSON" in capsys.readouterr().err

    def test_the_docs_spellings_of_a_bot_are_accepted(self):
        assert modes.plan_changes(paper=["band_breaker"], live=["momentum_runner", "Crowd-Fader"], off=["rapid_scalper"]) == \
            {"bandbreaker": "PAPER", "runner": "LIVE", "crowd": "LIVE", "scalper": "OFF"}
        assert modes.plan_changes(live=["momentum_rider", "Rapid-Momentum-Rider"], paper=["financial_ian"]) == \
            {"rider": "LIVE", "ian": "PAPER"}


class TestTheNewLineUp:
    """9 Oct: the Rapid Momentum Rider replaces the Rapid Scalper, and
    Financial Ian joins. Both keep their own files; the scalper is retired
    and can only be switched OFF."""

    def test_the_retired_scalper_can_go_off_but_never_on(self, tmp_path, capsys):
        p = a_config(tmp_path)
        for flag in ("--live", "--paper"):
            assert modes.main(["--config", str(p), flag, "scalper"]) == 2
            err = capsys.readouterr().err
            assert "Nothing changed: the Rapid Scalper is retired" in err and "--live rider" in err
        assert not (tmp_path / "scalper.json").exists()
        with pytest.raises(ValueError, match="retired"):
            modes.set_modes(p, {"scalper": "PAPER"})
        assert modes.main(["--config", str(p), "--off", "scalper"]) == 0
        assert ScalperConfig.load(tmp_path / "scalper.json").mode == "OFF"

    def test_all_never_includes_the_retired_scalper(self):
        assert set(modes.plan_changes(off=["all"])) == {"rider", "runner", "bandbreaker", "crowd", "ian"}

    def test_the_bots_own_files_keep_everything_else(self, tmp_path):
        p = a_config(tmp_path)
        (tmp_path / "rider.json").write_text(json.dumps({"mode": "PAPER", "user_pip_value_gbp": 1.5, "max_chop": 0.5}))
        (tmp_path / "ian.json").write_text(json.dumps({"mode": "OFF", "feed": {"vendor": "none", "schema": "mbo"},
                                                       "risk_money": 8.0}))
        modes.set_modes(p, {"rider": "LIVE", "ian": "PAPER"})
        assert json.loads((tmp_path / "rider.json").read_text()) == {"mode": "LIVE", "user_pip_value_gbp": 1.5, "max_chop": 0.5}
        assert json.loads((tmp_path / "ian.json").read_text()) == {"mode": "PAPER", "feed": {"vendor": "none", "schema": "mbo"},
                                                                   "risk_money": 8.0}
        r = RiderConfig.load(tmp_path / "rider.json")
        assert r.mode == "LIVE" and r.user_pip_value_gbp == 1.5 and r.max_chop == 0.5
        assert IanConfig.load(tmp_path / "ian.json").risk_money == 8.0

    def test_the_riders_gbp_per_pip_is_set_with_its_mode_and_checked(self, tmp_path, capsys):
        p = a_config(tmp_path)
        assert modes.main(["--config", str(p), "--live", "rider", "--off", "scalper", "--gbp-per-pip", "1.0"]) == 0
        out = capsys.readouterr().out
        assert "GBP 1.00 per pip" in out and mode_of(out, "Rapid Momentum Rider") == "LIVE"
        assert json.loads((tmp_path / "rider.json").read_text()) == {"mode": "LIVE", "user_pip_value_gbp": 1.0}
        assert RiderConfig.load(tmp_path / "rider.json").user_pip_value_gbp == 1.0
        stamp = (tmp_path / "rider.json").read_text()
        for bad in ("0", "-1", "50", "nan"):
            assert modes.main(["--config", str(p), "--paper", "rider", "--gbp-per-pip", bad]) == 1, bad
            assert "Nothing changed" in capsys.readouterr().err
            assert (tmp_path / "rider.json").read_text() == stamp                  # refused as a whole
        assert modes.main(["--config", str(p), "--gbp-per-pip", "2.5"]) == 0          # the setting alone
        assert json.loads((tmp_path / "rider.json").read_text()) == {"mode": "LIVE", "user_pip_value_gbp": 2.5}

    def test_ians_feed_is_named_and_the_rest_of_the_feed_kept(self, tmp_path):
        p = a_config(tmp_path)
        (tmp_path / "ian.json").write_text(json.dumps({"mode": "PAPER", "feed": {"vendor": "none", "schema": "mbo"}}))
        modes.set_modes(p, {}, ian_feed="databento")
        assert json.loads((tmp_path / "ian.json").read_text())["feed"] == {"vendor": "databento", "schema": "mbo"}
        assert IanConfig.load(tmp_path / "ian.json").feed["vendor"] == "databento"
        with pytest.raises(ValueError, match="none, databento"):
            modes.set_modes(p, {}, ian_feed="mt5")                            # MT5 depth is retail: never offered
        assert "databento_api_key" not in (tmp_path / "ian.json").read_text()

    def test_a_magic_in_ian_json_is_ignored_as_ian_itself_ignores_it(self, tmp_path):
        """Ian's engine is hard-wired to 990911 and ignores any "magic" in
        ian.json, so the check reads the number Ian's orders really carry:
        the Rider's number written there is no clash, and changes nothing."""
        from mintel.ian import MAGIC as IAN_MAGIC
        p = a_config(tmp_path)
        (tmp_path / "ian.json").write_text(json.dumps({"mode": "PAPER", "magic": 990811}))
        assert modes.magics(p)["ian"] == IAN_MAGIC == 990911 and modes.magics(p)["rider"] == 990811
        modes.set_modes(p, {"ian": "LIVE"})
        modes.set_modes(p, {"rider": "LIVE"})
        ian = IanConfig.load(tmp_path / "ian.json")
        assert ian.mode == "LIVE" and ian.magic == IAN_MAGIC
        assert json.loads((tmp_path / "rider.json").read_text())["mode"] == "LIVE"
        assert modes._ian_magic(tmp_path) == IAN_MAGIC


class TestTheHealthLine:
    """One plain line for the one-click scripts, from the bot's own status file."""
    import datetime as _dt
    NOW = _dt.datetime(2026, 10, 9, 9, 0, tzinfo=_dt.timezone.utc)

    def _rider_status(self, tmp_path, age_s=5.0, mode="LIVE", allowed=True, markets=28):
        import datetime as dt
        st = {"mode": mode, "updated_utc": (self.NOW - dt.timedelta(seconds=age_s)).isoformat(), "user_pip_value_gbp": 1.0,
              "health": {"entries_allowed": allowed, "summary": "all checks passing" if allowed else
                         "new entries paused (open positions are still managed): the FX market is closed - no fresh prices",
                         "checks": []},
              "scanner": [{"market": f"M{i}", "direction": "NONE", "score": 0, "state": "WATCHING", "reason": "x"}
                          for i in range(markets)]}
        (tmp_path / "rider-status.json").write_text(json.dumps(st))

    def test_the_rider_line(self, tmp_path):
        p = a_config(tmp_path)
        modes.set_modes(p, {"rider": "LIVE"})
        code, line = modes.bot_health(p, "rider", self.NOW)
        assert code == 1 and line == "RAPID MOMENTUM RIDER - NOT STARTED YET, set to LIVE (no report from it yet)"
        self._rider_status(tmp_path)
        code, line = modes.bot_health(p, "rider", self.NOW)
        assert (code, line) == (0, "RAPID MOMENTUM RIDER - HEALTHY, LIVE, scanning 28 markets, GBP 1.00 per pip")
        self._rider_status(tmp_path, allowed=False)
        code, line = modes.bot_health(p, "rider", self.NOW)
        assert code == 0 and line.startswith("RAPID MOMENTUM RIDER - RUNNING, LIVE") and "FX market is closed" in line
        self._rider_status(tmp_path, age_s=600, mode="PAPER")
        code, line = modes.bot_health(p, "rider", self.NOW)
        assert code == 1 and line == "RAPID MOMENTUM RIDER - NOT RUNNING, set to LIVE (last report 10 min ago)"
        self._rider_status(tmp_path, mode="PAPER")
        assert "(running PAPER: restart it for LIVE)" in modes.bot_health(p, "rider", self.NOW)[1]
        # the report came from a copy that has since been stopped and a new one is starting: wait for it
        st = json.loads((tmp_path / "rider-status.json").read_text())
        st["pid"] = 4242
        (tmp_path / "rider-status.json").write_text(json.dumps(st))
        (tmp_path / "rider.pid").write_text("4243")
        assert modes.bot_health(p, "rider", self.NOW) == (
            1, "RAPID MOMENTUM RIDER - STARTING, set to LIVE (a new copy has started; its first report is not in yet)")
        (tmp_path / "rider.pid").write_text("4242")
        assert "(running PAPER: restart it for LIVE)" in modes.bot_health(p, "rider", self.NOW)[1]
        modes.set_modes(p, {"rider": "OFF"})
        assert modes.bot_health(p, "rider", self.NOW) == (2, "RAPID MOMENTUM RIDER - OFF (rider.json says OFF, so it is not started)")

    def test_a_rider_still_waiting_for_metatrader_says_so(self, tmp_path):
        import datetime as dt
        p = a_config(tmp_path)
        (tmp_path / "heartbeats").mkdir()
        (tmp_path / "heartbeats" / "rider.heartbeat.json").write_text(json.dumps(
            {"name": "rider", "ts_utc": (self.NOW - dt.timedelta(seconds=10)).isoformat(),
             "detail": {"waiting": "trying to reach MetaTrader", "attempt": 3}}))
        code, line = modes.bot_health(p, "rider", self.NOW)
        assert code == 1 and line == "RAPID MOMENTUM RIDER - STARTING, PAPER: trying to reach MetaTrader (attempt 3)"

    def test_the_ian_line(self, tmp_path, capsys):
        import datetime as dt
        p = a_config(tmp_path)
        st = {"mode": "PAPER", "updated": (self.NOW - dt.timedelta(seconds=3)).isoformat(),
              "feed": {"state": "NOT CONFIGURED", "reason": "no feed configured"}, "mt5": {"connected": True}}
        (tmp_path / "ian-status.json").write_text(json.dumps(st))
        code, line = modes.bot_health(p, "ian", self.NOW)
        assert code == 0
        assert line == ("FINANCIAL IAN - HEALTHY, PAPER, feed NOT CONFIGURED "
                        "(no signals and no trades until a data-feed key is saved)")
        st["feed"] = {"state": "DOWN", "reason": "login refused"}
        st["mt5"] = {"connected": False}
        (tmp_path / "ian-status.json").write_text(json.dumps(st))
        assert modes.bot_health(p, "ian", self.NOW)[1] == \
            "FINANCIAL IAN - HEALTHY, PAPER, feed DOWN (login refused), MetaTrader not connected"
        with pytest.raises(ValueError, match="rider and ian"):
            modes.bot_health(p, "crowd", self.NOW)
        assert modes.main(["--config", str(p), "--check", "crowd"]) == 1
        assert "Cannot check" in capsys.readouterr().err
