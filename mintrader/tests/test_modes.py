"""python -m mintel.ops.modes: switching the bots between OFF, PAPER and
LIVE edits only the mode keys, atomically, and the bots read the result."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from mintel.config import Config
from mintel.ops import modes
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
        assert out == {"runner": "LIVE", "bandbreaker": "LIVE", "crowd": "LIVE", "scalper": "LIVE"}
        cfg = Config.load(p)
        assert cfg.runner.mode == "LIVE" and cfg.bandbreaker.mode == "LIVE" and cfg.crowd.mode == "LIVE"
        assert cfg.runner.magic == 990_511 and cfg.bandbreaker.magic == 990_611 and cfg.crowd.magic == 990_711
        s = ScalperConfig.load(tmp_path / "scalper.json")
        assert s.mode == "LIVE" and s.is_live and s.entry_hours_utc == (0,)

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

    def test_a_missing_block_or_scalper_file_is_created(self, tmp_path):
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"mode": "DEMO", "risk": {"base_risk_pct": 0.5}}))
        out = modes.set_modes(p, {"bandbreaker": "LIVE", "scalper": "LIVE"})
        assert out["bandbreaker"] == "LIVE" and out["scalper"] == "LIVE"
        assert out["runner"] == "PAPER" and out["crowd"] == "PAPER"          # untouched: the default
        raw = json.loads(p.read_text())
        assert raw["bandbreaker"] == {"mode": "LIVE"} and raw["risk"] == {"base_risk_pct": 0.5} and raw["mode"] == "DEMO"
        assert json.loads((tmp_path / "scalper.json").read_text()) == {"mode": "LIVE"}
        assert Config.load(p).bandbreaker.mode == "LIVE" and Config.load(p).runner.mode == "PAPER"
        assert ScalperConfig.load(tmp_path / "scalper.json").mode == "LIVE"

    def test_the_write_is_atomic_and_leaves_no_temporary_file(self, tmp_path):
        p = a_config(tmp_path)
        modes.set_modes(p, {"runner": "LIVE", "scalper": "PAPER"})
        assert not (tmp_path / "config.json.tmp").exists() and not (tmp_path / "scalper.json.tmp").exists()
        assert json.loads(p.read_text())["runner"]["mode"] == "LIVE"

    def test_back_to_paper_and_off(self, tmp_path):
        p = a_config(tmp_path)
        modes.set_modes(p, modes.plan_changes(live=["all"]))
        out = modes.set_modes(p, modes.plan_changes(paper=["runner", "scalper"], off=["crowd"]))
        assert out == {"runner": "PAPER", "bandbreaker": "LIVE", "crowd": "OFF", "scalper": "PAPER"}
        assert Config.load(p).crowd.mode == "OFF" and ScalperConfig.load(tmp_path / "scalper.json").mode == "PAPER"

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
            modes.plan_changes(live=["trend"])
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
        for name in ("Momentum Runner", "Band Breaker", "Crowd Fader", "Rapid Scalper"):
            assert mode_of(out, name) == "LIVE"
        assert "Restart the bot for this to take effect (Stop Trading Bot, then Start Trading Bot)" in out
        assert Config.load(p).runner.mode == "LIVE" and ScalperConfig.load(tmp_path / "scalper.json").mode == "LIVE"

    def test_show_changes_nothing(self, tmp_path, capsys):
        p = a_config(tmp_path)
        stamp = p.read_text()
        assert modes.main(["--config", str(p), "--show"]) == 0
        out = capsys.readouterr().out
        assert mode_of(out, "Momentum Runner") == "PAPER" and mode_of(out, "Rapid Scalper") == "PAPER"
        assert "Restart the bot" not in out and p.read_text() == stamp
        assert not (tmp_path / "scalper.json").exists()

    def test_an_unknown_bot_on_the_command_line_is_refused(self, tmp_path, capsys):
        p = a_config(tmp_path)
        stamp = p.read_text()
        assert modes.main(["--config", str(p), "--live", "runner", "trend"]) == 2
        assert "unknown bot 'trend'" in capsys.readouterr().err
        assert p.read_text() == stamp                                        # refused as a whole: nothing written

    def test_runnable_as_a_module(self, tmp_path):
        import subprocess, sys
        p = a_config(tmp_path)
        r = subprocess.run([sys.executable, "-m", "mintel.ops.modes", "--config", str(p), "--paper", "crowd", "--live", "scalper"],
                           capture_output=True, text=True, cwd=str(Path(__file__).resolve().parents[1]), timeout=60)
        assert r.returncode == 0, r.stderr
        assert mode_of(r.stdout, "Crowd Fader") == "PAPER" and mode_of(r.stdout, "Rapid Scalper") == "LIVE"
        assert "Restart the bot" in r.stdout
        assert ScalperConfig.load(tmp_path / "scalper.json").mode == "LIVE"


class TestEveryFileIsCheckedBeforeAnyIsWritten:
    """Review, 8 Oct: the tool read config.json, wrote it, and only then read
    scalper.json, so a broken scalper.json left three bots switched while the
    window said 'Nothing changed'; and a scalper-only switch wrote scalper.json
    next to whatever path was typed, never where the scalper reads it."""

    def test_a_broken_scalper_file_means_nothing_is_written(self, tmp_path):
        p = a_config(tmp_path)
        (tmp_path / "scalper.json").write_text("{not json")
        with pytest.raises(ValueError, match="scalper.json"):
            modes.set_modes(p, modes.plan_changes(live=["all"]))
        assert json.loads(p.read_text())["runner"]["mode"] == "PAPER"       # config.json untouched
        assert (tmp_path / "scalper.json").read_text() == "{not json"

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
        out = modes.set_modes(p, {"scalper": "LIVE"})
        assert out["scalper"] == "LIVE"
        assert json.loads((data / "scalper.json").read_text()) == {"mode": "LIVE"}
        assert not (tmp_path / "scalper.json").exists()                     # not next to config.json
        assert modes.read_modes(p)["scalper"] == "LIVE"
        assert ScalperConfig.load(ScalperConfig.default_path(cfg.ops.data_dir)).mode == "LIVE"   # as the scalper reads it
        assert modes.main(["--config", str(p), "--show"]) == 0
        assert f"{data / 'scalper.json'}: mode)" in capsys.readouterr().out       # the window shows the file

    def test_a_config_naming_no_data_folder_uses_its_own(self, tmp_path):
        p = tmp_path / "config.json"
        p.write_text(json.dumps({"mode": "DEMO"}))
        assert modes.scalper_path(p) == tmp_path / "scalper.json"

    def test_a_write_that_stops_part_way_says_which_file_holds_the_change(self, tmp_path, monkeypatch, capsys):
        p = a_config(tmp_path)
        real = modes._write_atomic
        def flaky(path, obj):
            if path.name == "scalper.json":
                raise OSError("disk full")
            real(path, obj)
        monkeypatch.setattr(modes, "_write_atomic", flaky)
        with pytest.raises(modes.PartialWrite, match="config.json.*already holds the change.*scalper.json.*does not"):
            modes.set_modes(p, {"runner": "LIVE", "scalper": "LIVE"})
        assert Config.load(p).runner.mode == "LIVE"                           # true: it was written
        assert modes.main(["--config", str(p), "--paper", "runner", "--live", "scalper"]) == 1
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
        assert modes.magics(p) == {"trend": 990_311, "scalper": 990_412, "runner": 990_511,
                                   "bandbreaker": 990_611, "crowd": 990_711}
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
        with pytest.raises(ValueError, match="scalper's magic 990511 is Momentum Runner's"):
            modes.set_modes(p, {"scalper": "LIVE"})
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
        assert "magic 990411; " in [l for l in out.splitlines() if l.startswith("Rapid Scalper:")][0]


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
