"""What the coach can never do (spec sections 1, 3, 9, 14, 18 and 21).

Source checks over ``mintel/coach/``: no order code, no broker object, no
forbidden import, no secrets file, never an assignment to Aaron's risk limits
or his mode; the writers of the envelope, HALT, COACH-OFF and the approvals
live only in the CLI. Runtime checks over a simulated week of ticks and
nightlies: nothing is written outside ``data/coach/`` (config.json, the
envelope and the approvals byte for byte unchanged), the secrets file is
never opened, and there is no heartbeat.

EVERY FIGURE IN THIS FILE IS MADE UP for the test: never a result.
"""
from __future__ import annotations

import ast
import builtins
import datetime as dt
import hashlib
import io
import os
import re
import sqlite3
from pathlib import Path

import pytest

from mintel.coach import UTC, Paths
from mintel.coach.run import Coach
from tests.coach_d_world import FakePage, data_folder, journal_row, write_rows, GOOD_KEY

COACH = Path(__file__).resolve().parents[1] / "mintel" / "coach"
SOURCES = sorted(COACH.glob("*.py"))
D_FILES = ("decide.py", "brakes.py", "ledger.py", "weather.py", "words.py", "feed.py", "run.py", "status.py",
           "report.py", "__main__.py")


def src(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def code_only(text: str) -> str:
    """The source without comments and docstrings (a docstring may name what the coach never does)."""
    tree = ast.parse(text)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)):
            body = getattr(node, "body", [])
            if body and isinstance(body[0], ast.Expr) and isinstance(getattr(body[0], "value", None), ast.Constant) \
                    and isinstance(body[0].value.value, str):
                body[0].value.value = ""
    out = ast.unparse(tree)
    return out


def test_every_builder_d_file_is_there():
    names = {p.name for p in SOURCES}
    for n in D_FILES:
        assert n in names, n


# ================================================================ no order code --
@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_no_order_code(path):
    code = code_only(src(path))
    for word in ("order_send", "send(", "modify_stops", "close_position", "place_order", "positions("):
        assert word not in code, (path.name, word)
    # every close( is a file or a database handle, never a position
    for m in re.finditer(r"([\w\.]*)close\(", code):
        defined = m.group(1) == "" and code[max(0, m.start() - 4):m.start()] == "def "
        assert defined or m.group(1) in ("os.", "self.con.", "self.ledger.", "led.", "c."), (path.name, m.group(0))


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_no_broker_object(path):
    code = code_only(src(path))
    found = re.findall(r"\w*Broker\(", code)
    if path.name == "records.py":                     # the paper book, read only (section 3)
        assert found == ["PaperBroker("] and "read_only=True" in code
    else:
        assert found == [], path.name
    assert "self.broker" not in code and "build_broker" not in code


FORBIDDEN = ("mintel.broker", "mintel.engine.execution", "mintel.engine.trader", "mintel.ops.modes", "mintel.lab")


def _imports(path: Path) -> list[str]:
    tree = ast.parse(src(path))
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                base = node.module or ""
            else:
                parts = ["mintel", "coach"][: 3 - node.level] if node.level <= 2 else []
                base = ".".join(parts + ([node.module] if node.module else []))
            out.append(base)
            out += [f"{base}.{a.name}" for a in node.names]
    return out


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_no_forbidden_imports(path):
    for name in _imports(path):
        for bad in FORBIDDEN:
            if name == bad or name.startswith(bad + "."):
                assert path.name == "records.py" and name.startswith("mintel.broker.paper"), (path.name, name)
        if name.startswith("mintel.ops.report_upload"):
            assert path.name in ("report.py", "feed.py"), (path.name, name)
            if path.name == "report.py":
                assert name in ("mintel.ops.report_upload", "mintel.ops.report_upload.upload")
            else:
                assert name in ("mintel.ops.report_upload", "mintel.ops.report_upload.status_json")


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_no_secrets_file(path):
    text = src(path)
    if path.name == "__init__.py":                    # the one constant that names it, so nothing else needs to
        assert code_only(text).count("secrets.json") == 1 and 'SECRETS_NAME = "secrets.json"' in text
        return
    assert "secrets.json" not in text, path.name
    assert "SECRETS_NAME" not in text and "read_token" not in text and "github_token" not in text, path.name
    assert "Config.load(" not in code_only(text), path.name


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_never_assigns_aarons_limits_or_his_mode(path):
    code = code_only(src(path))
    assert not re.search(r"\.(max_daily_loss_pct|max_risk_pct|daily_loss_counts_practice|top_size_enabled)\s*=(?!=)",
                         code), path.name
    assert not re.search(r"\bcfg\.mode\s*=(?!=)|\bconfig\.mode\s*=(?!=)", code), path.name
    assert "config.json" not in code or path.name in ("records.py", "gate.py") or \
        not re.search(r"config\.json['\"]\)?\.(write|open)", code), path.name


def test_the_writers_of_aarons_files_live_only_in_the_cli():
    pat = re.compile(r"paths\.(envelope|halt|coach_off|approvals)\b[^\n]*\.(write_text|write_bytes|unlink)|"
                     r"_atomic_json\(\s*paths\.(envelope|approvals)")
    for p in SOURCES:
        hits = pat.findall(code_only(src(p)))
        if p.name == "__main__.py":
            assert hits
        else:
            assert not hits, p.name
    run_src = code_only(src(COACH / "run.py"))
    for name in ("set_mode", "add_approval", "set_halt", "switch_off", "set_pin_formula1", "__main__"):
        assert name not in run_src


def test_no_heartbeat_in_the_source():
    for p in SOURCES:
        code = code_only(src(p))
        assert "heartbeats" not in code and "heartbeat(" not in code, p.name


# ================================================================ runtime --
def _snapshot(data: Path) -> dict:
    """Every file outside data/coach/ and its hash. SQLite's own -wal and -shm sidecars are left out: opening
    a WAL database read only makes them (on the Mac the bots hold them open anyway); the databases themselves
    are compared byte for byte."""
    out = {}
    for p in data.rglob("*"):
        if p.name.endswith(("-wal", "-shm")):
            continue
        if p.is_file() and Paths.of(data).coach_dir not in p.parents:
            out[str(p.relative_to(data))] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


def test_a_simulated_week_writes_only_under_data_coach_and_never_opens_the_secrets_file(tmp_path, monkeypatch):
    data, c, _ = data_folder(tmp_path)
    (data / "secrets.json").write_text('{"github_token": "MADE-UP"}')
    (data / "coach").mkdir(exist_ok=True)
    Paths.of(data).approvals.write_text('[{"id": "1", "action": "veto", "plan": "", "by": "aaron", "ts": ""}]')
    write_rows(data, [journal_row(GOOD_KEY, 0.4, dt.datetime(2026, 11, 9, 9, 0, tzinfo=UTC))])
    keep = {name: (data / name).read_bytes() for name in ("config.json", "coach.json", "secrets.json")}
    approvals = Paths.of(data).approvals.read_bytes()
    before = _snapshot(data)
    opened: list[str] = []
    real_open, real_connect, real_os_open = builtins.open, sqlite3.connect, os.open

    def spy_open(file, *a, **k):
        opened.append(str(file))
        return real_open(file, *a, **k)

    def spy_connect(db, *a, **k):
        opened.append(str(db))
        return real_connect(db, *a, **k)

    def spy_os_open(path, *a, **k):
        opened.append(str(path))
        return real_os_open(path, *a, **k)
    monkeypatch.setattr(builtins, "open", spy_open)
    monkeypatch.setattr(io, "open", spy_open)
    monkeypatch.setattr(sqlite3, "connect", spy_connect)
    monkeypatch.setattr(os, "open", spy_os_open)
    box = {"now": dt.datetime(2026, 11, 9, 7, 0, tzinfo=UTC)}
    co = Coach(data, cfg=c, clock=lambda: box["now"], fetch=FakePage(), shuffles=2)
    end = box["now"] + dt.timedelta(days=7)
    ticks = 0
    while box["now"] < end:
        assert co.tick()["ok"]
        ticks += 1
        box["now"] += dt.timedelta(hours=1)
    co.close()
    monkeypatch.undo()
    assert ticks == 168
    assert not [p for p in opened if "secrets" in p]
    for name, body in keep.items():
        assert (data / name).read_bytes() == body, name               # config.json byte for byte, the envelope too
    assert Paths.of(data).approvals.read_bytes() == approvals
    assert _snapshot(data) == before                                   # nothing written outside data/coach/
    assert not (data / "heartbeats").exists()
    assert len(list(Paths.of(data).coach_dir.glob("plan-*.json"))) == 7     # one nightly a night
