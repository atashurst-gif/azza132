"""The coach's policy file (spec section 10.2 and 10.3, step 3): canonical
JSON, the one validator (V1-V13) the writer and the gate share, and the
atomic write with its ``policy.prev.json`` rotation.

The helpers at the top are shared by the other test_coach_* files."""
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import threading
from pathlib import Path

import pytest

from mintel.coach import (F1_KEY, LEASE_SECONDS, POLICY_MAX_BYTES, UTC, Block, DemoLimits, Entry, Envelope, Floor,
                          Gates, Judgement, Paths, Pin, Policy)
from mintel.coach import envelope as envelope_mod
from mintel.coach import policy as P
from mintel.config import Config

NOW = dt.datetime(2026, 10, 13, 10, 0, tzinfo=UTC)          # a Tuesday, 11:00 UK
INDEX_KEY = "tnb|SESSION_EXPANSION|INDEX|TREND"
PRACTICE_KEY = "tnb|BREAKOUT_ACCEPTANCE|FX_MAJOR|*"


# ================================================================ helpers --
def coach_cfg(tmp_path) -> Config:
    c = Config()
    c.ops.data_dir = str(Path(tmp_path) / "data")
    Path(c.ops.data_dir).mkdir(parents=True, exist_ok=True)
    return c


def put_envelope(cfg, **over) -> str:
    """Aaron's envelope (PILOT unless said), written as the installer would; returns its sha."""
    over.setdefault("mode", "PILOT")
    env = Envelope(**over)
    path = Path(cfg.ops.data_dir) / "coach.json"
    path.write_text(json.dumps(env.to_dict(), indent=1))
    return envelope_mod.envelope_sha(json.loads(path.read_text()))


def grant(money=6.53, pct=0.35, state="LIVE_TRIAL", step="trial", rule="G1") -> Entry:
    return Entry("REAL", state, pct, money, step=step, rule=rule, why_id=1)


def make_policy(env_sha: str, *, seq: int = 1, now: dt.datetime = NOW, entries=None, equity: float = 1865.61,
                **over) -> Policy:
    if entries is None:
        entries = {INDEX_KEY: grant(), PRACTICE_KEY: Entry("PRACTICE", "BANNED_REAL", rule="S1", why_id=2)}
    kw = dict(seq=seq, written_utc=now.isoformat(), envelope_sha=env_sha, mode="PILOT", grants_allowed=True,
              entries=entries, account={"equity": equity, "e0": equity, "real_today": 0.0, "open_real_risk": 0.0,
                                        "peak": equity},
              brakes={"day": "OPEN", "week": "OPEN", "drawdown": "OPEN", "trial_budget_left_gbp": 40.0},
              data={"ok": True, "failed": [], "broker_check": {"records_today": 0.0, "page_today": 0.0}},
              headroom_gbp=50.0, gates=Gates())
    kw.update(over)
    return Policy(**kw)


def ctx_for(cfg, env_sha: str, now=NOW, last_seq: int = -1) -> P.CheckContext:
    st = envelope_mod.read(Path(cfg.ops.data_dir) / "coach.json", cfg.risk.max_risk_pct, cfg.risk.max_daily_loss_pct)
    assert st.ok, st.error
    return P.CheckContext.from_config(cfg, st.envelope, env_sha, now=now, last_seq=last_seq)


def put_policy(cfg, pol: Policy, now=NOW) -> Path:
    """Write through the coach's own writer (sealed and validated)."""
    return P.write(Paths.of(cfg.ops.data_dir), pol, ctx_for(cfg, pol.envelope_sha, now))


def put_raw(cfg, obj, *, seal: bool = True) -> Path:
    """Write any object as the policy file (a forged or broken one), sealed with its own sha256 or not."""
    if seal and isinstance(obj, dict):
        obj = dict(obj)
        obj["sha256"] = P.sha_of(obj)
    path = Paths.of(cfg.ops.data_dir).policy
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj) if not isinstance(obj, (bytes, str)) else obj)
    return path


def sealed_obj(pol: Policy) -> dict:
    return P.seal(pol).to_dict()


def codes(obj, ctx) -> set:
    return {c for c, _ in P.problems(obj, ctx)}


# ================================================================ canonical --
class TestCanonicalJson:
    def test_the_hash_is_the_canonical_json_without_the_sha_field(self, tmp_path):
        cfg = coach_cfg(tmp_path)
        sha = put_envelope(cfg)
        obj = sealed_obj(make_policy(sha))
        body = {k: v for k, v in obj.items() if k != "sha256"}
        text = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
        assert obj["sha256"] == hashlib.sha256(text.encode("ascii")).hexdigest()
        # the key order and spacing of the file make no difference
        shuffled = json.loads(json.dumps(obj, indent=3))
        assert P.sha_of(shuffled) == obj["sha256"]

    def test_nan_and_infinity_are_refused_both_ways(self, tmp_path):
        with pytest.raises(ValueError):
            P.canonical({"x": float("nan")})
        with pytest.raises(ValueError):
            P.canonical({"x": float("inf")})
        with pytest.raises(P.PolicyError) as e:
            P.parse_bytes(b'{"x": NaN}')
        assert e.value.code == "V1"
        with pytest.raises(P.PolicyError):
            P.parse_bytes(b'{"x": 1, "x": 2}')                   # a duplicate key is not one policy

    def test_a_valid_policy_round_trips(self, tmp_path):
        cfg = coach_cfg(tmp_path)
        sha = put_envelope(cfg)
        pol = make_policy(sha, floors={INDEX_KEY: Floor(min_score=76.0)},
                          blocks=(Block(("tnb",), ("NZDCHF",), "PRACTICE", "S2", None, 22),))
        path = put_policy(cfg, pol)
        back = P.load(path, ctx_for(cfg, sha))
        assert back.entries == pol.entries and back.floors == pol.floors and back.blocks == pol.blocks
        assert back.seq == 1 and back.sha256 == P.seal(pol).sha256


# ================================================================ V1 - V13 --
class TestEveryCheckRejects:
    def setup(self, tmp_path, **env):
        cfg = coach_cfg(tmp_path)
        sha = put_envelope(cfg, **env)
        return cfg, sha, ctx_for(cfg, sha)

    def bad(self, obj, ctx, code):
        obj = dict(obj)
        obj["sha256"] = P.sha_of(obj)
        found = codes(obj, ctx)
        assert code in found, (code, P.problems(obj, ctx))
        with pytest.raises(P.PolicyError):
            P.validate(obj, ctx)

    def test_the_good_one_passes(self, tmp_path):
        cfg, sha, ctx = self.setup(tmp_path)
        assert P.problems(sealed_obj(make_policy(sha)), ctx) == []

    def test_v1_not_json_or_too_large(self, tmp_path):
        cfg, sha, ctx = self.setup(tmp_path)
        for raw in (b"", b"not json", b"[1, 2]", b"{\"a\": 1"):
            with pytest.raises(P.PolicyError) as e:
                P.parse_bytes(raw)
            assert e.value.code == "V1"
        huge = sealed_obj(make_policy(sha))
        huge["build"] = "x" * (POLICY_MAX_BYTES + 10)
        huge["sha256"] = P.sha_of(huge)
        raw = json.dumps(huge).encode()
        with pytest.raises(P.PolicyError) as e:
            P.parse_bytes(raw)
        assert e.value.code == "V1"
        assert "V1" in codes(huge, ctx)

    def test_v2_schema(self, tmp_path):
        cfg, sha, ctx = self.setup(tmp_path)
        obj = sealed_obj(make_policy(sha))
        obj["schema"] = 2
        self.bad(obj, ctx, "V2")

    def test_v3_sha(self, tmp_path):
        cfg, sha, ctx = self.setup(tmp_path)
        obj = sealed_obj(make_policy(sha))
        obj["entries"][INDEX_KEY]["money_gbp"] = 6.0          # changed after sealing: forged
        assert "V3" in codes(obj, ctx)
        obj = sealed_obj(make_policy(sha))
        obj["sha256"] = "0" * 64
        assert "V3" in codes(obj, ctx)

    def test_v4_account(self, tmp_path):
        cfg, sha, _ = self.setup(tmp_path)
        cfg.account_login = 111
        ctx = ctx_for(cfg, sha)
        self.bad(sealed_obj(make_policy(sha, account_login=222)), ctx, "V4")
        assert P.problems(sealed_obj(make_policy(sha, account_login=111)), ctx) == []
        assert P.problems(sealed_obj(make_policy(sha, account_login=0)), ctx) == []     # not known: no check

    def test_v5_old_envelope(self, tmp_path):
        cfg, sha, ctx = self.setup(tmp_path)
        self.bad(sealed_obj(make_policy("f" * 64)), ctx, "V5")

    def test_v6_lease(self, tmp_path):
        cfg, sha, _ = self.setup(tmp_path)
        obj = sealed_obj(make_policy(sha))
        assert "V6" in codes(obj, ctx_for(cfg, sha, NOW + dt.timedelta(seconds=LEASE_SECONDS + 1)))
        assert "V6" in codes(obj, ctx_for(cfg, sha, NOW - dt.timedelta(seconds=121)))     # from the future
        assert codes(obj, ctx_for(cfg, sha, NOW + dt.timedelta(seconds=LEASE_SECONDS))) == set()
        self.bad(sealed_obj(make_policy(sha, valid_s=10_000)), ctx_for(cfg, sha), "V6")   # a lease too long
        self.bad(sealed_obj(make_policy(sha, valid_s=0)), ctx_for(cfg, sha), "V6")

    def test_v7_rolled_back(self, tmp_path):
        cfg, sha, _ = self.setup(tmp_path)
        obj = sealed_obj(make_policy(sha, seq=4))
        assert "V7" in codes(obj, ctx_for(cfg, sha, last_seq=5))
        assert codes(obj, ctx_for(cfg, sha, last_seq=4)) == set()

    def test_v8_routes_states_mode(self, tmp_path):
        cfg, sha, ctx = self.setup(tmp_path)
        self.bad(sealed_obj(make_policy(sha, entries={INDEX_KEY: Entry("YOLO", "LIVE")})), ctx, "V8")
        self.bad(sealed_obj(make_policy(sha, entries={INDEX_KEY: Entry("PRACTICE", "HAPPY")})), ctx, "V8")
        self.bad(sealed_obj(make_policy(sha, mode="TURBO")), ctx, "V8")
        self.bad(sealed_obj(make_policy(sha, entries={INDEX_KEY: Entry("REAL", "LIVE")})), ctx, "V8")   # no money
        blk = (Block(("tnb",), ("EURUSD",), "REAL"),)
        self.bad(sealed_obj(make_policy(sha, blocks=blk)), ctx, "V8")

    def test_v9_money(self, tmp_path):
        cfg, sha, ctx = self.setup(tmp_path)
        for pct, money in ((1.6, 10.0), (0.5, 1_000.0), (-1.0, 5.0), (0.5, -5.0)):
            e = {INDEX_KEY: Entry("REAL", "LIVE", pct, money, step="S1")}
            self.bad(sealed_obj(make_policy(sha, entries=e)), ctx, "V9")
        # the envelope's own step cap binds below the ceiling
        cfg2, sha2, ctx2 = self.setup(tmp_path / "b", max_step_pct=1.0)
        e = {INDEX_KEY: Entry("REAL", "LIVE", 1.25, 20.0, step="S3")}
        self.bad(sealed_obj(make_policy(sha2, entries=e)), ctx2, "V9")
        # a REAL route needs the equity to be checked against
        e = {INDEX_KEY: grant()}
        self.bad(sealed_obj(make_policy(sha, entries=e, account={})), ctx, "V9")
        # NaN and infinity cannot even be written canonically
        obj = sealed_obj(make_policy(sha))
        obj["entries"][INDEX_KEY]["money_gbp"] = float("nan")
        assert P.problems(obj, ctx)

    def test_v9_demo_block_within_the_ladder(self, tmp_path):
        cfg, sha, ctx = self.setup(tmp_path)
        ok = sealed_obj(make_policy(sha, demo=DemoLimits(max_daily_loss_pct=4.0, max_risk_pct=2.0)))
        assert P.problems(ok, ctx) == []
        for d in (DemoLimits(max_daily_loss_pct=4.5), DemoLimits(max_daily_loss_pct=12.0),
                  DemoLimits(max_risk_pct=6.0), DemoLimits(max_drawdown_pct=30.0), DemoLimits(max_total_risk_pct=20.0),
                  DemoLimits(max_daily_loss_pct=4.0, max_drawdown_pct=15.0)):
            self.bad(sealed_obj(make_policy(sha, demo=d)), ctx, "V9")
        # a demo step only with a demo block, and never above it
        e = {INDEX_KEY: Entry("REAL", "LIVE", 2.0, 37.31, step="S5")}
        self.bad(sealed_obj(make_policy(sha, entries=e)), ctx, "V9")
        assert P.problems(sealed_obj(make_policy(sha, entries=e, demo=DemoLimits(max_risk_pct=2.0))), ctx) == []
        self.bad(sealed_obj(make_policy(sha, entries=e, demo=DemoLimits(max_risk_pct=1.5))), ctx, "V9")

    def test_v10_grants(self, tmp_path):
        cfg, sha, ctx = self.setup(tmp_path)
        for key in (F1_KEY, "tnb|TREND_PULLBACK|FX_MINOR|TREND",       # pinned; C2 while Formula 1 is pinned
                    "tnb|SESSION_EXPANSION|SILVER|TREND",               # not an allowed kind
                    "tnb|SESSION_EXPANSION|INDEX|UNCLEAR",              # never granted
                    "tnb|*|INDEX|*",                                    # must name its approach
                    "runner|SESSION_EXPANSION|INDEX|TREND",             # no grants but Trend & Breakout's
                    "ian|*|CRYPTO|TREND", "bandbreaker|*|INDEX|*"):
            self.bad(sealed_obj(make_policy(sha, entries={key: grant()})), ctx, "V10")
        cfg2, sha2, ctx2 = self.setup(tmp_path / "b", govern=["runner", "ian"])
        self.bad(sealed_obj(make_policy(sha2, entries={INDEX_KEY: grant()})), ctx2, "V10")   # tnb not governed

    def test_v10_the_live_runner_rides_index_ideas(self, tmp_path):
        cfg, sha, _ = self.setup(tmp_path)
        cfg.runner.mode = "LIVE"
        ctx = ctx_for(cfg, sha)
        self.bad(sealed_obj(make_policy(sha)), ctx, "V10")
        # an approach the Runner never rides is not its idea
        e = {"tnb|MOMENTUM_CONTINUATION|INDEX|TREND": grant()}
        assert P.problems(sealed_obj(make_policy(sha, entries=e)), ctx) == []

    def test_v10_after_formula1_is_judged(self, tmp_path):
        cfg, sha, ctx = self.setup(tmp_path)
        minor = {"tnb|TREND_PULLBACK|FX_MINOR|TREND": grant()}
        judged = {F1_KEY: Judgement("KEEP", 20, (NOW - dt.timedelta(days=1)).isoformat())}
        assert P.problems(sealed_obj(make_policy(sha, entries=minor, judged=judged)), ctx) == []
        early = {F1_KEY: Judgement("KEEP", 12, NOW.isoformat())}                     # before 20 live trades
        self.bad(sealed_obj(make_policy(sha, entries=minor, judged=early)), ctx, "V10")
        self.bad(sealed_obj(make_policy(sha, judged={"tnb|X|INDEX|*": Judgement("KEEP", 25, NOW.isoformat())})),
                 ctx, "V10")                                                          # not one of Aaron's pins

    def test_v11_trials(self, tmp_path):
        cfg, sha, ctx = self.setup(tmp_path)
        three = {f"tnb|{a}|INDEX|TREND": grant() for a in ("SESSION_EXPANSION", "TREND_PULLBACK",
                                                             "BREAKOUT_ACCEPTANCE")}
        self.bad(sealed_obj(make_policy(sha, entries=three)), ctx, "V11")
        self.bad(sealed_obj(make_policy(sha, entries={INDEX_KEY: grant(money=9.0)})), ctx, "V11")   # over £8
        self.bad(sealed_obj(make_policy(sha, entries={INDEX_KEY: grant(pct=0.5)})), ctx, "V11")

    def test_v12_brakes_and_headroom(self, tmp_path):
        cfg, sha, ctx = self.setup(tmp_path)
        self.bad(sealed_obj(make_policy(sha, headroom_gbp=-1.0)), ctx, "V12")
        b = {"day": "OPEN", "week": "OPEN", "drawdown": "OPEN", "trial_budget_left_gbp": -5.0}
        self.bad(sealed_obj(make_policy(sha, brakes=b)), ctx, "V12")
        obj = sealed_obj(make_policy(sha))
        obj["headroom_gbp"] = 1e400                                       # infinity, as JSON writes it
        assert P.problems(obj, ctx)

    def test_v13_unknown_fields_anywhere(self, tmp_path):
        cfg, sha, ctx = self.setup(tmp_path)
        base = sealed_obj(make_policy(sha))
        for where in ("top", "entry", "data", "account", "brakes", "gates"):
            obj = json.loads(json.dumps(base))
            target = {"top": obj, "entry": obj["entries"][INDEX_KEY], "data": obj["data"],
                      "account": obj["account"], "brakes": obj["brakes"], "gates": obj["gates"]}[where]
            target["sneaky"] = 1
            self.bad(obj, ctx, "V13")
        for bad_key in ("tnb|A|B", "rider|*|*|*", "lab", "tnb|lower|INDEX|*"):
            obj = json.loads(json.dumps(base))
            obj["entries"][bad_key] = {"route": "PRACTICE", "state": "PRACTICE"}
            self.bad(obj, ctx, "V13")
        obj = json.loads(json.dumps(base))
        obj["grants_allowed"] = "yes"
        self.bad(obj, ctx, "V13")
        obj = json.loads(json.dumps(base))
        del obj["seq"]
        self.bad(obj, ctx, "V13")

    def test_a_crashing_check_is_a_failed_check(self, tmp_path):
        cfg, sha, ctx = self.setup(tmp_path)
        assert P.problems(["not", "a", "dict"], ctx)
        assert P.problems({"entries": None}, ctx)


# ================================================================ the writer --
class TestTheWriter:
    def test_writer_and_reader_share_one_validator(self, tmp_path, monkeypatch):
        """The writer refuses what the gate would refuse (nothing on disk changes), through the same function."""
        cfg = coach_cfg(tmp_path)
        sha = put_envelope(cfg)
        put_policy(cfg, make_policy(sha, seq=1))
        before = Paths.of(cfg.ops.data_dir).policy.read_bytes()
        with pytest.raises(P.PolicyError) as e:
            put_policy(cfg, make_policy(sha, seq=2, entries={F1_KEY: grant()}))         # a pinned grant
        assert e.value.code == "V10"
        assert Paths.of(cfg.ops.data_dir).policy.read_bytes() == before
        calls = []
        real = P.problems

        def spy(obj, ctx, **kw):
            calls.append(obj.get("seq"))
            return real(obj, ctx, **kw)
        monkeypatch.setattr(P, "problems", spy)
        put_policy(cfg, make_policy(sha, seq=3))
        P.load(Paths.of(cfg.ops.data_dir).policy, ctx_for(cfg, sha))
        assert calls == [3, 3]                                     # once writing, once reading: the same checks

    def test_the_writer_seals_whatever_it_is_given(self, tmp_path):
        cfg = coach_cfg(tmp_path)
        sha = put_envelope(cfg)
        put_policy(cfg, dataclasses.replace(make_policy(sha), sha256="0" * 64))
        assert P.load(Paths.of(cfg.ops.data_dir).policy, ctx_for(cfg, sha)).sha256 != "0" * 64

    def test_prev_rotation(self, tmp_path):
        cfg = coach_cfg(tmp_path)
        sha = put_envelope(cfg)
        paths = Paths.of(cfg.ops.data_dir)
        put_policy(cfg, make_policy(sha, seq=1))
        assert not paths.policy_prev.exists()
        first = paths.policy.read_bytes()
        put_policy(cfg, make_policy(sha, seq=2))
        assert paths.policy_prev.read_bytes() == first
        assert json.loads(paths.policy.read_text())["seq"] == 2
        put_policy(cfg, make_policy(sha, seq=3))
        assert json.loads(paths.policy_prev.read_text())["seq"] == 2
        assert not paths.policy_tmp.exists()

    def test_a_reader_alongside_sees_the_old_file_or_the_new_never_a_mix(self, tmp_path):
        cfg = coach_cfg(tmp_path)
        sha = put_envelope(cfg)
        paths = Paths.of(cfg.ops.data_dir)
        put_policy(cfg, make_policy(sha, seq=1))
        ctx = ctx_for(cfg, sha)
        stop = threading.Event()
        seen, bad = [], []

        def reader():
            while not stop.is_set():
                raw = P.read_bytes(paths.policy)
                try:
                    obj = P.parse_bytes(raw)
                    found = P.problems(obj, ctx)
                    if found:
                        bad.append(found)
                    seen.append(obj["seq"])
                except Exception as exc:                            # a torn read would land here
                    bad.append(str(exc))
        t = threading.Thread(target=reader)
        t.start()
        try:
            for seq in range(2, 80):
                big = {f"tnb|SESSION_EXPANSION|INDEX|{c}": Entry("PRACTICE", "PRACTICE", rule="S1", why="x" * 300)
                       for c in ("TREND", "RANGE", "SQUEEZE", "HIGH_VOL", "LOW_VOL", "NEWS")}
                put_policy(cfg, make_policy(sha, seq=seq, entries=big))
        finally:
            stop.set()
            t.join()
        assert not bad and seen and seen == sorted(seen)

    def test_over_256_kb_is_refused_at_the_writer(self, tmp_path):
        cfg = coach_cfg(tmp_path)
        sha = put_envelope(cfg)
        entries = {f"tnb|A{i}|INDEX|TREND": Entry("PRACTICE", "PRACTICE", why="y" * 400) for i in range(800)}
        with pytest.raises(P.PolicyError) as e:
            put_policy(cfg, make_policy(sha, entries=entries))
        assert e.value.code == "V1"
        assert not Paths.of(cfg.ops.data_dir).policy.exists()


# ================================================================ the envelope --
class TestTheEnvelope:
    def read(self, cfg, obj):
        p = Path(cfg.ops.data_dir) / "coach.json"
        p.write_text(json.dumps(obj) if not isinstance(obj, str) else obj)
        return envelope_mod.read(p, cfg.risk.max_risk_pct, cfg.risk.max_daily_loss_pct)

    def test_the_installer_example_is_valid(self, tmp_path):
        cfg = coach_cfg(tmp_path)
        st = self.read(cfg, Envelope(mode="PILOT").to_dict())
        assert st.ok and st.envelope.mode == "PILOT" and st.envelope.pins[0].key == F1_KEY
        assert st.envelope.pins[0].judge_after == 20

    def test_the_spec_example_pin_is_released_only_by_formula1s_judgement(self, tmp_path):
        cfg = coach_cfg(tmp_path)
        raw = Envelope(mode="PILOT").to_dict()
        raw["pins"] = [{"key": F1_KEY, "why": "Formula 1: flat £13 until 20 live trades, then judged"}]
        st = self.read(cfg, raw)
        assert st.ok and st.envelope.pins[0].judge_after == 20
        assert P.active_pins(st.envelope, {F1_KEY: Judgement("KEEP", 19, NOW.isoformat())})
        assert not P.active_pins(st.envelope, {F1_KEY: Judgement("KEEP", 20, NOW.isoformat())})
        raw["pins"] = [{"key": "tnb|SESSION_EXPANSION|INDEX|*", "why": "Aaron's"}]
        st = self.read(cfg, raw)
        assert P.active_pins(st.envelope, {"tnb|SESSION_EXPANSION|INDEX|*": Judgement("KEEP", 99, NOW.isoformat())})

    def test_invalid_means_off(self, tmp_path):
        cfg = coach_cfg(tmp_path)
        good = Envelope(mode="PILOT").to_dict()
        for change in ({"surprise": 1}, {"max_step_pct": 1.6}, {"day_pause_pct": 3.0}, {"day_line_pct": 2.25},
                       {"govern": ["tnb", "rider"]}, {"govern": ["lab"]}, {"trial_max_gbp": -1},
                       {"week_line_pct": float("inf")}, {"mode": "AUTO"}, {"schema": 2},
                       {"demo_max_step_pct": 6.0}, {"demo_max_daily_stop_pct": 11.0},
                       {"pins": [{"key": "tnb|nope"}]}, {"tnb_real_kinds": ["MOON"]}):
            obj = dict(good, **change)
            raw = json.dumps(obj) if all(not isinstance(v, float) or v == v and v != float("inf")
                                         for v in change.values()) else json.dumps(obj).replace("Infinity", "1e999")
            st = self.read(cfg, raw)
            assert not st.ok and st.mode == "OFF", change
        assert self.read(cfg, "{not json").mode == "OFF"
        assert envelope_mod.read(Path(cfg.ops.data_dir) / "nothing.json", 1.5, 3.0).mode == "OFF"

    def test_the_watch_day(self):
        env = Envelope(mode="PILOT", watch_until_utc="2026-10-13T21:00:00+00:00")
        assert envelope_mod.effective_mode(env, NOW)[0] == "WATCH"
        # after it, PILOT only from the next 06:30 UK release (Wednesday 05:30 UTC in BST)
        assert envelope_mod.effective_mode(env, dt.datetime(2026, 10, 14, 5, 0, tzinfo=UTC))[0] == "WATCH"
        assert envelope_mod.effective_mode(env, dt.datetime(2026, 10, 14, 5, 31, tzinfo=UTC))[0] == "PILOT"
        assert envelope_mod.effective_mode(Envelope(mode="GUARD"), NOW)[0] == "GUARD"
        assert envelope_mod.effective_mode(None, NOW)[0] == "OFF"
