"""Build the feed named in data/ian.json, never raising.

    "feed": {"vendor": "none"}                                   -> NOT CONFIGURED (the default)
    "feed": {"vendor": "databento", "schema": "mbp-10", "trades": true}
    "feed": {"vendor": "replay", "path": "data/ian/recordings/2026-10-08", "speed": "realtime"}
    "feed": {"vendor": "synthetic", "scenario": "trend_up"}       -> test data only; never trades
    "feed": {"vendor": "mt5"}                                    -> refused: MT5 depth is RETAIL, not the institutional book
"""
from __future__ import annotations

from pathlib import Path

from .base import FeedAdapter, NotConfiguredFeed


def build_feed(feed_cfg: dict, data_dir: str | Path, instruments=()) -> FeedAdapter:
    cfg = dict(feed_cfg or {})
    vendor = str(cfg.get("vendor") or "none").strip().lower()
    try:
        if vendor in ("", "none", "off"):
            return NotConfiguredFeed()
        if vendor == "databento":
            from .databento import DatabentoFeed
            return DatabentoFeed(data_dir, schema=str(cfg.get("schema") or "mbp-10"),
                                 dataset=str(cfg.get("dataset") or "GLBX.MDP3"), trades=bool(cfg.get("trades", True)),
                                 stype_in=str(cfg.get("stype_in") or "raw_symbol"))
        if vendor == "replay":
            from .replay import ReplayFeed
            path = cfg.get("path") or str(Path(data_dir) / "ian" / "recordings")
            return ReplayFeed(path, cfg.get("speed") or "asap")
        if vendor == "synthetic":
            from .synthetic import SyntheticFeed
            return SyntheticFeed(str(cfg.get("scenario") or "balanced"),
                                 instruments=list(cfg.get("instruments") or ["6EZ6"]), seed=int(cfg.get("seed") or 7))
        if vendor in ("mt5", "metatrader", "retail"):
            return NotConfiguredFeed("MT5 depth is RETAIL broker data, not the centralised futures book; it is never "
                                     "used as the institutional feed", vendor)
        return NotConfiguredFeed(f"unknown feed vendor '{vendor}' (supported: databento, replay, synthetic)", vendor)
    except Exception as exc:
        return NotConfiguredFeed(f"the {vendor} feed could not be set up: {exc}", vendor)
