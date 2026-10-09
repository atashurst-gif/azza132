"""Build the feed named in data/ian.json, never raising.

    "feed": {"vendor": "none"}                                   -> NOT CONFIGURED (the default)
    "feed": {"vendor": "databento", "schema": "mbp-10", "trades": true}
    "feed": {"vendor": "binance"}                                -> Binance's PUBLIC crypto book (no key): BTCUSDT and
                                                                    ETHUSDT, traded through the broker's crypto CFDs
    "feed": {"vendor": "binance", "transport": "rest"}           -> REST polling only (DEGRADED: no new trades)
    "feed": {"vendor": "replay", "path": "data/ian/recordings/2026-10-08", "speed": "realtime"}
    "feed": {"vendor": "synthetic", "scenario": "trend_up"}       -> test data only; never trades
    "feed": {"vendor": "mt5"}                                    -> refused: MT5 depth is RETAIL, not the institutional book
"""
from __future__ import annotations

from pathlib import Path

from .base import FeedAdapter, NotConfiguredFeed


def build_feed(feed_cfg: dict, data_dir: str | Path, instruments=(), crypto: dict | None = None) -> FeedAdapter:
    """``crypto``: the generic instrument definitions (symbol -> CryptoInstrument) for a Binance feed."""
    cfg = dict(feed_cfg or {})
    vendor = str(cfg.get("vendor") or "none").strip().lower()
    try:
        if vendor in ("", "none", "off"):
            return NotConfiguredFeed()
        if vendor == "binance":
            from .binance import DEPTH_LIMIT, BinanceFeed
            return BinanceFeed(data_dir, instruments=dict(crypto or {}),
                               transport=str(cfg.get("transport") or "auto"),
                               depth_limit=int(cfg.get("depth_limit") or DEPTH_LIMIT),
                               rest_poll_s=float(cfg.get("rest_poll_s") or 1.0),
                               allow_rest_entries=bool(cfg.get("allow_rest_entries", False)))
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
        return NotConfiguredFeed(f"unknown feed vendor '{vendor}' (supported: databento, binance, replay, synthetic)",
                                 vendor)
    except Exception as exc:
        return NotConfiguredFeed(f"the {vendor} feed could not be set up: {exc}", vendor)
