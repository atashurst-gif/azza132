"""Configuration.

Design rules:

* Risk boundaries are owned by the operator.  ``base_risk_pct`` and
  ``max_risk_pct`` are the only two numbers that decide how large a trade can
  be, and nothing in the codebase may exceed ``max_risk_pct``.
* ``live`` mode requires an explicit, persistent, deliberate marker.  A corrupt
  or missing config can only ever fall back to DEMO, never to LIVE.
* Secrets never live in source control.  They are read from the config file in
  the data directory (created by the setup command) or from the environment.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import math
import os
from dataclasses import dataclass, field, asdict, fields
from pathlib import Path
from typing import Any, Optional

CONFIG_VERSION = 1
LIVE_MARKER = "I-UNDERSTAND-THIS-TRADES-REAL-MONEY"

log = logging.getLogger("mintel.config")


@dataclass
class RiskConfig:
    # --- operator-owned boundaries -------------------------------------------
    base_risk_pct: float = 0.50          # risk at NORMAL confidence
    max_risk_pct: float = 1.50           # hard ceiling; never exceeded
    # A cap in MONEY (account currency) on what any one trade may lose at its
    # stop, on top of the percentages. 0 = no cap. 7 Oct, Aaron: "stick with
    # your advice" - every trade risks at most 10 GBP until two weeks of
    # results can be trusted (5-6 Oct STRONG setups were sized at 13-18).
    # Smaller size, same stop: the chart decides where the idea is wrong.
    max_risk_money: float = 10.0
    min_risk_pct: float = 0.10
    # --- top-opportunity size (version 5, 9 Oct) -----------------------------
    # Aaron, 8 Oct: "increase their pip size to 0.5 maximum for trades showing
    # the highest opportunity", read as up to 0.5 LOTS. A trade in a segment
    # that has EARNED it (top_segments: an approach in one kind of market,
    # as contracts.infer_group names it) risks up to top_risk_money instead
    # of the 10 above, and on FX and metals carries at most top_max_lots
    # (an index lot is a different thing, so indices get the money cap
    # alone). Still under max_risk_pct of the account (1.5% = 30 on 2,000,
    # which binds before 40), and the daily loss stop, the correlated and
    # total exposure caps, the margin checks and every breaker apply as to
    # any trade. The raw score plays no part: 82+ scores earned the least.
    # Evidence (docs/strategies/v2-commission-first.md, version 5): momentum
    # continuation on FX minor pairs, in the conditions it still trades
    # (fast and news markets), 38 trades over 10 days, 8 days up and 2 down,
    # +156.31 after 29.89 commission (+4.11 a trade, +0.82 R).
    # Before every such trade the segment's own newest top_lookback_trades
    # closed trades (journal; markets and conditions still on) must show at
    # least top_min_trades over top_min_days days, positive after costs per
    # trade and in R, and more days up than down - or it is sized as usual.
    top_size_enabled: bool = True
    top_risk_money: float = 40.0
    top_max_lots: float = 0.5
    top_segments: tuple[tuple[str, str], ...] = (("MOMENTUM_CONTINUATION", "FX_MINOR"),)
    top_min_trades: int = 20
    top_min_days: int = 4
    top_lookback_trades: int = 40
    # --- portfolio limits ----------------------------------------------------
    max_total_risk_pct: float = 4.00     # sum of open risk
    max_correlated_risk_pct: float = 2.00
    max_open_positions: int = 8
    max_positions_per_symbol: int = 1
    max_daily_loss_pct: float = 3.00
    max_drawdown_pct: float = 12.00
    min_margin_level_pct: float = 300.0
    # --- software-failure breakers -------------------------------------------
    max_rejects_per_10min: int = 6
    max_spread_multiple: float = 3.0     # vs typical spread
    # Round-trip commission per 1.0 lot in account currency. 0 means "learn it
    # from the broker's own deal records" (what IC Markets etc. actually
    # charged on this account); set it to override. Counted in every
    # reward:risk test, because on a 3-pip scalp it is most of the cost.
    commission_per_lot: float = 0.0
    # Until the bot has seen a market's commission in its own deals, it must
    # not assume zero. Measured at IC Markets Raw (GBP) from the broker's
    # deals, 1-5 Oct: about 5.50 a lot round turn on FX, gold and silver;
    # nothing on indices. A fresh account starts from these, then learns.
    commission_fallback_per_lot: float = 6.00
    max_orders_per_hour: int = 30
    # --- Kelly (conservative, capped) ---------------------------------------
    use_fractional_kelly: bool = True
    kelly_fraction: float = 0.25
    kelly_min_samples: int = 60

    def validate(self) -> list[str]:
        errs = []
        if self.base_risk_pct <= 0:
            errs.append("base_risk_pct must be > 0")
        if self.max_risk_pct < self.base_risk_pct:
            errs.append("max_risk_pct must be >= base_risk_pct")
        if self.max_risk_pct > 10.0:
            errs.append("max_risk_pct > 10% refused as a safety guard")
        if self.min_risk_pct > self.base_risk_pct:
            errs.append("min_risk_pct must be <= base_risk_pct")
        if self.max_total_risk_pct < self.max_risk_pct:
            errs.append("max_total_risk_pct must be >= max_risk_pct")
        if not (0 < self.kelly_fraction <= 0.5):
            errs.append("kelly_fraction must be in (0, 0.5]")
        if self.top_risk_money < 0 or self.top_max_lots < 0:
            errs.append("top_risk_money and top_max_lots must not be negative")
        return errs


@dataclass
class UniverseConfig:
    """Which instruments are allowed to compete.

    These are *groups*, not a whitelist of blessed symbols: every instrument the
    broker offers inside an enabled group is discovered automatically and
    competes on merit.  ``exclude_patterns`` exists only for junk symbols.
    """
    groups: tuple[str, ...] = ("FX_MAJOR", "FX_MINOR", "GOLD", "SILVER",
                               "INDEX", "ENERGY")
    max_symbols: int = 60
    min_history_bars: int = 400
    exclude_patterns: tuple[str, ...] = ("BTC", "ETH", ".b", "-2", "TEST")
    # Markets switched off on evidence (Trend & Breakout only; the Rapid
    # Scalper keeps its own list). Rule, as for approaches: a market that
    # was negative on at least two-thirds of the days it traded, over six
    # or more days, is switched off. 4 Oct review of 17 Sep - 2 Oct, from
    # the broker's records (net after commission, positive days / days):
    #   USDJPY -39.98 (3/9)  DE40 -34.80 (2/9)  XAUGBP -33.90 (1/9)
    #   CHFJPY -29.86 (1/7)  EURNZD -29.37 (1/6) UK100 -23.66 (1/8)
    #   GBPJPY -19.33 (2/7)  XAUAUD -8.80 (2/7)
    # 125 trades, -219.70 together.
    # 8 Oct, same rule on the record to 7 Oct: USDCHF negative on 5 of 6 days
    # (-38.77), XAUJPY on 7 of 10 (-14.09).
    excluded_symbols: tuple[str, ...] = ("USDJPY", "DE40", "XAUGBP", "CHFJPY",
                                         "EURNZD", "UK100", "GBPJPY", "XAUAUD",
                                         "USDCHF", "XAUJPY")
    require_full_spec: bool = True


@dataclass
class ScanConfig:
    scan_interval_seconds: float = 5.0
    manage_interval_seconds: float = 1.0
    news_refresh_seconds: float = 120.0
    max_tick_age_seconds: float = 20.0
    max_tick_age_seconds_weekend: float = 4 * 3600.0
    top_n_reported: int = 12
    # Confidence tiers on the 0-100 opportunity scale.
    tier_normal: float = 60.0
    tier_strong: float = 70.0
    tier_exceptional: float = 82.0
    # --- half the pace, wiser choices ----------------------------------------
    # Day two's lesson: many small-edge trades lose to the broker's fees even
    # when half of them win. NORMAL setups are still traded, but only when the
    # realistic move is at least this many times the risk after every cost
    # (day one accepted 1.15), and at half day one's pace.
    #
    # 1.8 sat out a whole London afternoon, then found four trades in the
    # evening including a 3.3:1 index trade that ran to its target - the best
    # trade so far. Quiet spells are the rule doing its job.
    # 4 Oct (week 28 Sep - 2 Oct, 104 scored trades): entries scoring under
    # 70 made 19 of 41 and lost about 21 GBP; entries at 70 and over made
    # 37 of 63 and earned about 73 GBP. The floor is now STRONG (70).
    entry_tier: str = "STRONG"
    min_reward_risk: float = 1.8
    # Per-approach rules from the day-three review (69 trades):
    # - NEWS_CONTINUATION trades into news spikes; 18 trades lost 16.52 and
    #   the day's three biggest losses were stops jumped by slippage. Off.
    # - MOMENTUM_CONTINUATION chases moves that have already run (29 trades,
    #   -43.76, half of them winners but stopped on the pullback). It must
    #   score STRONG before it may enter; BREAKOUT_RETEST, which waits for
    #   the pullback, was the best approach and is unchanged.
    # TREND_PULLBACK: three days traded, three negative (6 wins in 15,
    # about -31 GBP, 17-23 Sep). Same standard as the others: off.
    # FAILED_BREAKOUT_RECLAIM: two days traded, two trades, no wins, both
    # dead on arrival, about -12 GBP (23, 29 Sep). Off on 29 Sep.
    disabled_tactics: tuple[str, ...] = ("NEWS_CONTINUATION", "TREND_PULLBACK",
                                         "FAILED_BREAKOUT_RECLAIM")
    # An approach switched off in ONE market condition. MOMENTUM_CONTINUATION
    # in a TREND regime lost on every one of five days (77 trades, ~-118 GBP,
    # 22 Sep: 1 win in 13) even at STRONG scores: it chases a move that has
    # already run and is stopped on the pullback. In HIGH_VOL and NEWS it
    # is break-even to strongly positive, so it stays on there.
    # BREAKOUT_RETEST in HIGH_VOL: negative on three of the five days it
    # traded (17, 22, 23 Sep), 6 trades, about -13 GBP - a retest needs a
    # level that holds, and in high volatility it does not. In TREND it is
    # the main line and stays on.
    # An approach switched off in ONE KIND OF MARKET (the instrument group
    # from contracts.infer_group: GOLD, METAL, INDEX, FX_MAJOR, ...). 8 Oct:
    # MOMENTUM_CONTINUATION on gold and silver crosses was negative on the
    # last three days it traded (5, 6, 7 Oct: -7.89, -48.52, -28.62; 6 days
    # in all, 3 negative, net -52.94), while the same approach on indices
    # (+52.71) and FX (+38.63) is the bot's engine. Same standard as the
    # approach-in-condition rule.
    disabled_tactic_groups: tuple[tuple[str, str], ...] = (
        ("MOMENTUM_CONTINUATION", "GOLD"),
        ("MOMENTUM_CONTINUATION", "SILVER"),
        ("MOMENTUM_CONTINUATION", "METAL"),
        # 9 Oct (version 5), same standing rule, record to 8 Oct: momentum
        # continuation on the FX majors still traded (GBPUSD, EURUSD,
        # AUDUSD, NZDUSD, USDCAD) negative on 5 of the 8 days it traded,
        # 17 trades, net -13.87 after 17.48 commission (+3.61 before it:
        # the fees ate it). On FX minor pairs the same approach is the
        # best line on record (38 trades, +156.31) and stays on.
        ("MOMENTUM_CONTINUATION", "FX_MAJOR"),
    )
    disabled_tactic_regimes: tuple[tuple[str, str], ...] = (
        ("MOMENTUM_CONTINUATION", "TREND"),
        ("BREAKOUT_RETEST", "HIGH_VOL"),
        # 25 Sep: negative on five of the seven days it traded, 14 trades,
        # net about +1 GBP - a coin toss that pays fees. A retest wants a
        # trend behind it; a squeeze has none yet.
        ("BREAKOUT_RETEST", "SQUEEZE"),
        # 28 Sep: BREAKOUT_RETEST in TREND, negative on five of the seven
        # days it traded (86 trades, 28 wins, net about -36 GBP), including
        # the first day on the profit floor (0 wins in 4). The un-retested
        # BREAKOUT_ACCEPTANCE signal in TREND stays on.
        ("BREAKOUT_RETEST", "TREND"),
        # 30 Sep standing rule: BREAKOUT_ACCEPTANCE in TREND negative on three
        # of six days (net about -7 GBP); LIQUIDITY_SWEEP_REVERSAL in HIGH_VOL
        # negative on five of nine days, the last five in a row.
        ("BREAKOUT_ACCEPTANCE", "TREND"),
        ("LIQUIDITY_SWEEP_REVERSAL", "HIGH_VOL"),
        # 6 Oct standing rule: BREAKOUT_ACCEPTANCE in HIGH_VOL negative on all
        # three days it traded (17, 30 Sep, 1 Oct; 3 trades, 0 wins, -17.39).
        ("BREAKOUT_ACCEPTANCE", "HIGH_VOL"),
        # 2 Oct standing rule: SESSION_EXPANSION in SQUEEZE negative on three
        # of the four days it traded (25, 29 Sep, 2 Oct; 8 trades, 2 wins,
        # net about -16 GBP). A session expansion wants a direction to
        # expand into; a squeeze has not chosen one yet.
        ("SESSION_EXPANSION", "SQUEEZE"),
    )
    tactic_min_score: dict = field(default_factory=lambda: {"MOMENTUM_CONTINUATION": 70.0})
    # --- the twin (29 Sep) ---------------------------------------------------
    # Every second signal of a twin tactic is taken as its "_2X" twin: the
    # same entry and stop, a target twice as far, and a ladder of locked
    # profit instead of the runner (see FlowLockConfig.ladder_locks). Same
    # signals, alternate exits, so the two are judged on equal opportunity.
    # 4 Oct: the twin lost to its sibling on the same signals - 30 Sep +6.87
    # vs -11.02, 1 Oct -12.26 vs +29.46, 2 Oct -1.08 vs -8.34; twin -6.47
    # on 18 trades against +10.10 on 21. The ladder (ladder_for_all) now
    # does the "hold the winner" job for every trade. Twin off.
    twin_enabled: bool = False
    twin_tactics: tuple[str, ...] = ("SESSION_EXPANSION",)
    twin_target_multiple: float = 2.0
    twin_suffix: str = "_2X"
    cooldown_seconds_after_exit: float = 180.0
    cooldown_seconds_after_reject: float = 60.0
    # --- lessons from the first live day ------------------------------------
    # Round-trip costs (spread + slippage + commission) may not exceed this
    # fraction of the stop distance: a 3-pip stop with 0.9 pips of cost is a
    # coin toss that pays the broker whichever way it lands.
    # Day five tried 12% here and the win rate fell to 27% on the day;
    # reverted to the 2026-09-18 value (branch retest-2026-09-18).
    max_cost_fraction_of_stop: float = 0.20
    # The stop is never closer than this many ATRs (decision timeframe) from
    # the entry, whatever the structure says: inside that band is noise, and
    # noise took out most of day one's trades within minutes.
    # Day three: 10 of 26 trades were over inside five minutes on a 0.5 ATR
    # floor. One full ATR keeps the stop outside the wobble; position size
    # shrinks to match, so the money at risk per trade is unchanged.
    stop_noise_floor_atr: float = 1.0
    # After a losing trade, no re-entry in the same direction on that market
    # for this long; and after this many losses on one market in a day, no
    # more entries there until tomorrow. Re-entry stays possible - churn does not.
    cooldown_seconds_after_loss: float = 1800.0
    max_losses_per_symbol_per_day: int = 2
    # No bursts: day one's new build opened eight trades in forty seconds at
    # the Asian open and lost six of them inside half an hour. One entry at a
    # time, spaced out, capped per hour, and none during the daily rollover
    # when spreads are at their widest (times are UTC, "HH:MM-HH:MM").
    # No count limits (operator's decision, day three): if six setups clear
    # the bar it takes six, if twenty then twenty. The quality guards above,
    # the risk breakers and the exposure limits are what say no. Only a
    # one-minute gap between orders remains, so one scan cannot fire the
    # same idea into eight markets in the same second. Caps stay available
    # for anyone who wants them (0 / empty = off).
    min_seconds_between_entries: float = 60.0
    max_new_positions_per_hour: int = 0
    max_new_positions_per_day: int = 0
    session_caps_utc: tuple[tuple[str, str, int], ...] = ()
    no_entry_utc_windows: tuple[str, ...] = ("21:45-23:30",)


@dataclass
class FlowLockConfig:
    initial_atr_multiple: float = 1.6
    proving_r: float = 0.5            # R multiple that ends INITIAL
    strong_flow_atr: float = 2.6      # breathing room while flow is strong
    normal_flow_atr: float = 1.5
    decay_atr: float = 0.7
    # Winners are given room: nothing is locked in until +1.5R, a third is
    # banked only at +2.5R and the rest is trailed loosely, so a real move
    # can pay for several small losses.
    breakeven_at_r: float = 1.5
    # Day three: winners averaged +0.7R because a 50% giveback trail clipped
    # them on the first pullback. Until +1R only the structural stop applies
    # (proving_trail=False); after that a trade may give back 40% of its
    # best gain, not half.
    proving_trail: bool = False
    mfe_giveback_normal: float = 0.40  # fraction of MFE we allow back
    mfe_giveback_strong: float = 0.65
    # Day five tried 0.65 here alongside the 12% cost gate; both reverted
    # together to the 2026-09-18 rule set. 0 = off.
    profit_floor_giveback: float = 0.0
    # 25 Sep, from 174 trades over five days: winners that were trailed out
    # peaked at +1.45R and kept +0.50R (a 65% giveback, though the rule above
    # says 40% - the giveback level is one of three candidates and the
    # middle one wins, and STRONG_FLOW takes the loosest). 18 losers had been
    # +1R ahead. A hard floor in R: once a trade has been lock_at_r ahead its
    # stop never sits below lock_floor_r. On the same trades: +12 -> +95 GBP.
    lock_at_r: float = 1.0
    lock_floor_r: float = 0.5
    # 29 Sep: the runner. A trade in STRONG_FLOW (momentum still with it
    # past +1R) has its broker target pushed out to runner_target_multiple x
    # the original distance; when price reaches the ORIGINAL target the
    # bot banks (1 - runner_keep_fraction) of the position and lets the
    # rest run under the trail, with at least runner_floor_r locked. The
    # broker closes everything at the target otherwise, so the record has
    # never shown what price did afterwards; this measures it.
    runner_enabled: bool = True
    runner_keep_fraction: float = 0.3
    runner_target_multiple: float = 2.0
    runner_floor_r: float = 1.0
    # The twin's ladder: once a trade has been this far ahead (R), its stop
    # never sits below the locked level (R). +1R -> +0.5R comes from the
    # profit floor above; these extend it for the far-target twin.
    ladder_locks: tuple[tuple[float, float], ...] = ((2.0, 1.2), (3.0, 2.0), (4.0, 3.0))
    ladder_for_all: bool = True       # 30 Sep: the ladder applies to every trade
    mfe_giveback_decay: float = 0.22
    stall_bars_to_decay: int = 20
    reversal_thesis_floor: float = 15.0
    # A structure break against the trade closes it only while it has not yet
    # travelled this far in our favour; later on, the trailed stop decides.
    structure_break_max_r: float = 0.3
    partial_enabled: bool = True
    partial_at_r: float = 2.5
    partial_fraction: float = 0.3
    min_stop_step_points: float = 1.0   # ignore microscopic improvements


@dataclass
class NewsConfig:
    enabled: bool = True
    use_mt5_calendar: bool = True
    # A public weekly calendar feed as a second source. Needed on macOS and
    # Linux: the MetaTrader5 Python package that the bridge uses has no
    # calendar API at all, so without this the bot trades on charts alone.
    public_calendar_feed: bool = True
    store_path: str = "data/calendar.sqlite"
    external_adapters: tuple[str, ...] = ()
    api_keys: dict[str, str] = field(default_factory=dict)
    max_staleness_seconds: float = 900.0
    blackout_before_seconds: float = 90.0    # no new entries just before
    blackout_after_seconds: float = 45.0     # first-spike chaos window
    second_wave_window_seconds: float = 900.0
    min_importance_for_blackout: int = 3
    allow_social_sources: bool = False


@dataclass
class RunnerConfig:
    """Momentum Runner: the main bot's index entries ridden with a 3 R trail
    (mintel/runner). PAPER shadows the real trade; LIVE opens its own."""
    enabled: bool = True
    mode: str = "PAPER"             # PAPER simulates; LIVE places its own orders (magic below)
    magic: int = 990_511            # the Runner's own orders; never another bot's number
    trail_r: float = 3.0            # once +3 R, trail 3 R behind the best price
    window_hours: float = 8.0       # out at the price after 8 hours (the replay's window)
    markets: tuple[str, ...] = ()   # empty = every index market the main bot trades
    # 9 Oct (version 5): approaches the Runner does not ride. Replaying every
    # index trade to 7 Oct on the broker's minute bars, the 3 R trail LOST on
    # momentum continuation (33 trades, -14.4 R; down on 7 of the 9 days it
    # traded) and earned on everything else (64 trades, +43.5 R; down on 3
    # of 13). The Runner's own record agrees: momentum continuation 7 trades,
    # 1 win, -27.48; session expansion 1 trade, +56.67.
    skip_tactics: tuple[str, ...] = ("MOMENTUM_CONTINUATION",)
    # 0 = enter with the real trade (as always). Above 0: enter only once the
    # real trade is this many R ahead, at that price, with the stop one of
    # the real trade's R behind the Runner's own entry (same size, same money
    # at risk). NOT YET EVALUATED - the data on hand cannot say whether it
    # helps; `python -m mintel.ops.potential --runner` replays it on the
    # broker's minute bars.
    confirm_r: float = 0.0
    # --- the runner feed (only while Trend & Breakout is on PAPER) ----------
    # Approaches Trend & Breakout's scanner still OFFERS on index markets
    # while it is on paper, even where its own rules (approach in a market
    # condition, approach in a kind of market) have switched them off - so
    # the Runner keeps receiving them. "APPROACH" (any condition) or
    # "APPROACH:CONDITION" (e.g. "BREAKOUT_RETEST:TREND"). Everything else
    # still applies to them: excluded markets, the score floor, news, spread,
    # cost, risk, exposure caps, breakers and the daily loss stop. Such a
    # paper trade is tagged runner_feed in the journal (entry_state_json),
    # so Trend & Breakout's own record can leave it out. Never active in
    # LIVE, nor when index orders are kept live (tnb.live_groups).
    # EMPTY on evidence (9 Oct). The rule: positive under the 3 R trail on
    # at least ~8 trades over at least 4 days, more days up than down,
    # measured only on trades today's other rules would still allow. On the
    # broker's minute-bar replay (16 Sep - 6 Oct, 97 index trades) that
    # leaves 13 non-momentum index trades (score 70+, market not excluded),
    # +12.8 R together but up on 3 days of 8; per approach in its
    # condition: breakout retest in a trend 5 trades +7.1 R (2 days up, 3
    # down), liquidity sweep reversal in a fast market 5 trades +3.5 R (1 up,
    # 3 down), breakout retest in a fast market 1 trade -1.0 R. None
    # qualifies. See docs/strategies/momentum_runner.md, "The runner feed".
    feed_tactics: tuple[str, ...] = ()
    # --- the Runner's top size (LIVE only) ----------------------------------
    # A ride in a segment that has EARNED it on the Runner's own exit (the
    # 3 R trail on the replay, plus its own live rides) is sized for
    # top_risk_money at its stop instead of copying Trend & Breakout's
    # volume - still never above risk.max_risk_pct of the REAL account
    # equity (1.5% = 30 on 2,000), the broker's volume limits and the
    # margin checks; FX and metals at most top_max_lots, indices no lot cap.
    # Segments: (approach, kind of market[, condition]). The same evidence
    # rule as Trend & Breakout's top size: at least 20 trades over at least
    # 4 days, positive per trade and in R after costs, more days up than
    # down. Before each top-size order the Runner re-checks its own newest
    # LIVE rides there: once it has top_recheck_rides of them and they are
    # negative, it sizes normally and says why. A loss never raises a size.
    # EMPTY on evidence (9 Oct): on trades today's rules still allow, no
    # index approach has 20 (breakout retest 6, liquidity sweep reversal 5,
    # session expansion 2 with the Runner's own ride of 7 Oct, 3). Breakout
    # retest only reaches 36 (+17.9 R) when trades scored under 70 and the
    # excluded DE40 and UK100 are counted back in. So it is OFF.
    top_size_enabled: bool = True
    top_segments: tuple[tuple[str, ...], ...] = ()
    top_risk_money: float = 40.0        # the same as risk.top_risk_money
    top_max_lots: float = 0.5           # FX and metals only
    top_recheck_rides: int = 10
    top_lookback_rides: int = 40

    def feed_entries(self) -> tuple[tuple[str, str], ...]:
        """``feed_tactics`` as (APPROACH, CONDITION or "*") pairs. A malformed
        entry is left out (and said once in the log), never a crash."""
        out: list[tuple[str, str]] = []
        for raw in getattr(self, "feed_tactics", ()) or ():
            try:
                if isinstance(raw, (list, tuple)):
                    name = str(raw[0]).strip().upper()
                    cond = str(raw[1]).strip().upper() if len(raw) > 1 and raw[1] else "*"
                else:
                    text = str(raw).strip().upper().replace("@", ":")
                    name, _, cond = text.partition(":")
                    name, cond = name.strip(), (cond.strip() or "*")
            except Exception:
                name, cond = "", ""
            if not _word(name) or not (cond == "*" or _word(cond)):
                _warn_once(f"runner.feed_tactics entry {raw!r} is not understood and is left out")
                continue
            out.append((name, cond))
        return tuple(dict.fromkeys(out))


def _word(text: str) -> bool:
    """A setting name such as BREAKOUT_RETEST or HIGH_VOL."""
    return bool(text) and text.replace("_", "").isalnum()


_WARNED: set[str] = set()


def _warn_once(message: str) -> None:
    if message not in _WARNED:
        _WARNED.add(message)
        log.warning("%s", message)


TNB_MODES = ("LIVE", "PAPER")


@dataclass
class TnbConfig:
    """Trend & Breakout, the main trader (magic 990311): LIVE or PAPER.

    PAPER: ``mintel/run.py`` ``main()`` hands the trader
    ``PaperBroker.from_config(broker, cfg)`` (``mintel/broker/paper.py``):
    the same code decides on REAL prices and the real account; every order
    it sends is simulated. The other bots keep the real broker. LIVE (the
    default, so nothing changes until the installer's line-up step flips it)
    trades as before. See docs/strategies/tnb_paper.md.

    The other fields are the paper settings ``PaperBroker.from_config``
    reads (same names as ``PaperConfig``).
    """
    mode: str = "LIVE"                   # LIVE | PAPER; anything else is read as LIVE, with a warning
    slippage_points: float = 1.0         # adverse, on every paper market fill (never on a target)
    live_groups: tuple[str, ...] = ()    # kinds of market whose orders still go to the real broker; empty = all paper
    # A REAL Trend & Breakout trade still open at the switch is wound down at
    # the real broker (stop changes and closes, never a new order) to its
    # natural end. False leaves it to its own broker stop and target.
    manage_leftovers: bool = True
    commission: dict = field(default_factory=dict)   # {kind: [per lot per side, currency]} over IC Markets Raw
    fx_rates: dict = field(default_factory=dict)     # {"USD": 0.79}: account currency per unit, over the broker's
    max_tick_age_seconds: float = 120.0
    replay_ticks: bool = True
    replay_min_seconds: float = 1.0
    replay_chunk_minutes: float = 60.0
    replay_max_hours: float = 72.0
    replay_retry_seconds: float = 5.0
    replay_give_up_seconds: float = 900.0

    @property
    def is_paper(self) -> bool:
        return tnb_mode_of(self.mode) == "PAPER"

    def normalise(self) -> list[str]:
        """Put malformed values back to safe ones; returns what was fixed, in
        plain English. A mode that is not LIVE or PAPER becomes LIVE."""
        fixed: list[str] = []
        mode = tnb_mode_of(self.mode)
        if str(self.mode or "").strip().upper() != mode:
            fixed.append(f"tnb.mode {self.mode!r} is not LIVE or PAPER: Trend & Breakout stays LIVE")
        self.mode = mode
        defaults = TnbConfig()
        for name in ("slippage_points", "max_tick_age_seconds", "replay_min_seconds", "replay_chunk_minutes",
                     "replay_max_hours", "replay_retry_seconds", "replay_give_up_seconds"):
            val = getattr(self, name)
            try:
                ok = not isinstance(val, bool) and math.isfinite(float(val)) and float(val) >= 0
            except (TypeError, ValueError):
                ok = False
            if not ok:
                fixed.append(f"tnb.{name} {val!r} is not a number of zero or more: the default "
                             f"{getattr(defaults, name)!r} is used")
                setattr(self, name, getattr(defaults, name))
        if not isinstance(self.live_groups, (list, tuple)):
            fixed.append(f"tnb.live_groups {self.live_groups!r} is not a list: every market stays on paper")
            self.live_groups = ()
        self.live_groups = tuple(str(g) for g in self.live_groups)
        for name in ("commission", "fx_rates"):
            if not isinstance(getattr(self, name), dict):
                fixed.append(f"tnb.{name} is not a table: the default is used")
                setattr(self, name, {})
        for name in ("manage_leftovers", "replay_ticks"):
            if not isinstance(getattr(self, name), bool):
                setattr(self, name, bool(getattr(self, name)))
        return fixed


def tnb_mode_of(value: Any) -> str:
    """"PAPER" only for a clear PAPER; anything else (LIVE, empty, a typo, a
    wrong type) is LIVE."""
    try:
        text = str(value or "").strip().upper()
    except Exception:
        text = ""
    return text if text in TNB_MODES else "LIVE"


def tnb_mode(cfg: Any) -> str:
    """Trend & Breakout's mode from a Config: "LIVE" or "PAPER". Fail safe:
    a missing or malformed setting is LIVE, said once in the log."""
    block = getattr(cfg, "tnb", None)
    raw = getattr(block, "mode", "LIVE") if block is not None else "LIVE"
    mode = tnb_mode_of(raw)
    if str(raw or "").strip().upper() != mode:
        _warn_once(f"tnb.mode {raw!r} is not LIVE or PAPER: Trend & Breakout stays LIVE")
    return mode


@dataclass
class BandBreakerConfig:
    """Band Breaker: intraday index momentum from the published noise-band
    rule (mintel/bandbreaker). PAPER simulates; LIVE places real orders."""
    enabled: bool = True
    mode: str = "PAPER"             # PAPER simulates; LIVE places its own orders (magic below)
    magic: int = 990_611
    markets: tuple[str, ...] = ("US500", "US30", "USTEC")
    lookback_days: int = 14
    check_minutes: int = 30
    first_check: str = "10:00"          # New York
    last_entry: str = "15:30"
    flat_before_close_minutes: int = 5
    max_entries_per_day: int = 3
    risk_money: float = 10.0
    paper_slippage_points: float = 1.0


@dataclass
class CrowdConfig:
    """Crowd Fader: positioning from Coinversa Pulse, faded once the price
    turns (mintel/crowd). PAPER simulates; LIVE places real orders. The API
    key lives in secrets.json as "coinversa_api_key", never here."""
    enabled: bool = True
    mode: str = "PAPER"             # PAPER simulates; LIVE places its own orders (magic below)
    magic: int = 990_711
    markets: tuple[str, ...] = ("XAUUSD=xyz:GOLD", "XAGUSD=xyz:SILVER", "US500=xyz:SP500", "JP225=xyz:JP225",
                                "EURUSD=xyz:EUR", "GBPUSD=xyz:GBP", "XTIUSD=xyz:CL", "XBRUSD=xyz:BRENTOIL")
    poll_minutes: float = 10.0
    stretch: float = 0.5
    agree_cap: float = 0.1
    min_score: float = 60.0
    min_wallets: int = 200
    heatmap_range_pct: float = 3.0
    heatmap_buckets: int = 20
    trigger_bars: int = 8
    stop_bars: int = 4
    min_stop_pct: float = 0.15
    max_stop_pct: float = 1.5
    target_r: float = 2.0
    cluster_min_r: float = 1.0
    cluster_max_r: float = 4.0
    hold_hours: float = 24.0
    max_entries_per_day: int = 2
    risk_money: float = 10.0
    max_risk_money: float = 30.0
    paper_slippage_points: float = 1.0
    entry_hours_utc: tuple[int, ...] = (7, 20)
    weekend_flat_utc: str = "20:30"


@dataclass
class OpsConfig:
    data_dir: str = "data"
    log_dir: str = "logs"
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8787
    heartbeat_stale_seconds: float = 45.0
    watchdog_interval_seconds: float = 15.0
    watchdog_restart_backoff_seconds: float = 20.0
    max_restarts_per_hour: int = 12
    min_free_disk_mb: float = 500.0
    max_memory_pct: float = 92.0
    max_clock_skew_seconds: float = 30.0


# Bump this whenever the trading rules change in a way that makes earlier
# results a different experiment. The status page resets to it automatically.
STRATEGY_VERSION = "2026-10-05 complete reset to zero - fresh 2,000 account, paper never in the account total"
# The name of the rule set Trend & Breakout runs. Change it whenever the
# rules change, so results are always tied to the rules that made them.
# Version 2, "Commission First", from the 5 Oct reset: entries 70+, twin off,
# eight losing markets off, session expansion in a squeeze off, commission
# never assumed to be zero. See docs/strategies/v2-commission-first.md.
# Version 3 from 7 Oct: the same, plus breakout acceptance in a fast market
# off (standing rule). The measuring clock is unchanged: still from the reset.
# Version 4 from 8 Oct: USDCHF and XAUJPY off (market rule), momentum
# continuation off on gold and silver crosses (the approach rule applied per
# kind of market). The measuring clock is unchanged: still from the reset.
# Version 5 from 9 Oct: momentum continuation off on the FX majors (the
# approach rule per kind of market), the top-opportunity size for momentum
# continuation on FX minor pairs (risk.top_*), a calibration that one broken
# trade can no longer poison, and the Momentum Runner no longer riding
# momentum continuation. The measuring clock is unchanged: still from the
# reset. See docs/strategies/v2-commission-first.md, "Version 5".
STRATEGY_NAME = "Commission First (version 5)"


@dataclass
class Config:
    version: int = CONFIG_VERSION
    mode: str = "DEMO"                  # DEMO | LIVE
    live_marker: str = ""
    magic: int = 990_311
    # Where "since start" measurement begins (ISO UTC). Set by the installer
    # the first time the bot is set up; the status page measures the bot's
    # own trades from here using the broker's records.
    tracking_start_utc: str = ""
    # The ACCOUNT's anchor for the page's big figure: when the demo account was
    # last reset and to what balance. Never moved by a change of rules (the
    # tracking start above is). Aaron's account was reset to 2,000 GBP on
    # 5 Oct 2026 at 12:30 UK; set both again in config.json after another reset.
    account_reset_utc: str = "2026-10-05T11:30:42+00:00"
    account_reset_balance: float = 2000.0
    # Which strategy the measurement belongs to. When the code's strategy
    # changes, the bot restarts the measuring clock itself on start-up, so the
    # status page only ever shows the CURRENT strategy's trades.
    tracking_strategy: str = ""
    # Where the nightly record is published (a folder in a GitHub repository)
    # so it can be read without anyone pasting screenshots. The token lives
    # in secrets.json as "github_token", never here.
    report_repo: str = "atashurst-gif/azza132"
    report_branch: str = "reports"
    account_login: int = 0
    account_password: str = ""
    account_server: str = ""
    mt5_terminal_path: str = ""
    # How the engine reaches MetaTrader 5.
    #   "direct" - this Python imports MetaTrader5 itself (Windows).
    #   "bridge" - MT5 and a small Windows Python run inside a Wine prefix and
    #              are reached over a localhost socket (macOS and Linux).
    broker_mode: str = "direct"
    bridge_host: str = "127.0.0.1"
    bridge_port: int = 8790
    wine_prefix: str = ""
    wine_python: str = ""
    # Full path to the wine binary. launchd starts the watchdog with a bare
    # PATH, so "wine" is not findable there unless we are told where it is.
    wine_binary: str = ""
    aggression: str = "NORMAL"          # CONSERVATIVE|NORMAL|AGGRESSIVE|MAXIMUM
    risk: RiskConfig = field(default_factory=RiskConfig)
    universe: UniverseConfig = field(default_factory=UniverseConfig)
    scan: ScanConfig = field(default_factory=ScanConfig)
    flowlock: FlowLockConfig = field(default_factory=FlowLockConfig)
    news: NewsConfig = field(default_factory=NewsConfig)
    ops: OpsConfig = field(default_factory=OpsConfig)
    runner: RunnerConfig = field(default_factory=RunnerConfig)
    bandbreaker: BandBreakerConfig = field(default_factory=BandBreakerConfig)
    crowd: CrowdConfig = field(default_factory=CrowdConfig)
    tnb: TnbConfig = field(default_factory=TnbConfig)

    # ------------------------------------------------------- Trend & Breakout --
    @property
    def tnb_paper(self) -> Optional[TnbConfig]:
        """The paper settings while Trend & Breakout is on PAPER, else None.
        ``PaperBroker.from_config`` reads its fields under this name."""
        return self.tnb if tnb_mode(self) == "PAPER" else None

    # ------------------------------------------------------------- live gate --
    @property
    def is_live(self) -> bool:
        """LIVE requires both the mode *and* the explicit marker.

        Two independent conditions mean neither a typo in ``mode`` nor a stale
        marker can move real money on its own.
        """
        return self.mode.upper() == "LIVE" and self.live_marker == LIVE_MARKER

    @property
    def effective_mode(self) -> str:
        return "LIVE" if self.is_live else "DEMO"

    # ----------------------------------------------------------- aggression --
    def aggression_scale(self) -> float:
        """Multiplier applied to the *confidence-to-risk* curve, not to the cap.

        MAXIMUM lets the engine reach ``max_risk_pct`` on merely STRONG evidence;
        it never lets it exceed ``max_risk_pct``, and it never disables a
        circuit breaker.
        """
        return {"CONSERVATIVE": 0.45, "NORMAL": 1.0,
                "AGGRESSIVE": 1.5, "MAXIMUM": 2.2}.get(self.aggression.upper(), 1.0)

    # ------------------------------------------------------------ validation --
    def validate(self) -> list[str]:
        errs = list(self.risk.validate())
        if self.mode.upper() not in ("DEMO", "LIVE"):
            errs.append(f"mode must be DEMO or LIVE, got {self.mode!r}")
        if self.mode.upper() == "LIVE" and self.live_marker != LIVE_MARKER:
            errs.append("mode=LIVE requires live_marker to be set exactly; "
                        "refusing to trade live")
        if self.aggression.upper() not in ("CONSERVATIVE", "NORMAL",
                                           "AGGRESSIVE", "MAXIMUM"):
            errs.append(f"unknown aggression {self.aggression!r}")
        if not self.universe.groups:
            errs.append("universe.groups is empty - nothing would be traded")
        if not (0 < self.scan.tier_normal < self.scan.tier_strong
                < self.scan.tier_exceptional <= 100):
            errs.append("confidence tiers must be strictly increasing")
        if self.magic <= 0:
            errs.append("magic must be a positive integer")
        if self.broker_mode not in ("direct", "bridge"):
            errs.append(f"broker_mode must be 'direct' or 'bridge', "
                        f"got {self.broker_mode!r}")
        if self.broker_mode == "bridge" and not (0 < self.bridge_port < 65536):
            errs.append("bridge_port must be a valid port number")
        # Trend & Breakout's mode never stops the start: a malformed value is
        # put back to a safe one (LIVE for the mode) and said in the log.
        for fixed in self.tnb.normalise():
            _warn_once(fixed)
        return errs

    @property
    def bridge_token_file(self) -> str:
        return str(Path(self.ops.data_dir) / "bridge-token.txt")

    # ------------------------------------------------------------------- io --
    def to_dict(self) -> dict:
        d = asdict(self)
        for k in ("groups", "exclude_patterns", "external_adapters"):
            for sub in ("universe", "news"):
                if k in d.get(sub, {}):
                    d[sub][k] = list(d[sub][k])
        return d

    def save(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        d = self.to_dict()
        secrets = {"account_password": d.pop("account_password", "")}
        d["news"]["api_keys"] = {k: "" for k in d["news"].get("api_keys", {})}
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(d, indent=2, sort_keys=True))
        tmp.replace(path)
        # Secrets go to a sibling file with tight permissions.
        sec = path.parent / "secrets.json"
        payload: dict = {}
        try:
            existing = json.loads(sec.read_text()) if sec.exists() else {}
            if isinstance(existing, dict):
                payload.update(existing)        # keep tokens etc. we do not own
        except Exception:
            pass
        payload.update({"account_password": secrets["account_password"],
                        "api_keys": self.news.api_keys})
        sec.write_text(json.dumps(payload, indent=2))
        try:
            os.chmod(sec, 0o600)
        except OSError:
            pass

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        """Load, tolerating unknown/missing keys.

        A partially corrupt config yields defaults for the damaged sections and
        DEMO mode - the failure mode is always "trade nothing / trade demo",
        never "trade live by accident".
        """
        path = Path(path)
        if not path.exists():
            return cls()
        try:
            raw = json.loads(path.read_text())
            if not isinstance(raw, dict):
                raise ValueError("config root is not an object")
        except Exception:
            return cls()
        cfg = cls()
        nested = {"risk": RiskConfig, "universe": UniverseConfig,
                  "scan": ScanConfig, "flowlock": FlowLockConfig,
                  "news": NewsConfig, "ops": OpsConfig, "runner": RunnerConfig,
                  "bandbreaker": BandBreakerConfig, "crowd": CrowdConfig,
                  "tnb": TnbConfig}
        for f in fields(cls):
            if f.name in nested:
                sub = raw.get(f.name)
                if f.name == "tnb" and isinstance(sub, dict) and isinstance(sub.get("live_groups"), str):
                    sub = dict(sub, live_groups=[sub["live_groups"]])     # "INDEX" means ["INDEX"]
                if isinstance(sub, dict):
                    setattr(cfg, f.name, _build(nested[f.name], sub))
                elif f.name == "tnb" and f.name in raw:
                    _warn_once(f"the tnb block in {path.name} is not a table: Trend & Breakout stays LIVE")
            elif f.name in raw:
                try:
                    setattr(cfg, f.name, type(getattr(cfg, f.name))(raw[f.name]))
                except Exception:
                    pass
        sec = path.parent / "secrets.json"
        if sec.exists():
            try:
                s = json.loads(sec.read_text())
                cfg.account_password = str(s.get("account_password", "") or "")
                if isinstance(s.get("api_keys"), dict):
                    cfg.news.api_keys.update(
                        {str(k): str(v) for k, v in s["api_keys"].items()})
            except Exception:
                pass
        cfg.account_password = os.environ.get("MINTEL_PASSWORD",
                                              cfg.account_password)
        if cfg.mode.upper() == "LIVE" and cfg.live_marker != LIVE_MARKER:
            cfg.mode = "DEMO"          # fail safe, loudly, downstream
        try:
            for fixed in cfg.tnb.normalise():     # never a crash: a bad value becomes a safe one
                _warn_once(fixed)
        except Exception as exc:
            _warn_once(f"the tnb block could not be read ({exc}): Trend & Breakout stays LIVE")
            cfg.tnb = TnbConfig()
        return cfg


def _build(kls, raw: dict):
    obj = kls()
    for f in fields(kls):
        if f.name not in raw:
            continue
        cur = getattr(obj, f.name)
        val = raw[f.name]
        try:
            if isinstance(cur, tuple):
                setattr(obj, f.name, tuple(val))
            elif isinstance(cur, bool):
                setattr(obj, f.name, bool(val))
            elif isinstance(cur, dict):
                setattr(obj, f.name, dict(val))
            else:
                setattr(obj, f.name, type(cur)(val))
        except Exception:
            pass
    return obj
