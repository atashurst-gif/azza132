"""Every sentence the coach writes, from fixed templates (spec section 14).

The ledger refuses a change without a sentence of at least 20 characters, so
every decision the coach makes goes through :func:`say`. Plain UK English,
the pound sign (never "GBP"), and every money figure with what it is.

Nothing here decides anything: it only turns a decision's numbers into words.
"""
from __future__ import annotations

import math
from typing import Any, Optional

from . import BOT_LABEL

MIN_WORDS_CHARS = 20


def gbp(v: Optional[float], signed: bool = False) -> str:
    """£13.00, -£8.52, +£5.52 (signed); "-" for nothing."""
    if v is None or (isinstance(v, float) and not math.isfinite(v)):
        return "-"
    v = float(v)
    if signed:
        return f"{'-' if v < 0 else '+'}£{abs(v):,.2f}"
    return f"{'-' if v < 0 else ''}£{abs(v):,.2f}"


def pct(v: Optional[float]) -> str:
    if v is None:
        return "-"
    return f"{float(v):g}%"


def r(v: Optional[float]) -> str:
    """+0.21 R"""
    if v is None:
        return "-"
    return f"{float(v):+.2f} R"


def key_words(key: str) -> str:
    """'tnb|MOMENTUM_CONTINUATION|FX_MINOR|HIGH_VOL' -> 'Trend & Breakout: momentum continuation on minor
    pairs in fast markets'."""
    parts = (str(key or "").split("|") + ["*", "*", "*", "*"])[:4]
    bot, approach, kind, cond = parts
    who = BOT_LABEL.get(bot, bot)
    bits = []
    if approach != "*":
        bits.append(approach.replace("_", " ").lower())
    if kind != "*":
        bits.append("on " + KIND_WORDS.get(kind, kind.replace("_", " ").lower()))
    if cond != "*":
        bits.append("in " + CONDITION_WORDS.get(cond, cond.replace("_", " ").lower() + " markets"))
    return f"{who}: {' '.join(bits)}" if bits else who


KIND_WORDS = {"FX_MINOR": "minor pairs", "FX_MAJOR": "major pairs", "FX_EXOTIC": "exotic pairs",
              "INDEX": "indices", "GOLD": "gold", "SILVER": "silver", "METAL": "metals", "ENERGY": "energy",
              "EQUITY": "shares", "CRYPTO": "crypto", "UNKNOWN": "other markets"}
CONDITION_WORDS = {"TREND": "trending markets", "RANGE": "ranging markets", "SQUEEZE": "quiet squeezes",
                   "HIGH_VOL": "fast markets", "LOW_VOL": "slow markets", "NEWS": "news markets",
                   "UNCLEAR": "unclear markets"}
STEP_WORDS = {"trial": "the trial size", "base": "the normal size"}


def step_words(step: Optional[str], money: Optional[float] = None, risk_pct: Optional[float] = None) -> str:
    name = STEP_WORDS.get(str(step), f"step {step}") if step else "its size"
    extra = []
    if money is not None:
        extra.append(f"{gbp(money)} a trade")
    if risk_pct is not None:
        extra.append(f"{pct(round(float(risk_pct), 3))} of the account")
    return f"{name} ({', '.join(extra)})" if extra else name


# The templates. Each takes keyword arguments; anything missing reads "-".
TEMPLATES: dict[str, str] = {
    # moves up (nightly, released 06:30 UK)
    "G1": ("{what} earns a live trial at {size}: {trades} trades on {days} UK days, chance of a real edge "
           "{p_plus}, lower bound {lcb80} a trade after costs. From the 06:30 UK release unless you veto it."),
    "G2": ("{what} passed its live trial ({trades} real trades over {days} UK days, {net} after costs, {up} days up "
           "and {down} down): it moves to {size}."),
    "G2_FAIL": ("{what} did not pass its live trial ({trades} real trades over {days} UK days, {net} after costs, "
                "{up} up and {down} down): back to practice, no new trial for 10 UK trading days."),
    "G2_BAN": ("{what} failed its live trial a second time in this epoch: no real money for it until the next "
               "epoch; it keeps practising."),
    "G3": ("{what} steps up from {from_size} to {size}: {trades} real trades at its present size, chance of a real "
           "edge {p_plus}, at a new high of its real record, and the Kelly cap ({kelly}) allows it. Announced "
           "tonight; it starts at the 06:30 UK release unless you veto it."),
    "STOP_UP": ("Demo only: the coach's daily stop goes from {from_pct} to {to_pct} of the account: {keys} proven "
                "keys live and the stop blocked a proven trade on a day that still ended up. Announced tonight; it "
                "starts at the 06:30 UK release unless you veto it."),
    "RELEASE": "{what}: tonight's plan item {item} is in force from the 06:30 UK release.",
    "VETOED": "{what}: plan item {item} was vetoed by you, so nothing changes.",
    "RELEASE_SKIP": "{what}: plan item {item} was not released ({why}).",
    # moves down (any tick)
    "D1": ("{what}: the running check of its real results (CUSUM {cusum} in units of its spread) has gone past "
           "{limit}: back to practice, no new trial for 10 UK trading days."),
    "D2": ("{what}: one real trade lost {x} (past the -2.5 R line): paused for the rest of the UK day. It keeps "
           "practising."),
    "D3_STEP": "{what}: its chance of a real edge on real trades is down to {p_plus} (under 0.80): one step down to {size}.",
    "D3_BASE": "{what}: its chance of a real edge on real trades is down to {p_plus} (under 0.60): back to {size}.",
    "D3_PRACTICE": "{what}: its chance of a real edge on real trades is down to {p_plus} (under 0.50): back to practice.",
    "D4_STEP": ("{what}: {dd} below the best point of its real record ({limit} or more): one step down to {size}."),
    "D4_PRACTICE": ("{what}: {dd} below the best point of its real record ({limit} or more): back to practice."),
    "D5": ("{what}: its trial has lost {net} with real money ({limit} is the most a trial may lose): back to "
           "practice, no new trial for 10 UK trading days."),
    "S1": ("{what}: {detail}. Your three-days rule switches it off for real money; it keeps practising."),
    "S2": ("{symbol} is negative on {neg} of {days} UK days traded: practice-only for the coach's keys. The nightly "
           "note suggests adding it to your excluded markets at the next install."),
    "K1": ("{what} has its {n} real trades and the plan's rule says back to paper ({why}): no new real entries. "
           "It has no paper path while LIVE, so switching it to PAPER is your call."),
    "F1_KEEP": ("Formula 1 has its {n} live trades - {net} after costs, {up} days up and {down} down: the plan's rule "
                "says keep. From the 06:30 UK release it stays at {size} and the coach may step it up on evidence."),
    "F1_FAIL": ("Formula 1 has its {n} live trades - {net} after costs, {up} days up and {down} down: the plan's rule "
                "says back to practice. From the 06:30 UK release it trades on paper; 'pin formula1' puts it back."),
    "F1_JUMP": "You approved Formula 1's jump to the top size: {size} from the 06:30 UK release.",
    "PLACED": "{what} is yours: it trades on your own settings until it is judged at {n} real trades.",
    "PINNED": "{what} is pinned by you: the coach leaves it alone (only HALT applies).",
    "PRACTICE_KEY": "{what} practises; it can earn a live trial on its own evidence.",
    "BACK_TO_SETTINGS": "{what} is back on your own settings: {why}.",
    # brakes
    "B1": ("The account is down {loss} today, past the coach's line of {line} ({line_pct}): no new grants, trials "
           "paused and every key at its normal size for the rest of the UK day."),
    "B2": ("The account is down {loss} today, past the coach's pause line of {line} ({line_pct}): {what} paused "
           "until midnight UK. Open trades are still managed."),
    "B3": ("The last 5 UK trading days are down {loss}, past {line_pct} of the account: no new grants and no steps "
           "until it is back above -{release_pct}."),
    "B4": ("The account is {dd} below the coach's saved peak of {peak} ({line_pct} line): no grants, no steps, every "
           "key at its normal size."),
    "B4_PAUSE": ("The account is {dd} below the coach's saved peak of {peak} ({line_pct} line): {what} paused. "
                 "Needs you."),
    "B7": ("Trial losses this week are {loss} (budget {budget}): no new trials until Monday's 06:30 UK release."),
    "B8": "The coach's data checks failed ({why}): no grants; its restrictions stay.",
    "B8_PAUSE": "{what}'s own record could not be read ({why}): paused until it can be read again.",
    "B9": "The coach has put itself in GUARD ({why}): it may only restrict until you look at it.",
    "BRAKE_OFF": "{what}: released ({why}).",
    # the coach itself
    "MODE": "The coach's mode is now {to} ({why}).",
    "GRANTS": "Grants are now {to} ({why}).",
    "DEMO": "Demo limits {what}: {detail}.",
    "SCORECARD_GUARD": ("Over {days} UK trading days and {trades} affected trades the coach's own effect is {net}: "
                        "it has put itself in GUARD and says so."),
    "WATCH_FAIL": "The WATCH-day self-check did not pass ({why}): the coach stays watching only.",
    "FLOOR": "{what}: {action} the floor {field} {value} (paired test on its own trades, chance {p}).",
    "BLOCK_OFF": "{symbol} is no longer practice-only for the coach's keys ({why}).",
    "ENTRY_GONE": "{what} has no coach instruction any more ({why}): your own settings.",
}


def say(template: str, **kw: Any) -> str:
    """The sentence for ``template`` (one of TEMPLATES) with ``kw`` filled in; never shorter than 20
    characters and never with a gap left in it."""
    text = TEMPLATES[template]

    class _D(dict):
        def __missing__(self, k):
            return "-"
    out = text.format_map(_D({k: ("-" if v is None else v) for k, v in kw.items()}))
    if len(out) < MIN_WORDS_CHARS:
        out = (out + " (the coach's record)").strip()
    return out


def short(text: str, n: int = 400) -> str:
    """At most ``n`` characters (the policy's ``why`` field)."""
    text = str(text or "")
    return text if len(text) <= n else text[: n - 3].rstrip() + "..."


def bot_words(bot: str) -> str:
    return BOT_LABEL.get(bot, bot)
