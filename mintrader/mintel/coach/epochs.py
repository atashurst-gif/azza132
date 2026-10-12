"""Epochs: the dates after which a key's older evidence counts for less.

A trade that closed before the latest epoch touching its key keeps half its
weight (``v = 0.5`` in the coach's weight ``w = a x d x v``; spec section 4.2),
and Aaron's three-days rule (S1) counts only the days inside the current
epoch. These constants are written by Claude and changed only by an install:
the coach never edits them, and nothing here is learned or tuned.

Each epoch names the keys it touches. A key is ``bot|APPROACH|KIND|CONDITION``;
a pattern has the same four parts, where ``*`` matches anything, and the
pattern ``"*"`` on its own means every key.

The dates are UTC. One differs from the spec's table, on the code's evidence:
the account reset is 2026-10-05 11:30:42 UTC (12:30 UK), as
``Config.account_reset_utc`` records it; the spec's table wrote the UK time
in its UTC column.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

# CLASSIFIER_VERSION: the market-condition classifier the evidence was recorded
# with. A change to the classifier is a new epoch for every key (added here,
# with its date, by the install that changes it).
from . import CLASSIFIER_VERSION, OLD_EPOCH_WEIGHT, UTC


@dataclass(frozen=True)
class Epoch:
    when_utc: Optional[dt.datetime]       # None: no date (a version marker, touches nothing by itself)
    name: str
    keys: tuple[str, ...]                 # patterns; ("*",) = every key
    why: str = ""


EPOCHS: tuple[Epoch, ...] = (
    Epoch(dt.datetime(2026, 10, 5, 11, 30, 42, tzinfo=UTC), "reset", ("*",),
          "the demo account was reset to 2,000 (12:30 UK)"),
    Epoch(dt.datetime(2026, 10, 9, tzinfo=UTC), "13 a trade, commission recorded", ("tnb|*|*|*",),
          "Trend & Breakout at 13 a trade, and its journal records both halves of the commission"),
    Epoch(dt.datetime(2026, 10, 10, tzinfo=UTC), "news block, live tactics", ("tnb|*|*|*",),
          "the 15-minute news block before high-importance releases, and tnb.live_tactics"),
    Epoch(dt.datetime(2026, 10, 12, 7, 58, tzinfo=UTC), "Formula 1 choices", ("tnb|MOMENTUM_CONTINUATION|FX_MINOR|*",),
          "Formula 1's choices installed"),
    Epoch(None, f"classifier version {CLASSIFIER_VERSION}", (),
          "the market-condition labels the evidence was recorded with"),
)


def _parts(text: str) -> list[str]:
    parts = [p.strip().upper() for p in str(text or "").split("|")]
    return parts + ["*"] * (4 - len(parts)) if len(parts) < 4 else parts[:4]


def matches(pattern: str, key: str) -> bool:
    """Does ``pattern`` name ``key``? ``"*"`` alone names every key; in a
    four-part pattern ``*`` matches any part (a key's own ``*`` included).
    Bot names compare without case."""
    pat = str(pattern or "").strip()
    if pat == "*":
        return True
    if not pat:
        return False
    return all(p == "*" or p == k for p, k in zip(_parts(pat), _parts(key)))


def epochs_for(key: str, epochs: Sequence[Epoch] = EPOCHS) -> list[Epoch]:
    """The dated epochs that touch ``key``, oldest first."""
    out = [e for e in epochs if e.when_utc is not None and any(matches(p, key) for p in e.keys)]
    return sorted(out, key=lambda e: e.when_utc)


def latest_epoch(key: str, as_of: Optional[dt.datetime] = None,
                 epochs: Sequence[Epoch] = EPOCHS) -> Optional[dt.datetime]:
    """When the newest epoch touching ``key`` began (at or before ``as_of``
    when given), or None: the key has had no epoch."""
    best = None
    for e in epochs_for(key, epochs):
        if as_of is not None and e.when_utc > _utc(as_of):
            continue
        best = e.when_utc
    return best


def epoch_weight(key: str, closed_utc: dt.datetime, as_of: Optional[dt.datetime] = None,
                 epochs: Sequence[Epoch] = EPOCHS, old_weight: float = OLD_EPOCH_WEIGHT) -> float:
    """``v``: ``old_weight`` (0.5) for a trade that closed before the key's
    latest epoch, else 1.0. Never compounding: one halving at most."""
    start = latest_epoch(key, as_of, epochs)
    if start is None:
        return 1.0
    return old_weight if _utc(closed_utc) < start else 1.0


def in_current_epoch(key: str, closed_utc: dt.datetime, as_of: Optional[dt.datetime] = None,
                     epochs: Sequence[Epoch] = EPOCHS) -> bool:
    """Did this trade close inside the key's current epoch?"""
    start = latest_epoch(key, as_of, epochs)
    return start is None or _utc(closed_utc) >= start


def touched_since(since: dt.datetime, until: dt.datetime, keys: Iterable[str],
                  epochs: Sequence[Epoch] = EPOCHS) -> list[str]:
    """The keys an epoch dated in (since, until] touches (for the nightly
    digest: "evidence before this date now counts half")."""
    a, b = _utc(since), _utc(until)
    return [k for k in keys if any(a < e.when_utc <= b for e in epochs_for(k, epochs))]


def _utc(t: dt.datetime) -> dt.datetime:
    return t.astimezone(UTC) if t.tzinfo is not None else t.replace(tzinfo=UTC)
