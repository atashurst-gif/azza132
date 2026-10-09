"""The feed adapter interface: every source of order-book data looks like this.

The strategy never knows which vendor it is reading. An adapter turns the
vendor's records into the normalised events of :mod:`mintel.ian.book` and
reports its own health in plain words. Adding a vendor (Rithmic, CQG, a
futures broker's API, another historical provider) means writing one more
adapter; nothing else changes.

An adapter must NEVER raise into the engine: a missing package, a missing
key, a refused login or a dropped connection is a state (NOT CONFIGURED,
DOWN) with a reason, not an exception.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Optional, Sequence

from .. import INSTITUTIONAL
from ..book import Event

NOT_CONFIGURED = "NOT CONFIGURED"
CONNECTING = "CONNECTING"
LIVE = "LIVE"
DEGRADED = "DEGRADED"
DOWN = "DOWN"
FINISHED = "FINISHED"          # a replay or a synthetic scenario that has run out of events


@dataclass
class FeedHealth:
    state: str
    reason: str
    vendor: str
    data_class: str = INSTITUTIONAL
    synthetic: bool = False
    connected: bool = False
    events: int = 0
    last_event_utc: Optional[str] = None
    last_error: str = ""
    reconnects: int = 0
    subscribed: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"state": self.state, "reason": self.reason, "vendor": self.vendor, "data_class": self.data_class,
                "synthetic": self.synthetic, "connected": self.connected, "events": self.events,
                "last_event_utc": self.last_event_utc, "last_error": self.last_error,
                "reconnects": self.reconnects, "subscribed": list(self.subscribed)}


class FeedAdapter:
    """Base class. Subclasses override connect/subscribe/events/health."""

    vendor = "none"
    data_class = INSTITUTIONAL
    synthetic = False               # True for generated data: never performance, never LIVE orders
    virtual_time = False            # True when time is the events' own (a replay, a scenario)
    sequence_scope = "instrument"   # "instrument": contiguous per instrument; "channel"/"none": per-instrument gaps are normal

    def connect(self) -> bool:
        return False

    def subscribe(self, instruments: Sequence[str]) -> None:
        pass

    def events(self, max_events: int = 10_000) -> list[Event]:
        """Whatever has arrived, oldest first; never blocks for long, never raises."""
        return []

    def health(self) -> FeedHealth:
        return FeedHealth(NOT_CONFIGURED, "no feed", self.vendor, self.data_class, self.synthetic)

    def recover(self, reason: str = "") -> bool:
        """Try to get a clean book again (reconnect, resubscribe, ask for a
        snapshot). Returns True if an attempt was made."""
        try:
            self.close()
        except Exception:
            pass
        try:
            return bool(self.connect())
        except Exception:
            return False

    def now(self) -> Optional[dt.datetime]:
        """For a virtual-time feed, the time of the last event delivered."""
        return None

    def close(self) -> None:
        pass


class NotConfiguredFeed(FeedAdapter):
    """What the engine gets when no institutional feed is set up. It produces
    nothing and says why, so the bot sits in DATA-DEGRADED: no signals, no
    trades."""

    def __init__(self, reason: str = "no institutional order-book feed is configured", vendor: str = "none"):
        self.reason = reason
        self.vendor = vendor

    def connect(self) -> bool:
        return False

    def health(self) -> FeedHealth:
        return FeedHealth(NOT_CONFIGURED, self.reason, self.vendor, INSTITUTIONAL, False, False)
