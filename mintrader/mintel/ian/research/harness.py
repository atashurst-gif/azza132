"""Replay events through the intelligence layer and keep what it saw.

Used by the feature test, the benchmark and the test suite. Deterministic:
features are sampled at fixed steps of EVENT time.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from ...broker.base import CalendarEvent
from ..book import received_at
from ..features import FlowConfig, FlowSnapshot, InstrumentFlow
from ..mapping import root_of
from ..regime import RegimeRead, classify
from ..score import Opportunity, news_context, score


@dataclass
class Sample:
    ts: dt.datetime
    fs: FlowSnapshot
    regime: RegimeRead
    opp: Opportunity


def sample(events: Iterable, instrument: Optional[str] = None, every_s: float = 1.0,
           calendar: Sequence[CalendarEvent] = (), cfg: Optional[FlowConfig] = None,
           with_score: bool = True) -> list[Sample]:
    """Feed the events of ONE instrument through features -> regime -> score, sampling every ``every_s``
    seconds of arrival time. ``instrument`` defaults to the first one seen."""
    flow: Optional[InstrumentFlow] = None
    out: list[Sample] = []
    nxt: Optional[dt.datetime] = None
    step = dt.timedelta(seconds=every_s)

    def take(t: dt.datetime) -> None:
        fs = flow.compute(t)
        news = news_context(root_of(flow.instrument), calendar, t) if calendar else None
        reg = classify(fs, news, flow.absorption_episodes)
        opp = score(fs, reg, news=news) if with_score else None
        out.append(Sample(t, fs, reg, opp))

    for ev in events:
        inst = getattr(ev, "instrument", "")
        if not inst:
            continue
        if instrument is None:
            instrument = inst
        if inst != instrument:
            continue
        if flow is None:
            flow = InstrumentFlow(instrument, cfg)
        t = received_at(ev)
        if nxt is None:
            nxt = t
        while t >= nxt:
            take(nxt)
            nxt += step
        flow.on_event(ev)
    if flow is not None and nxt is not None:
        take(nxt)
    return out
