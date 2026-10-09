"""Financial Ian: fixes from the fourth review (9 October).

* A fill the executor closes at once is booked even when the broker's closing deal reaches its history
  only after the bridge's quick checks: the order is on record as UNKNOWN with the fill's ticket, and the
  sweep books it (NO_STOP, the broker's own figure) as soon as that deal shows - never marked ABSENT with
  no trade row - so this bot's own daily loss limit, its entries per day and today's figures count it.
* This bot's own daily loss limit is asked again for every candidate in a pass: a fill closed at once and
  booked as a loss earlier in the same pass stops the next candidate being sent.

All data here is synthetic test data; nothing in this file is a result.
"""
import datetime as dt

import pytest

import mintel.broker.sim as sim_module
from mintel.ian import MAGIC
from mintel.ian.book import received_at
from mintel.ian.engine import IanConfig
from mintel.ian.feeds.synthetic import generate
from tests.test_ian_engine import drive
from tests.test_ian_review_fixes import BlindBroker, live_engine, minutes, no_retry_pause


class LateDealBroker(BlindBroker):
    """The broker fills the order but drops the stop sent with it and refuses the stop again, so the executor
    closes the position at once - and that close goes through. The broker's deal history, though, shows no
    closing deal for the first ``lag`` reads (the history lags the close), so the bridge's quick checks
    cannot confirm the close."""

    def __init__(self, *a, lag=4, **k):
        super().__init__(*a, **k)
        self.sim.fault.drop_stops_on_send = True
        self.sim.fault.fail_modify = True
        self.lag = lag

    def closed_deal(self, ticket):
        if self.lag > 0:
            self.lag -= 1
            return None
        return self.sim.closed_deal(ticket)


class TestAFillClosedAtOnceWhoseDealShowsLateIsBooked:
    def test_it_is_booked_once_the_deal_shows_and_counts_today(self, tmp_path, monkeypatch):
        no_retry_pause(monkeypatch)
        cfg = IanConfig(mode="LIVE", record=False, max_daily_loss_money=0.01)
        e, feed, clock, broker = live_engine(tmp_path, cfg=cfg, broker_cls=LateDealBroker)
        notes = drive(e, feed, clock, generate("sweep_up")[0])
        notes += minutes(e, clock, 4)                              # long enough for the sweep to mark it ABSENT
        assert broker.sim.positions(MAGIC) == [] and e.open == {}, notes
        (closed,) = broker.sim.closed
        row = e.journal.trade(closed["ticket"])
        assert row is not None, f"ticket {closed['ticket']} closed at the broker but never booked: {notes}"
        deal = broker.sim.closed_deal(closed["ticket"])
        assert row["exit_reason"] == "NO_STOP" and row["net_pnl"] == pytest.approx(round(deal["pnl"], 2))
        assert row["mode"] == "LIVE" and row["data_source"] == "LIVE FEED" and row["root"] == "6E"
        assert row["entry"] == pytest.approx(closed["entry"]) and row["volume"] == pytest.approx(closed["volume"])
        # booked on the pass right after the send, not minutes later
        opened, booked = (dt.datetime.fromisoformat(row[k]) for k in ("opened_utc", "closed_utc"))
        assert booked - opened <= dt.timedelta(seconds=2), (opened, booked)
        order = e.journal.order(row["signal_id"])
        assert order["state"] == "REJECTED" and order["ticket"] == closed["ticket"]
        assert not any(ev["kind"] == "ABSENT" for ev in e.journal.events())
        today = e.status(clock.t)["today"]
        assert today["trades"] == 1 and today["net"] < 0
        assert "lost" in e._entry_block(clock.t)                    # this bot's own daily loss limit sees it
        assert len(broker.sends) == 1, notes
        e.close()


class TestTheDailyLimitIsAskedForEveryCandidate:
    def test_a_loss_booked_earlier_in_the_pass_stops_the_next_candidate(self, tmp_path, monkeypatch):
        no_retry_pause(monkeypatch)
        # SimBroker picks each symbol's market from hash(symbol), which Python salts per process: here every
        # spot chart is the same trending one in every run, so both futures' signals stand on the same footing
        monkeypatch.setattr(sim_module, "hash", lambda s: 0, raising=False)
        cfg = IanConfig(mode="LIVE", record=False, max_daily_loss_money=0.01)
        e, feed, clock, broker = live_engine(tmp_path, cfg=cfg)
        broker.sim.fault.drop_stops_on_send = True                  # the broker ignored the stop ...
        broker.sim.fault.fail_modify = True                         # ... and refused it again: closed at once
        both = sorted(generate("sweep_up")[0] + generate("sweep_up", instrument="6BZ6")[0], key=received_at)
        # 6E alone turns tradeable a little before 6B: entries are held back until the first pass in which
        # both are candidates, and run as normal from then on
        seen = []                                                   # (candidates, market notes after that pass)
        orig = e._entries

        def entries(now, snaps):
            cand = sorted(r for r, o in e.opps.items() if o.tradeable and r in snaps)
            if seen:
                return orig(now, snaps)
            if len(cand) < 2:
                return []
            out = orig(now, snaps)
            seen.append((cand, dict(e.notes)))
            return out
        e._entries = entries
        notes = drive(e, feed, clock, both)
        cand, after = seen[0]
        assert cand == ["6B", "6E"], seen                           # one pass, two candidates
        assert len(broker.sends) == 1, notes                        # the first one's loss stopped the second
        (closed,) = broker.sim.closed
        row = e.journal.trade(closed["ticket"])
        assert row["exit_reason"] == "NO_STOP" and row["net_pnl"] < 0
        other = next(r for r in cand if r != row["root"])
        assert "Not entered: this bot has lost" in after[other], after[other]
        e.close()
