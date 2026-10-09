"""RAPID MOMENTUM RIDER - replaces the Rapid Scalper (magic 990811).

The idea in one line: find a market that has started moving hard, get on
early, ride it while it is strong, get out very quickly when the move dies.
It is not a prediction bot. Every second it asks "which market is running
right now?" and "is this a real directional move or noise?".

Aaron's brief is docs/specs/rapid_momentum_rider_SPEC.md; the plain-English
description is docs/strategies/rapid_momentum_rider.md.

Layout:

* ``config.py``      - data/rider.json (mode, GBP per pip, thresholds)
* ``pipvalue.py``    - GBP per pip -> lots, per instrument, never guessed
* ``bars.py``        - M1/M5 bars and session levels built from ticks
* ``features.py``    - the evidence FAMILIES of the spec, measured
* ``strength.py``    - currency strength and cross-market confirmation
* ``score.py``       - MOMENTUM_OPPORTUNITY_SCORE and the entry states
* ``flowlock_x.py``  - FLOWLOCK X, the stop manager (NEVER WIDEN)
* ``learning.py``    - bucket statistics for the HISTORICAL MATCH category
* ``core.py``        - RiderCore: the pure decision object (no broker IO)
* ``engine.py``      - RiderEngine: RiderCore wired to the broker
* ``journal.py``     - data/rider.sqlite
* ``run.py``         - ``python -m mintel.rider.run``
* ``synth.py``       - SYNTHETIC tick paths for the self-tests (never results)
"""
STRATEGY_ID = "momentum_rider"
STRATEGY_LABEL = "Rapid Momentum Rider"
TAGLINE = ("Finds the FX market that has just started running, gets on early with a real stop at the "
           "broker, rides it while it is strong and gets out fast when the move dies (FlowLock X).")
MAGIC = 990_811
TAG = "RIDER"
