"""Rapid Momentum Rider - the research suite.

The question (Aaron's brief): WHAT CONDITIONS EXIST IMMEDIATELY BEFORE A
LARGE CLEAN SHORT-TERM PRICE RUN? - and does the bot, as built, make money
after every cost when it is replayed tick by tick over real history?

One command on the machine that has MetaTrader (the Mac, through the
bridge)::

    python -m mintel.rider.research --config data/config.json --days 10 --upload

fetches the broker's own tick history, runs everything below and writes
``data/rider-research/<date>/report.md`` and ``summary.json``.
``--synthetic`` runs the same pipeline on made-up prices to prove the
machinery works; everything it writes is labelled SYNTHETIC - NOT
PERFORMANCE.

Modules:

* ``tickstore.py``  - real ticks from ``broker.ticks_range`` in hour chunks,
  stored as ``ticks/<SYMBOL>/<YYYY-MM-DD>.csv.gz`` (time_ms, bid, ask),
  with resume, progress, a size budget and a coverage report; an empty
  busy hour is never stored as final (MetaTrader is asked to load it and
  asked again), an answer for the wrong hour is measured and corrected, and
  ``--probe`` checks in one command that past ticks can be read
* ``synthetic.py``  - the seeded SYNTHETIC generator (trends, chop,
  squeezes, news-like jumps) - never a result
* ``labeler.py``    - SUCCESSFUL MOMENTUM RUN labels: X pips favourable
  before Y pips adverse within H
* ``eventstudy.py`` - what the evidence families looked like 30 s and 10 s
  before a clean run and at its start, against ordinary moments
* ``costs.py``      - spread, slippage, commission and their stress
* ``replay.py``     - RiderCore driven tick by tick exactly as live, with
  FlowLock X managing every position
* ``walkforward.py``- time-ordered, purged folds; a small grid; out-of-sample
  only
* ``registry.py``   - the research registry (sqlite): every variant run
* ``stats.py``      - trade statistics shared by everything above
* ``report.py``     - report.md + summary.json in plain English
* ``publish.py``    - the GitHub reports branch
* ``pipeline.py``   - runs it all (in parallel, with a resumable cache)

Honesty rules kept everywhere: every figure comes from a run that was
actually done and carries its data source; nothing is invented; when the
history is too short the report says INSUFFICIENT DATA; the best of many
variants is never presented as the result.
"""
SYNTHETIC_LABEL = "SYNTHETIC - NOT PERFORMANCE"
REAL_SOURCE = "REAL broker ticks (MetaTrader ticks_range: every tick, bid and ask)"
SYNTHETIC_SOURCE = "SYNTHETIC seeded price paths (made up - not market data)"
