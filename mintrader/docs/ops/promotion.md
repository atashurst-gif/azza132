# How a test earns a place on the demo, and how it loses it

Aaron, 14 Oct 2026. This replaces the 13 Oct weekly pick: the Friday cadence,
the 30-trade bar and the "back to paper after 20 trades" rule. Those rows stay
in `docs/REQUIREMENTS_LEDGER.md`, marked SUPERSEDED.

The code is `mintel/ops/weekly.py`, which the watchdog runs every five
minutes. The module keeps its old name so every page that reads it still works.
The lab-demo engine is `mintel/labdemo/`.

**Words.** PAPER means practice on live prices with no money. DEMO means
genuine orders on the IC Markets demo account, which the code calls LIVE. The
two are never mixed in any figure.

## Who can be promoted

Everything that practises sits in one pool:

- the old bots;
- Trend & Breakout's parts;
- every lab idea (about 640).

Since 14 Oct a lab idea has a real path to the demo. When it is promoted, it
is added to `data/labdemo/roster.json`. The lab-demo engine inside the trader
reads that file and places the orders.

The clock trades (the CONTROL_TIME_OF_DAY family) are never promoted. They
trade at a fixed time with no idea behind them, so they show what pure luck
looks like.

## When

- **The pick:** every UK trading day at 22:30, UK time, in BST and GMT alike.
- **The release:** 06:30 the next UK trading day. Friday's pick is released on
  Monday.
- **The veto:** `python -m mintel.ops.weekly veto` cancels the latest pick. A
  pick already on the demo is taken back. A pick vetoed before it went live,
  and every part that would have gone live with it, gets a `veto` line in the
  history. Its evidence then starts again from the veto, as it does after a
  demotion, so the next day's pick cannot take it again on the same trades.
- **HALT:** while `data/HALT` exists, nothing is applied. A pick that cannot be
  released lapses at the next day's pick.
- **No second chances from one plan:** a pick that was taken back is never
  released again by the same day's plan.

## The early-promotion bar

Only paper trades from the last 28 days count, after all costs. For a test that
was taken back to paper before, or vetoed before it went live, only trades since
then count.

**1. Enough trades for how often it trades.**

| How often it trades | Trades it needs |
|---|---|
| Frequent: 3 or more a UK trading day | 15 |
| Medium: 1 to 3 a day | 12 |
| Slow: under 1 a day | 10 |

The trades must always be spread over at least 3 UK days.

**2. It makes money.** It must be:

- positive after all costs;
- at a profit factor of at least 1.3, which means £1.30 won for every £1 lost;
- still positive with its single best trade taken out.

That last check stops one lucky winner, or a short streak, counting as proof.

**3. It is better than luck.** With about 640 ideas being tested, some will
look good by chance. Two guards deal with that.

- **Shrinkage.** Each test's average result per trade is measured in R: the
  profit after costs divided by the money the trade risked. That average is
  pulled toward the whole pool's average, using a method called
  empirical-Bayes shrinkage:
  - B = noise ÷ (noise + spread);
  - shrunk = B × the pool's average + (1 − B) × its own average.

  Here, *noise* is the test's own sampling variance (the spread of its R values
  divided by its number of trades). *Spread* is how far the pool's true
  averages really differ: the variance of the tests' averages minus their
  average noise. Both are measured on the tests that have at least 5 trades.

  A test with few or erratic trades is pulled most of the way to the pool's
  average. A steady test with many trades keeps most of its own figure. The
  shrunk figure must be above zero.
- **The clock-trade benchmark.** The best clock trade's shrunk figure (among
  clock trades with at least 10 trades) is what luck can do. A candidate must
  beat it by at least 0.05 R a trade.

**4. The lab's own tests are a weight, not a gate.** Each lab idea also has a
test on past prices and a test on fresh prices. Only the latest of each kind
counts:

- a strongly negative one (20 or more trades, losing, profit factor under 0.8)
  halves the idea's rank;
- a mildly negative one (losing) takes 15% off its rank.

A negative test never stops an idea on its own.

## The ranking and the places

The **score** is shrunk R per trade × trades a week × the lab-test weight. In
plain words, it is the R a week the test should make, estimated cautiously.

**The number of places.** Every release fills the demo's free places, best
first. The size of the squad is `demo_target` in `data/weekly.json`:

- the default is 10;
- the most it can be set to is 12;
- Aaron's range is 8 to 12.

A test that was demoted frees its place, and the next release fills it.

**Diversity.** At most 2 tests may be on the demo at once for each market and
direction:

- the market is FX majors, FX minors, indices or gold, or a bot's own market;
- the direction is LONG, SHORT or BOTH.

A Trend & Breakout part that goes live with a pick counts here too. It trades
on the demo like any other test, even though it holds no place in
`demo_target`. A pick waits when it, or any part it would take live with it,
would put a third test in one market and direction.

**Spread.** The queue prefers tests that add variety:

- each test already on the demo in the same market multiplies a candidate's
  place in the queue by 0.85;
- each test already in the same session multiplies it by 0.9.

This changes the order in the queue, never a test's own figures.

**Capacity.** The engine checks the account at every order. It counts the risk
of every position on the account at its stop, across all bots and all ideas.
That combined risk must stay under `demo_risk_cap_pct` in `data/weekly.json`
(the default is 10% of equity). One new position may also use at most half the
free margin.

When the full size does not fit, the order is made smaller. It is never made
larger. When even the broker's smallest lot will not fit, the entry is skipped
and the reason is recorded.

## Sizing tiers on the demo

Each lab idea promoted gets the next risk tier in turn: 0.5%, then 1%, then 2%
of equity a trade, then 0.5% again, and so on. That way the tiers can be
compared.

The tier is stored in four places:

- the roster;
- the promotion in `data/weekly/state.json`;
- its history line;
- every trade in `data/labdemo/labdemo.sqlite`.

The size is the tier's money divided by the stop distance, using the broker's
own money-per-lot figure. It is rounded down to the lot step.

The old bots and Trend & Breakout keep their own sizing, as they always have.

Every demo trade also records its **£1,500-account equivalent**: its R × 0.5%
of £1,500, which is R × £7.50. This shows what the trade would have meant on a
realistic small account. It is stored beside the demo money and never added
into it.

## How a lab idea trades on the demo

The engine is `mintel/labdemo/engine.py`, inside the trader.

**The same rules as practice.** It uses the lab practice engine's own code for:

- the features;
- the family's signals;
- the caps;
- the news-avoid rule;
- the exits: the stop, the target, the trail, the time exit, and the 21:55 UK
  flat (21:00 on Fridays).

**The paper shadow.** The lab keeps practising every idea on paper, including
the promoted ones. Paper and demo can then be compared side by side.

**Every order:**

- carries its stop at the broker, plus a take-profit when the exit has a fixed
  target;
- carries the idea's own magic number, from the reserved range 992001 to
  992999. One number belongs to one idea version, for good;
- first asks the broker for the account, and is refused unless the broker
  says the account is a demo AND config.json says DEMO. Real money cannot
  be reached from this path.

**Managing a position:**

- a trail can only tighten the broker's stop;
- every closed position is booked from the broker's own closed-deal record;
- after a restart, open positions are taken back under management;
- a position under a lab-demo magic number with no record is closed straight
  away.

**When an idea leaves the roster,** its positions are closed straight away. No
restart is needed.

**When new entries stop.** No new entry is made:

- while HALT is on;
- when the daily-loss stop, the drawdown breaker or safe mode shuts the
  account gate;
- when the roster cannot be read.

Managing open positions never stops.

## Judged on the demo

The demo judgement uses the broker's own figures after costs:

- MetaTrader's closed positions, by magic number;
- for a lab idea, `data/labdemo/labdemo.sqlite`, booked from the broker's
  deals.

| When | Back to paper if |
|---|---|
| At any point | its demo loss reaches 15× its risk a trade (the early exit) |
| At any point | it gives back 20× its risk from its best point (the drawdown guard) |
| After 20 demo trades | even the hopeful bootstrap bound (90th percentile) of its average a trade is below zero |
| After 40 demo trades | it is not in profit after costs |

After 20 demo trades, a test is **kept**, and that is recorded once, when even
the cautious bound (10th percentile) is above zero. Otherwise it keeps going
until the evidence is clear.

Every promotion, keep, demotion and veto is one line of
`data/weekly/history.jsonl`, with its reason.

## Safeguards carried over unchanged

- **Intent first.** Each switch is saved as an intent in `state.json` before it
  is made.
- **Read back.** Each switch is read back after it is made.
- **Undone on failure.** If a switch fails, the files are put back exactly as
  they were.
- **Crash-safe.** A switch made just before a crash is recorded once, never
  made twice.
- **Fail closed.** A broken `state.json`, `config.json` or roster stops every
  decision that depends on it, and is never overwritten.

This document describes the rules. Their behaviour against the simulated broker
is checked by `tests/test_weekly_pick.py`, `tests/test_promotion_rules.py` and
`tests/test_lab_demo.py`. Real demo behaviour is confirmed on Aaron's Mac.
