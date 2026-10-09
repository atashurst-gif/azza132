# Trend & Breakout on PAPER

**The same code decides; only the order goes nowhere.**

Decided on 8 October 2026 (about 22:30 UTC). Aaron asked for the weaker of
Trend & Breakout and the Momentum Runner to go to paper and the winner to
stay live. On the same 14 index entries since the 5 October reset, Trend &
Breakout's ladder made +79.12 GBP and the Runner's 3 R trail made +114.89.
Trend & Breakout's overall total was -12.40, because its 28 FX and metals
trades lost 91.52. So Trend & Breakout goes to PAPER and the Momentum Runner
stays LIVE.

## What PAPER means here

Trend & Breakout keeps running in full: the same scan, the same scores, the
same sizing against the real account, the same stops, the same trailing and
the same journal. The only difference is the broker it is given. It gets a
wrapper (`mintel/broker/paper.py`, `PaperBroker`) around the real one:

- **Prices, charts, symbols, contract details, the calendar and the account
  come from the real broker.** The account balance and equity are the real
  ones, so trade sizes are exactly what they would be live.
- **Every order is simulated.** Opening, moving a stop or target, closing and
  part-closing never reach MetaTrader. The tests prove this with a broker
  whose order functions raise an error if they are ever called.
- Paper trades are kept in `data/tnb_paper.sqlite`, so a restart keeps them.
  Their ticket numbers start at 960000001, so they can never be confused
  with a real ticket or another bot's paper tickets.
- Only the running bot may write to that record. The wrapper has a
  read-only mode for reports run in other programs, which can never change
  the record or book a trade twice. Every report reads the record that way
  (step 3 of "The wiring" below), and so does the installer's check.
- Paper money never reaches the real account. The status page labels
  Trend & Breakout PAPER, shows its paper result as practice on its own
  card and leaves it out of Overall and out of the Account figures, which
  come from MetaTrader alone.

## How a paper trade is filled

- **Entry:** at the real price at that moment. A buy fills at the ask and a
  sell at the bid, plus a slippage against the trade (`slippage_points`).
  It is 1 point by default, the same as the other paper bots.
- **Refused,** in the same form as the real broker's refusal, when:
  - there is no price;
  - the last price is more than 2 minutes old (reported as "market closed"
    at the weekend);
  - the broker has the symbol closed to new orders;
  - the size is below the minimum, above the maximum or not a whole number
    of lot steps;
  - the stop or target is on the wrong side of the price, or closer than the
    broker allows.
- **Stop:** held as the broker would hold it. It is checked every time the
  bot reads a price for that market and every time it looks at its
  positions. Every price printed since the last check is replayed too,
  oldest first, so a quick spike through the stop is not missed, however
  often the bot reads. Only prices that have already printed are used,
  never a later one.
  - A buy is stopped when the bid reaches the stop, and a sell when the ask
    does.
  - The fill is at the stop, or at the worse price if the market jumped
    past it, and then the slippage worse again (a triggered stop is a
    market order). It is never better than the stop.
  - Before a stop or target is moved, and before a close or part-close,
    every earlier price is checked against the old levels and the full
    size. If the price history cannot be read at that moment, the change
    is refused with a plain message and nothing changes; the bot tries
    again on its next pass.
- **Target:** filled only when the price has gone *through* it, not just
  touched it, and then exactly at the target. It is never better than the
  target.
- **Moving the stop or target:** treated as the broker would treat it. A
  valid level is accepted and an invalid one is refused. The bot's own
  "never widen a stop" rule still applies, one layer above. A stop of zero
  keeps the existing stop: paper never removes a protective stop.
- **Closing:** at the current bid (for a buy) or ask (for a sell), less the
  slippage.

## Money

- **Profit** is in pounds, worked out from the broker's own value of a price
  step, the same figure every other part of the system uses.
- **Commission** is charged on the way in and on the way out, at IC Markets
  Raw rates:
  - USD 3.50 per lot per side on FX and metals, converted to pounds at the
    broker's rate. That is about 5.50 GBP a lot for the round trip, which is
    what the broker's own deals showed in early October.
  - Nothing on indices.
  - USD 3.50 per side on anything else, so a cost is never assumed to be
    zero.
  - The rates can be changed per kind of market.
- **Swap is not simulated.** It is always 0.
- The paper deal records have exactly the same form as MetaTrader's. So the
  journal books a paper trade the same way as a real one, the commission the
  bot learns comes from them, and the daily-loss stop counts paper money.
  The daily-loss stop's code is unchanged.

## How paper can differ from the real broker

- **No queue and no requotes.** A paper order always fills at the current
  price in full, with no partial fills and no delay. The real broker can
  requote, part-fill, reject at busy moments or take a moment to fill.
- **Stops fill on the price printed, plus a fixed slippage.** On the real
  broker a stop becomes a market order and can slip further than 1 point
  in a fast market. Paper fills it at the stop (or the printed price if
  the market jumped past it) and 1 point worse, never more.
- **Targets are slightly pessimistic.** Paper needs the price to go through
  a target. The real broker fills a target as soon as the price touches it.
- **Swap is ignored,** so a trade held overnight looks a little better on
  paper than it would have been for real (or a little worse when the swap
  is positive).
- **Margin is not used.** The real account's margin does not change when
  paper trades are open, so paper is never refused for lack of margin.
- **Price checks depend on looking.** Stops and targets are checked when the
  bot reads prices (about once a second) and by replaying the price
  history since the last full check. After a gap (a restart, a sleeping
  Mac) the whole gap is read, an hour at a time, back to 3 days. Anything
  older than 3 days is checked on one-minute bars, which is coarser: a bar
  that reaches both the stop and the target counts as the stop, and the
  exact second of the fill is not known. If the history cannot be read,
  the current price is still checked and the gap is read again later; it
  is only given up (with a warning in the log) if it stays unreadable for
  15 minutes.

## The Momentum Runner still rides its entries with real orders

The Runner (magic 990511) rides Trend & Breakout's index entries. Trend &
Breakout still decides every entry on real prices, and the Runner still sees
each fresh one. It opens its **own real order** beside each fresh index
entry, with the same market, side and size and the same original stop, as
before. The only difference is that the trade it rides is now a paper one.

Two things keep this safe:

1. The Runner is given the **real** broker, never the paper wrapper: the
   trader (`Trader._bot_kwargs`) hands every other bot
   `real_broker(self.broker)`, the broker behind the wrapper (wired 9 Oct).
   If it were ever given the wrapper by mistake, the wrapper would refuse
   its orders with a plain message and send nothing; nothing is silently
   simulated, but the Runner would then stop entering, and its open real
   trades would no longer be trailed or closed at the 8-hour mark (only
   their first stop would protect them). A test proves the trader itself
   gives it the real broker, and that its order reaches the broker under
   990511 while Trend & Breakout's side stays on paper.
2. The wrapper shows Trend & Breakout only its own trades: its paper ones,
   and any real ones carrying its own number (990311). It never shows,
   moves or closes another bot's real position. Asking it for another bot's
   positions, or for the whole account, gets the real broker's answer
   unchanged.

## Keeping some markets live (optional)

The wrapper can send chosen kinds of market straight to the real broker while
everything else stays on paper. For example, Trend & Breakout could stay live
on indices only, beside the Runner (`live_groups: ["INDEX"]`). The default is
empty, which puts everything on paper.

With live markets set:

- Trend & Breakout sees its real and paper trades together.
- Each change goes where the trade was born. A paper trade never reaches
  the broker, and a real one always does.
- The status summary marks every trade as REAL or PAPER.

## A real trade left open at the switch

A real Trend & Breakout trade that is still open when the switch happens
stays on the real broker, with its own stop and target. Trend & Breakout can
still see it.

- **As set up (`tnb.manage_leftovers`, default `true`),** Trend & Breakout
  winds it down normally at the real broker to its natural end: it can
  tighten the stop and close the trade, but it never places a new order.
  The journal books the broker's real figure. (A test opens a real trade,
  switches to paper, and follows it to its broker stop.)
- **With `manage_leftovers` set to `false`,** nothing is sent for it. The
  wrapper refuses any change, and the trade ends at its own stop or target.
  (The wrapper's own default, used when it is built by hand, is this one.)

## The setting

```json
"tnb": {"mode": "PAPER", "slippage_points": 1.0, "live_groups": [], "manage_leftovers": true}
```

in `config.json` (`mintel/config.py`, `TnbConfig`). `mode` is `LIVE` (the
default, so nothing changes until the installer's line-up step sets it)
or `PAPER`. Anything else - a typo, a wrong type, a block that is not a
table - is read as **LIVE**, with a warning in the log, and never stops the
start; a bad number falls back to its default the same way. The other
fields are the wrapper's settings (`PaperConfig` names: `slippage_points`,
`live_groups`, `manage_leftovers`, `commission`, `fx_rates`,
`max_tick_age_seconds`, `replay_*`); `PaperBroker.from_config` reads them
through `cfg.tnb_paper`, which is the block while the mode is PAPER and
nothing otherwise. Code that needs the mode reads `config.tnb_mode(cfg)`.

At start the log says, in one line:

    Trend & Breakout: PAPER - real prices, simulated orders; the Momentum Runner still rides its index entries with real orders

(or `Trend & Breakout: LIVE - its orders go to the broker`). If the paper
record cannot be opened, the bot does not start (exit code 6) rather than
trade Trend & Breakout live.

## The wiring

All four steps were made on 9 October. One thing in step 3 is still not
wired, and it is said there.

1. **Done - where the wrapper goes: `mintel/run.py`, in `main()`,** at the
   Trader's construction (after `wait_for_broker`):
   `Trader(trend_and_breakout_broker(broker, cfg), cfg)`, which is
   `PaperBroker.from_config(broker, cfg)` on PAPER and `broker` itself on
   LIVE. There and nowhere else. **Never inside `build_broker()`:**
   Financial Ian, the scalper and the ops tools (reconcile, day review,
   report upload, ledger check, potential) all call it. Each would get its
   own paper record writer on the same file: their real orders would be
   refused, and the same paper stop could be booked twice.
2. **Done - the other bots get the real broker: `mintel/engine/trader.py`.**
   `Trader._bot_kwargs` hands the Momentum Runner, the Band Breaker and the
   Crowd Fader `real_broker(self.broker)`, and the account health checks
   and the broker's calendar get it too (`Trader.real_broker`). Trend &
   Breakout's own executor and scanner keep the wrapper. The "expected to
   fail" mark in `tests/test_paper_broker.py` is gone: the test passes.
   While on paper the scanner may also offer the Runner's feed approaches
   on indices (`runner.feed_tactics`, empty for now; see
   docs/strategies/momentum_runner.md), tagged `runner_feed`.
   A dry-run Trader (the installer's check, below) is given a read-only
   view of the paper record instead (`dry_run_view`), and starts none of
   the other bots.
3. **Done - the reports.** Each reads Trend & Breakout's paper figures
   from its record, read-only (`mintel.ops.standing.TnbPaperRecord`, which
   opens `PaperBroker(..., read_only=True)`), and its real ones from the
   real broker. Paper is never added to the account.
   - `mintel/ops/attribution.py` (the strategy tabs): each Trend & Breakout
     row is marked PAPER or LIVE by the paper record; the PAPER ones stay
     on its own tab and are left out of Overall.
   - `mintel/ops/standing.py` (the Account card and the bot cards): the
     account's figures are MetaTrader's alone, read from the real broker
     behind the wrapper; Trend & Breakout's card says PAPER and shows its
     practice result apart.
   - `mintel/ops/day_review.py`, `mintel/ops/ledger_check.py` and
     `mintel/ops/report_upload.py` (the nightly report): its real deals
     from the broker, its practice from the record, each labelled.
   - `mintel/ops/pulse.py` (the ten-minute pulse): `today_pnl` is the
     account's result for the UK day (from 00:00 UK, each closed trade in full), `open_positions` every real
     open position on the account, and on PAPER a separate
     `tnb_practice_today` and `tnb_mode`.
   - `mintel/ops/reconcile.py` keeps the real broker alone: it checks the
     account itself.
   - **Not wired yet:** no report leaves out trades tagged `runner_feed`
     (`mintel.runner.feed.is_feed_trade(row)` exists for it). With
     `runner.feed_tactics` empty, as it is by default, no such trade is
     made.
4. **Done - the setting: `mintel/config.py`,** the `tnb` block above. The
   Mac installer's one-time line-up step (marker `.lineup-2026-10-09`)
   sets it to PAPER.

## The installer's check

`python -m mintel.verify` (run by the installers after the bots start)
runs a dry-run Trader beside the running one, on the same journal. On PAPER
it reads the paper record read-only and prints "Trend & Breakout on PAPER"
with the number of paper trades open; it never books or writes anything
there. It reads the paper record again once it has started up, so a paper
trade the running bot opens in the meantime is never marked closed in the
journal they share. On PAPER and on LIVE alike every call that would open, move or close
a real position is refused before it reaches MetaTrader (on LIVE the dry
run would otherwise manage Trend & Breakout's real trades too), and a
trade the running bot already holds is never reported as "an order was
sent".
