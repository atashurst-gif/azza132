# The coach: how it runs on the Mac

Aaron, 12 Oct 2026: "become a master trader, continuously learn, tweak bots off the back of real
results ... make as much profit after commission as possible." And, the same day: "Stop worrying
about limits now, remove them as and when as this is not REAL money ... aim for the stars but keep
in mind that we treat trades as if it were real money."

The coach decides three things: **which work uses real money, how much, and in which market
conditions.** It never decides whether practice happens (everything that is switched on keeps
practising), never invents a rule, never loosens a scanner filter, and **cannot place, close or
change an order**. Its one instruction to the bots is the file `data/coach/policy.json`, which each
bot's gate reads at every entry decision.

The design is `docs/conductor/spec.md` (section 21, the demo instruction, overrides the earlier
sections where they conflict). This page is the operator's view.

## What runs, and when

| part | what | when |
|---|---|---|
| the coach | `python -m mintel.coach run --config data/config.json` | the watchdog keeps it running (nice 10) while the envelope does not say OFF. The watchdog checks that `data/coach/status.json` is fresh (180 s): the coach writes **no heartbeat** |
| the tick | data checks, the page's figures, the brakes, the day's checks (only ever restrict), the policy rewritten | every 60 seconds, every day (Financial Ian trades at weekends) |
| the nightly review | the full refit, the luck check, Aaron's standing rules, the moves up into tomorrow's plan, the scorecard, the digest | 22:20 UK (BST or GMT, whatever the clocks say); if the coach was down, at its next start before the 06:30 release |
| the release | last night's plan takes effect unless an item was vetoed | 06:30 UK on the next UK trading day (a Friday night's plan: Monday) |

## The ratchet: loosen only at night, tighten at any time

* **Up** (a live trial, trial to the normal size, a step up, a demo daily-stop level, Formula 1's
  judgement, a floor): computed only at 22:20, announced that evening on the page and in
  `data/coach/report-<date>.md`, and released at 06:30 UK unless vetoed.
* **Down** (a key's running check of its real results, a gap of -2.5 R, its drawdown, a trial losing
  £30, Aaron's three-days rule, a market negative on 2/3 of 6 or more days, the brakes): at once, on
  any tick, each with its sentence and its numbers in the ledger.
* Today's profit or loss, and progress against £100 a day, are never inputs to anything that adds
  risk. A step up needs the key at a **new high** of its real record: it never grows right after a
  loss. Nothing ever doubles after a loss, averages into a loser, runs a grid or adds exposure
  because the last trade lost.

## The demo (section 21)

While **both** config.json says DEMO **and** the broker reports a demo account (the trader writes
that check into `data/coach/weather.json` every minute; older than 180 s counts as no), the coach
may lift limits step by step, only for keys that have earned it on real results:

* sizes past 1.5% a trade: S5 2%, S6 2.5%, S7 3%, S8 4%, S9 5% (half-Kelly on the lower bound, at a
  new high, 20 real trades at the current size, at most one step a night and three a week, the
  stop-safety check run against the daily stop in force);
* trials start at the normal size (at most 4 at once, £150 a week of trial losses);
* the daily stop 3 -> 4 -> 5 -> 6 -> 8 -> 10%, one level a night, only with two or more proven keys
  live, a bootstrap that trips the new level on 5% of days or fewer, and a day in the last ten when
  the stop blocked a proven key and the day still ended up.

The bots read a lifted value only through the gate (`effective_limits()`), only from a fresh lease,
and only while the demo test passes. **On a real-money account every lifted value falls back to the
v1 limits at once** and the page says "Real-money limits in force". For every real trade the coach
sized, the ledger keeps what it would have risked at real-money sizes and its result at that size
(`shadow` table), so the page can say "today +£64; at real-money sizes +£19". config.json is never
written.

## Aaron's switches

| what | how | effect |
|---|---|---|
| coach off | The Coach icon, option 2, or `python -m mintel.coach off` (writes `data/COACH-OFF`) | every bot back on your own settings at its next decision; `on` removes it |
| HALT | icon option 3 (typed YES), or `python -m mintel.coach halt` | no new real-money entries in any bot; open trades managed as usual; practice goes on. `resume` lifts it |
| veto | icon option 5, or `python -m mintel.coach veto <item>` | that item of tonight's plan is not released |
| approve | `python -m mintel.coach approve f1-jump` (Formula 1's jump to the top size, after a "keep" judgement) or `approve guard` (lets the coach out of its own GUARD once you have looked) | |
| mode | `python -m mintel.coach watch | guard | pilot` | rewrites only the envelope's `mode` |
| Formula 1's pin | `pin formula1` / `unpin formula1` | |

These commands are the only writers of `data/coach.json` (the envelope), `data/HALT`,
`data/COACH-OFF` and `data/coach/approvals.json`. The coach process never writes any of them.

## If something goes wrong

| situation | real money |
|---|---|
| COACH-OFF, the envelope OFF or invalid | exactly your settings |
| the coach dead more than 3 minutes | your settings with its last restrictions; its grants lapse |
| the policy corrupt, forged, rolled back or from an old envelope | the last good restrictions, else your settings |
| the page or the broker check down | no grants; restrictions continue; brakes never released |
| 3 failed ticks in a row, more than 20 policy changes a day, more than 3 trial starts a week, or the audit finds a real decision above its grant | the coach puts itself in GUARD (may only restrict) and asks you |

## Its files (all under `data/coach/`)

| file | what |
|---|---|
| `policy.json`, `policy.prev.json` | the instruction to the bots (a 180-second lease, rewritten every minute) and the one before |
| `status.json` | what the coach is doing, for the page, the pulse and the watchdog |
| `ledger.sqlite` | every decision with its sentence and numbers (append-only), each key's state, the policies, the saved equity peak, the luck checks, the plans, the scorecard and the real-money shadow figures |
| `plan-<date>.json`, `report-<date>.md`, `reports/<date>/` | tomorrow's plan and the nightly digest (`report.md`, `decisions.csv`, `plan.json`, `policy.json`) |
| `gate/<bot>-<date>.jsonl` | written by each bot's gate: every real or would-be-real decision |
| `weather.json` | written by Trend & Breakout: the market board and the demo-account check |

The nightly digest goes to `reports/coach/<date>/` through `report_upload.upload`. The GitHub token
lives only in the secrets file, which the coach never opens, so the coach leaves the files in
`data/coach/reports/<date>/` and the pulse (which holds the token) sends them
(`mintel.coach.report.upload_files`).

## Read it

* `python -m mintel.coach show --once` - one line; `show` - the whole status in plain words.
* `python -m mintel.coach explain "tnb|SESSION_EXPANSION|INDEX|TREND"` - one key: its state, its
  numbers, its decisions.
* `python -m mintel.coach replay --from 2026-09-17` - what the coach would have done each day on the
  record so far, each day from earlier data only. It is stamped "flatters: the segments were picked
  by people looking at this same record".
