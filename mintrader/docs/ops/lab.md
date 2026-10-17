# The lab: how it runs on the Mac

Aaron, 10 Oct 2026: "Build the lab ... ensure that consistency stays, luck doesn't".

The lab tries trading ideas on old prices, then once on prices it has never seen, then
practises the best on live prices with no money. It **cannot place an order**: its price
feed answers seven read-only questions and nothing else, and its practice trades are rows
in `data/lab/lab.sqlite` marked PRACTICE. Anything to do with real money or sizes is
Aaron's call; the lab's last word is READY FOR LIVE.

Since 13 Oct (Aaron: "I just want all in practice unless any earn the right") EVERY idea
practises, binned ones included: `"practise_all": true` in `data/lab.json` (the default; set
it to `false` for the old roster of six). Each idea is simulated on its own on every stored
market of its kind (up to 24), with its own caps (one position a market, three open, a
five-bar pause, six entries a market a UK day) and no cap across the lab. The nightly past
and fresh tests still run and are shown; they no longer decide who practises. What goes to
real money is the weekly pick's call (`mintel/ops/weekly.py`), on each idea's own practice
record. Features are worked out once a market a bar and shared; measured on made-up prices,
632 ideas x 24 markets cost about 0.1 s a minute (0.3 s at worst) after a start-up of about
13 s. The grid is 732 ideas since 17 Oct (764 when NEWS_MOVE wakes). The 36 "Into the close"
ideas (INTRADAY_MOMENTUM) add 12 signal calls a market a minute on the indices only, during
the US session, each a few look-ups (on an index that is not a US one, a currency check and
nothing more). That cost was not measured separately. The 64 "Bounce off the average" ideas
(VWAP_TREND) add 16 signal settings, about 1.5 ms a market in their worst minute (the London
close; measured on made-up prices, 17 ms for 7 majors and 4 indices), so about 0.02 s on up to
13 markets.

Every Saturday night the nightly's `refresh` step (or `python -m mintel.lab refresh`) refreshes
the pool (`mintel/lab/refresh.py`): an idea with 20+ practice trades in 28 days, net <= 0 and
a profit factor under 0.9 is RETIRED (kept in the ledger, off practice); the ten best by
practice £ get up to three neighbours each (one tunable setting a step away, children of
their parent, judged only on their own trades); grid ideas still waiting - a new family file
dropped into `mintel/lab/families/` is found by itself - fill the pool back to `pool_target`
(640). The bots and Trend & Breakout's parts are never retired: the losing ones are listed
for a look. The report is `reports/lab/<night>/refresh.md` and summary.json's `refresh`.
The weekly pick then sends up to `weekly_max_promotions` (3, `data/weekly.json`) proven
winners to real money each week.

The design is `docs/lab/lab-spec.md`. This page is the operator's view.

## What runs, and when

| part | what | when |
|---|---|---|
| the night | `python -m mintel.lab.nightly --config data/config.json --upload --workers 1` | LaunchAgent `com.mintel.lab`, 23:10 local, Monday to Saturday, `nice -n 19`, stops by 06:00 |
| practice | `python -m mintel.lab.practice --config data/config.json` | the watchdog keeps it running (nice 10) while `data/lab.json` does not say OFF and the lab has chosen its markets |
| a missed night | the launcher starts it in the background | only when Start Trading Bot is double-clicked between 23:10 and 06:00 and tonight's run has not finished |
| by hand | The Lab, option 2 | any time at the weekend; on a weekday only between 23:10 and 06:00 (in the day it says the practice day is not over and stops). A run is never longer than 11 hours, so its lock is never mistaken for a dead one |

The night's ten steps: markets (first night only), old prices (gently, in the quiet hours,
skipped when MetaTrader is not connected), the cut between past and fresh (first night of
a month), worked-out numbers, the practice looks and the bench, the ideas due, the past
tests, at most three fresh looks, the board, the upload to `reports/lab/<night>/`. Each step
is recorded in `data/lab/runs/<night>/state.json`; a run cut short resumes where it left
off, and the deadline stops the past tests between ideas and still writes the board
("ran out of time at 06:00: 412 of 632 tested; the rest tonight").

The night measures how long an idea takes on this Mac (the first ten) and then takes on no
more than 60% of the time left. Until 40 complete trading days of prices are stored it
only collects ("collecting past prices: 31 of 40 days").

## Its files

| file | written by | read by |
|---|---|---|
| `data/lab.json` | `python -m mintel.lab mode off\|paper` (the only writer) | everything in the lab; the watchdog |
| `data/lab/bars/`, `data/lab/universe.json`, `data/lab/cut.json` | the night | the night, practice |
| `data/lab/lab.sqlite` | the night; practice writes only its `practice_*` tables | the board, practice |
| `data/lab/summary.json` | the night's board; practice refreshes its own figures every 30 s | the page, the pulse |
| `data/lab-status.json` | practice, every 30 s | the watchdog (its sign of life), the page, the pulse |
| `data/lab/runs/<night>/` | the night: `state.json`, `report.md`, `tests.csv`, `done` | Claude's nightly review |
| `logs/lab-nightly.log`, `logs/lab.out.log` | the night, practice | people |

The lab writes **no heartbeat file**: a heartbeat the trader does not know would stop the
trader's own entries. The watchdog judges practice by `data/lab-status.json` instead
(older than 3 minutes: it is restarted). The lab never opens the secrets file; the
upload's GitHub token is read by the report uploader's own reader.

## Commands

    python -m mintel.lab show --config data/config.json            # the board in plain text
    python -m mintel.lab show --one-line --config data/config.json # the lab in one sentence
    python -m mintel.lab mode show|off|paper --config data/config.json
    python -m mintel.lab reopen <id> --reason "..." --config data/config.json   # Claude's call, logged
    python -m mintel.lab nightly --config data/config.json --no-fetch --max-candidates 0

To stop a night cleanly: create `data/lab/STOP` (it stops between steps); delete it to let
the lab run again. Pausing (`mode off`, or the icon's option 3) stops practice within a
minute and makes the night skip. A practising idea keeps its look dates while the lab is
paused, so a long pause can cost it its place ("trades too rarely").

## The Lab icon

The Desktop icon prints the lab in one line, then: 1) open its page (`/lab`), 2) run the
lab now (in the window, `nice -n 19`, the log follows), 3) pause or resume, 4) quit. It
never asks about live: the lab has no live path.

## First nights on the Mac (in this order)

1. `nightly --no-fetch --max-candidates 0`: chooses the markets (universe only).
2. The first fetch, at the weekend or in the night window: read `depth_days` per market in
   `logs/lab-nightly.log`. This is where IC Markets' minute-price depth becomes a number.
3. `--max-candidates 50` one night: read `seconds_per_candidate` on the page or in
   `runs`. This is where the Mac's speed becomes a number.
4. Then the full nights. Watch the pulse's scan timings while the fetch runs; if they rise,
   fetch at weekends only.

## Made-up prices

A store filled from the simulated broker (or `mintel/lab/synth.py`) is stamped SYNTHETIC,
and every result built from it says `synthetic: true` all the way to the page, which then
shows "TEST DATA - not a result" in place of every lab figure. `report.md` says the same at
its top.
