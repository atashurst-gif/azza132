# The lab - build spec v1 (10 Oct 2026)

Aaron, 10 Oct 2026: "Build the lab let's do it but just ensure that consistency stays, luck doesn't, the quicker we can find winning trade functions the sooner we can go live with real money. Make sure everything is presented in the bot showcase really simply for me and not over the top, put it all nice and clear in layman terms and change the ui to suit this" and "We can then just add in the current ones to that and have them all together so it's simpler, each day the winners stand out and we watch them closely like scouts".

This file is the design that three independent designers produced and a judge merged. Builders follow it exactly unless the code proves a point wrong; any deviation is reported with its evidence. Paper-side calls are Claude's (Aaron, 9 Oct: "anything on paper is your call"); LIVE and live sizes are Aaron's. Never invent performance. Legal market behaviour only. Never touch the daily-loss stop. Secrets only in data/secrets.json (the lab never opens it).

## 0. Decisions on one screen

- One store of M1 bars in integer points, sliced by a date cut into PAST (before) and FRESH (from the cut). The cut moves forward only, once a month. A candidate gets exactly one fresh look, ever; at most 30 fresh looks per stretch and 3 per night.
- Candidates are frozen definitions (never fitted) from pre-registered grids. The grid set is hashed into the stretch, so N is known before anything runs and cannot grow mid-stretch.
- One simulator (`mintel/lab/sim.py`) does every fill and every cost, for past, fresh and practice alike. Pessimistic by construction. GBP 13 risk a trade (Formula 1's figure) so practice money reads like live money.
- Stages, exact words: **IDEA - PAST TEST - FRESH TEST - ON THE BENCH - PRACTICE - READY FOR LIVE - LIVE TRIAL - PROVEN - BINNED**. Everything up to READY FOR LIVE and every BIN is the rule's; anything to do with real money or sizes is Aaron's.
- Five checks on every row, bots included: **Enough trades - Steady - Fresh prices - Costs - Luck check**, each Passed / Failed / Running (n of m) / Not yet / Not tested this way.
- The lab cannot place an order. Its price feed exposes read-only methods and nothing else. `mintel/lab/` holds no order code, writes no heartbeat (the watchdog probes its status file), never opens `secrets.json`. Lab trades live only in `data/lab/lab.sqlite` with `mode='PRACTICE'`.
- Nightly at 23:10 local, `nice -n 19`, one process, hard deadline 06:00, resumable. The practice process is managed by the watchdog like the Rider and Ian (nice 10).
- Pessimistic planning: IC Markets' M1 depth is UNVERIFIED. The lab starts when the store holds 40 complete trading days and says "collecting past prices: D of 40 days" until then. Throughput on the Mac is UNMEASURED: the nightly records seconds per candidate and sizes the next batch itself.
- Runtime is stdlib only (no numpy/pandas on the Mac venv).

## 1. Files and ownership (disjoint)

**Builder A - data, simulator, families, statistics** (new files only): `mintel/lab/__init__.py` (constants, paths, `LabSettings`, stage/check/chip constants, schema version, the dataclasses of section 3), `store.py`, `fetch.py`, `features.py`, `candidate.py`, `families/__init__.py` + one module per family, `sim.py`, `stats.py`, `ledger.py`, `cut.py`. Tests: `tests/test_lab_store.py`, `test_lab_fetch.py`, `test_lab_sim.py`, `test_lab_families.py`, `test_lab_stats.py`, `test_lab_ledger.py`, `test_lab_cut.py`.

**Builder B - stages, nightly, practice, ops wiring**: new `mintel/lab/stages.py`, `nightly.py`, `practice.py`, `prices.py`, `status.py`, `report.py`, `__main__.py`, `deploy/mac/The Lab.command`, `docs/ops/lab.md`. Small appended edits: `mintel/ops/watchdog.py` (one `ManagedProcess` in `build_default`), `mintel/ops/pulse.py` (`lab_json`, `LAB_PATH`, one parameter on `pulse_files`), `deploy/mac/mintel_mac.sh` (`install_lab_agent`, `apply_lab_2026_10_11`, `install_lab_icon`, `start_lab_nightly_if_missed`, a stop line, a `show_health` line), `deploy/mac/Start Trading Bot.command` (call lines). Tests: `tests/test_lab_stages.py`, `test_lab_nightly.py`, `test_lab_practice.py`, `test_lab_safety.py`, `test_lab_ops.py`, `test_lab_end_to_end.py` (slow).

**Builder C - the showcase**: new `mintel/ops/showcase.py`, `tests/test_lab_page.py`, `tests/fixtures/lab_summary_example.json` (clearly MADE-UP, `"synthetic": true`). Edits confined to `mintel/ops/dashboard.py`: `render_home` composition, a `/lab` route in `do_GET`, `render_lab_board`, `render_candidate`, `HOME_CSS` additions, the footer link. Nobody edits `modes.py`, `verdicts.py`, `plan.json` or any existing test file; new page assertions go in `tests/test_lab_page.py` only.

A writes `mintel/lab/__init__.py` first; B and C read it and never edit it. B codes against stub `sim`/`stats` until A lands; C codes against the fixture summary alone and never opens `lab.sqlite`.

## 2. Data

**Paths.** Root `data/lab/`. Mode file `data/lab.json` (beside `rider.json`/`ian.json`; the watchdog's `own_file_mode` reads its `mode`). Status `data/lab-status.json`. Pid `data/lab.pid`. Logs `logs/lab.out.log` (practice), `logs/lab-nightly.log`. Trial ledger: the existing `data/research_trials.sqlite` via `TrialLedger`, study `lab:<FAMILY>`.

**`data/lab.json`** (`LabSettings.load(data_dir)`, every key optional): `{"mode": "PAPER"|"OFF", "risk_gbp": 13.0, "practice_slots": 6, "fresh_cap_per_stretch": 30, "fresh_per_night": 3, "history_days": 130, "max_mb": 300, "deadline_local": "06:00", "symbols": null}`. Written atomically (tmp + `os.replace`). A mode of `LIVE` is read as `PAPER`: the lab has no live path.

**Bar store** (`store.py`). M1 bars only; coarser frames resampled on load. Per symbol, per calendar month, seven little-endian column files:

```
data/lab/bars/<SYMBOL>/<YYYY-MM>.t.bin   int32  bar open time, UTC epoch minutes
                          .o.bin .h.bin .l.bin .c.bin   int32  bid price in POINTS (round(price/point))
                          .s.bin   uint16 spread_points (MT5's per-bar spread)
                          .v.bin   uint16 tick volume, capped 65535
data/lab/bars/<SYMBOL>/index.json   {"point":..., "digits":..., "source":"bridge"|"SYNTHETIC",
     "days":{"2026-09-17":{"bars":1398,"complete":true}}, "first_day","last_day","history_exhausted","fetched_utc"}
data/lab/universe.json   {"written_utc","symbols":[{"name","kind","spec":{SymbolSpec.to_dict()},"spec_taken_utc"}],"note"}
```
Files are rewritten whole (tmp + replace), times strictly ascending, duplicates dropped. A UTC weekday is `complete` when its bar count is >= 85% of that symbol's median weekday count; today is never complete. Tests use complete days only. A store filled by `SimBroker` is `source: "SYNTHETIC"` and every result built from it carries `synthetic: true` through to the page.

**Universe** (`store.build_universe(feed)`): symbols + specs from the feed; kind from `contracts.infer_group`; keep FX_MAJOR, FX_MINOR, INDEX, GOLD; drop names matching the trader's `exclude_patterns` and any symbol without a full spec (point, tick_value, tick_size, volume_min, volume_step, contract_size all > 0); rank FX_MAJOR (all), FX_MINOR by spread (<= 12), INDEX by spread (<= 4), GOLD = XAUUSD; cap 24. The trader's `excluded_symbols` are KEPT (another rule may win where Trend & Breakout lost). Fixed once written; `settings.symbols` overrides.

**Fetch** (`fetch.py`). `update(feed, store, *, now, budget_s, pace_s=1.0, page=3000, log)`: first a forward top-up (re-fetch the last 3 UTC days per symbol, rewriting the overlap so gaps heal), then a backward backfill (`bars(symbol, TF.M1, 3000, end=oldest)`) until `history_days` complete trading days are held, an empty page comes back (`history_exhausted`), the budget is spent, or the store exceeds `max_mb`. Windows (Europe/London): weekdays 23:10-06:00 (budget 30 min); Saturday 00:00 to Sunday 20:00 any time (budget 90 min). Skipped when MetaTrader is not connected. Pacing: one page a second (sleep injectable); one timeout retried once after 8 s then stop for the night. The feed object is duck-typed: `bars()`, `symbols()`, `spec()`, `ping()`. Depth is unverified: the first run logs `depth_days` per symbol; the page says "collecting past prices: D of 40 days" until 40 complete trading days exist across >= 80% of symbols.

**Calendar.** Read-only through `CalendarStore`; `coverage()` decides which days are news-known. NEWS_MOVE is dormant until >= 40 covered trading days lie in the past stretch. The AVOID_15M axis is evaluated only on covered days (both IGNORE and AVOID_15M variants run on the same covered days).

**The cut** (`cut.py`, `data/lab/cut.json`): `{"cut_day","stretch_id","set_utc","grid_hash","fresh_used","fresh_cap","history":[...]}`. PAST = complete trading days `< cut_day`; FRESH = complete trading days `>= cut_day` up to yesterday. Windows get 400 warm-up bars before their first day; a PAST window never contains a bar at or after the cut (the loader slices `t < cut 00:00 UTC`, a test asserts it). Set on the first nightly of each month and the first run ever, only when the store holds D >= 40 complete trading days: `fresh_days = clamp(round(0.25*D), 20, 45)`; `cut_day` = the fresh_days-th complete trading day counted back from the latest. `set_cut` refuses an earlier date. A move opens a new `stretch_id`, resets `fresh_used`, records the stretch's `grid_hash` (sha1 of every registered definition id), and marks every candidate still at PAST TEST passed-but-not-fresh-tested as `retest_due`. Fresh verdicts already given stand.

`store.fresh_once(candidate_id, ledger, stretch) -> Window` is the ONLY way to read fresh bars. In one sqlite transaction it checks: a PAST row with `passed=1` exists; no `tests` row with `kind='fresh'` exists for the id in any stretch (except one `TOO_FEW_TRADES` retry); `fresh_used < fresh_cap`; then inserts the `tests` row (`status='running'`), increments `fresh_used`, commits, and only then returns the window. A trigger forbids a second update of `net_gbp` on a fresh row. A source-grep test asserts `fresh_once` is referenced only in `store.py` and `stages.py`.

**Features** (`features.py`). Computed once per (symbol, month, feature set) on the M1 index, coarser-frame values forward-filled, cached under `data/lab/cache/features/` keyed by a code stamp. Sets: `atr` (ATR14 on M5 and M15), `ema` (EMA20/50/200 on M15 and H1), `hl` (rolling 60/120/240-minute high/low), `sess` (masks for ASIA, LONDON, NEWYORK, US_CASH and overlap from `mintel/clock.py` SESSIONS; London 08:00 and New York 14:30 UK opening ranges at 15 and 30 min; Asia range; UK day index; FX day roll 22:00 UTC), `vwap` (session VWAP and sigma), `pdl` (previous UK day high/low), `bb` (Bollinger(20,2) width percentile over 100 M15 bars), `news` (minutes to next high-importance event on either currency; -1 where no coverage). A `features.Streaming` class applies the same code one M1 bar at a time for the practice process, with a test that streaming equals batch.

## 3. The candidate model

```python
@dataclass(frozen=True)
class Candidate:
    family: str; version: int
    markets: str      # FX_MAJOR | FX_MINOR | INDEX | GOLD
    session: str      # LONDON | NEWYORK | US_CASH | ASIA | BOTH | ALL
    news: str         # IGNORE | AVOID_15M   (Formula 1's 15-minute rule)
    exit: str         # t1 | t2 | t15w | tr1 | tr2 | time4h | vwap
    params: tuple[tuple[str, float|int|str], ...]   # sorted
    risk_gbp: float = 13.0
    # id = "lab-" + sha1(json.dumps(asdict(self), sort_keys=True, separators=(",",":")))[:10]   (never includes data, dates or results)

@dataclass(frozen=True)
class Signal: i: int; side: int; stop_points: Optional[int]; ref: str   # known at the CLOSE of M1 bar i
class Family(Protocol): code; label; version; features; grid; sensitive; signals(day, p, session, news); name(c); words(c)
@dataclass class Trade: symbol, side, volume, entry, stop, target, opened_utc, closed_utc, exit_price, exit_reason (STOP|TARGET|TRAIL|TIME|SESSION_END|FEED_GAP), gross_gbp, cost_gbp, net_gbp, r, bars_held, spread_assumed, fill_source
@dataclass class Result: candidate_id, window, trades, daily {UK day: (net, trades)}, skipped {reason: count}, synthetic, seconds
```
`name(c)` is generated, never typed ("London opening range (30 min) on the majors, avoid news 15 min; target 2R").

**Named exits** (crossed with every family; all flat by 21:55 UK, 21:00 UK Fridays; no overnight holds): `t1` stop 1.0 ATR(14,M5), target 1R; `t2` stop 1.0 ATR, target 2R; `t15w` stop 1.5 ATR, target 1.5R; `tr1` stop 1.0 ATR, trail 1 ATR behind the peak once +1R; `tr2` stop 1.0 ATR, trail 2R behind the peak once +1R (the Momentum Runner's idea); `time4h` stop 1.0 ATR, flat after 240 min; `vwap` target = session VWAP. A family may return `stop_points` (the other side of a range) overriding the stop.

**Fills and costs** (`sim.py`, the single implementation):
1. Signal at bar `i` close. Entry at bar `i+1` open: BUY `o + spread*point + slip`, SELL `o - slip`; `slip = max(1 point, 0.25*spread)`.
2. `spread = max(bar's recorded spread, that symbol's hour-of-day median over the past stretch)`; a bar with spread 0 uses `replay.default_spec` (15 points FX, 30 gold, 10 others) and the trade is `spread_assumed`; > 10% assumed spreads cannot pass PAST.
3. Skips (counted and shown): spread > 3x the symbol-day median; spread + commission > 20% of the stop distance.
4. Size: `volume = floor_to_step(risk_gbp / (stop_points * gbp_per_point_per_lot))`, `gbp_per_point_per_lot = tick_value*point/tick_size`; skipped below `volume_min` or if `volume_min` risks > 1.5 x `risk_gbp`; `stops_level_points` respected.
5. Exits on every bar `j > i`, STOP FIRST: BUY stopped when `l[j] <= stop` (fill `stop - slip`, or the open if it opened through it); SELL stopped when `h[j] + spread*point >= stop`. Target only if not stopped on that bar and only when the extreme reaches it by >= 1 point (touching is not enough); no slippage on targets. Trail from the peak BEFORE bar `j`. Time and session-end exits at the bar's close less spread (sells) and slip.
6. One position per symbol per candidate, at most 3 open across symbols, cooldown 5 bars after an exit, at most 6 entries per symbol per UK day, 400 warm-up bars.
7. Money: `gross = side*(exit-entry)/tick_size*tick_value*volume`; commission imported from `paper.DEFAULT_COMMISSION` (USD 3.50 a side FX and metals, 0 indices) at `DEFAULT_FALLBACK_FX["USDGBP"]`, entry and exit; `cost_gbp` = spread + slippage + commission (stored per trade so `cost_stress` can re-price); `r = net/risk_gbp`.
8. In practice the same functions receive live M1 bars; entries use one real tick (`fill_source="tick"`); a feed gap > 5 minutes with a position open closes it at the first bar back (`FEED_GAP`).

## 4. Families (night-1 registered sets; counts pinned by a test)

Shared axes: markets in {FX_MAJOR, FX_MINOR, INDEX, GOLD}; news in {IGNORE, AVOID_15M}.

| code | Aaron's name | idea | grid | n |
|---|---|---|---|---|
| OPENING_RANGE | Opening range | break of the first minutes of London or New York (Band Breaker's structure) | open {LONDON,NEWYORK} x range_min {15,30} x buffer_atr {0.1} x stop {RANGE,ATR} x exit {t1,t2,tr1} x markets {FX_MAJOR,FX_MINOR,INDEX} x news {2} | 144 |
| RANGE_BREAK | Breakout | close beyond the N-minute high/low | lookback_min {60,120,240} x session {LONDON,NEWYORK} x exit {t2,tr1,tr2} x markets {3} x news {2} | 108 |
| MOMENTUM_CONTINUATION | Momentum burst (Formula 1's idea) | impulse of k ATR over 6 M5 bars with >= 4 closes in direction, entry on the break of the impulse extreme, with or without a shallow pullback | impulse_atr {1.5,2.0,2.5} x pullback {NONE,HALF} x session {BOTH} x exit {t15w,tr1,tr2} x markets {FX_MINOR,FX_MAJOR,INDEX} x news {2} | 108 |
| TREND_PULLBACK | Trend pullback | fast EMA above slow, pullback touches the fast EMA, next bar closes back with the trend | tf {M15,H1} x emas {20/50,50/200} x touch_atr {0.25,0.5} x exit {t2,tr2} x markets {3} x news {IGNORE} | 48 |
| VWAP_FADE | Back to the average (Band Breaker's idea) | fade a close k sigma from session VWAP, target VWAP | sigma {2.0,2.5,3.0} x window {ASIA,LONDON,US_CASH} x stop_atr {1.0,1.5} x exit {vwap} x markets {FX_MAJOR,INDEX,GOLD} x news {2} | 108 |
| PREV_DAY_LEVELS | Yesterday's high and low | break of, or rejection at, the previous day's high/low | mode {BREAK,FADE} x confirm {M5_CLOSE,M15_CLOSE} x session {LONDON,NEWYORK} x exit {t2,tr1} x markets {FX_MAJOR,INDEX,GOLD} x news {IGNORE} | 48 |
| SQUEEZE_RELEASE | Squeeze release | Bollinger width in its lowest p% of 100 M15 bars, then the break | width_pct {10,20} x buffer_atr {0,0.25} x exit {t2,tr2,time4h} x markets {3} x news {IGNORE} | 36 |
| NEWS_MOVE | After the news | with or against the first 5-minute move after a high-importance release | direction {WITH,AGAINST} x delay_min {5,15} x stop {ATR,SPIKE} x exit {t15w,tr1} x markets {FX_MAJOR,FX_MINOR} | 32, DORMANT until >= 40 covered trading days |
| CONTROL_TIME_OF_DAY | Clock trade (control) | buy or sell at a fixed UK hour, exit after 1 h or 4 h; expected to fail; its pass rate at every gate calibrates the gates | hour_uk {08,10,13,15} x side {BUY,SELL} x hold {1h,4h} x markets {FX_MAJOR,FX_MINOR} | 32 |
| INTRADAY_MOMENTUM (17 Oct, found by file name) | Into the close | market intraday momentum (Gao, Han, Li and Zhou 2018; Baltussen, Da, Lammers and Martens 2021): the sign of the 9:30-10:00 New York move, on one-minute closes, measured from 9:30am New York (the papers use the previous close), traded at a fixed later New York time; US stock indices only (the series' profit currency is USD, or, when its spec gives none, its name starts with US30, US500, USTEC, US100, NAS, SPX or DJ): among the lab's markets, on this account US30 and US500, so at most 2 at once (IC Markets' Nasdaq, USTEC, is not in contracts.INDEX_HINTS, so store.lab_kind('USTEC') is '' and it is never stored; it is not added there, as that would change Trend & Breakout's own market classification); strength = abs(move) / (or_N30_hi - or_N30_lo) read at the signal bar, at least the setting (0 = any non-zero move; an unknown or zero range passes only 0); optionally the move since 10:00 must agree; one signal a UK day; stop one atr15 from the signal close | exit {time1h,tr1,t2} x markets {INDEX} x session {US_CASH} x news {IGNORE} x entry_ny {15:00,15:30} x day_agrees {NO,YES} x strength {0.0,0.5,0.75} (exit first, so the refresh's first 12 ideas are every signal setting on time1h) | 36 |
| VWAP_TREND (17 Oct, `families/vwap_trend.py`) | Bounce off the average | the opposite of VWAP_FADE: every M1 close on one side of the session VWAP for N minutes and EMA20 over EMA50 (M15 or H1) agreeing sets the trend; only a fresh dip arms it: a bar whose low (high, for a sell) comes within k x sig of the VWAP straight after a bar of that run that closed more than k x sig from it, with the trend set and any 30-minute wait over (a dip already under way when the trend comes on, the N-minute count completes or the wait ends is spent: it needs a close back out of the zone, then a new dip); the first M1 close back out of the zone is the trade (a one-bar wick dip is its own close back out), provided the trend is still set (if not, it waits for the next fresh dip); stop 0.5 ATR(M5) beyond the VWAP or the dip's extreme (the lowest low, highest high, from its first bar to the close back out), whichever is farther; at most 2 a session a day, no dip counting until 30 min after the first signal's minute, a close through (or on) the VWAP in between not ending the wait (Zarattini and Aziz 2023, SSRN 4631351). Fresh dips only since the review before its first run (17 Oct); the ids stay, as an id hashes the definition and the family had never run. On INDEX the 0.25 zone's stops are usually tighter than the 0.5 zone's, so it pays proportionally more in costs and has more trades refused for costs (costs_vs_stop): a weak result there is partly costs. News IGNORE only (an AVOID half would wait for calendar coverage before any past test and doubles N); exits t2 and tr2 (tr1 overlaps tr2 for a trend-joining rule). Axes in this order so the weekly refresh, which adds ideas in grid order as room allows, first takes all 16 signal settings on INDEX x t2 | markets {INDEX,FX_MAJOR} x exit {t2,tr2} x news {IGNORE} x session {US_CASH,LONDON} x trend_tf {M15,H1} x touch_sd {0.25,0.5} x side_min {30,60} | 64 |

Night-1 total 632 (664 when NEWS_MOVE wakes); 732 (764) since INTRADAY_MOMENTUM (36) and VWAP_TREND (64) joined on 17 Oct; the weekly refresh adds waiting ideas in registry order, so INTRADAY_MOMENTUM's come before VWAP_TREND's. Later nights run only new or `retest_due` definitions. Grid extensions and refinements (`parent_id`) are registered only at a stretch roll. Not expressible from bars, so present only as bots with their own records: Crowd Fader, Financial Ian, the Rider (binned).

## 5. The checks and the luck rules

Per-day figures are per UK trading day net GBP (0 on days without a trade; the calendar of `standing.period_bounds`). The p-value everywhere is a one-sided block bootstrap over days (blocks of 5, 2,000 resamples, seed = `int(id[4:12], 16)`, null: mean daily net <= 0): deterministic and cheap.

| check | PAST TEST | FRESH TEST (one go) | PRACTICE (two looks) | LIVE TRIAL / PROVEN |
|---|---|---|---|---|
| Enough trades | >= 60 trades on >= 20 trading days | >= 30 trades | >= 30 trades on >= 10 UK trading days | 20 trades judged; PROVEN by `verdicts.py`'s rule |
| Steady | net > 0 and PF >= 1.2; 4 equal contiguous chunks of the past days, net > 0 in >= 3 and worst chunk >= -0.5 x total; days up > days down (strict); best day <= 40% of net; plateau | net > 0, PF >= 1.1, days up > down | PF >= 1.15, days up > down | the existing verdict rule |
| Fresh prices | - | p <= 0.005 (= 0.15 / the 30 looks allowed per stretch; constant written once) | carried forward | carried forward |
| Costs | `cost_stress` (1, 1.5, 2, 3x): net > 0 at 1.5x; 2x shown | same | same | from the bot's own commission column |
| Luck check | p <= 0.05 (ranking only; shown) | "Chance this is luck: about 1 in N", N = round(1/min(1, p x 30)) | at a look: p <= 0.05, m = 2 | p from the bot's daily real net, m = 1, noted as hand-picked |

**Plateau** (inside Steady, PAST only): for each `(param, values)` in `family.sensitive`, siblings are identical candidates except that param. If >= 3 values: `parameter_stability({v: net}, plateau_tolerance=0.5)` must say stable; `None` ("extend the range") marks the family `extend_grid` and the candidate is not forwarded this stretch. If 2 values: the sibling's net must be > 0. Families with no numeric axis rely on the fresh test alone and the page says so.

**Family gates** (nightly, over a family's past results): PBO from `cpcv_splits(n_days, groups=6, test_groups=2)` through `probability_of_backtest_overfitting`; needs >= 8 members with >= 20 trades else "n/a"; PBO > 0.35 blocks every forward from that family that night. DSR per candidate via `deflated_sharpe_ratio(daily_net_past, n_trials=N_family, trial_sharpes=...)`: SHOWN, NOT GATED (at 60 days nothing clears it), used as the forwarding rank. The control family's pass rate at each gate is printed.

**Trial count.** A `tests` row (`running`) is committed BEFORE a simulation starts; N on the page is the count of `tests` rows (running, crashed and done). After each run a `Trial` goes to `TrialLedger` (`study="lab:<FAMILY>"`, `is_oos=1` for fresh).

**Honest arithmetic.** Under the null (mean 0 R, sd ~1 R) PAST gates pass ~2% of candidates: ~10-100 past passes a stretch, at most 30 get a fresh look; expected null fresh passes <= 30 x 0.005 = 0.15 per stretch; practice passes a null ~10% of the time, so ~0.015 false "Ready for live" a month (about one in five to six years), and that one then risks 20 x GBP 13 in a live trial. Price of that: p <= 0.005 is z ~ 2.58, so a true edge mu (R a trade) needs ~ (2.58/mu)^2 fresh trades for a coin-flip chance (0.3 R: 74; 0.25 R: 107; 0.2 R: 166; 0.1 R: 666). Edges under ~0.2 R, or ideas that trade rarely, show "too few trades" rather than pass; they get one retry on the next stretch. The page says this in one line.

**Throughput.** Night 1: ~630 past tests at an unmeasured 0.5-5 s each = 5-50 min. Per stretch: 10-100 past passes -> <= 30 fresh looks -> 0-3 fresh passes -> up to 6 practising -> 0-2 ready a month. Calendar: PRACTICE 10-20 UK trading days; LIVE TRIAL 20 trades. Earliest "Ready for live" about 3 weeks after the first full night if a real edge exists; earliest PROVEN 6-8 weeks. The computer part is fast; the calendar part cannot be rushed without it being luck.

## 6. Stages and moves

| from -> to | rule | who |
|---|---|---|
| IDEA -> PAST TEST | every registered definition not in `candidates`, and every `retest_due` one, is run that night (BINNED ids skipped) | nightly |
| PAST TEST -> FRESH TEST | all PAST checks passed; family PBO <= 0.35 or n/a; not `extend_grid`; ranked by DSR then p; <= 3 a night; `fresh_used < 30` | nightly |
| PAST TEST -> BINNED | any PAST check failed, reason = first failed check | nightly |
| FRESH TEST -> ON THE BENCH | all FRESH checks passed | nightly |
| FRESH TEST -> BINNED | LOST (p > 0.005, PF < 1.1, up <= down or 1.5x cost <= 0): final; TOO_FEW_TRADES (< 30): one retry on a later stretch, else BINNED | nightly |
| ON THE BENCH -> PRACTICE | a slot is free (6), <= 2 of one family practising, ordered by past-test DSR (fresh results are NEVER a ranking input; property test: shuffling fresh nets never changes the order) | nightly |
| ON THE BENCH -> BINNED | 60 days without a place | nightly |
| PRACTICE -> READY FOR LIVE | look 1 at the 10th UK trading day of practice (only if >= 30 trades; else the first day with 30, up to day 20) or look 2 at day 20: >= 30 trades, net > 0, p <= 0.05, PF >= 1.15, up > down, 1.5x cost > 0. Looks are dates computed at promotion and stored; nothing is judged on any other day | nightly |
| PRACTICE -> BINNED | look 1 net <= 0; look 2 failed; day 20 with < 30 trades ("trades too rarely"); practice drawdown > 15 R (GBP 195) at any nightly check | nightly |
| READY FOR LIVE -> LIVE TRIAL | **Aaron's word only.** v1 has no live executor: the first candidate is wired by hand as Formula 1 was. It keeps practising meanwhile; the practice rules can still bin it | Aaron |
| LIVE TRIAL -> PROVEN / back / BINNED | `verdicts.py`'s rule and the plan's 20-trade judgement | rule; money is Aaron's |

**Current bots on the ladder** (derived in `showcase.bot_stage(verdict)`, never stored): OFF -> BINNED (the Rider; Fresh prices: Failed from its research); LIVE -> LIVE TRIAL unless verdict PROVEN -> PROVEN (Momentum Runner, Financial Ian); PAPER -> PRACTICE (Band Breaker, Crowd Fader); Trend & Breakout's mixed row -> LIVE TRIAL (Formula 1, "n of 20" from the plan's count). Their Enough trades / Steady / Costs come from their records via `verdicts.stats` and `period_view`; Fresh prices reads "Not tested this way - it went straight to practice; its idea is in the lab as 'Momentum burst'" (T&B) / "Not tested this way" (others; Band Breaker -> "its idea is in the lab as 'Back to the average'"); Luck check from their own daily net with m = 1 and "picked by hand, not from a grid". Their figures stay exactly what `all_verdicts`/`period_view`/`practice_figures` give today.

**Memory of the binned.** `candidates` keeps every id for ever with `stage='BINNED'`, `bin_stage`, `bin_reason`, `binned_utc`. The generator skips any id present. Only ways back: the one TOO_FEW_TRADES retry, or a family `version` bump (new ids; the new page links its binned ancestor). `python -m mintel.lab reopen <id> --reason "..."` writes an `events` row `by='claude'` (Claude's paper-side calls only, logged on /lab).

## 7. Data formats

**`data/lab/lab.sqlite`** (WAL; `ledger.py` owns the DDL; the page reads it read-only through `showcase`, never writes):

```sql
candidates(id TEXT PRIMARY KEY, family, version INT, def_json, name, stage, stage_since_utc, created_utc,
           parent_id, retest_due INT DEFAULT 0, bin_stage, bin_reason, binned_utc, rank_score REAL);
tests(id INTEGER PRIMARY KEY, candidate_id, kind TEXT CHECK(kind IN ('past','fresh')), stretch_id, cut_day,
      status TEXT CHECK(status IN ('running','done','crashed')), started_utc, finished_utc, seconds REAL,
      window_from, window_to, days INT, trades INT, net_gbp REAL, cost_gbp REAL, pf REAL, win_rate REAL,
      up_days INT, down_days INT, best_day_gbp REAL, p REAL, folds_up INT, folds_worst REAL,
      plateau TEXT CHECK(plateau IN ('ok','spike','extend','na')), cost15_net REAL, cost20_net REAL,
      dsr REAL, max_dd_r REAL, passed INT, reasons_json, skipped_json, synthetic INT, ledger_row_id INT,
      UNIQUE(candidate_id, kind, stretch_id));
daily(test_id INT, day TEXT, net_gbp REAL, trades INT, PRIMARY KEY(test_id, day));
families(code TEXT, stretch_id TEXT, n INT, past_passed INT, pbo REAL, extend_grid_json, wf_ok INT,
         control_pass_json, PRIMARY KEY(code, stretch_id));
practice_trades(ticket INTEGER PRIMARY KEY, candidate_id, symbol, side INT, volume, entry, stop, target,
                opened_utc, closed_utc, exit_price, exit_reason, gross_gbp, commission_gbp, spread_points INT,
                slippage_points INT, net_gbp, risk_gbp, r REAL, signal_bar_utc, tick_at_signal_json,
                fill_source, mode TEXT DEFAULT 'PRACTICE', source TEXT DEFAULT 'bridge');   -- tickets from 970000001
practice_open(candidate_id, symbol, side INT, volume, entry, stop, target, peak, opened_utc, PRIMARY KEY(candidate_id, symbol));
practice_days(candidate_id, day, net_gbp, trades INT, PRIMARY KEY(candidate_id, day));
looks(candidate_id, look INT, due_day, done_utc, trades INT, net_gbp, p REAL, pf REAL, up_days INT, down_days INT,
      cost15_net REAL, passed INT, PRIMARY KEY(candidate_id, look));
roster(candidate_id PRIMARY KEY, slot INT, since_utc, symbols_json);
events(id INTEGER PRIMARY KEY, ts_utc, candidate_id, kind, from_stage, to_stage, why, by TEXT DEFAULT 'rule');
runs(date TEXT PRIMARY KEY, started_utc, finished_utc, ok INT, step, tested INT, fresh INT, seconds REAL,
     seconds_per_candidate REAL, deadline_hit INT, note);
-- trigger: BEFORE UPDATE OF net_gbp ON tests WHEN OLD.kind='fresh' AND OLD.status='done' -> RAISE(ABORT,'fresh result is final')
```

**`data/lab/summary.json`** (schema 1; written atomically by the nightly's `board` step; the practice process refreshes only `candidates[*].figures/practice` and `written_utc` every 30 s; the only lab file the page reads besides `lab-status.json`):

```json
{"schema":1,"written_utc":"...","synthetic":false,
 "store":{"symbols":24,"first_day":"...","last_day":"...","complete_days":72,"needed_days":40,"past":["..","..."],"fresh":["..",".."],
          "stretch_id":"F-2026-10","fresh_used":9,"fresh_cap":30,"news_from":"..."},
 "funnel":{"ideas":632,"past_tested":632,"past_passed":14,"fresh_tested":9,"fresh_passed":3,"bench":1,"practice":2,"ready":0,"live":0,"proven":0,"binned":617,"waiting_for_calendar":32},
 "trials":{"lab":632,"by_family":{"OPENING_RANGE":144},"control":{"past_pass_rate":0.0,"fresh_pass_rate":null}},
 "families":[{"code","label","n","past_passed","pbo","fits_noise","extend_grid":[]}],
 "candidates":[{"id","name","family","stage","stage_since","chip":{"code":"TESTING","colour":"grey"},"headline","what_to_do","ready":false,
   "checks":[{"key":"trades|steady|fresh|costs|luck","label":"Enough trades","state":"Passed|Failed|Running|Not yet|Not tested this way","text":"...","n":18,"of":30}],
   "figures":{"today_gbp","today_trades","week_gbp","overall_gbp","trades","wins","up_days","down_days"},
   "tests":{"past":{...},"fresh":{...}},"practice":{"since","day","look1_due","look2_due","trades","net_gbp"},
   "words":["Trades the majors.","..."],"story":[{"when","from","to","why"}],"sources":["data/lab/lab.sqlite tests 4411, ..."]}],
 "changes_24h":[{"when","id","name","text"}],"binned_recent":[{"id","name","stage","reason","when"}],
 "nightly":{"date","started","finished","ok","step","tested","fresh","seconds","seconds_per_candidate","deadline_hit","note"}}
```
`candidates` lists every id at ON THE BENCH, PRACTICE, READY FOR LIVE, LIVE TRIAL, PROVEN, plus those BINNED in the last 30 days; the rest are counts. Every money figure has a `source`.

**`data/lab-status.json`** (practice process, every 30 s, atomic): `{"updated_utc","mode","roster":[{"id","name","symbols","open","today_trades","today_gbp"}],"open_positions":[...whitelisted...],"last_bar_utc":{...},"feed_ok","feed_age_s","last_error"}`.

**`data/lab/runs/<date>/`**: `state.json` (steps done), `report.md`, `tests.csv`, `done`.

## 8. Runtime

**Practice process** - `python -m mintel.lab.practice --config <config>`: prices through `prices.PriceFeed`, which builds a `BridgeBroker` inside `_open()`, keeps only its bound `_rpc`, deletes the broker reference, and exposes only `ping, server_time, symbols, spec, tick, ticks, bars`, each with its method name hard-coded; `ALLOWED = frozenset({...})`; any other name raises `LabMayNotTrade` before anything is written; no `send`, `modify_stops`, `close`, `_rpc` attribute. `mintel/lab/` imports `BridgeBroker` in `prices.py` only and imports no `OrderRequest`, `PaperBroker`, `Mt5Broker`, `execution`, `Trader` or `modes`; no file under it contains `order_send`, `modify_stops`, `position_close` or `secrets.json`. Loop: every minute at :03 s, `bars(sym, TF.M1, 3)` for the roster's symbols (<= 12 calls) -> `features.Streaming` -> `family.signals` on the last bar -> entry fills on one `ticks(symbols)` call at the real ask/bid + slip -> stops/targets/trails/time exits on each completed bar, stop first. A missed minute is read back; a gap > 5 min with a position open closes it `FEED_GAP`. Nothing on weekends. `os.nice(10)`. Caps: `practice_slots` 6, <= 12 distinct symbols, <= 3 open per candidate, <= 12 open across the lab, <= 6 entries per symbol per candidate per UK day, every simulated position has a stop. Records: `practice_trades`, `practice_open` (a restart resumes), `practice_days` rolled at UK midnight. NO heartbeat file (a stale unknown heartbeat would block the trader's entries): a test asserts `data/heartbeats/` holds nothing named `lab`. Watchdog (`build_default`, after `ian`): `ManagedProcess(name="lab", command=[python,"-m","mintel.lab.practice","--config",config], pid_file=data/lab.pid, log_file=logs/lab.out.log, grace_seconds=120, kill_pattern="mintel.lab.practice", probe=status_fresh(data/lab-status.json, 180), enabled=lambda: own_file_mode(data/lab.json) != "OFF" and universe.json exists, max_restarts_per_hour=...)`, probed exactly as the bridge is.

**Nightly** - `python -m mintel.lab.nightly --config <config> --upload --workers 1 [--deadline 06:00] [--max-candidates N] [--no-fetch]`. LaunchAgent `com.mintel.lab`, `StartCalendarInterval` 23:10 local on Weekday 1-6, `nice -n 19`, log `>> logs/lab-nightly.log`; `install_lab_agent` like the pulse agent. Guards: `data/lab/nightly.lock` with pid (a dead pid's lock is removed); `data/lab/STOP` aborts cleanly between steps; free disk >= 1 GB; another `mintel.lab.nightly` running -> exit 0; `runs/<date>/done` -> exit 0. Steps, each idempotent with a line in `state.json` and its seconds in the log: 1 universe (first run) 2 fetch (skipped when MT5 not connected) 3 cut (first run of the month) 4 features (missing symbol-months) 5 grade_practice (looks due today, drawdown kill, "trades too rarely", bench promotions, roster write, events) 6 generate (registered grids -> definitions -> skip known ids -> `tests` rows `running`) 7 past (run each; family stats after a family's grid is complete; batch size = floor(0.6 x remaining_budget / measured_seconds_per_candidate), measured from the first 10) 8 fresh (<= 3, ranked; `fresh_once`) 9 board (`summary.json`, `report.md`, `tests.csv`) 10 upload (`report_upload.upload(files, cfg.report_repo, cfg.report_branch, read_token(config), day=f"lab/{date}")`). The deadline stops step 7 between candidates and still runs 8-10 ("ran out of time at 06:00: 412 of 632 tested; the rest tonight"). A missed night is caught up by the next; the page says "last night's run: missed".

**`python -m mintel.lab`** subcommands: `nightly`, `practice`, `show` (the board in text; `--one-line`), `mode off|paper|show` (writes `data/lab.json` atomically; the only mode writer; `modes.py` untouched), `reopen <id> --reason`.

**Pulse** (`pulse.py`): `LAB_PATH = "reports/live/lab.json"`, `LAB_MAX_BYTES = 20_000`; `lab_json(cfg)` copies whitelisted keys from `summary.json` and `lab-status.json` through `_scrub`, trimmed in order (binned_recent -> words/story -> candidates beyond 12); `pulse_files(p, lines, bots, lab=None)` adds it when not None. The nightly Claude review reads `reports/lab/<date>/report.md`, `summary.json` and `reports/live/lab.json`; it may add a `lab` key to `docs/plan.json` and never edits a lab record.

**Installer** (bash 3.2, `mintel_mac.sh`): `apply_lab_2026_10_11` (marker `.lab-2026-10-11`, only on success): `mkdir -p data/lab`, write `data/lab.json {"mode":"PAPER"}` if missing, `install_lab_agent`, `CODE_CHANGED="yes"` + `remember_restart`. `install_lab_icon` (marker `.desktop-icon-lab-placed`) copies `deploy/mac/The Lab.command` to the Desktop. `start_lab_nightly_if_missed`: on launcher runs between 23:10 and 06:00 only, when `runs/<today>/done` is absent and none running, `nohup nice -n 19 ... &`. Call lines after `apply_formula1_choices`; `stop_everything` kills `mintel.lab.practice` (never the LaunchAgent); `show_health` prints "The lab: practising 2 ideas; last night 632 tested, 3 fresh looks, 0 passed". `bash -n` passes.

**Desktop icon** `The Lab.command`: prints the funnel line, then a menu: 1) open the page (`/lab`), 2) run the lab now (foreground, nice 19, tails the log), 3) pause / resume (`python -m mintel.lab mode off|paper`), 4) quit. It never asks about LIVE: the lab has no live path.

## 9. The showcase (Builder C)

**Main page "/"** keeps every pinned order and string (`.s-acct` < `<h2>The bots` < `<h2>The plan</h2>`; no `<table`, `class="tile"`, `id="more"`, `<div class="hc`, NOW_TAG, `class="quiet"`, "BOT: RUNNING" on "/"; viewport meta; `background:#fff`; the four footer links; `.s-old table{...}` CSS verbatim):

```
head (unchanged) - problem line - period buttons - account card (unchanged; lab money never enters it)
<section class="s-card s-scout"><h2>Today's standouts</h2>
  up to 3 x <a class="s-pick" href="/lab?id=..."|"/?...&bot=..."><b class="s-pick-name">Name</b> <span class="s-stage">PRACTICE</span>
       <div class="s-hl">Why: ...</div><div class="s-rec"><b>Next:</b> ...</div></a>
  or <p class="s-none">No standouts today. Nothing has earned it yet - that is the test working.</p>
  <h3>What changed since yesterday</h3> up to 5 lines, or "Nothing moved."
</section>
<section class="s-card"><h2>The bots and the lab's candidates - tap one for what happened, what to do and the evidence</h2>
  <h3 class="s-group">Real money</h3>   PROVEN, then LIVE TRIAL (Trend & Breakout / Formula 1, Momentum Runner, Financial Ian, then candidates)
  <h3 class="s-group">Practising</h3>   READY FOR LIVE, then PRACTICE (Band Breaker, Crowd Fader, then candidates), then ON THE BENCH
  <details class="s-funnel"><summary>The lab: 632 ideas tried on past prices - 14 passed; 9 of 30 fresh looks used, 3 passed; 1 on the bench, 2 practising, 617 binned. Last night: 632 tested in 38 min. Prices: 24 markets, ...</summary> bench list - binned this week - <a href="/lab">The lab in full</a></details>
  <h3 class="s-group">Binned</h3>       the Rider (as today), candidates binned in the last 7 days
  <p class="s-src">Past test = tried on old prices. Fresh test = tried once on prices it had never seen. On the bench = passed both, waiting for a practice place. Practice = live prices, no money. Ready for live = the lab's work is done; Aaron's call. Live trial = a little real money, judged after 20 trades. Proven = keep and grow. Binned = out, not retried.</p>
</section>
plan box (unchanged) - footer (unchanged links + <a href="/lab">The lab</a>)
```
Empty states: no `data/lab/` -> "The lab has not run yet." and nothing else changes; broken `summary.json` -> "The lab's summary could not be read (reason)." (never a guess); short store -> "The lab is collecting past prices: 31 of 40 days needed."; `synthetic: true` -> every lab figure is replaced by "TEST DATA - not a result".

**A row** (same `.s-bot` grid for bot and candidate): bots keep `<span class="s-name">`; candidates use `<span class="s-cname">`; the bots' LIVE/PAPER pill and verdict chip untouched; new `<span class="s-stage">` after the chip; `.s-hl` headline; `<div class="s-rec"><b>What to do:</b> ...</div>` on EVERY row; figures (candidates grey: "This week (practice)", "Overall (practice)", "18 practice trades, 61% won"; a bench or test-stage candidate shows "On old prices (computer test): +GBP 312 over 143 trades" in `.s-sim` and never the word "made"); new `<div class="s-checks">` with five pills in this order and wording: Enough trades, Steady, Fresh prices, Costs, Luck check, each `<span class="s-ck ok|no|run|ny">` with the state word and a `title` holding the sentence; phone: wraps. Candidate chips: TESTING (grey) for computer stages and early practice, PASSED (sky) for a passed fresh test, YOUR CALL (sand) for READY FOR LIVE, FAILED (amber) when binned; the six existing bot chips for anything with practice or real trades.

**Candidate wording (exact):**
- ON THE BENCH: "Passed both tests; waiting for a practice place (6 of 6 taken, 2nd in line)." / "Nothing - it starts practising when a place frees."
- PRACTICE: "Practising for 7 days: +GBP 38 after costs over 19 practice trades, 5 days up, 2 down." / "Watch - judged on 20 Oct after 10 trading days and 30+ trades (11 to go)."
- READY FOR LIVE: "Ready for a live trial: 34 practice trades over 12 days, +GBP 71 after costs, 8 days up, 4 down, every check passed." / "Your call: say the word and it goes live at GBP 13 a trade, judged after 20 live trades - or leave it practising."
- LIVE TRIAL (candidate): "Live trial: 8 real trades, +GBP 21 after costs." / "Keep it at GBP 13 a trade - judged after 20 live trades (12 to go)."
- PROVEN: "Proven: 20 live trades, +GBP 96 after costs, 9 days up, 4 down." / "Keep and grow - sizes are your call."
- BINNED: "Binned on 12 Oct: failed the fresh test (-GBP 44 over 31 trades on prices it never saw)." / "Nothing - it stays binned and is not retried."
- Waiting (NEWS_MOVE): "Waiting for enough news history to test." / "Nothing."
Bots keep their six codes and recommendations verbatim. (Use the pound sign in the page, not "GBP".)

**Today's standouts - the rule** (`showcase.standouts(rows, now)`, deterministic, never from today's money): eligible when ALL hold: stage PRACTICE, READY FOR LIVE, LIVE TRIAL or PROVEN; no check is Failed; luck check computed (a row without one can never stand out); >= 10 closed trades in the last 10 UK trading days; net > 0 after costs in that window; days up > days down in it; for bots, verdict PROMISING or PROVEN. Rank by profit factor over the window, ties by trades; at most 3; never padded. Why line: "Ahead +GBP 58 after costs over the last 10 trading days: 14 trades, 7 days up, 2 down; every check it has reached passed." Next line = the stage's "What to do". `changes_24h` = `events` in the last 24 h plus bot mode changes seen in the modes files.

**Candidate page** `/lab?id=lab-xxxx` (`render_candidate`, routed in `do_GET` before the home fallback; the refresh JS reloads the same address so the id stays): back link - name, stage pill, chip - headline - What happened - What to do - **Is it luck? The five checks** (five sentences with the numbers and the threshold; a cross says plainly why) - Its rules in plain words (`words`) - Its story (events with dates) - Its trades (practice rows, newest 50, inside `<div class="s-old">`) - Sources (every figure's file and row). `/lab` (`render_lab_board`): coverage line, the fresh-stretch line ("14 Sep-9 Oct 2026 is kept back and never used for choosing; 9 of 30 looks used"), the funnel, each family with n / passed / PBO / "fits noise" flag, the control family's pass rates ("the clock-trade control passed the past test 0 times in 32: the gates are not letting noise through"), the honest-report line ("632 ideas tried; the best of 632 always looks good, so every result is judged against that"), bench and binned lists folded, last night's run and its log tail. `/lab` and the candidate page may use `<table>` inside `.s-old`; "/" may not. `/lab?id=unknown` -> "No such idea."

**CSS additions** to `HOME_CSS`: `.s-stage`, `.s-checks{display:flex;flex-wrap:wrap;gap:6px;grid-column:1/-1;font-size:13px}`, `.s-ck.ok{color:#047857}.s-ck.no{color:#b91c1c}.s-ck.run{color:#075985}.s-ck.ny{color:#9ca3af}`, `.s-group`, `.s-pick`, `.s-pick-name`, `.s-cname`, `.s-sim`, `.s-funnel`; inside the existing `@media (max-width:700px)` block `.s-scout{grid-template-columns:1fr}`.

## 10. Tests each builder writes

**A**: `test_lab_store.py` (round trip of a month in points, ascending times, completeness flag, PAST window never holds a bar at or after the cut - property over random cuts, `fresh_once` twice raises, `set_cut` refuses an earlier date, SYNTHETIC propagates); `test_lab_fetch.py` (paging backwards on a `SimBroker` feed with a call log: gaps >= `pace_s`, no call outside the window, budget honoured, `history_exhausted`, overlap healing); `test_lab_sim.py` (stop-first on a both-sides bar, gap through the stop, touch-is-not-a-fill target, entry-bar stop, trail from the previous peak, session-end exit in BST and GMT, commission by kind equals `paper.DEFAULT_COMMISSION`, size rounding and the smallest-size skip, spread floor and `spread_assumed`, cost skips; a slow timing test: 24 symbols x 60 synthetic days under 5 s per candidate); `test_lab_families.py` (grid sizes exactly 144/108/108/48/108/48/36/32/32, ids stable across runs and machines, `name()` unique per id, plateau siblings exist, the control grid); `test_lab_stats.py` (block bootstrap on a known series, the 0.005/30 constant written once, chunk rule, best-day rule, plateau on 2- and 3-valued axes and the "extend" case, PBO plumbing, DSR shown-not-gated, a planted lucky family fails fresh, a planted 0.3 R edge passes); `test_lab_ledger.py` (`tests` row exists before the simulator runs and after a crash, the fresh-final trigger, `TrialLedger` row with `study="lab:..."` and `is_oos`); `test_lab_cut.py` (month roll, `retest_due`, `fresh_used` reset, `grid_hash` refuses a changed grid mid-stretch).

**B**: `test_lab_safety.py` (`dir(PriceFeed)` has none of `send, modify_stops, close, _rpc`; a fake bridge records every method over a full practice day and the set is a subset of `ALLOWED`; source grep over `mintel/lab/*.py` for forbidden imports and strings; no `lab` heartbeat file; the fake account's deals unchanged); `test_lab_practice.py`; `test_lab_stages.py` (every row of section 6, the two looks on their dates only, the -15 R kill, "trades too rarely", bench order ignores fresh figures - shuffle property, <= 2 per family, the one retry, BINNED never regenerated); `test_lab_nightly.py` (every guard, each step idempotent and resumable, deadline with a fake clock stops step 7 and still writes the board, `--no-fetch`, upload call shape, a synthetic store yields `synthetic: true`); `test_lab_ops.py` (watchdog `lab` entry with a probe and no heartbeat, `pulse_files` adds `reports/live/lab.json` <= 20 KB, installer `bash -n`, markers only on success, call order, `nice -n 19` and `--workers 1`, icon placed once); `test_lab_end_to_end.py` (slow: SimBroker -> fetch -> nightly -> board -> "/" -> `/lab` -> a candidate page).

**C**: `test_lab_page.py` with the made-up fixture: the standouts rule (one Failed check or < 10 trades is never a pick; a huge net alone is never a pick; empty-state wording; at most 3), `changes_24h`, exactly one `s-name` per bot label with lab rows present, the three groups and their order, every row has `What to do:`, the five pills in order with state words, the bots' stage mapping and "Not tested this way" sentences, a candidate with no record shows "Not yet" never a figure, `.s-sim` never says "made", account figures identical with and without lab data, synthetic summary renders "TEST DATA - not a result" with no pound sign on lab rows, missing/broken summary wording, `/lab`, `/lab?id=`, unknown id, escaping of every summary string, the phone CSS, the refresh keeps `?id=`. C also runs `tests/test_simple_dashboard.py`, `test_formula1_page.py`, `test_page_figures.py`, `test_lineup.py`, `test_true_numbers.py` unchanged and reports (does not edit) any failure.

## 11. Verification, uncertainties and cut order

1. Each builder's own tests green; A also `tests/test_backtest.py`, `test_potential.py`; B `test_health.py`, `test_mac_pipeline.py`, `test_always_watching.py`, `test_report_upload.py`, `test_lineup.py`; C the five page files.
2. A end to end on a `SimBroker` store (stamped SYNTHETIC): ids stable, N recorded, cut refusals, a planted lucky family fails, a planted edge passes, grid counts.
3. B: `nightly --no-fetch` on A's synthetic store produces a valid `summary.json` with `synthetic: true`; a fake-bridge practice session proves the allow-list; watchdog start/stop with the probe.
4. C renders that synthetic `summary.json`: no money shown, "TEST DATA" everywhere, standouts empty.
5. `PYTHONHASHSEED=0 python -m pytest` whole suite (about 14 min; the `test_running_stamp_is_stable_and_shown` test fails if files change during the run - rerun alone).
6. On the Mac in this order: `nightly --no-fetch --max-candidates 0` (universe only); first fetch at the weekend or in the window and read `depth_days` per symbol in the log (this is where the unverified M1 depth becomes a number); `--max-candidates 50` one night and read `seconds_per_candidate` (this is where throughput becomes a number); then the full batch; watch the pulse scan timings while the fetch runs.

**Honest uncertainties.** M1 depth unknown (under 40 complete days the lab only collects; under 90 the fresh stretch is 20 days and the page says "short history: results are weaker"; thresholds are never lowered). Throughput unmeasured on the Mac (the nightly self-sizes and resumes; if too slow cut families in this order: NEWS_MOVE, PREV_DAY_LEVELS, SQUEEZE_RELEASE, TREND_PULLBACK; never the control). Bridge contention (one page a second at night, <= 13 small calls a minute in practice; if pulse scan timings rise, halve the pace and fetch at weekends only). Correlated candidates (family-block grading, PBO and the one-go fresh test are the mitigations). Hold-out leakage through Claude (grids frozen per stretch; documented, not solved). No live path in v1: READY FOR LIVE is the lab's last word; the first live candidate is hand-wired as Formula 1 was.

**Cut order if time is short:** the Windows mirror (not in this build at all) -> the `/lab` board (fold into the main page's `<details>`) -> `binned_recent` and the Binned group on "/" -> "What changed since yesterday" -> family walk-forward and Monte Carlo sentences -> the feature cache. **Never cut:** the single-store date cut with `fresh_once`, the 30-look cap and the 0.005 gate, the ledger-row-first rule, the allow-list, the two-look practice rule, the standouts rule's "no Failed check, luck check computed", the sources on every figure.
