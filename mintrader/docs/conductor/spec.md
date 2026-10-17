# The coach (conductor) - build spec v1 (12 Oct 2026)

Aaron, 12 Oct 2026: "I need you to now start getting the bot to learn from everything and make tweaks to all bots, it's sole purpose and target is to become a master trader, continuously learn, tweak bots off the back of real results and aim to get an average of £100 profit as a minimum per day in the next 3 weeks. It has the ability to change tactics throughout the day dependent on market move and is hyper intelligent in this area, deciding which bots to use and why. It's overall target isn't £100 a day. it's overall target is make as much profit after commission as possible."

Three independent designs (statistics first, trading intelligence first, safety and control first) were scored and merged into this spec by a judge. Builders follow it exactly unless the code proves a point wrong; any deviation is reported with its evidence.

Standing rules, all in force: never invent performance. Legal market behaviour only: every order has genuine intent; no spoofing, layering, wash trading, quote stuffing or fake liquidity. Never touch the daily-loss stop (3%). `risk.max_risk_pct` (1.5%) is the operator-owned hard ceiling that nothing may exceed. Secrets live only in `data/secrets.json`, which the coach never opens. Never double after losses, average into a losing trade, run a grid, or add exposure because the previous trade lost. "Ensure that consistency stays, luck doesn't." Aaron's review rules stand:
- an approach in a condition that is negative on 3 or more days traded is switched off;
- a market negative on at least 2/3 of 6 or more days is excluded;
- a working rule is not changed on 1-2 days of evidence.

Paper is Claude's call. This request delegates three things to the system: which bots to use, switching tactics, and tweaks to all bots. Section 9 states exactly what the coach may now do alone with real money. Formula 1 stays at a flat £13 a trade until its 20-live-trade judgement (Aaron, 10 Oct).

## 0. The judge's scores

| | Statistics first | Trading intelligence first | Safety and control first |
|---|---|---|---|
| Noise resistance | 9 | 8 | 7 |
| Profit potential after commission | 6 | 8 | 6 |
| Intraday intelligence | 5 | 9 | 6 |
| Safety | 8 | 7 | 10 |
| Buildability on this codebase | 7 | 6 | 8 |
| Clarity for Aaron | 7 | 8 | 9 |
| **Total** | **42** | **46** | **46** |

**What each design contributes to this spec:**
- **Statistics first:**
  - the leave-one-out hierarchy and the sceptical cap;
  - forward-only evidence;
  - the nightly luck shuffle that blocks promotions;
  - the "up only at night, down at any time" ratchet;
  - the monotonicity tests;
  - the tighten-only paired tweaks.
- **Trading intelligence first:**
  - counting trades from the same move as one bet (clustering);
  - Kelly on the lower bound, divided by the cluster size;
  - "one idea, one real ticket";
  - the market board and session phases in each market's own time zone;
  - the coach's own scorecard;
  - the page wording.
- **Safety and control first:**
  - Aaron's envelope;
  - the 180-second lease;
  - the gate algorithm and its failure table;
  - three-way cap enforcement (gate clamp, `RiskManager.size`, `PaperBroker.send`);
  - the data checks and broker reconciliation;
  - the WATCH day;
  - the append-only ledger;
  - the "Needs you" list.

**Errors found and corrected here:**
- **Statistics first: wrong test for real versus paper.** It classed a trade as paper by ticket number (960,000,001 and above). The 436 real IC Markets tickets in the reports run from 1,937,975,584 to 1,997,194,567, so every real trade would have been classed as paper. Paper is decided by membership of the paper book, as `paper.py` says: "never by the number alone".
- **Trading intelligence first: a coach heartbeat.** It had the coach write a heartbeat. A stale heartbeat that is not on the known list blocks Trend & Breakout's entries (`health.py:593-621`).
- **Trading intelligence first: changed Formula 1 early.** It changed Formula 1's position counting before Aaron's judgement.
- **Trading intelligence first: unvetoed size growth.** It grew sizes above £13 with no veto.
- **Safety and control first: mixed units.** It mixed price-R (before commission) with net-R.
- **Safety and control first: fail-closed would stop live bots.** "Fail closed when nothing is usable" would stop the LIVE Momentum Runner and Financial Ian on a coach bug.
- **Safety and control first: streak brakes without evidence.** Its streak brakes have no evidence behind them. The record shows no hot or cold hand: on the same day, after a win the next trade averaged +£0.57 and after a loss −£0.61 (t ≈ 1.8), mostly the same move continuing.
- **All three: Formula 1's count.** None noticed that `verdicts.formula1_live` counts every real Trend & Breakout position on a minor pair, whatever its approach (`verdicts.py:605-628`). A coach grant on another minor-pair approach would therefore silently enter Formula 1's 20-trade judgement.
- **All three: unmanageable real positions.** None noticed that `PaperBroker._route_real` refuses to manage a real position outside `live_groups` (`paper.py:1046-1060`, `manage_leftovers` False). A coach-granted real index trade could not have its stop moved or be closed.
- **All three: the R unit.** All three measured R as net £ ÷ 10. Formula 1's 38 trades risked a median of **£6.09** (gross ÷ price-R, 35 of 38 trades), so its own record is **+0.70 R a trade after costs (sd 1.40 R)**, not 0.41 R. Section 17 uses the corrected unit.

## 1. Decisions on one screen

1. **The coach is a separate process.** It runs as `python -m mintel.coach run`, is kept alive by the watchdog at nice 10, and lives in a new package, `mintel/coach/`.
   - It has no broker object and no order code.
   - It never opens `secrets.json` and writes no heartbeat.
   - It reads every bot's records read-only.
   - It writes only under `data/coach/`.
   - Its one instruction to the bots is the file `data/coach/policy.json`.
2. **The bots obey through a small gate, `mintel/coach/gate.py`, inside each bot process.**
   - The gate is asked once, at each entry decision.
   - It checks the policy file's modification time and reloads the file when it has changed.
   - No restart is needed, and after the one install Aaron runs no further command.
3. **The coach decides three things: which work uses real money, how much, and under which market conditions.**
   - It never decides whether practice happens: every switched-on approach keeps practising, so the evidence keeps flowing and stays unbiased.
   - It never invents a rule, and it never loosens a scanner filter.
4. **Changing tactics with the market.** The policy is a table keyed by *bot, approach, kind of market, condition*.
   - At each entry the gate looks up the classifier's label at that moment.
   - When EURGBP moves from TREND to HIGH_VOL, a different set of approaches carries real money on the next 5-second scan.
   - The switch is decided in advance from evidence. It is never re-learned during the day.
5. **It loosens only at night and tightens at any time.**
   - Promotions, trials and size steps are computed in the nightly review (22:20 UK).
   - They are released at 06:30 UK on the next UK trading day, unless Aaron vetoes them.
   - During the day the coach can only remove risk.
   - Today's profit or loss, and progress against £100, are never inputs to anything that adds risk.
6. **Every grant is a 180-second lease.**
   - The coach rewrites the policy every 60 seconds.
   - If the coach dies, its grants lapse and its restrictions stay.
   - A missing, corrupt, forged, rolled-back or stale policy can never add risk beyond Aaron's own settings.
7. **The caps are enforced three times:** in the gate's clamp, in the existing `RiskManager.size`, and in a new ceiling check in `PaperBroker.send`.
   - The 3% daily stop and the 1.5% ceiling are only ever read.
   - A source check forbids assigning them.
8. **Two switches belong to Aaron.**
   - `data/COACH-OFF` (or "The Coach" icon, option 2) puts every bot back on Aaron's settings at the next decision.
   - `data/HALT` (icon option 3, typed YES) stops new real-money entries in every bot.
   - The coach never writes either file.
9. **Formula 1 is pinned until its 20-live-trade judgement.** The coach shows it and never changes its route, size or filters.
   - After the plan's rule has judged it:
     - "back to practice" is applied by the coach;
     - "keep" makes it coach-governed at £13, with steps above £13 only on evidence;
     - "jump to the top size" is Aaron's choice alone.
10. **The honest answer on £100 a day:** it is not reachable in 3 weeks on £1,865 within the rules.
    - The central estimate is about £7 a day; a realistic range is £3-£15 a day.
    - Even if Formula 1's look-back record holds in full, it is about £25 a day.
    - Over three weeks the coach earns its keep mainly by:
      - cutting losses;
      - protecting Formula 1;
      - testing new ideas cheaply;
      - building the squad of independent edges that £100 a day really needs.

## 2. Facts in the code (commit 31587ce8) that the build must respect

| # | Fact | Where | Consequence for the build |
|---|---|---|---|
| C1 | Paper versus real is decided by the paper book, not by ticket number (real tickets are about 1.94e9-2.0e9, above the paper start of 960,000,001) | `paper.py:181-187, 1007-1010`; reports' tickets | Evidence uses the `tnb_paper.sqlite` ticket set (`PaperBroker(read_only=True).is_paper`), never a number threshold |
| C2 | `formula1_live` counts every Trend & Breakout (990311) real position on FX_MINOR opened since `tnb.live_since_utc`, any approach | `verdicts.py:605-628` | While Formula 1 is pinned, the coach may grant REAL to **no** other `tnb\|*\|FX_MINOR\|*` key |
| C3 | `_route_real` passes stop changes and closes only for real positions in `live_groups` (or with `manage_leftovers`) | `paper.py:1046-1060`; `manage_leftovers=False` at `:344` | Builder P persists coach-real tickets and lets `_route_real` manage them (test required) |
| C4 | `PaperBroker.positions(magic)` is real plus paper. The per-symbol limit, exposure register, 60-second spacing and hourly and daily caps count paper entries. | `paper.py:1034-1044`; `trader.py:1331-1340, 1449`; `_entry_gate` | A practice trade can block a real Formula 1 trade on the same symbol. The coach **measures** this (§12) and asks Aaron; it does not change it. |
| C5 | A stale heartbeat that is not on the known list blocks Trend & Breakout entries | `health.py:593-621` | No coach heartbeat; the watchdog probes `data/coach/status.json` |
| C6 | `RiskManager.size(..., risk=RiskConfig)` sizes with other settings and always clamps at `max_risk_pct` after lot rounding. 0.7% of £1,865.61 is £13.06, so the £13 money cap binds. | `risk.py:610-700` | Coach sizes go through `risk=dataclasses.replace(cfg.risk, max_risk_money=gbp)`; `max_risk_pct` is never replaced |
| C7 | No bot re-reads `config.json`; the scan gating lists are read through `self.cfg` on every call | `run.py:656`; `modes.py:139-142` | Runtime change goes only through the gate and the policy file. The coach never writes `config.json`. |
| C8 | A Runner copy volume of 0 means no ride | `runner/shadow.py:576-590` | Denying a Runner ride is `runner_volume = 0.0` |
| C9 | Band Breaker and Crowd Fader take `entries_allowed` | `bandbreaker/engine.py:140-145, 426-429`; `crowd/engine.py:158-164, 327-330`; `trader.py:944-976` | Gate per bot through `_bot_kwargs`. Practice is never gated: a PAPER bot always gets True. |
| C10 | Financial Ian is a separate process with its own £50 day limit, outside the 3% stop | `ian/engine.py:87-93, 815-829`; `trader.py:449-461` | The gate is built in `ian/run.py` and consulted in `_entry_block`; money can only go down |
| C11 | Page figures: `standing.account_standing` gives `today` (positions closed since 00:00 UK, every bot, after commission) and `open` (floating), per bot by magic; `period_view(sd, deals, "custom", from, to)` covers any UK-day range | `standing.py:377-560, 600-760`; `dashboard.py:3150-3180` | The progress line is `period_view` custom from 2026-10-12, the same arithmetic as the page |
| C12 | The Mac's exposure caps are 2.25% correlated and 4.5% total (installer), not the code defaults | `mintel_mac.sh:867-868` | The coach reads `config.json` at start and on its mtime, never code defaults |
| C13 | The Rider is OFF and binned; Band Breaker and Crowd Fader build their executors at start | `modes.py`; `shadow.py:203-237` | No coach path to make Band Breaker or Crowd Fader real in v1: recommend only |

## 3. Files, owners and order (disjoint)

| Builder | Writes (new unless marked *edit*) | Tests | When |
|---|---|---|---|
| **P: policy, gate and hooks** | **First:** `mintel/coach/__init__.py` (paths, constants, `STEPS_PCT`, frozen thresholds, schema version, the dataclasses `Envelope`, `Policy`, `Entry`, `Decision`, `Key`); the others read it and never edit it. Then `policy.py` (canonical JSON, validator, atomic write/read, used by both writer and reader), `envelope.py`, `gate.py`. *Edits:* `mintel/broker/base.py` (`OrderRequest.coach_route: str = ""`), `mintel/engine/execution.py` (`submit(..., coach_route="")` copies it into the request), `mintel/broker/paper.py` (`static_goes_real`, `goes_real`, coach-real tickets table, `_route_real`, ceiling check in `send`), `mintel/engine/trader.py` (gate in `__init__`, `act`, `_runner_copy_volume` call site, `_bot_kwargs`, `weather.json` writer), `mintel/ian/run.py`, `mintel/ian/engine.py` | `test_coach_policy.py`, `test_coach_gate.py`, `test_coach_caps.py`, `test_coach_halt.py`, `test_coach_hooks.py` | now (no lab overlap) |
| **E: evidence and statistics** | `mintel/coach/records.py`, `estimate.py`, `rules.py` (standing rules, luck shuffle), `tweaks.py`, `epochs.py` | `test_coach_records.py`, `test_coach_estimate.py`, `test_coach_rules.py`, `test_coach_luck.py`, `test_coach_tweaks.py` | now, against P's dataclasses |
| **D: decisions and runtime** | `mintel/coach/decide.py`, `brakes.py`, `ledger.py`, `weather.py`, `words.py`, `feed.py` (reads the page's `/api`, `weather.json`, approvals, `config.json`), `run.py` (tick, nightly, writer), `status.py`, `report.py`, `__main__.py`, `deploy/mac/The Coach.command`, `docs/ops/coach.md` | `test_coach_decide.py`, `test_coach_monotone.py`, `test_coach_brakes.py`, `test_coach_ledger.py`, `test_coach_run.py`, `test_coach_safety.py`, `test_coach_replay.py` | now |
| **G: page and ops** | **Now:** `mintel/ops/coach_page.py` (render functions and `progress()`), `tests/fixtures/coach_status_example.json` (`"synthetic": true`, clearly made up), `test_coach_page.py`. **After the lab's Builders B and C have merged** (they own these files today), append-only *edits*: `mintel/ops/dashboard.py` (import; one card in `render_home` after the lab's "Today's standouts"; a `/coach` route before the home fallback; `coach_progress` added to the `/api` payload), `mintel/ops/pulse.py` (`COACH_PATH = "reports/live/coach.json"`, `COACH_MAX_BYTES = 20_000`, `coach_json(cfg)`, `pulse_files(..., lab=None, coach=None)`), `mintel/ops/watchdog.py` (`ManagedProcess("coach")` after `"lab"`), `deploy/mac/mintel_mac.sh` (`apply_coach_2026_10_xx`, `install_coach_icon`, a stop line, a `show_health` line), `deploy/mac/Start Trading Bot.command` (call lines) | `test_coach_ops.py`, then rerun the lab's `test_lab_ops.py`, `test_lab_page.py` and `test_mac_pipeline.py` | page now; edits after the lab |

**Rules for every builder:**
- Nobody edits `mintel/lab/`, `verdicts.py`, `modes.py`, `health.py`, `report_upload.py`, `docs/plan.json`, `config.py` defaults or any existing test.
- The coach uploads through `report_upload.upload(...)` and does not edit that file.
- `mintel/coach/` imports nothing from `mintel.broker`, `mintel.engine.execution`, `mintel.engine.trader`, `mintel.ops.modes` or `mintel.lab`, except as listed here:
  - `gate.py` is imported by the bots;
  - `records.py` imports `PaperBroker` read-only and `contracts.infer_group`;
  - `weather.py` imports the pure registry `engine.tactics.TACTICS`;
  - `report.py` imports `report_upload.upload`;
  - `feed.py` imports `report_upload.status_json`.

**What works without the page.** D, E and P together deliver the whole control path plus the text view (`python -m mintel.coach show`). If the lab slips, G's edits wait for a later install. The coach is fully safe without them.

## 4. The evidence store (Builder E, `records.py`)

### 4.1 Sources

All sqlite sources are opened `file:<path>?mode=ro` with `uri=True` and a 2-second busy timeout. Nothing is ever written to them.

| Source | Rows | Real or practice | Planned risk (for R) |
|---|---|---|---|
| `data/journal.sqlite` `trades` (Trend & Breakout) | closed rows; approach = `tactics.approach_of(tactic)` (`_2X` twins stripped); condition = `regime`; kind = `contracts.infer_group(symbol)`; `pnl_money`, `entry_commission`, `opportunity`, `reward_risk`, `entry_state_json` | practice when the ticket is in `PaperBroker(data_dir, read_only=True)`'s book, else real. Rows tagged `runner_feed` in `entry_state_json` are excluded from Trend & Breakout keys. | `risk_money` |
| `data/runner.sqlite` | closed shadows | `mode` LIVE = real, PAPER = practice | `risk_money` if the column exists, else `runner.max_risk_money` at the time (10), flagged `risk_estimated` |
| `data/bandbreaker.sqlite`, `data/crowd.sqlite` | closed trades | `mode` column | `risk_money` column if present, else the bot's configured `risk_money` (10), flagged |
| `data/ian.sqlite` | closed trades with `data_source` | `mode` column | `risk_money` |
| `data/lab/lab.sqlite` `practice_trades` | lab candidates | practice (λ = 0.5, own key only) | `risk_gbp` |
| `data/coach/gate/<bot>-<date>.jsonl` | the gate's decision per entry (ticket → key, route, `policy_seq`) | — | joins coach-granted tickets to their keys |
| The page's `/api` (`report_upload.status_json(cfg)` → `standing`) | account `today`, `open`, `equity`, `balance`; per bot `magic`, `today`, `open` | broker truth | — |
| `config.json`, `data/ian.json`, `data/rider.json`, installed `app/docs/plan.json` | Aaron's baseline: `tnb.live_groups` / `live_tactics` / `live_since_utc`, bot modes, `risk.*`, `scan.disabled_*`, `universe.excluded_symbols`, Formula 1's `judge_after` | read only | — |

**Commission before 9 Oct.** Before 9 Oct, `pnl_money` lacks the opening commission. Add it with the estimator `RiskManager.segment_record` already uses (`risk.py:459`). Import it, do not copy it, and flag the row `commission_estimated`.

**Builder E checks every column it reads with `PRAGMA table_info`.** A missing column means the fallback in the table plus a flag, never a guess.

### 4.2 Unit, key and weight

- **Outcome of a trade:** `x = net_after_commission_gbp / planned_risk_gbp`, clipped to `[-3.0, +5.0]`.
  - This is R after costs, so trades at £6, £10 and £13 are comparable.
  - `realised_r` in the journal is price-R before commission and is never used.
- **Key:** `bot|APPROACH|KIND|CONDITION`.
  - Trend & Breakout: e.g. `tnb|MOMENTUM_CONTINUATION|FX_MINOR|HIGH_VOL`.
  - Runner: `runner|<approach it rode>|INDEX|<condition>`.
  - Band Breaker and Crowd Fader: `bandbreaker|*|<KIND>|*` and `crowd|*|<KIND>|*` (they store no condition).
  - Ian: `ian|*|CRYPTO|<condition>`.
  - Lab: `lab|<candidate_id>|<markets>|*`.
  - The condition is the **raw label at entry**, the one the evidence was recorded with.
- **Weight:** `w = a × d × v`.
  - `a`: 1.0 for a real trade, λ = 0.5 for bot practice and lab practice.
  - `d = 0.5 ** (age_in_UK_trading_days / 30)`.
  - `v`: 0.5 if the trade closed before the key's latest epoch in `epochs.py`, else 1.0.
- **Practice haircut.** Practice outcomes have `h` subtracted before use:
  - `h = 0.10 R` until a key has at least 20 real and at least 20 practice trades in overlapping weeks;
  - then `h = max(0.05, min(0.30, mean(practice) − mean(real)))`, updated only when that gap moves by more than 2 standard errors.
  - Practice is never credited above real.
- **Epochs** (`epochs.py`; constants written by Claude, changed only by an install; the coach never edits them):

  | Date (UTC) | Epoch |
  |---|---|
  | 2026-10-05 12:30 | reset |
  | 2026-10-09 | £13 a trade and commission recorded |
  | 2026-10-10 | 15-minute news block, `live_tactics` |
  | 2026-10-12 07:58 | Formula 1 choices installed |
  | (no date) | `CLASSIFIER_VERSION = 1` |

  Each epoch names the keys it touches; `"*"` means all.

### 4.3 Clusters (one move counts as one bet)

A cluster is a set of trades with the same bot and the same direction, sharing a leg, where each trade entered within 60 minutes of the cluster's first entry.

Legs:

| Market | Leg(s) |
|---|---|
| FX | base and quote currency |
| US500, US30, USTEC | `USEQ` |
| DE40 | `EUEQ` |
| UK100 | `UKEQ` |
| JP225 | `JPEQ` |
| Other indices | their own symbol |
| XAU* | `XAU` |
| XAG* | `XAG` |
| BTC, ETH | separate legs |

For a BUY of EURGBP the legs are (EUR long, GBP short). Clusters are built greedily in time order.

### 4.4 Data checks (on every tick; no grant on bad data)

Each source is marked `ok`, `stale` or `unreadable`. It must pass all of these:
- it opens read-only;
- its row count has not shrunk;
- its newest closed time is not in the future;
- under 5% of rows are excluded (risk ≤ 0 or missing, or a non-finite net);
- tickets are de-duplicated.

**Broker reconciliation.** The sum of real net today (UK day) from the records must equal the page's `standing.today` within `max(£3, 10% × |standing.today|)`.

**If any check fails:**
- `grants_allowed = false` and restrictions continue;
- a governed bot whose own record is unreadable is paused, unless it is pinned;
- brake states are never released on unknown data.

## 5. The estimator (Builder E, `estimate.py`; stdlib only)

For each node (key or parent), from its weighted trades:

```
n      = number of trades;  Σw;  n_eff = (Σw)² / Σw²
ȳ      = Σ w·x / Σw
σ̂      = sqrt(Σ w·(x−ȳ)² / Σw), floored at max(1.0, pooled σ̂ of its bot×kind)
v_clu  = Σ_clusters ( Σ_{i∈c} w_i·(x_i − ȳ) )² / (Σw)²          (cluster-robust)
v_data = max(v_clu, σ̂² / n_eff)
c̄      = clip(n / number_of_clusters, 1, 3)
```

**Hierarchy.** L0 is the bot, L1 approach, L2 kind of market, L3 condition. **The key is L3.** Session phase is not a level in v1.

- **Root prior:** N(m0 = −0.05 R, s0² = 0.10²). A rule with no skill loses about its costs. Trend & Breakout's 436 real trades broke even after £159 of commission.
- **Each level's prior** comes from its parent's posterior, computed **leave-one-out** (the parent's trades minus this child's trades): mean `m_par`, variance `v_par + τ_L²`.
- **Update:**
  ```
  v_post = 1 / ( 1/(v_par + τ_L²) + 1/v_data )
  m_post = v_post · ( m_par/(v_par + τ_L²) + ȳ/v_data )
  ```
  A node with no trades keeps `m_par` with variance `v_par + τ_L²`.
- **τ_L:**
  - Defaults: τ1 (approach) = 0.10, τ2 (kind) = 0.10, τ3 (condition) = 0.15 R.
  - Re-estimated nightly by DerSimonian-Laird across sibling nodes with n_eff ≥ 5, but only when there are at least 5 such siblings, and clipped to [0.05, 0.30].
  - Held fixed between nights.
- **Sceptical cap.** For a key that has never been real under the coach and is not pinned, `m_par` is capped at **+0.05 R** before its own data is added. History used to pick a key never also promotes it.

**Outputs per key:**

| Output | Formula or meaning |
|---|---|
| `m`, `s = √v_post` | posterior mean and standard deviation |
| `P⁺ = Φ(m/s)` | chance the edge is real |
| `LCB80 = m − 0.8416·s` | 80% lower bound |
| `LCB90 = m − 1.2816·s` | 90% lower bound |
| `m_cost15` | the posterior with every trade's commission × 1.5 (`x − 0.5·commission_gbp/risk_gbp`) |
| `λ` | trade rate: posterior mean of Gamma(1 + n, 1 + active UK trading days) |
| `σ̂`, `c̄` | as defined above |
| `P⁺_real`, `LCB80_real` | the same chain with practice weight a = 0, with real-only siblings in the parents. **Used for every real-money size decision and every demotion of something live.** |
| `m_fwd`, `P⁺_fwd` | only trades closed after the key's registration (ledger `segments.registered_utc`), from the root prior with no pooling |

**When it runs:**
- the full refit runs nightly;
- after each close, only the affected key's chain is updated, with τ held at the nightly value.

**Lab family prior** (cut-able):
- when a lab family has at least 8 past-tested members, the root mean of the Trend & Breakout keys whose approach matches that family becomes `min(m0, median past net R)`, floored at −0.15;
- the lab can only make the coach more sceptical.

## 6. States, moves and sizes (Builder D, `decide.py`)

**Key states:** `PRACTICE`, `LIVE_TRIAL`, `LIVE(step)`, `BANNED_REAL` (practice continues), `PINNED` (Aaron), `PLACED` (a bot Aaron made live, before its own judgement), `PAUSED(until)`.

Constants are frozen in `mintel/coach/__init__.py`. The coach never tunes them; changing one takes an install.

### 6.1 Moves up (computed nightly; released at 06:30 UK on the next UK trading day; logged and shown that evening)

| Move | All of these must hold |
|---|---|
| **G1** PRACTICE → LIVE_TRIAL (Trend & Breakout keys only in v1) | **Eligibility:**<br>• `tnb` in envelope `govern`, and `tnb.mode` not OFF<br>• kind in envelope `tnb_real_kinds`<br>• **not** `tnb\|*\|FX_MINOR\|*` while Formula 1 is pinned (C2)<br>• **not** a key the Momentum Runner rides while the Runner is PLACED (`runner.feed_tactics` × INDEX), so one idea is never two real bets<br>• condition is not UNCLEAR<br>• its symbols are not practice-only<br>• not in cool-off<br>**Evidence:**<br>• ≥ 30 trades (any source), n_eff ≥ 20, ≥ 12 clusters, on ≥ 8 distinct UK trading days<br>• ≥ 15 trades closed after registration under the current epoch<br>• `P⁺ ≥ 0.90`, `LCB80 > 0`, `m_cost15 > 0` and `m_fwd > 0`<br>• in its last 30 UK trading days: weighted net > 0, days up > days down, best day ≤ 40% of net<br>• no standing rule fires on its record<br>**Conditions:**<br>• last night's luck check ≤ 2%<br>• fewer than 2 trials open, at most 1 new trial a night, trial budget left this week, no brake<br>• ties broken by LCB80, never by recent profit |
| **G2** LIVE_TRIAL → LIVE(base) | • ≥ 20 real trial trades over ≥ 5 UK days<br>• real net > 0 after costs and up days > down days (Aaron's own Formula 1 test)<br>• `P⁺_real ≥ 0.80`<br>• practice-versus-real gap ≥ −0.15 R once measurable<br>Failure: PRACTICE with a 10-UK-trading-day cool-off. A second failure in the same epoch: BANNED_REAL for that epoch. |
| **G3** LIVE(step k) → step k+1 (Trend & Breakout keys only in v1) | • ≥ 20 real trades at the current step (Formula 1's 20 judged trades count for its first step) and ≥ 5 UK trading days since the key's last change<br>• `P⁺_real ≥ 0.90` and `LCB80 > 0`<br>• its real record at the current step: net > 0 and up days > down days<br>• at a new high of its cumulative real R since going live, so it **never grows right after losses**<br>• **Kelly cap:** `k% = 100 × 0.25 × max(0, LCB80) / (σ̂² × c̄) ≥ next step %`<br>• **Stop-safety:** a fixed-seed bootstrap (seed = the date as an int) of 2,000 days, every LIVE key at its proposed size, each key's count ~ Poisson(λ), outcomes resampled from its real x (or its all-source x with the haircut if it has fewer than 20 real), must hit −3% of equity on ≤ 2% of days<br>• no brake<br>• ≤ 1 step-up a night and ≤ 2 an ISO week across all keys<br>• **Announced on the page the evening before; Aaron can veto with one command** (§14) |

**Size steps** (Trend & Breakout; money at £1,865.61):

| Step | % of equity | £ |
|---|---|---|
| trial | 0.35% (never above £8; no trial if the minimum lot risks more than £8) | 6.53 |
| base | `cfg.risk.base_risk_pct` with `max_risk_money` (today 0.70% capped at £13) | 13.00 |
| S1 | 0.85% | 15.86 |
| S2 | 1.00% | 18.66 |
| S3 | 1.25% | 23.32 |
| S4 | 1.50% | 27.98 |

Each step is never above `cfg.risk.max_risk_pct × equity` or envelope `max_step_pct`.

**Money a real coach-granted trade may risk** is the smallest of:
- the key's step;
- the 1.5% ceiling;
- the envelope cap;
- the headroom (§7);
- any brake's cap.

The Runner, Band Breaker, Crowd Fader and Ian have no steps in v1. Their money can only go down.

**Kelly in numbers (illustrative, not a result).** With Formula 1 at m = 0.20 R, s = 0.12 R, σ̂ = 1.40 and c̄ = 2.24:
- LCB80 = 0.20 − 0.84 × 0.12 = 0.10 R;
- k = 0.25 × 0.10 / (1.96 × 2.24) = 0.57%, below base;
- reaching S1 (0.85%) needs LCB80 ≥ 0.149 R.

The page says it plainly: "The evidence supports today's size, not more, yet."

### 6.2 Moves down (any tick, at once; recorded with the numbers)

| Trigger | Action | Kind |
|---|---|---|
| **D1 CUSUM** on each coach-governed real key, in σ units: `z = x/σ̂`, `S = max(0, S + 0.05 − z)`, alarm at `S ≥ 12`. On Formula 1's outcome shape: false alarm 13% within 100 trades for a true +0.2 R edge (2% for +0.3 R); it catches −0.2 R within 60 trades 68% of the time. | back to PRACTICE, 10-day cool-off | judgement |
| **D2 gap:** one real fill at ≤ −2.5 R | that key paused for the rest of the UK day | risk limit |
| **D3 nightly:** `P⁺_real` < 0.80 | one step down (S1 and above) | judgement |
| **D3 nightly:** `P⁺_real` < 0.60 | to base | judgement |
| **D3 nightly:** `P⁺_real` < 0.50 (n_eff ≥ 20) | PRACTICE | judgement |
| **D4 key drawdown** ≥ 8σ̂ below its real R peak (≈ 11 R at σ̂ 1.4; ≈ 10% false within 40 trades at +0.2 R) | one step down | risk limit |
| **D4 key drawdown** ≥ 12σ̂ (≈ 17 R; ≈ 1% false) | PRACTICE | risk limit |
| **D5 trial money:** a trial key's cumulative real result ≤ −£30 | PRACTICE, 10-day cool-off | risk limit |
| **S1 (Aaron's rule, exactly):** approach × condition traded on ≥ 3 UK days in the current epoch with net < 0 (real trades if it has ≥ 3 real traded days, else practice with the haircut) | BANNED_REAL (practice continues); back only through G1 after a 10-day cool-off with net > 0 in the newer window | judgement |
| **S2 (Aaron's rule):** a symbol negative on ≥ 2/3 of ≥ 6 traded days | practice-only for coach-granted keys; the nightly digest recommends adding it to `universe.excluded_symbols` at the next install | judgement |
| **K1 placed bot judged "back to paper":** the 30-real-trade judgement of a bot Aaron placed (Runner or Ian) says "back to paper" (verdicts' rule) | SKIP for new entries (no paper path while LIVE); "Needs you" asks Aaron to switch it to PAPER | the plan's rule |

**S3 (Aaron's rule):** nothing is promoted or demoted as a *judgement* on fewer than 3 UK days of evidence. D2, D4, D5 and the brakes are risk limits, not judgements, and are labelled that way in the record.

**Hysteresis:**
- a key that drops to practice cannot be re-promoted for 10 UK trading days;
- a step down cannot be reversed for 5.

**Pinned and placed keys.**
- For Formula 1 (pinned), D1-D5, S1 and S2 run **in shadow**: shown under "Needs you", never applied.
- For the Runner and Ian (placed), performance moves wait for their 30-real-trade judgement. Brakes B2 and B4 and HALT apply to them.

### 6.3 The coach's own luck check (nightly)

1. Shuffle key labels among Trend & Breakout trades **within each UK day**, 200 times.
2. Rerun the evidence part of G1 on every shuffled key.
3. If more than **2%** of shuffled keys pass, **no promotion of any kind that night**.

The page shows the rate: "0 of 40 made-up segments passed tonight's test".

The control family of the lab (clock trades) is shown beside it when the lab has graded it.

### 6.4 The coach's scorecard

Nightly, the coach works out its own effect from what actually happened:
- **saved:** the practice result of the trades it routed away from real money, priced at the real size they would have had;
- **added:** the real result of its trials and steps, less what base size would have made;
- **net:** saved plus added.

If net < 0 over ≥ 20 UK trading days and ≥ 40 affected trades with P(net effect < 0) > 0.8, the coach puts itself in GUARD and says so on the page.

## 7. Brakes and limits (Builder D, `brakes.py`)

All of these only restrict.

**What they count:** real money on the UK day, every magic number including Ian, closed `today` plus floating `open` from the page's standing block, after commission. E0 is the equity at UK midnight. The trader's in-process gate also uses its live `RiskSnapshot`.

**Untouched:** the 3% stop, the 12% drawdown breaker and every existing breaker stay exactly as they are.

| Brake | Trigger (£ at E = 1,865.61) | Effect | Applies to |
|---|---|---|---|
| B1 day line | real today ≤ −1.5% of E0 (−£27.98) | no grants; trials paused; every governed key at base for the rest of the UK day | governed keys |
| B2 day pause | ≤ −2.25% of E0 (−£41.98) | every governed real entry paused until UK midnight: Trend & Breakout granted keys → practice; Runner rides skipped; Ian no new entries | governed keys, Runner, Ian (not pinned Formula 1) |
| B3 week line | last 5 UK trading days ≤ −5% (−£93.28) | no grants and no steps until back above −2.5% | governed keys |
| B4 drawdown | equity ≤ 95% of the coach's **saved** peak | no grants, no steps, governed keys at base | governed keys |
| B4 drawdown (deeper) | equity ≤ 92% of that peak | every governed real entry paused, Runner and Ian included; "Needs you" | as B2 |
| B5 headroom | a governed real entry needs `new_risk ≤ H`, `H = L·E0 + min(0, real_today) − open_real_risk_all_bots`, `L = cfg.risk.max_daily_loss_pct/100`. Ian receives `H` in the policy (60-second lag; up to one trade of overlap is possible and documented). The trader takes the smaller of the policy's H and its own live H (broker's day, real only). | the entry goes to practice (Trend & Breakout) or is skipped (others), with a sentence | governed keys, Runner, Ian (not Formula 1) |
| B6 one idea, one real ticket | inside the trader process: a governed real entry is refused if another bot holds a real position on the same symbol in the opposite direction, or opened one in the same direction within 5 minutes | practice or skip | governed keys |
| B7 trial budget | trial losses this ISO week ≥ £40 | no new trials until Monday 06:30 UK | trials |
| B8 data | any §4.4 check fails | no grants; an unreadable governed bot is paused | all governed |
| B9 coach breaker | > 20 policy changes in 24 h, > 3 trial starts in a week, any audit violation, or 3 tick errors in a row | the coach puts itself in GUARD and asks Aaron | the coach |

- The coach saves its own equity peak in its ledger, so the peak survives a restart (unlike `RiskManager.peak_equity`). Drawdown brakes are released at the nightly review once the drawdown is under half the trigger.
- No brake looks at streaks of wins or losses; the record shows none (§0).
- No brake ever raises size.

**Two recommendations only.** Each goes in "Needs your yes" with its measured count, and the coach never applies either:
- make the 3% stop count Financial Ian;
- apply headroom to Formula 1 too, so stacked stops can never take a day past 3%.

## 8. The day: ticks, the nightly review, and switching with the market

| Cadence | What happens |
|---|---|
| **Gate** (each entry decision, in each bot) | `stat()` of `data/HALT`, `data/COACH-OFF`, `data/coach.json` and `data/coach/policy.json`; reload on mtime change; check the lease; look up the key from the **live** condition label: exact `bot\|APPROACH\|KIND\|CONDITION`, then `…\|*`, then `bot\|*\|KIND\|*`, then `bot\|*\|*\|*`, then BASELINE. UNCLEAR is never granted. |
| **Tick, every 60 s, every day** (Ian trades weekends) | data checks; read the page's standing block, `weather.json`, approvals and new closes; update affected keys; brakes; D1, D2, D4, D5, S1, S2; write the policy (always; lease 180 s); `status.json`; audit (every real decision in the gate logs must be ≤ min(1.5% × E, its grant), and any violation trips B9). **Never loosens.** |
| **Nightly review, 22:20 UK** (after the 22:10 upload, before the lab at 23:10; if the coach was down, at its next start before 06:30) | full refit; τ; luck check; standing rules; G1, G2, G3 and D3; the scorecard; **tomorrow's game plan** (`data/coach/plan-<date>.json`): which keys are real in which conditions, sizes, trials, announced steps, DST notes; the digest `report-<date>.md`; upload |
| **Release, 06:30 UK on UK trading days** | grants and steps from last night's plan take effect unless vetoed |

**The market board** (`weather.py`).
- **Source.** The trader writes `data/coach/weather.json` atomically every 60 s from its latest scan states, one entry per scanned symbol: `{symbol, kind, regime, confidence, vol_state, spread_points, news_state, tradable, as_of_utc}`.
- **What the coach builds.** A board per kind of market, for example "FX minors: 5 of 14 in a fast market". For each live key it also lists the symbols where it may trade with real money **now**, using `TACTICS` together with `cfg.scan.disabled_tactic_regimes`, `disabled_tactic_groups` and `excluded_symbols`, all read from `config.json`.
- **Smoothing is for display only.** A label must hold for 2 consecutive minutes before the board shows it (NEWS shows at once). Gating always uses the raw label at entry, because the evidence was recorded with it. The scout found 44% of same-symbol entries within 2 hours carried different labels.
- **Session phases.** Each phase is computed in its market's own time zone through `clock.py`, so UK and US clock changes (25 Oct and 1 Nov) are handled:

  | Phase | When |
  |---|---|
  | ASIA | Tokyo 09:00-15:00 |
  | LONDON_AM | London 08:00-12:00 |
  | OVERLAP | NY 08:00 to London 16:30 |
  | NY_PM | until NY 16:45 |
  | ROLLOVER | NY 16:45-17:15 |
  | LATE | after the rollover |

- **Two market-hours gates**, for coach-granted keys only. Pinned and placed keys are never affected.
  - No new granted entry in ROLLOVER. Until 1 Nov that is 20:45-21:15 UTC, outside the existing 21:45-23:30 UTC no-entry window.
  - No new granted FX or index entry after 16:00 NY on Fridays.

**Not learned in v1:** hour or weekday rules, session-phase overrides, index "day types", exits. Each day is one sample, so none of these can be tested inside 3 weeks. They are logged as hypotheses and shown on `/coach` only.

## 9. Powers: what the coach may do alone, and what needs Aaron

Envelope modes, from Aaron's file `data/coach.json`:

| Mode | Meaning |
|---|---|
| OFF | out of the loop |
| WATCH | computes and shows everything, changes nothing |
| GUARD | may only restrict |
| PILOT | GUARD plus grants within the envelope |

| Action | PILOT, on its own | GUARD | Needs Aaron |
|---|---|---|---|
| Send any governed real key, symbol or bot to practice, pause it for the day, reduce any size, apply any brake | yes, any tick, recorded with its sentence | yes | — |
| Practice on or off | never stops practice (paper is Claude's call, at an install) | — | — |
| Live trial of a Trend & Breakout key at 0.35% (≤ £8), ≤ 2 at once, ≤ £40 of trial losses a week (G1) | yes; nightly, released 06:30 UK | no | — |
| Trial → base £13 (G2) | yes | no | — |
| Steps above £13 up to 1.5% (G3) | yes, **announced the evening before**; one command vetoes it | no | the veto |
| Tighten a filter for real money (§13) | yes, after the paired test | yes | loosening for real money |
| Formula 1 before its 20 live trades | **nothing** (shadow only) | nothing | everything |
| Formula 1 after the plan's rule | applies "back to practice", or governs "keep" at £13 with steps by evidence | — | "jump to the top size"; re-pinning |
| Runner and Ian before 30 real trades | brakes B2, B4, B5 and HALT only | same | their judgement |
| Make Band Breaker or Crowd Fader real; resume a bot paused by K1 | no (needs a restart): recommend | no | yes |
| Lab READY FOR LIVE → live trial; anything for the Rider | no | no | yes |
| 3% stop and what it counts; 1.5% ceiling; 12% breaker; exposure caps; news block; top-size switch; `config.json`, `ian.json`, `rider.json`, `lab.json`, `plan.json`; secrets; HALT and COACH-OFF | **never** | never | Aaron (operator-owned) |
| Place, close or modify any order; start or stop any process | **never**; it only routes and sizes the bots' own genuine orders | never | Aaron, by hand |

## 10. Aaron's envelope, the policy file and the gate (Builder P)

### 10.1 The envelope: `data/coach.json`

Written only by the installer's one-time step and by Aaron's CLI commands. The coach process reads it and never writes it, and a test asserts that.

```json
{"schema":1,"mode":"PILOT","watch_until_utc":"<end of the first full UK trading day after install>",
 "govern":["tnb","runner","ian","bandbreaker","crowd"],
 "tnb_real_kinds":["FX_MINOR","FX_MAJOR","INDEX","GOLD"],
 "trial_risk_pct":0.35,"trial_max_gbp":8.0,"max_trials":2,"trial_loss_budget_gbp_week":40.0,
 "max_step_pct":1.5,"announce_steps":true,
 "day_line_pct":1.5,"day_pause_pct":2.25,"week_line_pct":5.0,"drawdown_line_pct":5.0,"drawdown_pause_pct":8.0,
 "pins":[{"key":"tnb|MOMENTUM_CONTINUATION|FX_MINOR|*","why":"Formula 1: flat £13 until 20 live trades, then judged (Aaron, 10 Oct)"}],
 "set_by":"installer, Aaron's request of 12 Oct 2026","set_utc":"…"}
```

**The envelope is invalid if:**
- it has an unknown key;
- `max_step_pct` is above `cfg.risk.max_risk_pct`;
- `day_pause_pct` ≥ `cfg.risk.max_daily_loss_pct`;
- `day_line_pct` ≥ `day_pause_pct`;
- `rider` or `lab` appears in `govern`;
- any number is negative or not finite.

**An invalid envelope means OFF**: Aaron's settings, plus a red line on the page.

**WATCH day.**
- Before `watch_until_utc`, PILOT behaves as WATCH.
- After it, PILOT starts at the next 06:30 UK release, but only if the WATCH-day self-check passed:
  - no validation error;
  - no would-be decision above a cap;
  - broker reconciliation ok on ≥ 95% of ticks.
- Otherwise it stays WATCH and says why.
- This is a timer the installer set on Aaron's behalf, not the coach raising its own powers.

### 10.2 The policy: `data/coach/policy.json` (schema 1)

```json
{"schema":1,"seq":1842,"written_utc":"…","valid_s":180,"account_login":0,"envelope_sha":"…","build":"<code stamp>",
 "mode":"PILOT","grants_allowed":true,
 "data":{"ok":true,"failed":[],"broker_check":{"records_today":-12.40,"page_today":-12.40}},
 "account":{"equity":0,"e0":0,"real_today":0,"open_real_risk":0,"peak":0},
 "brakes":{"day":"OPEN|LINE|PAUSE","week":"OPEN|LINE","drawdown":"OPEN|LINE|PAUSE","trial_budget_left_gbp":40.0},
 "headroom_gbp":0,
 "entries":{
   "tnb|MOMENTUM_CONTINUATION|FX_MINOR|*":{"route":"PIN","state":"PINNED","why_id":17},
   "tnb|SESSION_EXPANSION|INDEX|TREND":{"route":"REAL","state":"LIVE_TRIAL","risk_pct":0.35,"money_gbp":6.53,"rule":"G1","why_id":18},
   "tnb|BREAKOUT_ACCEPTANCE|FX_MAJOR|*":{"route":"PRACTICE","state":"BANNED_REAL","rule":"S1","why_id":19},
   "runner|*|*|*":{"route":"BASELINE","state":"PLACED","why_id":20},
   "ian|*|*|*":{"route":"SKIP","state":"PAUSED","rule":"B2","until_utc":"…","why_id":21}},
 "floors":{"tnb|SESSION_EXPANSION|INDEX|TREND":{"min_score":76}},
 "blocks":[{"bots":["tnb"],"symbols":["NZDCHF"],"route":"PRACTICE","rule":"S2","until_utc":null,"why_id":22}],
 "gates":{"rollover_ny":["16:45","17:15"],"friday_cutoff_ny":"16:00"},
 "sha256":"<sha256 of the canonical JSON without this field>"}
```

**Fields:**
- **Routes:**
  - `BASELINE`: Aaron's static settings decide.
  - `PIN`: the same, marked as Aaron's pin.
  - `REAL`: a coach grant; needs `risk_pct` and `money_gbp`.
  - `PRACTICE`: forced to paper; for a bot with no paper path this means SKIP.
  - `SKIP`: no new entry.
- **Canonical JSON:** `json.dumps(obj, sort_keys=True, separators=(",",":"), ensure_ascii=True, allow_nan=False)`.

**The writer** (`run.py`, using `policy.py`) works in this order; a failure at any step aborts the write:
1. Build the policy.
2. Run **the same validator the gate runs**.
3. Diff it against the previous policy.
4. Commit one ledger `changes` row per difference. A difference without a sentence aborts the write.
5. Write a temporary file, `fsync`, copy the current file to `policy.prev.json`, then `os.replace`.
6. Mark the `policies` row valid.
7. Write `status.json`.

### 10.3 Gate algorithm (`gate.CoachGate(bot, data_dir, cfg).decide(...)`; never raises)

`decide(approach, kind, condition, symbol, side, score, rr, now, static_real: bool, risk_ctx)` returns `Decision(route, risk_pct, money_gbp, why, seq, source)`, where `source` is one of `halt`, `off`, `watch`, `policy`, `fallback`, `error`.

1. `data/HALT` exists → no real entry: Trend & Breakout goes to PRACTICE, others SKIP. Practice is unaffected.
2. `data/COACH-OFF` exists, or the envelope is OFF or invalid, or the bot is not in `govern` → **BASELINE exactly** (today's code path).
3. Load and validate the policy. Any failure makes the whole file invalid; there is no partial use.
   - V1: JSON, ≤ 256 KB.
   - V2: schema 1.
   - V3: sha256 matches.
   - V4: `account_login` matches (when known).
   - V5: `envelope_sha` equals the current envelope's.
   - V6: written no more than 120 s in the future and no more than `valid_s` old.
   - V7: `seq` ≥ the last seen.
   - V8: every route is in the allowed set.
   - V9: every `risk_pct` and `money_gbp` is finite, ≥ 0, `risk_pct ≤ min(cfg.risk.max_risk_pct, envelope.max_step_pct)` and `money_gbp ≤ equity × that / 100`.
   - V10: every REAL key is in a governed bot, in an allowed kind, not pinned, and not forbidden by C2 or the Runner rule.
   - V11: trial count and trial size are within the envelope.
   - V12: brakes and headroom are finite and not negative.
   - V13: there is no unknown key or field.
4. WATCH (or before `watch_until`) → BASELINE, and log the would-be decision.
5. Valid policy → the most specific entry, then `floors`, `blocks`, `gates`, B5 headroom (governed keys), B6 one idea.
   - GUARD: the more restrictive of the entry and BASELINE (REAL becomes BASELINE).
6. Policy invalid only through V6 (stale, integrity fine), or a last-good policy in memory, or `policy.prev.json` passing V1-V5 and V13 → **BASELINE tightened by that policy's restrictions only** (PRACTICE, SKIP, floors, blocks whose `until_utc` has not passed). Its grants are dropped.
7. No policy has ever been seen and none is on disk (first install) → BASELINE.
8. Clamp REAL: `money = min(money_gbp, risk_pct × E/100, cfg.risk.max_risk_pct × E/100, envelope.max_step_pct × E/100, H)`.
9. Any exception → BASELINE and an error log line. This can never exceed Aaron's own settings.
10. Each decision for a real or would-be-real entry is appended to `data/coach/gate/<bot>-<UK date>.jsonl`: `{ts, ticket?, key, route, money, seq, source, why}`.

**Cost:** four `stat()` calls per entry decision, plus a parse only when the file changes.

### 10.4 What happens in each failure

| Situation | Real money |
|---|---|
| COACH-OFF, envelope OFF or invalid | exactly Aaron's settings |
| HALT | no new real entries anywhere; open trades managed as usual |
| Coach dead more than 3 minutes | Aaron's settings with the coach's last restrictions; grants lapsed |
| Policy corrupt, forged, rolled back, or from an old envelope | the last good restrictions, else Aaron's settings |
| First install, no policy yet | Aaron's settings |
| A bot's records unreadable | that governed bot paused (if a valid policy says so); no grants anywhere |
| Page or broker check down | no grants; restrictions continue; brakes never released |
| Bug inside the gate | Aaron's settings, logged |
| A REAL order above `max_risk_pct × equity` reaches `PaperBroker.send` | refused, logged CRITICAL, the coach is told through the gate log and puts itself in GUARD |

## 11. Hooks in each bot (Builder P; shipped once)

| File | Change |
|---|---|
| `broker/base.py` | `OrderRequest.coach_route: str = ""`. Never sent to the broker. |
| `engine/execution.py` | `Executor.submit(state, spec, volume, coach_route: str = "")` copies it into the request. |
| `broker/paper.py` | `static_goes_real(symbol, tactic, spec)` holds today's logic verbatim. `goes_real(req, spec)`: `coach_route == "REAL"` and `self.coach_confirm(req)` (an independent envelope re-check the trader attaches: key governed, kind allowed, not C2) **or** (`static_goes_real` and `coach_route not in ("PRACTICE", "SKIP")`). A new table `coach_real(ticket INTEGER PRIMARY KEY, opened_utc, key)` in `tnb_paper.sqlite` records every coach-real ticket after a fill. `_route_real` also returns real own positions whose ticket is in it (C3). `send` adds, for every order that goes real: risk at the stop = `spec.money_per_lot(abs(price − sl)) × volume` ≤ `cfg.risk.max_risk_pct × equity / 100`, else refuse with a CRITICAL log. |
| `engine/trader.py` | `__init__`: build `CoachGate("tnb", ...)` (a BASELINE-only object if that fails), attach `coach_confirm` to the PaperBroker.<br>`act()`: after `risk_pct_for`, compute `static_real` with `static_goes_real`, call `decide`, then:<br>• BASELINE/PIN: today's path exactly;<br>• PRACTICE/SKIP: `coach_route="PRACTICE"`, normal sizing (paper);<br>• REAL: `risk.size(..., risk_pct=money/E×100, top=False, risk=dataclasses.replace(cfg.risk, max_risk_money=money))`, then assert `sizing.risk_money ≤ money + 0.01` and `≤ max_risk_pct × E`, else route to practice.<br>The decision's sentence goes into the ENTRY event's `sizing_reasons`. After a fill, the gate log gets the ticket.<br>`_runner_copy_volume` call site: `decide` for `runner\|<tactic>\|INDEX\|<regime>` → SKIP gives `runner_volume = 0.0`; a lower money cap gives `min(copy, money / money_per_lot)` rounded down.<br>`_bot_kwargs`: `entries_allowed = lambda: self._bots_may_enter() and self.coach_gate.bot_may_enter(name, now)`, where `bot_may_enter` is always True when that bot's static mode is PAPER.<br>Every 60 s: write `data/coach/weather.json` (§8). |
| `ian/run.py`, `ian/engine.py` | `run.py` builds `CoachGate("ian", ...)`. `_entry_block` returns the gate's reason when it says SKIP (HALT, B2, B4, B5 using the policy's `headroom_gbp` less Ian's open risk). `risk_money = min(cfg.risk_money, gate money)` on the way down only. Its own £50 limit is untouched. |
| `runner/shadow.py` | none (C8). |
| `bandbreaker/engine.py`, `crowd/engine.py` | none (C9). |

Formula 1 sits on the pinned key, so its code path is today's path byte for byte. A test replays recorded scans with the coach in PILOT and asserts Formula 1's orders are identical to COACH-OFF.

## 12. Formula 1, the placed bots, and the lab's candidates

**Formula 1.**
- **Until 20 live trades (counted by `verdicts.formula1_live`):** PINNED; nothing from the coach applies except Aaron's HALT. D1-D5, S1 and S2 are computed in shadow and shown under "Needs you" ("the 3-days rule would switch it off: your call").
- **Practice blocking real.** The coach also counts, from `journal.decisions` blockers, the real Formula 1 signals it measured as blocked by a practice position, spacing or exposure (C4). It shows that count under "Needs you" as a proposed fix: "practice never blocks real money", which counts real caps on real positions only. That fix is Aaron's call and is not in this build.
- **At the nightly after the 20th live trade,** the coach evaluates `verdicts.formula1_judgement` (the plan's rule: net > 0 after costs and up days > down days):
  - **Fail:** from 06:30 UK the key is routed PRACTICE (a downward move, which the coach may make alone), and "Needs you" says so. Aaron can re-pin it with `pin formula1`. COACH-OFF would put it back on his config, which still lists it as live.
  - **Keep:** from 06:30 UK it is `LIVE(base)` at £13 and coach-governed: G3 steps by evidence, with each announced the evening before and vetoable. "Needs your yes" offers the plan's original "grow it" as the alternative: "jump to the top size (1.5%, £27.98)". If Aaron approves, the key goes straight to S4. Brakes and headroom still apply; the Kelly and stop-safety checks are shown but do not block Aaron's explicit choice.
- **Other FX-minor approaches** can be granted only after the judgement is recorded (C2).

**Momentum Runner and Financial Ian.**
- They are PLACED until 30 real trades each. Only B2, B4, B5 and HALT apply.
- At 30 they are judged by `verdicts.decide`. If the verdict says "back to paper", K1 applies (SKIP, and ask Aaron). Otherwise they continue at their own money, governed by D2, D3 and D4 (money down only).
- Their real money never rises in v1.

**Band Breaker and Crowd Fader** stay on PAPER and keep practising.
- If a bot-level key passes G1's evidence test, "Needs your yes" says: "Band Breaker's practice clears the trial test. Making it live at £10 a trade needs a restart, so it is your call."
- If Aaron makes one LIVE, it is PLACED as above.

**The lab's READY FOR LIVE candidates.**
- They are listed under "Needs your yes" with the lab's own evidence sentence, unchanged. The lab spec makes a live trial Aaron's word only, and lab v1 has no live executor.
- After Aaron's word, and once an executor exists (not this build), a candidate enters as a `LIVE_TRIAL` key `lab|<id>|<markets>|*`:
  - at the lab's £13;
  - judged after 20 trades by the lab and verdicts rule;
  - under D1-D5 and the brakes.
- Its lab practice trades count at λ = 0.5 for its own key only.
- Lab past and fresh tests never feed live decisions, except the family prior in §5, which can only make the coach more sceptical.
- The coach reads only `data/lab/summary.json`, read-only. It never opens `lab.sqlite` for writing and never imports `mintel.lab`.

## 13. Tweaks: tighten-only floors, judged on the same trades (Builder E, `tweaks.py`)

"Tweak bots off the back of real results" is done in the one way that is sound on this much data: **a floor that sends a key's weaker signals to practice.**

- Each floor is tested on the trades the key actually took, so the comparison is paired.
- A sent-away trade keeps practising, so a floor can be judged and reverted later.
- New entry or exit rules, and any loosening, belong to the lab (frozen grids, a fresh hold-out) or to Claude's paper-side calls at an install.

**Pre-registered challengers.** At most 3 per bot, written in code:

| Bot | Challengers |
|---|---|
| Trend & Breakout key | `min_score` = the bot's threshold + 3, + 6 (from `opportunity`); `min_rr` = 2.0, 2.2 (from `reward_risk`) |
| Ian | `min_score` + 5 (its `score` column) |

**Paired statistic.** For an incumbent trade *i*, `Δ_i = −x_i` if the challenger would have skipped it, else 0. Weights and clusters are as in §4.

- Prior `μ_Δ ~ N(0, 0.10²)`; `σ_Δ` is cluster-robust.

| Action | Needs all of |
|---|---|
| **Switch** (nightly, released 06:30 UK) | ≥ 40 incumbent trades over ≥ 8 UK days with ≥ 10 would-be-skipped trades; mean Δ ≥ +0.05 R per incumbent trade; `P(μ_Δ > 0) ≥ 0.983` (0.95 corrected for 3 challengers); at most 1 floor change per bot a night |
| **Revert** | 40 forward incumbent trades (the floored ones counted through their practice continuation) give `P(μ_Δ > 0) < 0.5` |

- A floor never applies to pinned Formula 1 and never relaxes anything in `config.json`.
- Loosening suggestions, such as "re-enable X in condition Y on practice", go in the nightly digest for Claude's review.

## 14. Ledger, commands and the icon (Builder D)

**`data/coach/ledger.sqlite`** (WAL):

```sql
changes(id INTEGER PRIMARY KEY, ts_utc, policy_seq INT, kind, target, from_state, to_state, rule, numbers_json, words TEXT CHECK(length(words) >= 20), by TEXT CHECK(by IN ('coach','aaron','installer')));
segments(key TEXT PRIMARY KEY, registered_utc, epoch, state, step, since_utc, cooloff_until, live_since_utc, trial_pnl_gbp REAL, peak_r REAL, cusum REAL, real_n INT);
policies(seq INTEGER PRIMARY KEY, written_utc, sha256, valid INT, json_gz BLOB);   -- kept 14 days
ticks(ts_utc, ok INT, ms REAL, err TEXT);
peaks(ts_utc, equity_peak REAL);
daily(day TEXT PRIMARY KEY, real_net REAL, source TEXT, est_lo REAL, est_mid REAL, est_hi REAL);
luck(date TEXT PRIMARY KEY, shuffles INT, pass_rate REAL, blocked INT);
plans(date TEXT PRIMARY KEY, json TEXT, released_utc, vetoed_json);
scorecard(date TEXT PRIMARY KEY, saved_gbp REAL, added_gbp REAL, trades INT);
-- triggers: no UPDATE or DELETE on changes; UPDATE on policies only of `valid` from 0 to 1
```

- Every sentence comes from fixed templates in `words.py`.
- `daily.real_net` is copied each night from the page's `coach_progress` per-day rows. `source` names the file and field. A day the page could not be read is stored as NULL ("could not be counted"), never estimated.

**Commands:** `python -m mintel.coach run | show [--once] | explain <key> | off | on | watch | guard | pilot | halt | resume | approve <id> | veto <id> | pin formula1 | unpin formula1 | replay --from 2026-09-17 | envelope show`.
- `halt` and `resume` write or delete `data/HALT` after a typed `YES`.
- `off` and `on` toggle `data/COACH-OFF`.
- `approve` and `veto` append to `data/coach/approvals.json` (`by: "aaron"`, a predefined item id from the plan).
- Mode commands rewrite only the envelope's `mode`, atomically.
- These CLI functions are the only writers of the envelope, HALT, COACH-OFF and approvals.

**Desktop icon `The Coach.command`.** It prints the one-line status, then offers:
1. open the page (`/coach`);
2. coach off: back to my settings;
3. HALT all new real-money entries;
4. resume;
5. approve or veto tonight's announced changes;
6. quit.

## 15. The page, in plain words (Builder G, `coach_page.py`)

**A card on "/", directly after the lab's "Today's standouts".**
- It obeys the lab spec's pins for "/": no `<table`, no `class="tile"`, and the existing strings and order are kept.
- `/coach` holds the full record and may use tables inside `.s-old`.
- The figures in this example are **made up**, to show the wording:

```
The coach's call - Fri 16 Oct, 10:15 · in charge within your limits · checked 40 seconds ago
Real money now
  Formula 1 (momentum trades on minor currency pairs): £13 a trade - your choice until 20 live trades (6 so far). The coach leaves it alone until you judge it.
  Momentum Runner: about £10 a ride, waiting for New York (14:30 UK).
  Financial Ian (Bitcoin, Ether): £10 a trade; 8 real trades - too few to judge (judged at 30).
Practising, no money: Band Breaker, Crowd Fader and the rest of Trend & Breakout. They keep practising - that is how they earn a place.
Markets now: 5 of 14 minor pairs are in a fast market - Formula 1 may trade them. GBPCAD is trending - momentum is not traded there.
Changed today: 09:42 Financial Ian paused until midnight: the account is down £42.10 today, past the coach's line of £41.98 (2.25%). Its open trades are still managed.
Against £100 a day: real money since 12 Oct: +£23.10 over 5 trading days, +£4.62 a day. To average £100 a day by Fri 30 Oct, the 10 trading days left need £147.69 a day.
  Source: MetaTrader's record, every bot, after commission.
Honest outlook: about £3 to £15 a day on the evidence so far - an estimate, not a result. £100 a day is 5.4% of the account every day; the one idea with an edge on record trades about 3 times a day. The coach never sizes up to catch up.
Needs your yes: (1) Formula 1's judgement after 20 live trades (14 to go). (2) Let the 3% stop also count Financial Ian. (3) Practice blocked 2 real Formula 1 trades this week - fix it? (4) Lab ideas ready for live: none yet.
Stop the coach: double-click "The Coach" on the desktop and choose 2 - everything goes back to your own settings at once.
```

**Rules for the card:**
- **Every money figure has a source.** Progress is `coach_page.progress(sd, deals, now)`: `standing.period_view(sd, deals, "custom", "2026-10-12", <today UK>)["trading"]`, the page's own arithmetic. It covers real money, every bot, after commission and swap, positions closed on UK days.
- **What is counted:**
  - Trading days are UK weekdays from 12 Oct up to today.
  - The "need a day" figure is `(100 × 15 − total) / days_left`.
  - Practice money never counts.
- **No pace line** ("£ a day", "need a day") before the 5th trading day (today counts). Before then: "Too early to say a pace: 2 trading days so far."
- **The outlook is always labelled "an estimate, not a result".**
  - It is the nightly bootstrap's 10th to 90th percentile for the current line-up (posterior draws × rates × sizes).
  - It is never added to a total.
  - Before the first nightly: "not estimated yet".
- **Words for unreadable or missing data:**
  - MetaTrader unreadable: "cannot be counted just now".
  - Broken coach file: "The coach's instructions could not be read (reason): the bots are on your own settings."
  - Coach OFF: "Switched off - the bots are on your own settings."
  - WATCH: "Watching only - it changes nothing today."
  - Synthetic data: every figure replaced by "TEST DATA - not a result".
- Use the pound sign, never "GBP".

**`/coach` shows:**
- each key in words, for example: "momentum on minor pairs in fast markets: 38 past real trades, about 17 separate bets, +£156 after costs; after allowing for luck about +0.2 times its risk a trade; chance of a real edge about 93%";
- its state, size, P⁺, LCB80 and trades;
- tonight's plan and the announced steps (with veto instructions);
- the decision log (when, what, why, the rule, the numbers);
- the brakes, headroom and data checks;
- the policy's age and sequence number;
- the luck-check rate;
- the scorecard;
- the honest arithmetic of §17;
- the sources.

**The pulse adds `reports/live/coach.json`** (≤ 20 KB, trimmed in order: decision log, keys beyond 12, the market rule's rows beyond 20, then suggestions, plan, brake words and sources). It is built from `status.json` and `/api`'s `coach_progress`.

**Nightly upload:** `reports/coach/<date>/` holding `report.md`, `decisions.csv`, `plan.json` and `policy.json`, sent through `report_upload.upload(..., day=f"coach/{date}")`.

## 16. The nightly Claude review's role

The review cannot change the Mac between installs; the coach on the Mac is the learner. Each night it:
1. Reads `reports/live/coach.json`, `reports/coach/<date>/*`, `bots.json` and `rules.json`.
2. **Audits:**
   - any real decision above its grant or the ceiling;
   - any figure without a source;
   - any grant that ignored C2 or the Runner rule;
   - the luck-check rate;
   - the scorecard.
   Any finding is written in the daily record (`docs/strategies/`) and raised to Aaron.
3. Updates `epochs.py` and the paper-side settings only through a commit that Aaron installs.
4. May add a `coach` key to `docs/plan.json` (text only).
5. Proposes loosening, new challengers or threshold changes as code changes for Aaron's next install.

It never edits the coach's ledger, envelope, approvals, HALT or COACH-OFF, and it never presents an estimate as a result.

## 17. The honest projection, with arithmetic

**The size of the target.**
- £100 a day is 100 / 1,865.61 = **5.36% of the account a day**.
- Over the 15 UK trading days from 12 to 30 Oct that is **£1,500 (+80.4%)**.
- The 3% stop is £55.97, so the target is **1.79 times the stop**.
- The 1.5% ceiling is £27.98 a trade.

**Formula 1's record** (re-derived from the uploaded `trades.csv`, 17 Sep-8 Oct; `scratchpad/coach/judge/proj.py`):
- **Totals:** 38 trades on 10 days, +£156.31 after £29.89 commission, which is £4.11 a trade (sd £8.52), t = 2.94.
- **Without its best day** (1 Oct, +£75.21): 25 trades, +£81.10, t = 1.75.
- **Rate:** 38 trades over 14 weekdays with report files = **2.7 a weekday**. The 3.8 figure counts only days that traded.
- **Real risk:** the trades risked a median **£6.09**, so the record is **+0.70 R a trade after costs, sd 1.40 R**.
- **Independence:** about 17 separate bets.
- **Allowing for luck:** it was picked as the best of 27 segments. After allowing for that, the three designs' empirical-Bayes estimates were +0.13 and +0.14 in "net £ ÷ 10" units (and 0.23-0.30 price-R before commission). Converted at the real £6.09 risk, 0.13-0.14 × 10 / 6.09 = 0.21-0.23 R. The true edge is therefore about **0.2 R a trade (range 0.1-0.3 R), with a chance of a real edge of about 0.92-0.94**. At £13 that is about £2.60 a trade.

**Simulation.** 20,000 runs of 15 days. Each day's trade count is Poisson. Outcomes are resampled from Formula 1's 38 net-R outcomes, shifted to each scenario's edge. The 3% stop is applied to Formula 1 alone.

| Scenario | Edge | Size | Trades a weekday | Mean £ a day | 10-90% of the 3-week average | Chance 3 weeks end negative | Chance of averaging £100 |
|---|---|---|---|---|---|---|---|
| No edge | 0 | £13 | 2.7 | £0.0 | −£9.7 to +£9.7 | 50% | 0 |
| Low | 0.10 R | £13 | 2.7 | £3.4 | −£6.2 to +£13.3 | 33% | 0 |
| **Central (after luck)** | **0.20 R** | **£13** | **2.7** | **£7.0** | **−£2.8 to +£16.9** | **18%** | **0** |
| Central, more trades | 0.20 R | £13 | 3.8 | £9.7 | −£2.0 to +£21.5 | 15% | 0 |
| Central, Aaron jumps to 1.5% after the judgement (day 8) | 0.20 R | £13 → £27.98 | 2.7 | £10.8 | −£5.5 to +£27.7 | 21% | 0 (the 3% stop hit on 8% of days) |
| Record holds in full | 0.70 R | £13 | 2.7 | £24.6 | £13.8 to £35.7 | ≈ 0 | 0 |
| Record holds and jump | 0.70 R | £13 → £27.98 | 3.8 | £54.0 | £31.4 to £77.1 | ≈ 0 | 0.75% |

**Everything else on record:**

| Source | Record | Planned contribution in 3 weeks |
|---|---|---|
| Trend & Breakout overall | 436 real trades, −£7.58 after £159 commission | |
| Rider | no edge (1,073 trades, −1.33 pips a trade); binned | |
| Runner | 2 rides, −£14.28 | |
| Ian | about 8 trades, about +£5 | |
| Band Breaker | 2 trades, −£9.23 | |
| Crowd Fader | 4 trades, −£29.00 | |
| All of the above | priors −0.05 to 0 R | **£0 ± £1-2 a day** |
| Coach's trials | G1 needs ≥ 15 trades after install over ≥ 8 days | **0-1 trials** before 30 Oct |
| Formula 1's steps | the Kelly cap at an illustrative LCB80 of 0.10 R is 0.57%, below base | most likely **no step** before 30 Oct, unless its live trades run clearly above the luck-adjusted estimate |
| Lab | needs 40 complete days of minute bars first; earliest READY FOR LIVE about 3 weeks after its first full night | **£0** |

**Realistic outcome, 12-30 Oct: about £3-£15 a day, central about £7.** There is roughly a 1-in-5 chance the three weeks end negative.

**The chance of averaging £100 a day is under 1% in every scenario, including the most optimistic one with Aaron's jump to the ceiling.**

**What £100 a day needs.** Daily £ = edge × money a trade × trades a day.

| Edge | Size | Trades a day needed |
|---|---|---|
| 0.2 R | £27.98 | 17.9 |
| 0.2 R | £13 | 38.5 |
| 0.7 R (the record, if it were true) | £27.98 | 5.1 |
| 0.7 R | £13 | 11.0 |

Formula 1 trades 2.7 times a weekday. £100 a day therefore needs about **3 to 6 independent edges the size of Formula 1, each proven and running at or near the ceiling**. The alternatives are a much larger account, or an edge far stronger than anything on record.

Sizes scale with equity, so this is a months-long project of finding and proving edges (the lab's and the coach's job), not of sizing one idea up. Sizing up early mostly buys days that hit the 3% stop: 8% of days at the ceiling with a 0.2 R edge.

**How progress is reported.**
- One real figure: `period_view` custom from 12 Oct, every bot, after commission, as MetaTrader records it.
- The gap to £100 a day.
- One labelled estimate.
- Nothing else.

The coach's decisions are identical whatever the target is set to, and a test pins this.

## 18. Tests each builder writes

**P**
- **`test_coach_policy.py`**
  - each of V1-V13 rejects;
  - canonical sha;
  - `allow_nan=False`;
  - atomic write (a reader running alongside sees the old or the new file, never a mix);
  - `prev` rotation;
  - over 256 KB refused;
  - writer and reader share one validator.
- **`test_coach_gate.py`**
  - the full table: HALT × COACH-OFF × 4 modes × policy state (valid, stale, corrupt, absent, first install, rolled back, old envelope) × pinned × governed × `static_real`;
  - **cap property:** 20,000 seeded random policies (1e9, −1, NaN, strings, huge keys) and random equity. Decided money ≤ `min(1.5% × E, envelope cap, H)` always. No exception escapes. Random byte corruption of the file never produces a grant;
  - GUARD never exceeds BASELINE (property);
  - COACH-OFF takes effect on the next call with no restart;
  - the fallback keeps restrictions and drops grants.
- **`test_coach_caps.py`**
  - `risk.size` with `risk=replace(..., max_risk_money=…)` never above `max_risk_pct` after lot rounding;
  - `PaperBroker.send` refuses an oversized REAL request;
  - a policy granting £1,000 gives at most 1.5%;
  - the step table never exceeds `cfg.risk.max_risk_pct`.
- **`test_coach_halt.py`**
  - every bot (Trend & Breakout, Runner, Band Breaker or Crowd Fader if LIVE, Ian) refuses new real entries with HALT, independent of the coach process;
  - open positions are still managed;
  - practice continues.
- **`test_coach_hooks.py`** (SimBroker + PaperBroker + Trader)
  - Formula 1's orders are byte-identical with the coach OFF, WATCH, GUARD and PILOT before its judgement;
  - a demoted key goes to practice;
  - a granted INDEX key goes real **and its stop can be modified and closed after a PaperBroker restart** (C3);
  - Runner copy volume 0 means no ride;
  - Band Breaker and Crowd Fader on PAPER are never gated;
  - Ian's `_entry_block` honours the gate and its £50 limit is unchanged;
  - `weather.json` is written atomically;
  - existing suites run unchanged: `test_paper*`, `test_trader*`, `test_formula1*`, `test_ian*`, `test_true_numbers.py`.

**E**
- **`test_coach_records.py`**
  - real versus paper by book membership: a real ticket of about 1.99e9 is real;
  - `runner_feed` rows are excluded;
  - estimated commission before 9 Oct;
  - the fallback risk is flagged;
  - clusters by leg (the 1 Oct Swiss franc trades form a few clusters);
  - data-check exclusions;
  - broker reconciliation.
- **`test_coach_estimate.py`**
  - hand-worked posteriors;
  - leave-one-out parents;
  - the sceptical cap;
  - DerSimonian-Laird clipping;
  - cluster-robust variance;
  - Kelly on LCB80 divided by c̄;
  - the forward-only posterior;
  - the real-only chain;
  - on a fixture of the reports branch, Formula 1's key gives `m` between 0.1 and 0.3 R and P⁺ between 0.85 and 0.97.
- **`test_coach_rules.py`**
  - S1 and S2, codified exactly, reproduce from the reports fixture the dated `scan.disabled_tactic_regimes` entries added from 25 Sep onward; any entry the fixture cannot reproduce (for example, files missing for 29 Sep and 2 Oct) is listed in the test as a named exception with its reason;
  - S3: no judgement on fewer than 3 days.
- **`test_coach_luck.py`**
  - 1,000 synthetic null keys (mean 0, sd 1.4 R, clustered, 2-4 trades a day, 60 days): P(trial) ≤ 5%, P(LIVE base) ≤ 2%, P(any step) ≤ 0.5%;
  - a planted +0.3 R key reaches a trial ≥ 50% of the time within 60 days;
  - a key with one huge day (best day > 40% of net) never passes;
  - fewer than 15 forward trades means no trial;
  - a leaky gate (shuffle pass rate > 2%) blocks promotions;
  - **thresholds may only be made stricter to pass.**
- **`test_coach_tweaks.py`**
  - paired Δ on a hand fixture;
  - 0.983;
  - never applies to the pinned key;
  - never loosens;
  - revert;
  - at most 1 change a night.

**D**
- **`test_coach_decide.py`**: every row of §6.1 and §6.2; C2 and the Runner rule; Formula 1's judgement (fail → PRACTICE, keep → LIVE(base) next day, jump only with an approval); K1; hysteresis.
- **`test_coach_monotone.py`** (no chasing):
  - making any one trade's outcome worse never raises any route, state or size;
  - adding a loss never raises size;
  - progress-against-£100 values give an identical policy, and `decide.py` cannot import `report.py` or `coach_page`;
  - **grants and steps appear only in the nightly plan and take effect only after 06:30 UK** (property over random intraday event streams with a fake clock).
- **`test_coach_brakes.py`**:
  - B1-B9;
  - Ian's money counted;
  - the saved peak survives a restart;
  - headroom including Ian;
  - trial budget across a week rollover;
  - no brake released on unreadable data.
- **`test_coach_ledger.py`**:
  - a change without a sentence is refused;
  - ledger rows are committed before the policy replace (crash injection);
  - append-only triggers.
- **`test_coach_run.py`**:
  - tick on a fake clock;
  - lease rewrite;
  - nightly at 22:20 UK in BST and GMT;
  - catch-up after downtime;
  - the WATCH-day self-check;
  - the scorecard drops the coach to GUARD.
- **`test_coach_safety.py`** (source checks over `mintel/coach/`):
  - no `order_send`, `send(`, `modify_stops`, `close(` or `secrets.json`;
  - no forbidden imports (§3);
  - writes only under `data/coach/` (the CLI's envelope, HALT, COACH-OFF and approvals writers excepted, and called only from `__main__` functions);
  - never assigns `max_daily_loss_pct`, `max_risk_pct`, `daily_loss_counts_practice`, `top_size_enabled` or `mode` in config;
  - nothing named `coach` under `data/heartbeats/`;
  - the run loop never writes the envelope or approvals.
- **`test_coach_replay.py`**: `replay --from 2026-09-17` on the reports fixture makes each day's decision from earlier data only. The output is stamped "flatters: the segments were picked by people looking at this same record".

**G**
- **`test_coach_page.py`** (made-up fixture):
  - the card's order and wording;
  - every money figure has a source;
  - no pace line before 5 trading days;
  - "an estimate, not a result";
  - "cannot be counted just now";
  - broken-file, OFF and WATCH wording;
  - synthetic gives "TEST DATA - not a result" and no pound sign;
  - escaping;
  - the lab's "/" pins hold (no `<table`, the order kept);
  - `progress()` equals `period_view` custom from 12 Oct on a fixture;
  - account figures are identical with and without coach data.
- **`test_coach_ops.py`**:
  - the watchdog `coach` entry probes `status.json` freshness (180 s) and has no heartbeat;
  - it is enabled when the envelope mode is not OFF;
  - `pulse_files` adds `coach.json` ≤ 20 KB;
  - installer `bash -n` passes;
  - the marker is written only on success;
  - call order after the lab's steps;
  - the icon is placed once.

## 19. Verification and rollout

1. Each builder's tests are green. P also runs `test_paper*`, `test_trader*`, `test_formula1*`, `test_ian*` and `test_true_numbers.py`; G reruns the lab's page and ops tests and `test_mac_pipeline.py`.
2. End to end on SimBroker with a synthetic store (stamped synthetic): records → nightly → policy → Trader, PaperBroker and Ian gate → page. Then the whole suite with `PYTHONHASHSEED=0`.
3. `replay --from 2026-09-17` on the reports fixture. Read the decisions; none may be a promotion before day 8.
4. **On the Mac, in order:**
   1. Aaron's one-liner install. `apply_coach_2026_10_xx` (marker only on success) creates `data/coach/`, writes the envelope (PILOT with `watch_until_utc`), sets `CODE_CHANGED` and `remember_restart`, and places the icon.
   2. `python -m mintel.coach show --once`.
   3. **One full WATCH trading day:** read `data/coach/gate/*.jsonl` and confirm zero would-be decisions that add risk beyond a grant, zero cap violations, and reconciliation ok.
   4. PILOT from the next 06:30 UK release.
   5. Read the daily audit line for the first week.
   6. Formula 1's judgement at about 20 trades (2.7 a weekday from 12 Oct gives about 21 Oct).
   7. 26 Oct: Claude's review of the coach's own numbers (scorecard, luck rate, gate logs); Aaron decides any envelope change.
   8. 30 Oct: the three-week report against the single real figure.

## 20. Uncertainties and cut order

**Uncertainties, stated plainly:**
- **New code on the trading path.** A gate bug could block real trades. Mitigations: pinned Formula 1 bypasses the policy logic; the gate cannot raise; COACH-OFF returns exactly to baseline.
- **Weights and defaults are judgement calls** until measured: the practice weight (0.5), the haircut (0.10 R) and the τ defaults. They are shown on `/coach`.
- **The coach's lines lag by up to 60 s, and Ian's headroom can overlap by one trade.** The 3% stop itself is real-time in the trader.
- **Formula 1's trade rate may differ from its record** (C4, the 15-minute news block). The coach measures this and does not adjust it.
- **The units correction** (median risk of £6.09) rests on gross ÷ price-R for 35 of 38 trades. The coach recomputes it from the journal's `risk_money`.

**Cut order if time is short** (the first item is cut first):
1. the lab family prior;
2. tweaks (§13);
3. the session-phase gates;
4. the market board on the page (keep `weather.json`);
5. the scorecard;
6. `/coach` detail (keep the card).

**Never cut:**
- the envelope;
- the lease and fail-safe;
- the three-way cap enforcement;
- C2, C3 and the Runner rule;
- Formula 1's pin;
- the night-only loosening;
- the luck check;
- the data checks;
- the sources on every figure;
- HALT and COACH-OFF.

## 21. Aaron's 12 Oct instruction: no limits on the demo (OVERRIDES the sections above where they conflict)

Aaron, 12 Oct 2026, after this spec was designed: "Stop worrying about limits now, remove them as and when as this is not REAL money, I can add money whenever however much we need, although it's important for when we do use real money. right now it doesn't matter, we are testing the bots of fake demo money so aim for the stars but keep in mind that we treat trades as if it were real money."

**The principle.** On the DEMO account only, the coach may lift limits **step by step, when the evidence says a limit is holding back a proven winner** ("as and when"), never all at once and never for an unproven key. Everything that makes a trade a real-money trade stays. On a real-money account every lifted limit reverts automatically to the v1 values of sections 6-9, and the page says so.

**Demo test (fail-safe).** Lifted values apply only while BOTH hold: `config.json` mode is DEMO, and the broker reports a demo account (the trader's ACCOUNT health check: trade mode demo / server name contains "Demo"), as written by the trader into `data/coach/weather.json` (`account_demo: true`, refreshed every 60 s). Missing, stale (> 180 s) or contradictory = real-money values. A test pins each case.

**What the coach may do on the demo (PILOT; every up-move still nightly, released 06:30 UK, announced the evening before, one-command veto):**

| Limit | v1 (real money) | Demo ladder (max) | Each step up needs |
|---|---|---|---|
| Per-trade size for a governed key (section 6.1 steps) | trial 0.35%, base, S1-S4 up to 1.5% | after S4: S5 2.0%, S6 2.5%, S7 3.0%, S8 4.0%, S9 5.0% (envelope `demo_max_step_pct` 5.0) | G3 as written but with **half-Kelly** (0.5 instead of 0.25) on LCB80; at a new high of its real R; >= 20 real trades at the current step; <= 1 step-up a night, <= 3 an ISO week; the stop-safety bootstrap run against the EFFECTIVE daily stop |
| Per-trade ceiling `risk.max_risk_pct` | 1.5%, read only | the gate passes `risk=dataclasses.replace(cfg.risk, max_risk_pct=<step>, max_risk_money=<step GBP>)` for that one sizing call; the PaperBroker ceiling check uses the envelope's effective ceiling | only for keys at a demo step; config.json never written |
| Trials (G1) | 0.35% (<= 8), <= 2 at once, <= 40 a week of trial losses | start at base (Trend & Breakout's base, today 13); <= 4 at once; <= 150 a week | G1's evidence rules unchanged |
| Daily stop (`risk.max_daily_loss_pct`) | 3%, read only | effective stop 3 -> 4 -> 5 -> 6 -> 8 -> 10% (envelope `demo_max_daily_stop_pct` 10) | >= 2 governed keys LIVE at base or above with P+_real >= 0.90; the stop-safety bootstrap of the proposed day trips the new level on <= 5% of days; and in the last 10 UK trading days the current stop blocked at least one entry of a proven key on a day that still ended positive. The trader reads the effective limit through `gate.effective_limits()`, which returns config.json's values unless a valid lease carries demo values AND the demo test passes |
| Coach brakes B1/B2 | 1.5% / 2.25% of E0 | half / three-quarters of the effective daily stop | follow the stop |
| Drawdown breaker (12%) and the coach's B4 (5% / 8%) | read only | 25% / 10% / 15% | follow the stop's level: 25% once the stop is >= 6% |
| Exposure caps (correlated 2.25%, total 4.5% on the Mac) | read only | correlated = 1.5 x the highest live step, total = 3 x it (never above 15%) | follow the steps |
| Formula 1 | pinned until its 20-live-trade judgement | unchanged: pinned; after the judgement "keep" makes it governed and it climbs the demo ladder by the same rules | the judgement |
| Runner, Ian, Band Breaker, Crowd Fader, lab READY FOR LIVE | v1 rows of section 9 | v1 rows stand (no executor or steps in v1); the nightly Claude review may act on them by a tested install, as Aaron delegated on 12 Oct | - |

**Unchanged even on the demo (real-money discipline):** every trade has a broker stop from the moment it opens; never double after losses, never average into a losing trade, no grids, never add exposure because the last trade lost; never grow right after losses (new-high rule); today's profit or loss and progress against 100 a day are never inputs to anything that adds risk; legal market behaviour only; never invent performance; HALT and COACH-OFF are Aaron's; the coach never writes config.json, never places, closes or modifies an order and never starts or stops a process.

**Treat it as real money - the shadow figure.** For every real trade the coach sized, the ledger stores `real_money_gbp` = what the same trade would have risked under the v1 (real-money) rules, and `real_money_net_gbp` = its result rescaled to that risk. The page shows both ("today +£64; at real-money sizes +£19"), and the PROVEN judgement and every step-up use R, never the demo pound figure, so a winner here is a winner with real money.

**Page line (plain words):** "Demo mode: limits are lifted step by step as the bots earn it (your instruction of 12 Oct). Every trade still has a stop. At real-money sizes today would have been £X." When the demo test fails: "Real-money limits in force".

**Section 17 on the demo ladder (arithmetic, not a forecast).** At 5% a trade (93 on 1,865) a 0.20 R edge at 2.7 trades a weekday is about 50 a day on average with very wide days (the effective stop would need to be ~10%); a 0.40 R edge at the same size is about 100 a day; Formula 1's full look-back record (0.70 R) would be about 176 a day. So 100 a day needs a confirmed edge of about 0.4 R a trade or several independent edges, and the coach reaches those sizes only by earning each step on real results - which takes weeks, not days. The page tracks the real daily average from 12 Oct against 100, never a projection.

**Tests added for this section:** the demo test's fail-safe cases; no demo value is used when the lease is stale or the account is not demo; every step and stop raise is announced the evening before and vetoable; the ladder never skips a step and never steps within 5 UK trading days of the last change; no step-up after a loss without a new high; the shadow figure equals the v1 sizing for the same trade; config.json bytes unchanged after a full simulated week.
