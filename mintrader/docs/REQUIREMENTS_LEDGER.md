# Requirements ledger

This is the project's permanent memory of Aaron's instructions. Every important
instruction gets a row.

- **New instructions are additions or refinements.** They never permit wiping
  out earlier work.
- **Old requirements are never deleted silently.** When one is replaced, its
  row is marked SUPERSEDED and names what replaced it.
- **Conflicts:** when two instructions genuinely conflict, the most recent
  explicit instruction on that exact subject wins.

Read this, `CLAUDE.md` and `docs/MASTER_MISSION.md` before any major change.

**Terms:**
- **PAPER** (practice): simulated orders on live prices.
- **DEMO** (in code: LIVE, "real money"): real orders on the IC Markets demo
  account 53020675 (GBP).

## Standing rules

| Date | Area | Requirement | Status | Files affected | Superseded by |
|---|---|---|---|---|---|
| Sep 2026 | Security | No passwords in source control; secrets only in `data/secrets.json`. The lab and the coach never open it | ACTIVE | mintel/config.py, deploy/mac/* | |
| Sep 2026 | Conduct | Legal market behaviour only. No spoofing, layering, wash trading, quote stuffing or fake orders. Every order is genuine intent | ACTIVE | all order paths | |
| Sep 2026 | Risk | Never double after losses, average into losers, run grids, or add exposure because the last trade lost | ACTIVE | engine/risk.py, all bots | |
| Sep 2026 | Honesty | Never invent performance. Every money figure is the broker's or the bot's own record | ACTIVE | ops/*, reports | |
| Oct 2026 | Money | Never spend Aaron's money (e.g. Databento) without showing the exact cost and getting a typed yes | ACTIVE | ian/feed | |
| Oct 2026 | Process | Don't commit or push unverified trading-code changes. Test, review, then push. Commits carry the attribution lines | ACTIVE | all | |
| 13 Oct 2026 | Process | Don't overwrite previous tweaks. Treat a new instruction as an addition or refinement. Read CLAUDE.md, MASTER_MISSION.md and this ledger first; preserve compatible instructions; merge the new one in; the most recent explicit instruction wins only on genuine conflict | ACTIVE | CLAUDE.md, this file | |
| 13 Oct 2026 | Process | No accidental regressions. Changing one part never resets another (entry vs exit, FlowLock vs pair filters). Strategy configuration and presentation are separate systems | ACTIVE | all | |
| 13 Oct 2026 | Process | Version important strategy changes. A proven strategy is never silently overwritten: keep v1.3 when making v1.4 so they can be compared. Old bots: rename STRATEGY_NAME / the rule set and keep the old doc. Lab: a tweak is a new child variant; the parent stays | ACTIVE | mintel/config.py, docs/strategies/*, mintel/lab/refresh.py | |

## Trading rules and strategy tweaks (all still active)

| Date | Area | Requirement | Status | Files affected | Superseded by |
|---|---|---|---|---|---|
| Oct 2026 | FlowLock | The FlowLock stop can only tighten, never widen | ACTIVE | mintel/engine (flowlock), tests/test_end_to_end.py | |
| Oct 2026 | Stops | Every trade has a broker stop; stops sit outside the noise (floor) | ACTIVE | engine, all bots | |
| 5 Oct 2026 | Account | Demo reset to £2,000 on 5 Oct 12:30 UK; results are measured from then | ACTIVE | config tracking_start | |
| 9 Oct 2026 | Line-up | Rapid Momentum Rider binned: no edge on real prices | ACTIVE | rider.json OFF | |
| 9 Oct 2026 | Trend & Breakout | Momentum continuation in a fast market on FX majors is off (standing rule met) | ACTIVE | mintel/engine, docs/strategies/v2-commission-first.md | |
| 9 Oct 2026 | Runner | The Momentum Runner never rides momentum continuation (v5); never rides a trade already closed | ACTIVE | mintel/runner | |
| 9 Oct 2026 | Trend & Breakout | Formula 1: minor pairs on DEMO at about £13 a trade (0.7%, cap £13) | SUPERSEDED | config tnb.live_groups | 13 Oct: everything to PAPER |
| 10 Oct 2026 | Trend & Breakout | Formula 1 choices: momentum continuation only on minors; flat £13 (top size off) until 20 live trades | SUPERSEDED (Formula 1 is off) | config tnb.* | 13 Oct: everything to PAPER |
| 10 Oct 2026 | News | No new Trend & Breakout entry within 15 minutes before a high-importance release | ACTIVE | config news | |
| 10 Oct 2026 | Daily stop | The daily-loss stop counts real (DEMO) money only; paper can never cover a real loss | ACTIVE | engine/risk.py | |
| 12 Oct 2026 | Band Breaker | A VWAP stop nearer than half the hour's noise move gives way to the far band (min_stop_noise 0.5) | ACTIVE | mintel/bandbreaker/engine.py | |
| Daily | Standing rules | An approach-in-condition negative on 3+ days traded is switched off. A market negative on 2/3+ of its days over 6+ days is excluded | ACTIVE | nightly review | |
| 13 Oct 2026 | Paper/Demo | PAPER before DEMO. Every test starts on paper. DEMO performance is always reported separately from paper and never mixed | ACTIVE | mintel/ops/*, weekly.py | |

## The squad system: conveyor belt, promotion, research

| Date | Area | Requirement | Status | Files affected | Superseded by |
|---|---|---|---|---|---|
| 9 Oct 2026 | Autonomy | "Anything on paper is your call": Claude tweaks, changes and shuts off paper tests without asking | ACTIVE | nightly review | |
| 10 Oct 2026 | Lab | Build the lab: consistency stays, luck doesn't; winners stand out daily and are watched like scouts | ACTIVE | mintel/lab/* | |
| 12 Oct 2026 | Coach | Learn from everything, tweak all bots from real results, change tactics through the day; aim for at least £100/day average over 3 weeks; the overall aim is maximum profit after commission | ACTIVE | mintel/coach/*, nightly review | |
| 12 Oct 2026 | Demo limits | On the demo, lift limits step by step by evidence ("aim for the stars"), but treat every trade as real money: stops, discipline, and a real-money shadow figure | ACTIVE | mintel/coach (section 21) | |
| 12 Oct 2026 | Approval | Ask Aaron only for real money proper (a non-demo account) or for adding funds | ACTIVE | all | |
| 13 Oct 2026 | Line-up | Everything out of DEMO into paper; only proven tests move up | ACTIVE | deploy/mac (.all-practice-2026-10-13), data/coach.json | |
| 13 Oct 2026 | Pool | One pool of tests. Old bots, their parts and lab ideas are all just tests; don't report bot by bot by old names | ACTIVE | weekly.py, dashboard, reviews | |
| 13 Oct 2026 | Lab | Every lab idea practises on paper (practise_all). The past and fresh tests are information, not a gate | ACTIVE | mintel/lab/practice.py, stages.py | |
| 13 Oct 2026 | Promotion | Weekly pick: Friday 22:30 UK, up to X (weekly_max_promotions, default 3) that pass the bar go to DEMO from Monday 06:30 UK. Veto and HALT respected | SUPERSEDED (cadence and the weekly limit; the veto, HALT and the intent/rollback safeguards stay ACTIVE) | mintel/ops/weekly.py, watchdog.py | 14 Oct: daily promotion; 8–12 on demo |
| 13 Oct 2026 | Promotion | The weekly bar: 30+ paper trades over 5+ days in the last 28, profit after costs, profit factor 1.2+, more up days than down, a luck check | SUPERSEDED | mintel/ops/weekly.py | 14 Oct: the early-promotion bar |
| 13 Oct 2026 | Demotion | Back to PAPER after 20 DEMO trades not in profit after costs, or at once at 10× per-trade risk lost. A pick taken back is never re-released by the same plan | SUPERSEDED (the 20-trade rule and 10×; "never re-released by the same plan" stays ACTIVE) | mintel/ops/weekly.py | 14 Oct: evidence-based demotion |
| 13 Oct 2026 | Conveyor belt | Saturday refresh: retire clear losers (20+ trades, net ≤ 0, PF < 0.9), add neighbouring variants of the top 10, refill to 640 with new ideas. Claude adds 1 to 3 new families every Saturday | ACTIVE | mintel/lab/refresh.py, routine "MarketBot weekly strategies" | |
| 13 Oct 2026 | Specialisation | Tests are specialised by market (pair/kind), session and condition; market matching and current-condition research feed the daily team | ACTIVE | lab families, coach | |

## Reporting and UI

| Date | Area | Requirement | Status | Files affected | Superseded by |
|---|---|---|---|---|---|
| 10 Oct 2026 | UI | Bot showcase in simple layman terms | SUPERSEDED | mintel/ops/showcase.py | 13 Oct dashboard redesign |
| 12 Oct 2026 | UI | AI-hub-style layout with a sidebar and KPI cards | SUPERSEDED | mintel/ops/dashboard.py | 13 Oct dashboard redesign |
| 13 Oct 2026 | UI | Colours distinct from the AI hub (teal) | ACTIVE (carried into the redesign's palette) | dashboard.py | |
| 13 Oct 2026 | UI | Home: every test with its profit, winners at the top, an overall total | ACTIVE (becomes the PAPER LEAGUE) | dashboard.py, weekly.py | |
| 13 Oct 2026 | UI | Full frontend redesign: TRADING SQUAD; TODAY / 5 DAYS / 1 MONTH / ALL TIME; headline cards; PAPER LEAGUE (POS, BOT, PROFIT, WIN RATE, TRADES, STREAK, STATUS, badges, rank arrows, streak flames, DEMO READY); DEMO SQUAD only when it exists; LATEST ACTION; STRATEGY FACTORY; layman bot pages with advanced details behind a click; no technical clutter on the home screen; premium modern design; tasteful success moments; nothing deleted, only hidden; the backup kept | ACTIVE | mintel/ops/squad.py ("/", /bot, /demo, /factory), mintel/ops/dashboard.py (routes; the old Home at /advanced under ADVANCED), tests/test_squad_page.py | |
| 13 Oct 2026 | Reporting | Main report: 10 to 20 seconds to read, in the TRADING SQUAD daily format and the 5 DAY RESULT weekly format, one paragraph, then stop | ACTIVE | routines: nightly review, weekly strategies, email | |
| 13 Oct 2026 | Tone | Positive, confident, energetic and human. Bad results in one line, then what was learned and what's next. Never fake good news; numbers are always true | ACTIVE | routines, page words | |

## Leagues, careers, the road to profit and real money (14 Oct 2026)

| Date | Area | Requirement | Status | Files affected | Superseded by |
|---|---|---|---|---|---|
| 14 Oct 2026 | Leagues | Three leagues, never added together: PAPER LEAGUE (every test, its own paper record after costs); DEMO LEAGUE (on the demo account, ranked by the broker's results after all costs: MetaTrader by magic for the bots and Trend & Breakout's parts, the lab-demo record by ticket for lab ideas; unknown, never zero, while unreadable); REAL MONEY LEAGUE (present, empty: "Not connected yet — no real money is ever traded without your approval"). Sorted by profit, with the quality behind it: in profit, up on 55%+ of its days, net ≥ 2× its worst drawdown, 1+ trade a day (four stars; none before 10 trades) | ACTIVE | mintel/ops/leagues.py, mintel/ops/squad_pages.py (/leagues), tests/test_leagues.py | |
| 14 Oct 2026 | Careers | One career a strategy across the three leagues by one stable id (bot:…, tnb:…, lab:…): every trade by league kept apart; the daily ranking snapshot (data/leagues/YYYY-MM-DD.json, written once a finished UK trading day by the pulse, never rewritten or removed); promotions and demotions with their reasons (weekly history); versions (lab parent/child variants and version, Trend & Breakout's rule-set name); best and worst markets, sessions and conditions; risk a trade and worst drawdown. Retired ideas keep theirs: nothing is deleted | ACTIVE | mintel/ops/leagues.py, mintel/ops/pulse.py (live_upkeep), squad bot page (CAREER) | |
| 14 Oct 2026 | Road to Profit | Milestones £25 / £50 / £100 net a day (£250 from £5,000, £500 from £10,000) are ambitions, not targets. From actual evidence only, demo first and paper second, each projected on its own: contributors, per-trade net and trades a day, capital and size (sizing.py), drawdown, 20-trading-day bootstrap of their own trades at £1,500 / 0.5% (10th / 50th / 90th percentiles), uncertainty, what must improve, realistic or not. Under 30 trades over 5 days: no projection, only the plain reason. Barriers (too few trades, costs, correlation, drawdown) with what is being done. ROAD TO PROFIT page and front-page card | ACTIVE | mintel/ops/roadmap.py, mintel/ops/squad_pages.py (/road), tests/test_roadmap.py | |
| 14 Oct 2026 | Sizing | Size from equity, risk %, stop distance, the instrument's money maths (contracts.SymbolSpec.money_per_lot, the GBP conversion), the volume min/step/max (never rounded up; under the minimum lot = no trade), margin against free margin, spread + commission. Exposure scales with equity both ways. Proposed real-money tiers: 0.5% to start; 1% for an established performer (recommendation only); 1.5% needs approval; 2% combined planned-risk cap. A tier increase needs 60+ demo trades and the bootstrap lower bound above zero (a streak alone never counts) and always Aaron's approval | ACTIVE (review settings; nothing switched on) | mintel/ops/sizing.py, tests/test_sizing.py | |
| 14 Oct 2026 | £1,500 simulation | Each demo strategy's trades (and the top paper candidates', labelled paper, apart) replayed on £1,500 at 0.5% a trade, compounding, with the 2% combined cap and safeguards proposed for review: 2% daily, 5% weekly, 10% overall drawdown. Plus a table of £1,500 / £3,000 / £5,000 / £10,000 and £ a pip on typical instruments | ACTIVE | mintel/ops/sizing.py (simulate, balance_table), /road | |
| 14 Oct 2026 | Live readiness | Bar (proposed): 40+ demo trades over 10+ trading days; the broker's net after costs positive; bootstrap lower bound of per-trade net > 0; worst drawdown ≤ 10% of £1,500 at 0.5%; paper and demo agree. LIVE PREPARATION STARTED: the same at 30+ trades over 7+ days. Approaching: 20+ trades, in profit, last 10 in profit. data/live_readiness.json (stage, bots, results, why promising, evidence needed, £1,500 justified, earliest date or "no estimate yet", fingerprint, notify, notified), published by the pulse as reports/live/readiness.json; Claude's nightly routine sends the message when notify is true, then runs `python -m mintel.ops.readiness mark-notified` | ACTIVE | mintel/ops/readiness.py, mintel/ops/pulse.py, tests/test_leagues.py | |
| 14 Oct 2026 | Real money | Approval prepared, real trading DISABLED: `REAL_TRADING_ENABLED = False`; no real broker connection; a separate never-used data/realmoney/real_account.json with a verifier that never treats the demo login as real or the real as demo. Approval only at the Mac's command line (`python -m mintel.ops.realmoney approve …`) with a typed confirmation, a local PBKDF2-hashed passphrase (set-passphrase), the exact id + version (qualified in readiness), the real login and the risk tier; each higher tier approved separately; keep-demo and reject recorded; append-only hash-chained data/realmoney/approvals.jsonl. The web page cannot approve: its APPROVE LIVE / KEEP ON DEMO / REJECT only show the command. An approval enables nothing while the constant is False | ACTIVE (disabled by design) | mintel/ops/realmoney.py, /leagues#readiness, tests/test_realmoney.py | |
| 14 Oct 2026 | UI | In the existing TRADING SQUAD style (no redesign): front page gains three league lines (PAPER +£x · N strategies; DEMO +£y · N active, the broker's; REAL Not connected yet) and the ROAD TO PROFIT and LIVE READINESS cards; navigation gains Leagues and Road to Profit; the readiness section with the approval cards sits on /leagues (/readiness); bot pages gain the CAREER across leagues | ACTIVE | mintel/ops/squad.py, mintel/ops/squad_pages.py, mintel/ops/dashboard.py, tests/test_league_pages.py | |

## 14 Oct 2026: lab ideas on the demo, daily promotion, evidence-based judging

Aaron's priorities 1–3 (14 Oct). Every 13 Oct safeguard stays: veto, HALT,
intent records, put-back on failure, strict reads, never re-released by the
same plan. The method is in plain words in `docs/ops/promotion.md`. Verified
against the simulated broker; real demo behaviour is to be confirmed on Aaron's
Mac.

| Date | Area | Requirement | Status | Files affected | Superseded by |
|---|---|---|---|---|---|
| 14 Oct 2026 | Lab demo execution | Promoted lab ideas place genuine DEMO orders. Inside the trader, they use the lab practice engine's own features, signals, caps, news rule and simulator exits (stop, target, trail, time, 21:55 UK flat). Orders go through LiveExecutor with the stop at the broker. Each idea version has its own magic number (992001–992999, stored in data/labdemo/magics.json, never reused). Restarts re-adopt open positions; orphans are closed; a demoted idea is closed at once. The roster is hot-reloaded. Every order is refused unless the broker says is_demo AND config mode is DEMO. Every trade is booked from the broker's closed deal into data/labdemo/labdemo.sqlite, keyed by idea and version. The lab's paper shadow keeps practising. The lab itself still cannot place an order | ACTIVE | mintel/labdemo/*, mintel/engine/trader.py, mintel/ops/weekly.py, tests/test_lab_demo.py | |
| 14 Oct 2026 | Promotion | Daily promotion: pick every UK trading day at 22:30, release 06:30 the next trading day. Veto, HALT and intent/rollback kept; a pick taken back is never re-released by the same plan | ACTIVE | mintel/ops/weekly.py | |
| 14 Oct 2026 | Promotion | 8–12 on demo: `demo_target` in data/weekly.json (default 10, at most 12). Free places are filled with the best qualifiers; a demoted test's place is refilled at the next release. Capacity: a combined open demo-risk cap of `demo_risk_cap_pct` (default 10% of equity) across every position, and at most half the free margin per new position. An order is cut to fit, never enlarged, and skipped with the reason when the minimum lot will not fit | ACTIVE | mintel/ops/weekly.py, mintel/labdemo/engine.py | |
| 14 Oct 2026 | Promotion | The early-promotion bar: forward paper trades by frequency (15 frequent ≥3/day, 12 medium, 10 slow <1/day) over 3+ UK days; positive after all costs; PF ≥ 1.3; still positive without its best trade. Selection-bias protection: R per trade is empirical-Bayes shrunk toward the pool and must be > 0 and beat the best control (clock-trade) idea by 0.05 R. Lab past/fresh tests weigh the rank (×0.5 strongly negative, ×0.85 mildly), never gate it. At most 2 on demo per market and direction; a spread across markets and sessions is preferred. Controls are never promoted. Old bots and T&B parts are ranked the same way and promoted through modes | ACTIVE | mintel/ops/weekly.py, docs/ops/promotion.md, tests/test_promotion_rules.py | |
| 14 Oct 2026 | Demotion | Evidence-based demotion on the broker's results after costs. After ≥20 demo trades: back when the bootstrap 90th-percentile bound of mean net/trade is < 0, kept when the 10th-percentile bound is > 0. After ≥40: back when net ≤ 0. At any point: back at a loss of 15× per-trade risk (early exit) or a 20× drawdown from its best (drawdown guard). Every promotion, keep and demotion reason goes into history.jsonl | ACTIVE | mintel/ops/weekly.py | |
| 14 Oct 2026 | Sizing | Sizing tiers on demo: each promoted lab idea gets the next tier in turn (0.5%, 1%, 2% of equity a trade). It is stored in the roster, state, history and every trade. Genuine broker stops, margin and the combined cap always hold. Old bots and T&B keep their own sizing | ACTIVE | mintel/labdemo/roster.py, engine.py, mintel/ops/weekly.py | |
| 14 Oct 2026 | Reporting | The £1,500 equivalent: every demo trade stores R × 0.5% of £1,500 (R × £7.50), R being the broker's net ÷ the money really at its stop. It is kept beside the demo money and never mixed into it | ACTIVE | mintel/labdemo/record.py, mintel/ops/weekly.py | |
| 15 Oct 2026 | Promotion | Refines the 14 Oct early-promotion bar's shrinkage (that row stays ACTIVE). The pool's prior (its average and spread tau²) now uses each pool member's own unbiased sampling variance with no VAR_FLOOR_R2 floor: the floor inflated the pool's average noise on lumpy 5–10-trade lab records and biased tau² low (about 0.002–0.004 in simulations), shrinking every test more than the method intends. Exact clones (identical trade lists: close time, market, R) count once in the prior's average, spread and pool size. The floor is KEPT in each candidate's own shrinkage weight, so a short lucky streak with no spread gets no extra weight. Every other rule unchanged by this row (TAU2_FLOOR is raised separately by promotion rule v2, the next row; the fixed prior, POOL_MIN_TRADES/SIZE, the control benchmark, the bar) | ACTIVE | mintel/ops/weekly.py, docs/ops/promotion.md, tests/test_promotion_rules.py | |
| 15 Oct 2026 | Promotion | Promotion rule v2, Aaron's choice "Fast": the shrinkage prior's spread floor TAU2_FLOOR raised from 0.0025 (0.05 R, v1) to 0.04 (0.2 R). A strategy averaging about +1 R a trade reaches demo by about 15 trades roughly 7–9 times in 10 once the clock-trade benchmark applies (simulated; v1: about 1–2 in 10). Some lucky no-edge ideas may also qualify; the control benchmark, the diversity cap, the 8–12 demo places and the demo judgement / 15× early exit handle them. v1's value is kept as TAU2_FLOOR_V1 for comparison | ACTIVE | mintel/ops/weekly.py, docs/ops/promotion.md, tests/test_promotion_rules.py | |
| 15 Oct 2026 | Lab practice + lab demo exits | The 21:55 UK flat (21:00 UK Friday) fires on the clock even when a market has printed no bar (seen live 14 Oct: AUS200 positions left open past the flat). Practice books SESSION_END at that market's last bar before the flat, never at a price older than the position's last walked bar. Demo sends the close through DemoOnlyExecutor; a close refused because the market is shut is retried every minute (weekends included) and never dropped. Paper and demo decide alike | ACTIVE | mintel/lab/practice.py, mintel/labdemo/engine.py | |
| 15 Oct 2026 | Lab practice + lab demo entries | A session's first bar can signal on a live day, as the past test always allowed (live practice and the demo missed 07:00 London and 12:00 New York breakouts) | ACTIVE | mintel/lab/practice.py | |
| 15 Oct 2026 | Lab past test | MOMENTUM_CONTINUATION's past test no longer looks ahead: a signal is taken on the first minute any live push breaks, as live practice does. This corrects the past test; the rule, settings and ids are unchanged, so it is not a new strategy version. Its past-test results change from the next nightly run | ACTIVE | mintel/lab/families/momentum.py | |

