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
| 13 Oct 2026 | Promotion | Weekly pick: Friday 22:30 UK, up to X (weekly_max_promotions, default 3) that pass the bar go to DEMO from Monday 06:30 UK. Veto and HALT respected | ACTIVE | mintel/ops/weekly.py, watchdog.py | |
| 13 Oct 2026 | Demotion | Back to PAPER after 20 DEMO trades not in profit after costs, or at once at 10× per-trade risk lost. A pick taken back is never re-released by the same plan | ACTIVE | mintel/ops/weekly.py | |
| 13 Oct 2026 | Conveyor belt | Saturday refresh: retire clear losers (20+ trades, net ≤ 0, PF < 0.9), add neighbouring variants of the top 10, refill to 640 with new ideas. Claude adds 1 to 3 new families every Saturday | ACTIVE | mintel/lab/refresh.py, routine "MarketBot weekly strategies" | |
| 13 Oct 2026 | Specialisation | Tests are specialised by market (pair/kind), session and condition; market matching and current-condition research feed the daily team | ACTIVE | lab families, coach | |

## Reporting and UI

| Date | Area | Requirement | Status | Files affected | Superseded by |
|---|---|---|---|---|---|
| 10 Oct 2026 | UI | Bot showcase in simple layman terms | SUPERSEDED | mintel/ops/showcase.py | 13 Oct dashboard redesign |
| 12 Oct 2026 | UI | AI-hub-style layout with a sidebar and KPI cards | SUPERSEDED | mintel/ops/dashboard.py | 13 Oct dashboard redesign |
| 13 Oct 2026 | UI | Colours distinct from the AI hub (teal) | ACTIVE (carried into the redesign's palette) | dashboard.py | |
| 13 Oct 2026 | UI | Home: every test with its profit, winners at the top, an overall total | ACTIVE (becomes the PAPER LEAGUE) | dashboard.py, weekly.py | |
| 13 Oct 2026 | UI | Full frontend redesign: TRADING SQUAD; TODAY / 5 DAYS / 1 MONTH / ALL TIME; headline cards; PAPER LEAGUE (POS, BOT, PROFIT, WIN RATE, TRADES, STREAK, STATUS, badges, rank arrows, streak flames, DEMO READY); DEMO SQUAD only when it exists; LATEST ACTION; STRATEGY FACTORY; layman bot pages with advanced details behind a click; no technical clutter on the home screen; premium modern design; tasteful success moments; nothing deleted, only hidden; the backup kept | ACTIVE | mintel/ops/dashboard.py and new UI files | |
| 13 Oct 2026 | Reporting | Main report: 10 to 20 seconds to read, in the TRADING SQUAD daily format and the 5 DAY RESULT weekly format, one paragraph, then stop | ACTIVE | routines: nightly review, weekly strategies, email | |
| 13 Oct 2026 | Tone | Positive, confident, energetic and human. Bad results in one line, then what was learned and what's next. Never fake good news; numbers are always true | ACTIVE | routines, page words | |
