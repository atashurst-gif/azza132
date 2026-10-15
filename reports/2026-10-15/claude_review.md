# Nightly review: Thursday 15 October 2026 (full working)

## State
- **Build:** the Mac runs 71cbd0be44, which is commit e1088e03 (the 14 Oct upgrade). Tonight's 7a5f50fb ("Fast" promotion rule v2, the 21:55 UK flat on the clock, and the lab corrections) is **not installed yet**.
- **Up:** all day. 122 of 122 pulses were UP from 00:07 to 21:05 UTC, and no gap was longer than 11 minutes.
- **Account:** equity £1,840.54, no open positions, nothing traded with real money. Since the reset: −£159.46.
- **Real money:** `real_trading_enabled: false`.

## DEMO
- **Today:** £0.00. Nothing is on demo.
- **Tonight's daily pick** (plan made 21:34 UTC): none, "None yet: nothing has proved itself". It ran on the **v1 strict filter** because the Mac doesn't have 7a5f50fb yet.
- **Next pick:** Fri 16 Oct, 22:30 UK. Demo from Mon 06:30 UK if anything qualifies. It runs on v2 "Fast" only if Aaron installs before then.
- **Demo execution:** verified only on the simulated broker. No real demo order has been placed yet. The first promotion is when that gets checked against the broker.

## PAPER today

**Original bots** (their own paper records, after costs):

| Bot | Trades | Wins | Net | Notes |
|---|---|---|---|---|
| Momentum Runner | 2 | 1 | +30.41 | AUS200 +40.18 (peak +7.1 R, trailed out at +4.11 R after nearly 7 hours); US500 −9.77 (stopped in 92 s). +3.11 R against the ladder's +1.09 R on the same entries |
| Band Breaker | 2 | 1 | +8.29 | US30 +9.14, US500 −0.85, both held to the 15:55 NY flat. Second profitable day in a row |
| Financial Ian | 12 | 2 | −16.66 | Wins +5.14 and +1.84 against ten losses of −0.86 to −5.32 |
| Crowd Fader | 5 | 0 | −43.28 | Five gold and silver stops. Its 3-day winning run ended; still +51.89 over 12–15 Oct |
| Trend & Breakout | 19 | 9 | −62.33 | 22.44 simulated commission; 8 of 19 trades were over inside 5 minutes |
| **Total** | **40** | **13** | **−83.57** | |

**Trend & Breakout by approach:**

| Approach | Trades | Wins | Net |
|---|---|---|---|
| Momentum continuation in a fast market | 8 | 2 | −62.83 |
| Momentum continuation in news | 6 | 4 | −19.73 |
| Session expansion in a fast market | 1 | 0 | −14.92 |
| Session expansion in a trend | 4 | 3 | +35.15 |

- **Momentum continuation in a fast market:** 7 trades on minor pairs made −59.90 and 1 on US500 made −2.93. After two small overnight wins, five stops came from 08:18 UTC, four of them between 12:34 and 13:54 UTC.
- **Momentum continuation in news:** all six trades were minor pairs within 7 minutes from 00:30 UTC. EURAUD made −26.33 and was stopped in a minute.
- **The Runner:** it rode two of the session-expansion entries. Its 3 R trail made +3.11 R against +1.09 R for Trend & Breakout's ladder on the same entries. In money that is +30.41 against +12.14, even though its smaller size (about 9.8 at risk against 12.8–14.7) cost it about 7.40.

## The lab
632 ideas are practising on live prices. The leaders are ranked by the daily pick. None passes under v1:

- **Clock trade, minors, buy at 15:00** (a CONTROL idea): 7 trades, +£121.90, mean +1.34 R. This is a deliberately meaningless "luck" benchmark. It was today's top money-maker, and that is exactly why every idea must beat the best control before promotion.
- **NY opening range on indices** (2 twins): 11 trades over 3 days, +£113.08, mean +0.79 R. Shrunk to −0.056 R under v1; it needs 4 more trades.
- **Gold back-to-average, US session** (2 twins): 8 trades, +£109.26, mean +1.05 R.
- **Asia indices back-to-average:** 19 trades, +£97.47 over 2 days. It needs 1 more day.
- **NY 240-minute breakout, 2R trail** (2 twins): 17 trades, +£88.51, mean +0.40 R. Shrunk to −0.10 R.
- **London 15-minute opening range on minors** (2): 5 trades, +£85.10.

## Standing rules: one met tonight → Trend & Breakout version 6

The rule: an approach negative on 3+ of its days traded is switched off. Since version 4 it is also applied per kind of market. Under the 25 Sep precedent, a line still in profit and down on only a minority of its days stays on. A line down on most of its days can go off even while slightly positive.

**How it is counted** (the same rules as versions 4 and 5, now with paper trades from 9 Oct):
- every momentum continuation trade, in every market condition, once per ticket;
- only the trades today's rules still allow;
- the broker's figures to 8 Oct, paper from 9 Oct.

This count reproduces version 5's own figures exactly: minor pairs 38 trades, +156.31; FX majors 17 trades, −13.87.

| Kind of market | DEMO, to 8 Oct | PAPER, 9–15 Oct | Rule input (demo + paper combined) |
|---|---|---|---|
| Indices | 26 trades, +42.82 | 11 trades, −54.76, down all 4 days | 10 of 14 days down, −11.94 |
| FX minor pairs | 38 trades, +156.31 | 18 trades, −93.04, down 2 of 3 days | 4 of 13 days down, +63.27 |

- **Indices: met, and no longer in profit.** → **Switched off: "Commission First (version 6)"**, with `("MOMENTUM_CONTINUATION", "INDEX")` added to `scan.disabled_tactic_groups`.
  - Version 5's record is kept. Version 6 counts from 16 Oct, and the measuring clock is unchanged.
  - Trend & Breakout is on PAPER, so no real order changes. The Runner loses nothing, because it already skips momentum continuation.
- **Minor pairs: down on a minority of days and in profit, so the precedent keeps it on**, judged again nightly. The case is thin: all the profit is DEMO before 9 Oct, and version 5's rules were chosen on those same trades.
- **Momentum continuation in a fast market overall:** it stays on.
  - What remains after version 6 is the minor pairs only: 4 of 11 days down, +79.09.
  - The 13–14 Oct rows counted it on paper from 12 Oct only. On that count it would now be met (3 of 4 days down, −88.59). The record now says so, along with why the all-history count, the one every version has used, decides.
- **Watch:**
  - Momentum continuation in news: DEMO +50.12, PAPER −59.21, combined 2 of 5 days down, −9.09. A third down day switches it off.
  - Session expansion in a fast market on indices, now the only approach left there: 2 PAPER trades, −12.42.
- **Markets:** no market rule is met.

**Verification:**
- tests/test_version6.py was added, and the version 4/5 pins were updated.
- Full suite run twice.
  - First run: 4 failures. 3 fail the same way on the unchanged code. The end-to-end FlowLock test is a known load flake and passed 3 of 3 alone.
  - Second run, on the final code: only the 2 Rapid Scalper period tests failed, and both fail on the unchanged code too.
- Three independent reviews.
  - The first two found that the draft judged the minor pairs on fast-market trades only, counted one 8 Oct trade twice, and added DEMO and PAPER figures together without saying so.
  - The third recounted everything independently and confirmed every corrected figure. Its presentation points (the fast-market verdict, labelled splits, wording) are fixed above.

## Live readiness
Stage: none, fingerprint 8ed204f367dce7e8, notify false. Nothing was sent.

## Records
- Commit 780d0af7 on claude/eager-newton-ra06qu contains:
  - Trend & Breakout version 6 (mintel/config.py and tests/test_version6.py);
  - the 15 Oct daily-record rows in all five strategy docs;
  - the version 6 section in docs/strategies/v2-commission-first.md;
  - the ledger row.

## For Aaron
Install the update before Friday 22:30 UK. It brings 7a5f50fb (the "Fast" promotion rule, the clock flat, the lab corrections) and 780d0af7 (Trend & Breakout version 6).
