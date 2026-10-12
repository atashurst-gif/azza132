# Nightly review — Monday 12 October 2026

## The account (real money on the demo, the broker's figures)

| | |
|---|---|
| UK day | **−£7.81** (all of it Financial Ian) |
| Balance | about £1,852.97 (reset to £2,000 on 5 Oct) |
| Since the reset | −£147.03 over 6 trading days |
| Daily average against the £100 aim | since the coach started (12 Oct): **−£7.81 a day**; since the reset: about −£24.50 per trading day |

| 5 Oct | 6 Oct | 7 Oct | 8 Oct | 9 Oct | Weekend (Ian) | 12 Oct |
|---|---|---|---|---|---|---|
| +67.47 | −40.92 | −47.90 | −46.59 | −59.39 | about −11.89 | −7.81 |

Today was the smallest loss since the reset. It is still a loss, and £100 a day is a long way off. Nothing today changes that honestly.

## Each bot

| Bot | Mode | Trades | Result | In one line |
|---|---|---|---|---|
| Formula 1 (Trend & Breakout, minor pairs) | LIVE | 0 | £0 | No minor-pair trade all day. **0 of 20** towards its judgement |
| Trend & Breakout (everything else) | practice | 1 | −£12.41 practice | JP225 momentum continuation in a fast market, stopped in 6 minutes |
| Momentum Runner | LIVE | 0 | £0 | Nothing to ride (it doesn't ride momentum continuation) |
| Financial Ian (BTC) | LIVE | 12 | **−£7.81** | One win of about +£21 against eleven small losses of about −£2.80 |
| Band Breaker | practice | 4 | −£22.46 practice | One trade had a stop only 4 points away and was stopped in 5 seconds. **Fixed tonight** |
| Crowd Fader | practice | 3 (+1 open) | **+£30.87 practice** | Gold and silver shorts against an 82% long crowd. Two hit their target at +2 R |

Practice is not money and is never counted in the account.

## The lab: its first night

- **Data:** it fetched 2.39 million one-minute bars, covering 68 days (17 Jun to 12 Oct).
- **Tested:** 398 of its 632 ideas, in 20 minutes.
- **Result: none passed, and all 398 were binned.** The reasons:
  - **Not steady:** the money came from one or two lucky stretches. Most of the 398 failed this check.
  - **Costs:** after IC's commission and the spread, too little was left. 305 failed this check.
  - **Too few trades:** for some ideas.
- **By family:**
  - A few ideas made money before costs in four families: Opening range (5), VWAP fade (14), Prev-day levels (7) and the control family (4).
  - Breakout, Momentum, Trend pullback and Squeeze had no idea with a positive net.
- **The control family:** these are deliberately pointless "trade at a fixed time" ideas, there to prove the checks can't be fooled. 0 of 32 passed, which is the right answer.
- **Waiting:** 234 ideas are still untested, and 266 need more calendar history. They run on later nights.

What this means: the checks are doing their job. They throw out luck, and on 68 days of history nothing simple survives the costs. That is consistent with the account's last week. The lab keeps going every night at 23:10.

## The coach

- **Mode:** WATCH. It records what it would do but has no say until 23:00 UTC on 13 Oct. It switches to PILOT from 06:30 UK on 14 Oct.
- **Broker check:** ok. Equity £1,852.97.
- **Week brake:** at its line. The last 5 UK trading days total −£159.26.
- **Room before its daily stop:** £48.01.
- **Formula 1:** pinned until its 20-trade judgement.

## Costs and uptime

- **Commission:** none on the account today. Ian's BTC trades carry no commission line.
- **Uptime:** 95% (78 of 82 pulses). There were four single-pulse gaps:
  - 07:58: during the install.
  - 14:20: the page wasn't answering.
  - 16:25: MetaTrader lost its connection to the broker.
  - 17:57: the page wasn't answering.
- No overnight sleep this time.

## What changed tonight

- **Band Breaker (practice):** a VWAP stop that sits closer than half the hour's normal noise move now gives way to the far band. This rules out what happened to the 12:30 NY USTEC trade, which took 3.1 lots with its stop 4 points away and was stopped in 5 seconds. Two tests were added. The full suite was run: 4 tests fail, and none of them involve this change. 3 fail the same way without it (Rapid Scalper page periods, and a "lessons from day one" stop-floor check). One coach-hook test about the Runner passes or fails at random, with or without the change, so it probably depends on the clock. They get fixed tomorrow.

## The plan

1. **Formula 1:** keep it as it is until 20 live trades. Zero trades on day one isn't a verdict.
2. **Crowd Fader:** the best practice bot so far (+£11.43 over 6 trades in all; +£30.87 today). It stays on practice. It needs a proper run of trades before going live, and today is one day.
3. **Ian:** the shape is many small losses and a rare big win. Watch whether the wins keep paying for the losses over its 30-trade judgement.
4. **The lab:** the remaining 234 ideas run tomorrow night. Any idea that passes the past test goes on to the fresh test, then the bench, and is shown on the page.
5. **The coach:** takes control on 14 Oct. The week brake at its line means it starts cautiously.
6. **Housekeeping:** fix the 4 failing tests (the random coach-hook one first, because it guards the Runner).
7. **Cleanup:** finish the remaining coach review items: the blocked-by-practice count, the minimum-lot trial check, the Formula 1 headroom count and the lab family prior.
