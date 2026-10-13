# Master mission: the Trading Squad

Permanent. Every session reads this before any major change, together with
`CLAUDE.md` and `docs/REQUIREMENTS_LEDGER.md`.

## The mission (Aaron, 13 Oct 2026)

THE SYSTEM SHOULD BE EXTREMELY SOPHISTICATED BEHIND THE SCENES.
BUT EXTREMELY SIMPLE IN FRONT OF AARON.

He wants:

- THE SCORE
- THE WINNERS
- THE STREAKS
- THE PROMOTIONS
- THE MONEY
- THE EXCITEMENT

So:

- Keep the technical complexity out of his way.
- Report progress with confidence and energy.
- NEVER fake performance.
- NEVER lose previous strategy tweaks.
- Keep improving the squad.
- Make the experience feel like we are building something big.

The money aim: as much profit after commission as possible, with an average of
at least £100 a day as the three-week target set on 12 Oct 2026.

## How the squad works

Everything is one pool of tests, and every test starts on paper. That covers
the old bots, their parts and every lab idea.

- **PAPER:** practice money on live prices. Every test lives here until it
  earns more.
- **DEMO:** real orders on the IC Markets demo account. This is "the first
  team". In the code it is called LIVE or "real money".
- **Promotion:** every Friday at 22:30 UK, up to `weekly_max_promotions` tests
  (default 3) that pass the bar move up to DEMO from 06:30 UK the next trading
  day. The bar:
  - 30+ paper trades over 5+ days in the last 28;
  - profit after costs;
  - profit factor of 1.2 or more;
  - more up days than down;
  - a luck check.
- **Demotion:** a test goes back to PAPER after 20 DEMO trades if it is not in
  profit after costs. It goes back at once if its loss reaches 10× its
  per-trade risk.
- **The conveyor belt:**
  - Saturdays, automatic: clear losers are retired, winners get neighbouring
    variants, and new ideas refill the pool.
  - Saturdays, Claude: 1 to 3 new strategy families are added.
  - Every night: Claude tweaks, keeps or shuts off tests.
- **Real money proper** (a non-demo account) and adding funds always need
  Aaron's explicit yes.

## Rules that never change

- Legal market behaviour only. Every order is genuine trading intent. No
  spoofing, layering, wash trading, quote stuffing, or fake orders meant to
  deceive.
- Every trade has a stop.
- Never double after losses, average into losers, run grids, or add exposure
  because the last trade lost.
- Never invent or misrepresent performance. Paper and Demo figures are never
  mixed.
- No passwords in source control. Secrets live only in `data/secrets.json`.
- Never spend Aaron's money (data vendors and the like) without showing the
  exact cost and getting a typed yes.

## Reporting (permanent, 13 Oct 2026)

**Length.** The main report takes 10 to 20 seconds to read: money, winners,
streaks, promotions, progress, and what looks exciting next. Technical detail
lives behind the scenes, unless Aaron asks WHY, SHOW ME THE DETAIL, SHOW ME
THE RESEARCH or SHOW ME THE CODE.

**Tone.** Positive, confident, energetic, excited, forward-looking and human.
Never a compliance document, a research paper, a server log or a flat AI
voice.

**Bad results.** State the fact in one line, then move to what was learned,
what is being adjusted, which strategies are doing well, and the next
opportunity. The number always stays truthful.

**Never fake good news.** Confidence comes from progress, learning, strong bots,
new opportunities and improving evidence. It never comes from invented wins,
hidden losses or changed numbers.

**Momentum lines.** Use these naturally when something genuinely exciting
happens, and vary them:

- "🔥 We may have found something here."
- "🚀 That's six profitable days in a row."
- "🏆 NY Rocket has taken top spot."

### Daily format

```
# 🔥 TRADING SQUAD
TODAY: +£184 PAPER        (a DEMO line of its own once Demo bots exist)
WIN RATE: 69%
TRADES: 26
🔥 HOTTEST BOT   NY Rocket — +£61
🚀 BEST STREAK   GBP Hunter — 6 profitable days
⬆️ BIG MOVER     Gold Burst climbed to #2
🧪 FACTORY       50 fresh challengers entered testing
```

Then ONE short paragraph, then STOP. On a down day, use the same shape and the
same constructive tone, without drama.

### Weekly / 5-day format

```
# 🚀 5 DAY RESULT
PAPER: +£742   WIN RATE: 67%   TRADES: 118
🔥 #1 BOT           NY Rocket +£214
🏆 NEW CHAMPION     GBP Hunter
⬆️ PROMOTED TO DEMO Gold Burst
🧪 NEW IDEAS        250 tested this week
```

Then one short paragraph.

## The dashboard (permanent, 13 Oct 2026)

**The five-second rule.** Opening the dashboard answers five questions:

- Are we making money?
- Which bots are winning?
- Who is on a streak?
- Who just got promoted?
- How many new ideas are being tested?

That is all.

**Front page, top to bottom:**

1. **TRADING SQUAD,** with timeline buttons: TODAY · 5 DAYS · 1 MONTH · ALL
   TIME.
2. **Headline cards:** PAPER PROFIT, WIN RATE, TRADES, WINNING BOTS, BEST
   STREAK. A separate DEMO PROFIT card appears once Demo exists. Paper and
   Demo are never mixed.
3. **PAPER LEAGUE,** the centrepiece.
   - Columns: POS, BOT, PROFIT, WIN RATE, TRADES, STREAK, STATUS.
   - Badges: 🔥 HOT · 🚀 RISING · 🏆 CHAMPION · 🧪 TESTING · 🪑 BENCHED, plus
     DEMO READY.
   - Rank arrows (#4 → #2 ↑2) and streak flames (🔥 6).
4. **DEMO SQUAD,** shown only when Demo bots exist. It is cleaner than the
   league, "the first team": BOT, PROFIT, WIN RATE, TRADES, STREAK.
5. **LATEST ACTION:** the latest promotion or discovery.
6. **STRATEGY FACTORY:** active tests, new this week, promising, champions.

**Style.**

- Modern, premium, clean and exciting, like a sports league or a fintech
  product. Never a developer admin panel.
- Great spacing, large numbers, a premium dark mode, and a layout that works
  on mobile and desktop.
- Colour is used with purpose: no rainbow, no neon, no huge gradients.
- Tasteful small success moments for a new #1, a new Champion, a promotion, a
  best streak, or a record 5 days.

**Clicking a bot** shows, in plain words:

- WHAT IT DOES
- WHEN IT LIKES TO TRADE
- WHAT IT HAS MADE
- WHY IT IS DOING WELL

The advanced details sit behind "VIEW ADVANCED DETAILS".

**Never on the home screen:** logs, JSON, heartbeats, indicator readings,
parameters, API messages, database stats, error dumps, research statistics or
developer tools. They live under ADVANCED, RESEARCH and SYSTEM. A clean UI
never deletes data; it only hides complexity.

**The presentation is separate from trading.** A UI change never alters
trading rules, the research factory, paper or demo trading, promotion,
market matching, previous strategy tweaks or data persistence.
