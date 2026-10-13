# Project memory: Aaron's Trading Squad (mintrader)

The code lives in `mintrader/` (Python package `mintel`). It runs on Aaron's
Mac against an IC Markets DEMO account. Work on branch
`claude/eager-newton-ra06qu`, which is the branch the Mac installs from.

## Read first, every time (permanent, Aaron, 13 Oct 2026)

Before any major change:

1. Read this file.
2. Read `mintrader/docs/MASTER_MISSION.md`.
3. Read `mintrader/docs/REQUIREMENTS_LEDGER.md` and the current state.
4. Preserve every compatible previous instruction.
5. Merge the new instruction into the existing system.

**Never overwrite previous strategy tweaks.** Every earlier instruction stays
active unless Aaron explicitly says REMOVE or REPLACE. That covers:

- strategy tweaks: entries, exits, FlowLock, pair- and session-specific
  behaviour;
- the squad system: promotion and demotion, strategy churn, the conveyor
  belt, paper/demo rules, market matching, current-condition research, daily
  team selection, specialisation;
- risk controls and reporting requirements.

A new prompt is normally an ADDITION or a REFINEMENT. If two instructions
genuinely conflict, the most recent explicit instruction on that exact subject
wins. Never delete an old requirement silently: record every important
instruction in the requirements ledger (date, area, requirement, status, files,
superseded by).

**No accidental regressions.** Improving an entry never resets its exit.
Changing FlowLock never removes a pair filter. A UI redesign never changes
trading behaviour. Strategy configuration and presentation are separate
systems.

**Version strategy changes.** Changing a proven strategy makes a new version
(v1.3 becomes v1.4) and keeps the old one so they can be compared. Winners are
never silently overwritten by experiments.

## The mission in one breath

Extremely sophisticated behind the scenes, extremely simple in front of Aaron:

- the score, the winners, the streaks, the promotions, the money, the
  excitement;
- reports that are short (10 to 20 seconds), positive, confident and human,
  with every number true;
- never fake performance, never lose previous tweaks, keep improving the
  squad.

The full formats are in MASTER_MISSION.md.

## Hard rules

- Legal market behaviour only. Every trade has a stop.
- No doubling after losses, no averaging into losers, no grids.
- No passwords in source control; secrets only in `data/secrets.json`.
- Never invent performance. PAPER and DEMO are never mixed.
- Don't commit unverified trading code: test it, have it reviewed, then push.
  Commits end with the attribution lines.
- Ask Aaron only before real money proper (a non-demo account) or adding
  funds.
- The install one-liner for the Mac (press N at "Change them?"):

  ```
  cd ~/Downloads && curl -fsSL -o bot.zip https://github.com/atashurst-gif/azza132/archive/refs/heads/claude/eager-newton-ra06qu.zip && unzip -q -o bot.zip && rm bot.zip && bash "azza132-claude-eager-newton-ra06qu/mintrader/deploy/mac/Start Trading Bot.command"
  ```
