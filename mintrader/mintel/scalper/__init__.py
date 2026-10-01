"""RAPID SCALPER - a second, fully separate strategy.

"Fast in. Fast out. Cut failed trades quickly and give exceptional winners
room to run."

Everything here is isolated from the existing strategy: its own strategy
id, magic number, configuration file, state, ledger, risk engine, logs and
statistics. It shares only the broker connection and the status page.
"""
STRATEGY_ID = "rapid_scalper"
STRATEGY_LABEL = "Rapid Scalper"
TAGLINE = ("Fast in. Fast out. Cut failed trades quickly and give exceptional "
           "winners room to run.")
# The existing bot, as the page describes it. Internally it stays what it is.
EXISTING_STRATEGY_ID = "market_intelligence"
EXISTING_STRATEGY_LABEL = "Trend & Breakout"
