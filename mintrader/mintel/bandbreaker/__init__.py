"""BAND BREAKER - the fourth bot. Intraday momentum on the US indices, from
the published "noise band" rule.

Every day, for each half hour of the New York session, it knows how far
the index normally wanders from its open by that time (the average of the
last 14 sessions). That gives a band around the open and the previous
close. Price inside the band is noise: no trade. At a half-hour close
outside the band it goes with the move - long above, short below - with
the stop at the far band or the day's VWAP, whichever is nearer, trailed
only in the trade's favour. Everything is closed five minutes before the
cash close; nothing is held overnight.

Source: Zarattini, Aziz and Barbon (2024), "Beat the Market: An Effective
Intraday Momentum Strategy for S&P500 ETF (SPY)", SSRN 4824172: 2007-2024,
+19.6% a year, Sharpe 1.33 net of costs; several independent replications
reproduce it, one noting the edge has been thin since 2025. Indices cost no
commission at this broker, and the bot is flat overnight, so it pays only
the spread.

PAPER until it has proved itself on this account. It never places an order
in PAPER and its results are never added to the account.
"""
STRATEGY_ID = "band_breaker"
STRATEGY_LABEL = "Band Breaker"
TAGLINE = ("Intraday index momentum: a band of normal noise around the open; a half-hour "
           "close outside it is the trade; stop at the far band or VWAP; flat before the close.")
