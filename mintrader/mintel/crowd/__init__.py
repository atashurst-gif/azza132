"""CROWD FADER - the fifth bot. Positioning from Coinversa Pulse (who is
long and short the same markets on Hyperliquid's xyz dex, split by how
profitable the traders have been), traded on the broker's gold, silver,
index, FX and oil contracts.

The idea, in one line: when the losing crowd is piled up on one side and
the proven winners are not with them, lean the other way - but only once
the price itself has started to turn, and only with a structural stop,
10 at the stop, and the nearest liquidation cluster as the target.

Where it comes from. Retail positioning as a contrarian signal is the
best-documented sentiment edge there is: the IG client-sentiment series
and the old DailyFX SSI showed the retail crowd net wrong at extremes for
years, and the Commitments of Traders "index" is the same idea on the
futures market (fade the stretched speculators, side with the hedgers).
Liquidation clusters are the crypto-native version of a stop run: a dense
pool of forced closes just beyond the price is fuel for a move into it.
None of that is proof on this account, which is why the bot starts in
PAPER, records every read it makes and every trade it would have taken,
and is judged on those records like the others.

Honest caveat: Hyperliquid's gold or S&P perpetuals are a small sample of
the real market (hundreds of millions, not hundreds of billions), so this
is a sentiment read, not order flow. A sentiment read from a small book is
exactly what the IG series is too; it still has to earn its place.

PAPER. It never places an order and its results are never added to the
account. The API key lives in secrets.json, never in source.
"""
STRATEGY_ID = "crowd_fader"
STRATEGY_LABEL = "Crowd Fader"
TAGLINE = ("Positioning from Coinversa: when the losing crowd is stretched one way and the proven winners "
           "are not with them, lean the other way once the price turns; stop beyond the last hour, "
           "target the nearest liquidation cluster.")
