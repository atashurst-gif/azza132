"""MOMENTUM RUNNER - the third bot, a paper shadow on the indices.

Every time Trend & Breakout opens an index trade (US30, US500, ...), the
Runner takes the same entry on paper and manages it its own way: the stop
stays where the chart put it until the best price is 3 R ahead, then it
trails 3 R behind the best price. No break-even move, no ladder. A shadow
can outlive the real trade; that is the point.

Why: replaying every trade since 17 Sep against the broker's own minute
bars (7 Oct, `mintel.ops.potential`), on indices the "trail 3 R" rule made
+29.1 R where the ladder kept +10.4 R (97 trades); on FX and metals no
riding rule beat the ladder, and a wider (15) stop added nothing anywhere.

It never places an order. Its results are on its own tab and are never
added to the account.
"""
STRATEGY_ID = "momentum_runner"
STRATEGY_LABEL = "Momentum Runner"
TAGLINE = ("Paper shadow of Trend & Breakout's index trades: same entry, the stop "
           "left alone until +3 R, then trailed 3 R behind the best price.")
