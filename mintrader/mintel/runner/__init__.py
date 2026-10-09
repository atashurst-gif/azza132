"""MOMENTUM RUNNER - the third bot, riding Trend & Breakout's index trades.

Every time Trend & Breakout opens an index trade (US30, US500, ...), the
Runner takes the same entry and manages it its own way: the stop stays
where the chart put it until the best price is 3 R ahead, then it trails
3 R behind the best price. No break-even move, no ladder. The Runner's
trade can outlive the real one; that is the point.

Why: replaying every trade since 17 Sep against the broker's own minute
bars (7 Oct, `mintel.ops.potential`), on indices the "trail 3 R" rule made
+29.1 R where the ladder kept +10.4 R (97 trades); on FX and metals no
riding rule beat the ladder, and a wider (15) stop added nothing anywhere.

From 9 October (version 5) it does not ride momentum continuation: on the
same replay the 3 R trail lost there (33 trades, -14.4 R) and earned on every
other approach (64 trades, +43.5 R), and the Runner's own momentum trades
lost too (7 trades, -27.48). `RunnerConfig.skip_tactics` holds the list.

From the 9 October line-up Trend & Breakout trades on PAPER and the Runner
stays LIVE: it still rides each fresh index entry with its own real order
(the trader hands it the real broker, never the paper wrapper). Two ways to
strengthen it are built and left off on the evidence (see
docs/strategies/momentum_runner.md): the runner feed (`feed.py`,
`runner.feed_tactics`: approaches Trend & Breakout's own rules have off,
still offered on indices while it is on paper) and the Runner's own top
size for listed segments (`runner.top_segments`).

In PAPER it never places an order: it shadows the real trade on paper. In
LIVE it opens its own position beside the real trade, with its own magic
number (990511) and the comment RUNNER, and the broker's own figure is the
record. Its results are on its own tab and are never added to the account.
"""
STRATEGY_ID = "momentum_runner"
STRATEGY_LABEL = "Momentum Runner"
TAGLINE = ("Rides Trend & Breakout's index trades (not momentum continuation, which lost on the "
           "replay): same entry, the stop left alone until +3 R, then trailed 3 R behind the best "
           "price. PAPER shadows the real trade; LIVE holds its own position beside it.")
