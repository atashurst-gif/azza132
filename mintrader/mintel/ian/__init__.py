"""FINANCIAL IAN - order-flow trading from the institutional futures book.

A completely separate bot (magic 990911, order comment "IAN"). Its
intelligence comes from what buyers and sellers are actually doing at each
price in a genuine central limit order book - the CME FX futures book - and
the result is traded on the matching spot pair in MetaTrader 5.

Three layers, never mixed:

* DATA          mintel/ian/feeds/      one normalised internal format
                                       (mintel/ian/book.py) whatever the vendor
* INTELLIGENCE  book, features, regime, score, signal, trail
* EXECUTION     mintel/ian/bridge.py   validates and places through the shared
                                       executor (mintel/botexec.py)

Two kinds of data, always labelled and never confused:

* INSTITUTIONAL - the centralised futures order book (CME Globex via a vendor
  such as Databento, or a futures broker's API). This drives the order-flow
  intelligence.
* RETAIL - MetaTrader 5 spot quotes, bars and the MT5 calendar. Used for the
  chart context, the spot confirmation and the execution checks only. Retail
  MT5 depth is NOT the institutional book and is never used as one.

With no institutional feed configured the bot runs in a safe DATA-DEGRADED
state: no signals, no trades, a plain status saying why.

Without a CME data key it can instead read the closest free source: BINANCE's
public crypto order book (BTCUSDT, ETHUSDT - a real exchange book with every
trade's aggressor side, no account or key needed), trading the broker's
matching crypto CFDs (BTCUSD, ETHUSD). That data is labelled EXCHANGE and
"Binance public order book (crypto exchange)" everywhere - never CME, never
institutional FX (mintel/ian/feeds/binance.py, mintel/ian/instruments.py).
"""
STRATEGY_ID = "financial_ian"
STRATEGY_LABEL = "Financial Ian"
MAGIC = 990_911
TAG = "IAN"

INSTITUTIONAL = "INSTITUTIONAL"
EXCHANGE = "EXCHANGE"           # a public exchange book that is not the CME futures book (Binance's crypto book)
RETAIL = "RETAIL"
SYNTHETIC_LABEL = "SYNTHETIC - NOT PERFORMANCE"

TAGLINE = ("Reads the CME FX futures order book - who is taking liquidity, who is absorbing it, where it is "
           "being pulled - and trades the matching spot pair early, with a broker stop that only ever tightens.")
BINANCE_TAGLINE = ("Reads Binance's public crypto order book (BTCUSDT, ETHUSDT) - who is taking liquidity, who is "
                   "absorbing it, where it is being pulled - and trades the matching crypto CFD at the broker early, "
                   "with a broker stop that only ever tightens. Binance is a crypto exchange: its book is public "
                   "market data, not a futures market.")
