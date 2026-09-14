"""Two things from the first live evening: single-share CFDs (BILI.NAS,
CSGP.NAS) were classified as INDEX because of the 'NAS' hint and traded,
tying up margin until entries were paused; and the smoke test flagged
"an order was sent" because the executor had loaded the live trader's
journal intents."""
from types import SimpleNamespace

from mintel.contracts import infer_group, is_equity_cfd
from mintel.verify import sent_any_order


class TestShareCfdsAreNotIndices:
    def test_exchange_suffixes(self):
        for sym in ("BILI.NAS", "CSGP.NAS", "AAPL.US", "VOD.LSE", "SAP.DE", "MKTX.NAS"):
            assert is_equity_cfd(sym), sym
            assert infer_group(sym) == "EQUITY", sym

    def test_indices_still_indices(self):
        for sym in ("NAS100", "US100", "US500", "GER40", "UK100"):
            assert not is_equity_cfd(sym), sym
            assert infer_group(sym) == "INDEX", sym

    def test_broker_path_counts(self):
        assert infer_group("XYZ", path="CFD\\Stocks\\US") == "EQUITY"
        assert infer_group("EURUSD", path="Forex\\Majors") == "FX_MAJOR"

    def test_equity_is_not_in_the_default_universe(self):
        from mintel.config import UniverseConfig
        assert "EQUITY" not in UniverseConfig().groups


class TestSmokeCheckIgnoresTheLiveTradersIntents:
    def test_pre_existing_intents_are_not_a_leak(self):
        trader = SimpleNamespace(executor=SimpleNamespace(intents={"live-key": 1}))
        broker = SimpleNamespace()
        assert sent_any_order(broker, trader, {"live-key"}) is False
        assert sent_any_order(broker, trader) is True      # no snapshot: strict

    def test_a_new_intent_is_a_leak(self):
        trader = SimpleNamespace(executor=SimpleNamespace(intents={"live-key": 1, "new": 2}))
        assert sent_any_order(SimpleNamespace(), trader, {"live-key"}) is True
