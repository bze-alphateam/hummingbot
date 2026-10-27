import unittest
from decimal import Decimal
from unittest.mock import AsyncMock

from hummingbot.connector.exchange.beezee.beezee_api_data_source import BeezeeAPIDataSource


class BeezeeAPIDataSourceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.data_source = BeezeeAPIDataSource(
            api_factory=AsyncMock(),
            rest_endpoint="https://rest.getbze.com",
            chain_id="beezee-1",
            address_prefix="bze",
            native_denom="ubze",
            gas_price=Decimal("0.025"),
            account_address="bze1test",
        )

    async def test_get_all_markets_builds_symbol_map_and_market_cache(self):
        self.data_source._paged_request = AsyncMock(return_value=[{"base": "ubze", "quote": "uusdc"}])
        self.data_source.get_denom_metadata = AsyncMock(
            side_effect=[
                {
                    "symbol": "BZE",
                    "display": "bze",
                    "denom_units": [{"denom": "ubze", "exponent": 0}, {"denom": "bze", "exponent": 6}],
                },
                {
                    "symbol": "USDC",
                    "display": "usdc",
                    "denom_units": [{"denom": "uusdc", "exponent": 0}, {"denom": "usdc", "exponent": 6}],
                },
            ]
        )

        markets = await self.data_source.get_all_markets(refresh=True)

        self.assertIn("ubze/uusdc", markets)
        self.assertEqual("BZE-USDC", (await self.data_source.get_symbol_map())["ubze/uusdc"])

    async def test_get_last_traded_price_prefers_history(self):
        market = AsyncMock()
        market.market_id = "ubze/uusdc"
        market.chain_price_scaler = Decimal("1")
        self.data_source.get_all_markets = AsyncMock(return_value={"ubze/uusdc": market})
        self.data_source.get_market_history = AsyncMock(return_value=[{"price": "2.5"}])

        price = await self.data_source.get_last_traded_price("ubze/uusdc")

        self.assertEqual(Decimal("2.5"), price)

    async def test_get_last_traded_price_falls_back_to_mid_price(self):
        market = AsyncMock()
        market.market_id = "ubze/uusdc"
        market.chain_price_scaler = Decimal("1")
        self.data_source.get_all_markets = AsyncMock(return_value={"ubze/uusdc": market})
        self.data_source.get_market_history = AsyncMock(return_value=[])
        self.data_source.get_order_book = AsyncMock(
            return_value={"bids": [{"price": "2.0"}], "asks": [{"price": "4.0"}]}
        )

        price = await self.data_source.get_last_traded_price("ubze/uusdc")

        self.assertEqual(Decimal("3"), price)

    async def test_get_last_traded_price_returns_none_for_empty_order_book(self):
        market = AsyncMock()
        market.market_id = "ubze/uusdc"
        market.chain_price_scaler = Decimal("1")
        self.data_source.get_all_markets = AsyncMock(return_value={"ubze/uusdc": market})
        self.data_source.get_market_history = AsyncMock(return_value=[])
        self.data_source.get_order_book = AsyncMock(return_value={"bids": [], "asks": []})

        price = await self.data_source.get_last_traded_price("ubze/uusdc")

        self.assertIsNone(price)

    def test_candidate_order_ids_filters_by_market_and_side(self):
        candidate_ids = self.data_source.candidate_order_ids(
            refs=[
                {"id": "0002", "market_id": "ubze/uusdc", "order_type": "buy"},
                {"id": "0001", "market_id": "ubze/uusdc", "order_type": "buy"},
                {"id": "0003", "market_id": "ubze/uusdc", "order_type": "sell"},
                {"id": "9999", "market_id": "uatom/uusdc", "order_type": "buy"},
            ],
            market_id="ubze/uusdc",
            order_type="buy",
        )

        self.assertEqual(["0002", "0001"], candidate_ids)

    def test_fingerprint_history_trade_is_stable(self):
        trade = {
            "executed_at": "1710000000",
            "market_id": "ubze/uusdc",
            "order_type": "buy",
            "amount": "1000000",
            "price": "2.5",
            "maker": "bze1maker",
            "taker": "bze1taker",
        }

        self.assertEqual(
            self.data_source.fingerprint_history_trade(trade),
            self.data_source.fingerprint_history_trade(dict(trade)),
        )
