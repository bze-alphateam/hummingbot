import asyncio
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

from hummingbot.connector.exchange.beezee.beezee_api_order_book_data_source import BeezeeAPIOrderBookDataSource


class BeezeeAPIOrderBookDataSourceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.data_source = BeezeeAPIOrderBookDataSource(
            trading_pairs=["BZE-USDC"],
            data_source=Mock(),
            api_factory=Mock(),
            websocket_endpoint="wss://rpc.getbze.com/websocket",
        )

    def test_diff_levels_detects_updates_and_deletes(self):
        diffs = self.data_source._diff_levels(
            previous={"1.0": "5", "2.0": "4"},
            current={"1.0": "6", "3.0": "7"},
        )

        self.assertEqual([["1.0", "6"], ["2.0", "0"], ["3.0", "7"]], diffs)

    async def test_get_last_traded_prices_omits_non_finite_price(self):
        market = Mock(market_id="ubze/uusdc")
        self.data_source._data_source.get_market_by_trading_pair = AsyncMock(return_value=market)
        self.data_source._data_source.get_last_traded_price = AsyncMock(return_value=Decimal("NaN"))

        prices = await self.data_source.get_last_traded_prices(["BZE-USDC"])

        self.assertEqual({}, prices)

    async def test_listen_for_subscriptions_falls_back_to_polling(self):
        self.data_source._listen_new_blocks = AsyncMock(side_effect=Exception("ws unavailable"))
        self.data_source._poll_and_publish = AsyncMock(side_effect=[None, asyncio.CancelledError()])
        self.data_source._sleep = AsyncMock(side_effect=[None, asyncio.CancelledError()])

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_subscriptions()

        self.assertTrue(self.data_source._poll_and_publish.await_count >= 1)
        self.assertTrue(self.data_source._sleep.await_count >= 1)
