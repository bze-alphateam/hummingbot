import unittest
from decimal import Decimal
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_api_data_source import BeezeeAPIDataSource
from hummingbot.connector.exchange.beezee.beezee_api_order_book_data_source import BeezeeAPIOrderBookDataSource
from hummingbot.connector.exchange.beezee.beezee_utils import BeezeeMarket, BeezeeToken

MARKET_ID = "ubze/uusdc"


def history_trade(index: int) -> Dict[str, Any]:
    return {
        "market_id": MARKET_ID,
        "order_type": "sell" if index % 2 else "buy",
        "amount": str(1000 + index),
        "price": format(Decimal("0.7") + Decimal(index) / Decimal("1000"), "f"),
        "executed_at": str(1_760_000_000 + index),
        "maker": "bze1maker",
        "taker": "bze1taker",
    }


class FakeTradebinChain:
    """The tradebin market_history REST query: trades stored oldest first, paged by `pagination.limit`, newest first
    when `pagination.reverse` is true."""

    def __init__(self, trades: int):
        self.history: List[Dict[str, Any]] = [history_trade(index) for index in range(trades)]

    def add_trade(self) -> Dict[str, Any]:
        trade = history_trade(len(self.history))
        self.history.append(trade)
        return trade

    async def request(self, path_url: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> Dict[str, Any]:
        assert path_url == CONSTANTS.MARKET_HISTORY_PATH_URL
        params = params or {}
        assert params["market"] == MARKET_ID
        trades = list(self.history)
        if str(params.get("pagination.reverse", "")).lower() == "true":
            trades.reverse()
        return {"list": trades[:int(params.get("pagination.limit", 100))]}


def market() -> BeezeeMarket:
    return BeezeeMarket(
        market_id=MARKET_ID,
        base=BeezeeToken(denom="ubze", symbol="BZE", display_name="BZE", display_denom="bze", exponent=6),
        quote=BeezeeToken(denom="uusdc", symbol="USDC", display_name="USDC", display_denom="usdc", exponent=6),
    )


class BeezeeMarketHistoryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.chain = FakeTradebinChain(trades=CONSTANTS.TRADE_FEED_HISTORY_LIMIT + 10)
        self.api = BeezeeAPIDataSource(
            api_factory=AsyncMock(),
            rest_endpoint="https://rest.getbze.com",
            chain_id="beezee-1",
            address_prefix="bze",
            native_denom="ubze",
            gas_price=Decimal("0.025"),
            account_address="bze1test",
        )
        self.api._request = self.chain.request
        self.api.get_all_markets = AsyncMock(return_value={MARKET_ID: market()})
        self.api.get_market_by_trading_pair = AsyncMock(return_value=market())
        self.api.get_order_book = AsyncMock(return_value={"bids": [], "asks": []})
        self.feed = BeezeeAPIOrderBookDataSource(
            trading_pairs=["BZE-USDC"],
            data_source=self.api,
            api_factory=AsyncMock(),
            websocket_endpoint="wss://rpc.getbze.com/websocket",
        )

    def published_trades(self) -> List[Dict[str, Any]]:
        queue = self.feed._message_queue[self.feed._trade_messages_queue_key]
        trades = []
        while not queue.empty():
            trades.append(queue.get_nowait().content)
        return trades

    async def test_market_history_is_newest_first(self):
        history = await self.api.get_market_history(MARKET_ID, limit=3)

        self.assertEqual([self.chain.history[-1], self.chain.history[-2], self.chain.history[-3]], history)

    async def test_last_traded_price_is_the_newest_trade_after_more_than_fifty_trades(self):
        price = await self.api.get_last_traded_price(MARKET_ID)

        self.assertEqual(Decimal(self.chain.history[-1]["price"]), price)
        self.assertNotEqual(Decimal(self.chain.history[0]["price"]), price)

    async def test_trade_feed_publishes_a_trade_made_after_the_fiftieth(self):
        await self.feed._poll_and_publish()
        first_batch = self.published_trades()
        # The newest window, oldest to newest.
        self.assertEqual(
            [self.api.fingerprint_history_trade(trade) for trade in self.chain.history[-CONSTANTS.TRADE_FEED_HISTORY_LIMIT:]],
            [trade["trade_id"] for trade in first_batch],
        )

        new_trade = self.chain.add_trade()
        await self.feed._poll_and_publish()

        second_batch = self.published_trades()
        self.assertEqual([self.api.fingerprint_history_trade(new_trade)], [trade["trade_id"] for trade in second_batch])
        self.assertEqual(new_trade["price"], format(Decimal(second_batch[0]["price"]), "f"))

    async def test_trade_feed_does_not_republish_seen_trades(self):
        await self.feed._poll_and_publish()
        self.published_trades()

        await self.feed._poll_and_publish()

        self.assertEqual([], self.published_trades())

    async def test_trade_feed_remembers_only_the_latest_window(self):
        for _ in range(3):
            for _ in range(CONSTANTS.TRADE_FEED_HISTORY_LIMIT):
                self.chain.add_trade()
            await self.feed._poll_and_publish()

        self.assertEqual(CONSTANTS.TRADE_FEED_HISTORY_LIMIT, len(self.feed._seen_trade_ids["BZE-USDC"]))
