import unittest

from hummingbot.connector.exchange.beezee.beezee_order_book import BeezeeOrderBook
from hummingbot.connector.exchange.beezee.beezee_utils import market_from_data
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class BeezeeOrderBookTests(unittest.TestCase):
    def setUp(self) -> None:
        metadata = {
            "ubze": {
                "symbol": "BZE",
                "display": "bze",
                "denom_units": [{"denom": "ubze", "exponent": 0}, {"denom": "bze", "exponent": 6}],
            },
            "uusdc": {
                "symbol": "USDC",
                "display": "usdc",
                "denom_units": [{"denom": "uusdc", "exponent": 0}, {"denom": "usdc", "exponent": 6}],
            },
        }
        self.market = market_from_data({"base": "ubze", "quote": "uusdc"}, metadata)

    def test_snapshot_message_from_exchange(self):
        message = BeezeeOrderBook.snapshot_message_from_exchange(
            trading_pair="BZE-USDC",
            market=self.market,
            bids=[{"price": "2.5", "amount": "1000000"}],
            asks=[{"price": "2.7", "amount": "2000000"}],
            update_id=123,
            timestamp=456.0,
        )

        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual([[2.5, 1.0]], [[entry.price, entry.amount] for entry in message.bids])
        self.assertEqual([[2.7, 2.0]], [[entry.price, entry.amount] for entry in message.asks])

    def test_diff_message_from_exchange(self):
        message = BeezeeOrderBook.diff_message_from_exchange(
            trading_pair="BZE-USDC",
            market=self.market,
            bids=[["2.5", "0"]],
            asks=[["2.7", "3000000"]],
            update_id=999,
            timestamp=123.0,
        )

        self.assertEqual(OrderBookMessageType.DIFF, message.type)
        self.assertEqual(999, message.first_update_id)
        self.assertEqual([[2.5, 0.0]], [[entry.price, entry.amount] for entry in message.bids])
        self.assertEqual([[2.7, 3.0]], [[entry.price, entry.amount] for entry in message.asks])

    def test_trade_message_from_exchange(self):
        message = BeezeeOrderBook.trade_message_from_exchange(
            trading_pair="BZE-USDC",
            market=self.market,
            trade={"price": "2.5", "amount": "1000000"},
            timestamp=111.0,
            trade_id="trade-1",
            is_buyer_taker=True,
        )

        self.assertEqual(OrderBookMessageType.TRADE, message.type)
        self.assertEqual(float(TradeType.BUY.value), message.content["trade_type"])
        self.assertEqual("trade-1", message.trade_id)
        self.assertEqual("2.5", message.content["price"])
        self.assertEqual("1.0", message.content["amount"])
