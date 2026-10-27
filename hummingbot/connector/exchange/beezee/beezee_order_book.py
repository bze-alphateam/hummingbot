from typing import Any, Dict, List

from hummingbot.connector.exchange.beezee.beezee_utils import BeezeeMarket, chain_amount_to_display, chain_price_to_display
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class BeezeeOrderBook(OrderBook):
    @classmethod
    def snapshot_message_from_exchange(
        cls,
        trading_pair: str,
        market: BeezeeMarket,
        bids: List[Dict[str, Any]],
        asks: List[Dict[str, Any]],
        update_id: int,
        timestamp: float,
    ) -> OrderBookMessage:
        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": [[str(chain_price_to_display(item["price"], market)), str(chain_amount_to_display(item["amount"], market.base))] for item in bids],
                "asks": [[str(chain_price_to_display(item["price"], market)), str(chain_amount_to_display(item["amount"], market.base))] for item in asks],
            },
            timestamp=timestamp,
        )

    @classmethod
    def diff_message_from_exchange(
        cls,
        trading_pair: str,
        market: BeezeeMarket,
        bids: List[List[str]],
        asks: List[List[str]],
        update_id: int,
        timestamp: float,
    ) -> OrderBookMessage:
        return OrderBookMessage(
            OrderBookMessageType.DIFF,
            {
                "trading_pair": trading_pair,
                "update_id": update_id,
                "first_update_id": update_id,
                "bids": [[str(chain_price_to_display(price, market)), str(chain_amount_to_display(amount, market.base))] for price, amount in bids],
                "asks": [[str(chain_price_to_display(price, market)), str(chain_amount_to_display(amount, market.base))] for price, amount in asks],
            },
            timestamp=timestamp,
        )

    @classmethod
    def trade_message_from_exchange(
        cls,
        trading_pair: str,
        market: BeezeeMarket,
        trade: Dict[str, Any],
        timestamp: float,
        trade_id: str,
        is_buyer_taker: bool,
    ) -> OrderBookMessage:
        return OrderBookMessage(
            OrderBookMessageType.TRADE,
            {
                "trading_pair": trading_pair,
                "trade_type": float(TradeType.BUY.value) if is_buyer_taker else float(TradeType.SELL.value),
                "trade_id": trade_id,
                "update_id": int(timestamp * 1e6),
                "price": str(chain_price_to_display(trade["price"], market)),
                "amount": str(chain_amount_to_display(trade["amount"], market.base)),
            },
            timestamp=timestamp,
        )
