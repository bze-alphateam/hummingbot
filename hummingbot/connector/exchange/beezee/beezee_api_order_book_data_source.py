import asyncio
import json
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_order_book import BeezeeOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.beezee.beezee_api_data_source import BeezeeAPIDataSource


class BeezeeAPIOrderBookDataSource(OrderBookTrackerDataSource):
    def __init__(self, trading_pairs: List[str], data_source: "BeezeeAPIDataSource", api_factory: WebAssistantsFactory, websocket_endpoint: str):
        super().__init__(trading_pairs)
        self._data_source = data_source
        self._api_factory = api_factory
        self._websocket_endpoint = websocket_endpoint
        self._last_books: Dict[str, Dict[str, str]] = {}
        self._seen_trade_ids: Dict[str, set] = {}

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        prices: Dict[str, float] = {}
        for trading_pair in trading_pairs:
            market = await self._data_source.get_market_by_trading_pair(trading_pair)
            last_price = await self._data_source.get_last_traded_price(market.market_id)
            if last_price is not None:
                try:
                    normalized_price = Decimal(str(last_price))
                    if normalized_price.is_finite() and normalized_price > Decimal("0"):
                        prices[trading_pair] = float(normalized_price)
                except Exception:
                    continue
        return prices

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        market = await self._data_source.get_market_by_trading_pair(trading_pair)
        order_book = await self._data_source.get_order_book(market.market_id)
        update_id = int(time.time() * 1e6)
        return BeezeeOrderBook.snapshot_message_from_exchange(trading_pair, market, order_book["bids"], order_book["asks"], update_id, time.time())

    async def listen_for_subscriptions(self):
        while True:
            try:
                await self._listen_new_blocks()
                self.logger().warning("Beezee block websocket disconnected. Falling back to polling before reconnecting.")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().warning("Beezee block websocket unavailable. Falling back to polling before reconnecting.")
            try:
                await self._poll_and_publish()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Error polling Beezee order book after websocket disconnect.")
            await self._sleep(CONSTANTS.DEFAULT_ORDER_BOOK_POLL_INTERVAL)

    async def _listen_new_blocks(self):
        ws = await self._connected_websocket_assistant()
        self._ws_assistant = ws
        await self._subscribe_channels(ws)
        async for message in ws.iter_messages():
            data = message.data
            if isinstance(data, str):
                data = json.loads(data)
            if isinstance(data, dict) and ("NewBlock" in json.dumps(data) or data.get("method") == "subscription"):
                await self._poll_and_publish()

    async def _poll_and_publish(self):
        update_id = int(time.time() * 1e6)
        for trading_pair in list(self._trading_pairs):
            market = await self._data_source.get_market_by_trading_pair(trading_pair)
            order_book = await self._data_source.get_order_book(market.market_id)
            bids = {level["price"]: level["amount"] for level in order_book["bids"]}
            asks = {level["price"]: level["amount"] for level in order_book["asks"]}
            serialized = {**bids, **{f"ask:{price}": amount for price, amount in asks.items()}}
            if trading_pair not in self._last_books:
                self._message_queue[self._snapshot_messages_queue_key].put_nowait(
                    BeezeeOrderBook.snapshot_message_from_exchange(trading_pair, market, order_book["bids"], order_book["asks"], update_id, time.time())
                )
            else:
                previous_bids = {key: value for key, value in self._last_books[trading_pair].items() if not key.startswith("ask:")}
                previous_asks = {key[4:]: value for key, value in self._last_books[trading_pair].items() if key.startswith("ask:")}
                bid_diffs = self._diff_levels(previous_bids, bids)
                ask_diffs = self._diff_levels(previous_asks, asks)
                if bid_diffs or ask_diffs:
                    self._message_queue[self._diff_messages_queue_key].put_nowait(
                        BeezeeOrderBook.diff_message_from_exchange(trading_pair, market, bid_diffs, ask_diffs, update_id, time.time())
                    )
            self._last_books[trading_pair] = serialized
            history = await self._data_source.get_market_history(market.market_id, limit=50)
            seen = self._seen_trade_ids.setdefault(trading_pair, set())
            for trade in reversed(history):
                trade_id = self._data_source.fingerprint_history_trade(trade)
                if trade_id in seen:
                    continue
                seen.add(trade_id)
                self._message_queue[self._trade_messages_queue_key].put_nowait(
                    BeezeeOrderBook.trade_message_from_exchange(
                        trading_pair,
                        market,
                        trade,
                        float(trade["executed_at"]),
                        trade_id,
                        is_buyer_taker=trade.get("order_type") == CONSTANTS.SIDE_SELL,
                    )
                )

    @staticmethod
    def _diff_levels(previous: Dict[str, str], current: Dict[str, str]) -> List[List[str]]:
        diffs: List[List[str]] = []
        for price in sorted(set(previous) | set(current), key=lambda value: Decimal(value)):
            if previous.get(price) != current.get(price):
                diffs.append([price, current.get(price, "0")])
        return diffs

    async def _parse_trade_message(self, raw_message, message_queue: asyncio.Queue):
        if isinstance(raw_message, OrderBookMessage):
            message_queue.put_nowait(raw_message)

    async def _parse_order_book_diff_message(self, raw_message, message_queue: asyncio.Queue):
        if isinstance(raw_message, OrderBookMessage):
            message_queue.put_nowait(raw_message)

    async def _parse_order_book_snapshot_message(self, raw_message, message_queue: asyncio.Queue):
        if isinstance(raw_message, OrderBookMessage):
            message_queue.put_nowait(raw_message)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=self._websocket_endpoint, ping_timeout=CONSTANTS.DEFAULT_BLOCK_STREAM_TIMEOUT)
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        await ws.send(WSJSONRequest(payload={"jsonrpc": "2.0", "method": "subscribe", "id": 1, "params": {"query": "tm.event='NewBlock'"}}))

    def _channel_originating_message(self, event_message: Dict[str, any]) -> str:
        return ""

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        self.add_trading_pair(trading_pair)
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        self.remove_trading_pair(trading_pair)
        return True
