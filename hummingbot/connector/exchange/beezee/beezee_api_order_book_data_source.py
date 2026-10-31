import asyncio
import json
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_order_book import BeezeeOrderBook
from hummingbot.connector.exchange.beezee.beezee_utils import BeezeeMarket, BeezeeVenueError
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.exchange.beezee.beezee_api_data_source import BeezeeAPIDataSource


class BeezeeAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """The order books of the traded pairs, polled on every new block. A pair with only a liquidity pool has no book:
    it gets one empty snapshot (so the tracker is ready) and is never polled. The block stream runs whatever the pairs'
    venues, since the pool tracker refreshes on its blocks too.

    get_last_traded_prices answers every pair it is asked for: the tracker refreshes and throttles only the pairs that
    come back, so a missing one is asked for again at once, in a loop that never yields while the answer needs no I/O.
    A pool-only pair answers with its pool price (from `pool_price_provider`), any pair with no price with NaN."""

    def __init__(
        self,
        trading_pairs: List[str],
        data_source: "BeezeeAPIDataSource",
        api_factory: WebAssistantsFactory,
        websocket_endpoint: str,
        block_listener: Optional[Callable[[], None]] = None,
        pool_price_provider: Optional[Callable[[str], Optional[Decimal]]] = None,
    ):
        super().__init__(trading_pairs)
        self._block_listener = block_listener
        self._pool_price_provider = pool_price_provider
        self._data_source = data_source
        self._api_factory = api_factory
        self._websocket_endpoint = websocket_endpoint
        self._last_books: Dict[str, Dict[str, str]] = {}
        self._seen_trade_ids: Dict[str, set] = {}
        # The last book read per pair as the chain gave it: (price, amount) strings, best first. The OrderBook keeps
        # floats, which cannot hold every 18-decimal chain price exactly, and a fill must name the price exactly.
        self._chain_levels: Dict[str, Dict[str, List[Tuple[str, str]]]] = {}

    def chain_levels(self, trading_pair: str) -> Optional[Dict[str, List[Tuple[str, str]]]]:
        """{"bids": [...], "asks": [...]} of the last book read for the pair, in chain units, best price first."""
        return self._chain_levels.get(trading_pair)

    def _remember_levels(self, trading_pair: str, order_book: Dict[str, List[Dict[str, Any]]]):
        def levels(side: str, best_first_descending: bool) -> List[Tuple[str, str]]:
            rows = [(level["price"], level["amount"]) for level in order_book.get(side) or []]
            return sorted(rows, key=lambda row: Decimal(row[0]), reverse=best_first_descending)

        self._chain_levels[trading_pair] = {"bids": levels("bids", True), "asks": levels("asks", False)}

    async def _book_market(self, trading_pair: str) -> Optional[BeezeeMarket]:
        """The pair's order-book market, or None for a pair with only a liquidity pool. An unknown pair raises."""
        try:
            return await self._data_source.get_market_by_trading_pair(trading_pair)
        except BeezeeVenueError:
            return None

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        prices: Dict[str, float] = {}
        for trading_pair in trading_pairs:
            market = await self._book_market(trading_pair)
            if market is None:
                prices[trading_pair] = self._pool_price(trading_pair)
                continue
            prices[trading_pair] = self._positive_price(await self._data_source.get_last_traded_price(market.market_id))
        return prices

    def _pool_price(self, trading_pair: str) -> float:
        if self._pool_price_provider is None:
            return float("nan")
        try:
            return self._positive_price(self._pool_price_provider(trading_pair))
        except Exception:
            return float("nan")

    @staticmethod
    def _positive_price(price: Any) -> float:
        """The price as a float, or NaN when there is none or it is not a positive finite number."""
        if price is None:
            return float("nan")
        try:
            normalized_price = Decimal(str(price))
        except Exception:
            return float("nan")
        if normalized_price.is_finite() and normalized_price > Decimal("0"):
            return float(normalized_price)
        return float("nan")

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        market = await self._book_market(trading_pair)
        if market is None:
            return BeezeeOrderBook.snapshot_message_from_exchange(trading_pair, None, [], [], int(time.time() * 1e6), time.time())
        order_book = await self._data_source.get_order_book(market.market_id)
        self._remember_levels(trading_pair, order_book)
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
                if self._block_listener is not None:
                    self._block_listener()
                await self._poll_and_publish()

    async def _poll_and_publish(self):
        update_id = int(time.time() * 1e6)
        for trading_pair in list(self._trading_pairs):
            market = await self._book_market(trading_pair)
            if market is None:
                continue
            order_book = await self._data_source.get_order_book(market.market_id)
            self._remember_levels(trading_pair, order_book)
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
            # The latest trades, published oldest to newest. A trade that falls out of this window never comes back
            # into it, so only the window's own ids are remembered.
            history = await self._data_source.get_market_history(market.market_id, limit=CONSTANTS.TRADE_FEED_HISTORY_LIMIT)
            seen = self._seen_trade_ids.get(trading_pair, set())
            window = set()
            for trade in reversed(history):
                trade_id = self._data_source.fingerprint_history_trade(trade)
                window.add(trade_id)
                if trade_id in seen:
                    continue
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
            self._seen_trade_ids[trading_pair] = window

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
