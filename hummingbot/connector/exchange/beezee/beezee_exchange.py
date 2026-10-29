import asyncio
import re
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS, beezee_web_utils as web_utils
from hummingbot.connector.exchange.beezee.beezee_api_data_source import (
    BeezeeAPIDataSource,
    BeezeeTxRefusedError,
    is_denom_halted_error,
)
from hummingbot.connector.exchange.beezee.beezee_api_order_book_data_source import BeezeeAPIOrderBookDataSource
from hummingbot.connector.exchange.beezee.beezee_api_user_stream_data_source import BeezeeAPIUserStreamDataSource
from hummingbot.connector.exchange.beezee.beezee_auth import BeezeeAuth
from hummingbot.connector.exchange.beezee.beezee_events import TradebinEvent, block_tradebin_events, tx_tradebin_events
from hummingbot.connector.exchange.beezee.beezee_order_batcher import (
    BatchedOrderMessage,
    BeezeeCreateWithdrawnError,
    BeezeeOrderBatcher,
)
from hummingbot.connector.exchange.beezee.beezee_signer import BeezeeSigner
from hummingbot.connector.exchange.beezee.beezee_utils import (
    BeezeeConfigMap,
    BeezeeMarket,
    chain_amount_to_display,
    chain_price_to_display,
    display_price_to_chain,
    minimum_order_size_for_price,
    private_key_from_account_mode,
    token_from_metadata,
)
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


@dataclass
class QueuedOrderMessage:
    """A create or cancel message the account queued in a tx, followed until its outcome event.

    The tradebin order book is asynchronous: the tx only queues the message and the EndBlock queue executes it in
    the tx block or a later one. Chain v8.2.0 tags the tx event and every outcome event with the queue `message_id`.
    """

    client_order_id: str
    tx_hash: str
    # Creates: index of the message among the account's create messages in the tx. None after a restart from a
    # saved state without it; the message is then matched by its market, side, amount and price.
    msg_index: Optional[int] = None
    # Cancels: the exchange order id being cancelled.
    order_id: Optional[str] = None
    tx_height: Optional[int] = None
    message_id: Optional[str] = None
    # Next block to scan for the outcome.
    next_height: Optional[int] = None

    @property
    def last_height(self) -> int:
        return self.tx_height + CONSTANTS.ORDER_OUTCOME_BLOCK_WINDOW - 1

    def in_window(self, height: int) -> bool:
        # Blocks are taken one at a time from next_height on, so a message never jumps over its outcome block.
        return self.tx_height is not None and height == self.next_height and height <= self.last_height

    def to_json(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "QueuedOrderMessage":
        return cls(**{key: data.get(key) for key in cls.__dataclass_fields__})


class BeezeeExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 5.0

    web_utils = web_utils

    def __init__(
        self,
        connector_configuration: BeezeeConfigMap,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        **kwargs,
    ):
        self._connector_configuration = connector_configuration
        self._trading_pairs = trading_pairs
        self._trading_required = trading_required

        network = connector_configuration.network
        account_type = connector_configuration.account_type
        self._signer: Optional[BeezeeSigner] = None
        self._account_address = getattr(account_type, "address", None)
        private_key_value = private_key_from_account_mode(account_type)
        if private_key_value is not None:
            self._signer = BeezeeSigner(private_key_value, network.address_prefix)
            configured_address = self._account_address
            signer_address = self._signer.address
            if configured_address is not None and configured_address != signer_address:
                raise ValueError(
                    f"Beezee configured wallet address '{configured_address}' does not match signer-derived "
                    f"address '{signer_address}'. Check mnemonic/passphrase/hd_path or use the correct private key."
                )
            self._account_address = configured_address or signer_address
            self.logger().info(
                f"Beezee wallet mode initialized. Configured address: {getattr(account_type, 'address', None) or 'None'}; "
                f"signer address: {signer_address}; effective account address: {self._account_address}."
            )

        self._rest_endpoint = network.rest_endpoint
        self._rpc_endpoint = network.rpc_endpoint
        self._websocket_endpoint = network.websocket_endpoint or network.rpc_endpoint.replace("https://", "wss://").replace("http://", "ws://") + "/websocket"
        self._chain_id = network.chain_id
        self._native_denom = network.native_denom
        self._gas_price = network.gas_price
        self._gas_adjustment = network.gas_adjustment

        self._data_source: Optional[BeezeeAPIDataSource] = None
        self._trade_fees: Dict[str, Optional[TokenAmount]] = {}
        self._markets_by_trading_pair: Dict[str, BeezeeMarket] = {}
        self._halted_denoms: Set[str] = set()
        self._halted_denoms_loaded = False
        self._last_halted_denoms_refresh = 0.0
        # Markets the chain refused as halted although the last halted-denoms read did not list their denoms.
        self._refused_halted_market_ids: Set[str] = set()
        self._halted_trading_pairs: Dict[str, str] = {}
        self._halt_cancel_tasks: Dict[str, asyncio.Task] = {}
        self._queued_creates: Dict[str, QueuedOrderMessage] = {}
        self._queued_cancels: Dict[str, QueuedOrderMessage] = {}
        self._tx_lookup_tasks: Dict[Tuple[bool, str], asyncio.Task] = {}
        self._last_processed_height: Optional[int] = None
        self._block_events_cache: Dict[int, List[TradebinEvent]] = {}
        self._block_processing_lock = asyncio.Lock()
        # Resting orders restored from a saved state, checked against UserMarketOrders once fills are caught up.
        self._orders_to_verify: Set[str] = set()
        # Restored orders saved before their create tx was known: resolved against the book on the first update.
        self._restored_without_tx: Set[str] = set()
        # Orders whose fills up to a height were taken from the chain state instead of block events.
        self._fills_counted_through: Dict[str, int] = {}
        self._block_read_failures: Dict[int, int] = {}
        self._last_status_log_timestamp = 0.0
        # Orders asked to cancel while their create tx was on its way: cancelled once the chain gives them an id.
        self._cancel_requested: Set[str] = set()
        self._deferred_cancel_tasks: Dict[str, asyncio.Task] = {}
        self._cancels_in_progress: Dict[str, int] = defaultdict(int)
        # Set on every order state update, for cancels waiting for their order's chain id.
        self._order_progress = asyncio.Event()
        # The messages of one batch tx share one tx lookup.
        self._tx_fetch_tasks: Dict[str, asyncio.Task] = {}
        self._order_batcher = BeezeeOrderBatcher(
            send=self._send_order_batch,
            sleep=lambda delay: self._sleep(delay),
            window_s=connector_configuration.batch_window_ms / 1000,
            max_messages=connector_configuration.max_messages_per_tx,
        )

        super().__init__(balance_asset_limit=balance_asset_limit, rate_limits_share_pct=rate_limits_share_pct)

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self) -> AuthBase:
        return BeezeeAuth()

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._rest_endpoint

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.TRADING_RULES_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.TRADING_PAIRS_PATH_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.PING_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return False

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def status_dict(self) -> Dict[str, bool]:
        status = super().status_dict
        status["user_stream_initialized"] = True
        status["symbols_mapping_initialized"] = True
        if not all(status.values()):
            now = time.time()
            if now - self._last_status_log_timestamp >= 30.0:
                pending = [key for key, value in status.items() if not value]
                self.logger().warning(f"Beezee connector not ready. Pending status flags: {pending}")
                self._last_status_log_timestamp = now
        return status

    @property
    def ready(self) -> bool:
        return all(self.status_dict.values())

    def gas_fees_paid(self) -> Decimal:
        """Gas fees of every tx this connector broadcast since it started, in the native token (BZE)."""
        return self._build_data_source().gas_fees_paid

    def minimum_order_amount(self, trading_pair: str, price: Decimal) -> Decimal:
        """The chain's minimum order size at `price`, in base display units: ceil(1 / chain price) x 2 base units.
        Zero while the market is not loaded yet (the trading rules load it)."""
        market = self._markets_by_trading_pair.get(trading_pair)
        if market is None:
            return Decimal("0")
        return minimum_order_size_for_price(display_price_to_chain(price, market), market)

    def supported_order_types(self):
        return [OrderType.LIMIT]

    def _is_user_stream_initialized(self) -> bool:
        # Beezee v1 does not have a reliable private stream, so order and balance
        # state are maintained through polling. Do not block connector readiness
        # on a websocket-style user stream handshake that will never occur.
        return True

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return "not found" in str(status_update_exception).lower()

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return "not found" in str(cancelation_exception).lower()

    def _build_data_source(self) -> BeezeeAPIDataSource:
        if self._data_source is None:
            self._data_source = BeezeeAPIDataSource(
                api_factory=self._web_assistants_factory,
                rest_endpoint=self._rest_endpoint,
                chain_id=self._chain_id,
                address_prefix=self._connector_configuration.network.address_prefix,
                native_denom=self._native_denom,
                gas_price=self._gas_price,
                account_address=self._account_address,
                signer=self._signer,
                gas_adjustment=self._gas_adjustment,
                rpc_endpoint=self._rpc_endpoint,
            )
        return self._data_source

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(throttler=self._throttler, auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return BeezeeAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs or [],
            data_source=self._build_data_source(),
            api_factory=self._web_assistants_factory,
            websocket_endpoint=self._websocket_endpoint,
            block_listener=self._on_new_block,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BeezeeAPIUserStreamDataSource()

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal = s_decimal_NaN,
        is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        # Tradebin fees are flat amounts per order, paid on top of the trade (taker when the side is unknown).
        fee = self._trade_fees.get("maker" if is_maker else "taker")
        return AddedToCostTradeFee(flat_fees=[fee] if fee is not None else [])

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        **kwargs,
    ) -> Tuple[str, float]:
        if self._signer is None:
            raise ValueError("Beezee wallet mode is required for order creation.")
        data_source = self._build_data_source()
        market = await data_source.get_market_by_trading_pair(trading_pair)
        if not self._halted_denoms_loaded:
            await self._update_halted_denoms_if_due()
        halt_reason = self._halt_reason(market)
        if halt_reason is not None:
            raise ValueError(f"Beezee market {market.market_id} is halted ({halt_reason})")
        chain_price = display_price_to_chain(price, market)
        minimum_amount = minimum_order_size_for_price(chain_price, market)
        if amount < minimum_amount:
            raise ValueError(f"Beezee minimum size at price {price} is {minimum_amount} {market.base.symbol}.")
        side = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        # The tx hash stays the order's exchange id until the chain saves the order and gives it an id.
        tx_hash = await self._order_batcher.submit(BatchedOrderMessage(
            client_order_id=order_id,
            is_cancel=False,
            message=data_source.create_order_message(market=market, order_type=side, amount=amount, price=price),
            market=market,
            memo=f"hbot:{order_id}",
        ))
        return tx_hash, self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        if self._signer is None:
            raise ValueError("Beezee wallet mode is required for cancels.")
        # While a call waits for the order id or sends the cancel, the deferred-cancel retry leaves the order alone.
        self._cancels_in_progress[order_id] += 1
        try:
            return await self._send_cancel(order_id, tracked_order)
        finally:
            self._cancels_in_progress[order_id] -= 1
            if not self._cancels_in_progress[order_id]:
                del self._cancels_in_progress[order_id]

    async def _send_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        data_source = self._build_data_source()
        market = await data_source.get_market_by_trading_pair(tracked_order.trading_pair)
        if self._order_batcher.withdraw_create(order_id):
            # Its create task now ends the order CANCELED (see _on_order_failure): nothing reaches the chain.
            self.logger().info(f"Beezee order {order_id} cancelled before its create was sent.")
            return True
        if len(tracked_order.exchange_order_id or "") != CONSTANTS.ORDER_ID_LENGTH:
            # Its create tx is on its way. Meanwhile no retry or single send may place it again.
            self._cancel_requested.add(order_id)
            if not await self._wait_for_order_id(tracked_order):
                # It ended (filled, refunded or failed) while the cancel waited: nothing is left to cancel.
                return False
        exchange_order_id = tracked_order.exchange_order_id
        if not exchange_order_id or len(exchange_order_id) != CONSTANTS.ORDER_ID_LENGTH:
            self.logger().debug(
                f"Deferring Beezee cancel until the chain saves the order. client_order_id={order_id}"
            )
            return False
        if order_id in self._queued_cancels:
            return True
        await self._order_batcher.submit(BatchedOrderMessage(
            client_order_id=order_id,
            is_cancel=True,
            message=data_source.cancel_order_message(
                market_id=market.market_id, order_type=self._side(tracked_order.trade_type), order_id=exchange_order_id
            ),
            market=market,
            memo=f"hbot-cancel:{order_id}",
        ))
        return True

    # === Batched txs ===
    #
    # Creates and cancels are coalesced by the order batcher: everything submitted within `batch_window_ms` goes into
    # one tx (cancels first, at most `max_messages_per_tx` messages), simulated, signed and broadcast once. The i-th
    # create message of a tx is resolved by the tx's i-th OrderCreateMessageEvent, a cancel by its order id.

    def batch_order_create(self, orders_to_create: List[LimitOrder]) -> List[LimitOrder]:
        created = super().batch_order_create(orders_to_create)
        # The create tasks have not run yet: their tx goes out once they have all been submitted.
        self._order_batcher.expect(order.client_order_id for order in created)
        return created

    def batch_order_cancel(self, orders_to_cancel: List[LimitOrder]):
        super().batch_order_cancel(orders_to_cancel)
        self._order_batcher.expect(
            order.client_order_id for order in orders_to_cancel if order.client_order_id in self.in_flight_orders
        )

    async def _create_order(self, trade_type: TradeType, order_id: str, *args, **kwargs):
        try:
            await super()._create_order(trade_type, order_id, *args, **kwargs)
        finally:
            self._order_batcher.settle(order_id)

    async def _execute_cancel(self, trading_pair: str, order_id: str) -> str:
        try:
            return await super()._execute_cancel(trading_pair, order_id)
        finally:
            self._order_batcher.settle(order_id)

    def _on_order_failure(self, order_id: str, trading_pair: str, *args, exception: Exception, **kwargs):
        if isinstance(exception, BeezeeCreateWithdrawnError):
            order = self._order_tracker.fetch_tracked_order(order_id)
            if order is not None:
                self._update_order_state(order, OrderState.CANCELED)
            return
        super()._on_order_failure(order_id, trading_pair, *args, exception=exception, **kwargs)

    async def _wait_for_order_id(self, order: InFlightOrder) -> bool:
        """Waits up to CANCEL_WAIT_FOR_ORDER_ID for an order whose create tx is on its way to get its chain id.
        False when the order ended meanwhile; True otherwise, with or without the id."""

        def waiting() -> bool:
            return not order.is_done and len(order.exchange_order_id or "") != CONSTANTS.ORDER_ID_LENGTH

        if not waiting():
            return not order.is_done
        timer = asyncio.ensure_future(self._sleep(CONSTANTS.CANCEL_WAIT_FOR_ORDER_ID))
        try:
            while waiting() and not timer.done():
                progress = asyncio.ensure_future(self._order_progress.wait())
                try:
                    await asyncio.wait({timer, progress}, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    progress.cancel()
        finally:
            timer.cancel()
        return not order.is_done

    def _report_order_progress(self, _update_task: Optional[asyncio.Task] = None):
        self._order_progress.set()
        self._order_progress.clear()

    def _update_order_after_failure(self, order_id: str, trading_pair: str, exception: Optional[Exception] = None):
        super()._update_order_after_failure(order_id, trading_pair, exception)
        # The tracker applies the FAILED update in a task created just now, so this runs after it.
        asyncio.get_event_loop().call_soon(self._report_order_progress)

    def _without_cancelled_creates(self, items: List[BatchedOrderMessage]) -> List[BatchedOrderMessage]:
        # A create whose order was asked to cancel while its batch was being sent is never sent again.
        withdrawn = [item for item in items if not item.is_cancel and item.client_order_id in self._cancel_requested]
        for item in withdrawn:
            self._cancel_requested.discard(item.client_order_id)
            item.reject(BeezeeCreateWithdrawnError(item.client_order_id))
        return [item for item in items if item not in withdrawn]

    def _retry_deferred_cancels(self):
        """Sends the cancels deferred because their order had no chain id yet, once it has one."""
        for client_order_id in list(self._cancel_requested):
            order = self._order_tracker.fetch_tracked_order(client_order_id)
            if order is None or order.is_done or client_order_id in self._queued_cancels:
                self._cancel_requested.discard(client_order_id)
                continue
            if len(order.exchange_order_id or "") != CONSTANTS.ORDER_ID_LENGTH or self._cancels_in_progress.get(client_order_id):
                continue
            running = self._deferred_cancel_tasks.get(client_order_id)
            if running is None or running.done():
                self._deferred_cancel_tasks[client_order_id] = safe_ensure_future(
                    self._execute_cancel(order.trading_pair, client_order_id)
                )
        for client_order_id in [key for key, task in self._deferred_cancel_tasks.items() if task.done()]:
            del self._deferred_cancel_tasks[client_order_id]

    async def _send_order_batch(self, items: List[BatchedOrderMessage]):
        """Sends a batch in one tx. A simulation refusal of a batch is caused by one of its messages, so: a halted
        market's creates are dropped and the rest is tried once more, and otherwise every message is sent alone."""
        try:
            await self._broadcast_batch(items)
            return
        except BeezeeTxRefusedError as error:
            refusal = error
            if not self._is_message_refusal(refusal, items):
                await self._reject_batch(items, refusal)
                return
            remaining = await self._drop_halted_creates(items, refusal) if refusal.is_denom_halted else items
            if not remaining:
                return
            if len(remaining) < len(items):
                items = remaining
                try:
                    await self._broadcast_batch(items)
                    return
                except BeezeeTxRefusedError as retry_error:
                    refusal = retry_error
                    if not self._is_message_refusal(refusal, items):
                        await self._reject_batch(items, refusal)
                        return
            self.logger().warning(
                f"Beezee batch of {len(items)} messages was refused in simulation ({refusal.chain_message}). "
                f"Sending each message in its own tx."
            )
        for item in items:
            try:
                await self._broadcast_batch([item])
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._reject_batch([item], error)

    @staticmethod
    def _is_message_refusal(error: BeezeeTxRefusedError, items: List[BatchedOrderMessage]) -> bool:
        # Simulation runs the messages; a broadcast refusal or a sequence mismatch is about the tx itself.
        return len(items) > 1 and error.stage == "simulation" and not error.is_sequence_mismatch

    async def _broadcast_batch(self, items: List[BatchedOrderMessage]):
        # Every attempt, the first one as well as a retry or a single send, leaves out creates cancelled meanwhile.
        items = self._without_cancelled_creates(items)
        if not items:
            return
        memo = items[0].memo if len(items) == 1 else CONSTANTS.BATCH_MEMO
        response = await self._build_data_source().simulate_sign_and_broadcast(
            [item.message for item in items], memo=memo, extra_gas=self._queued_ahead_gas(items)
        )
        tx_hash = response["tx_response"]["txhash"]
        create_index = 0
        for item in items:
            if item.is_cancel:
                order = self._order_tracker.fetch_tracked_order(item.client_order_id)
                exchange_order_id = order.exchange_order_id if order is not None else None
                message = QueuedOrderMessage(client_order_id=item.client_order_id, tx_hash=tx_hash, order_id=exchange_order_id)
                self.logger().info(
                    f"Beezee cancel order transaction submitted. client_order_id={item.client_order_id}, "
                    f"exchange_order_id={exchange_order_id}, tx_hash={tx_hash}"
                )
            else:
                message = QueuedOrderMessage(client_order_id=item.client_order_id, tx_hash=tx_hash, msg_index=create_index)
                create_index += 1
                self.logger().info(
                    f"Beezee create order transaction submitted. client_order_id={item.client_order_id}, "
                    f"market={item.market.market_id}, tx_hash={tx_hash}, msg_index={message.msg_index}"
                )
            self._queue_message(message, is_cancel=item.is_cancel)
            item.resolve(tx_hash)
        if len(items) > 1:
            self.logger().info(f"Beezee batch tx {tx_hash} submitted with {len(items)} messages.")

    def _queued_ahead_gas(self, items: List[BatchedOrderMessage]) -> int:
        """Gas for the messages this account queued whose tx is not in a block yet: they may land in the same block
        ahead of this tx, and each create pays for every message queued before it on its market. The simulation
        cannot see them (it runs on the last committed state), and messages of the same tx it already counts."""
        queued_ahead: Dict[str, int] = {}
        for message in list(self._queued_creates.values()) + list(self._queued_cancels.values()):
            order = self._order_tracker.fetch_tracked_order(message.client_order_id)
            if message.tx_height is None and order is not None:
                queued_ahead[order.trading_pair] = queued_ahead.get(order.trading_pair, 0) + 1
        data_source = self._build_data_source()
        return sum(
            queued_ahead[item.market.trading_pair] * data_source.queued_message_gas(item.market.market_id)
            for item in items
            if not item.is_cancel and item.market.trading_pair in queued_ahead
        )

    async def _drop_halted_creates(
        self, items: List[BatchedOrderMessage], error: BeezeeTxRefusedError
    ) -> List[BatchedOrderMessage]:
        """Rejects the creates on the halted market the refusal names (and on any other market now known halted) and
        returns the rest. When the refusal names no known market, nothing is dropped."""
        named = re.search(r"market (\S+) is halted", error.chain_message)
        market = next((item.market for item in items if named and item.market.market_id == named.group(1)), None)
        if market is None:
            return items
        await self._mark_market_halted_by_chain(market)
        dropped = [item for item in items if not item.is_cancel and self._halt_reason(item.market) is not None]
        for item in dropped:
            item.reject(IOError(f"Beezee order create failed: {error.chain_message}"))
        return [item for item in items if item not in dropped]

    async def _reject_batch(self, items: List[BatchedOrderMessage], error: Exception):
        if isinstance(error, BeezeeTxRefusedError):
            halted_markets = {item.market.market_id: item.market for item in items if not item.is_cancel}
            if error.is_denom_halted and len(halted_markets) == 1:
                await self._mark_market_halted_by_chain(next(iter(halted_markets.values())))
            message = error.chain_message
        else:
            message = str(error)
        for item in items:
            kind = "cancel" if item.is_cancel else "order create"
            rejection = IOError(f"Beezee {kind} failed: {message}")
            rejection.__cause__ = error
            item.reject(rejection)

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        data_source = self._build_data_source()
        markets = await data_source.get_all_markets(refresh=True)
        self._markets_by_trading_pair = {market.trading_pair: market for market in markets.values()}
        self._trade_fees = await data_source.get_trade_fees()
        return [
            TradingRule(
                trading_pair=market.trading_pair,
                min_order_size=market.base.quantum,
                min_price_increment=market.min_price_increment,
                min_base_amount_increment=market.base.quantum,
                min_quote_amount_increment=market.quote.quantum,
                min_notional_size=market.quote.quantum * Decimal("2"),
                buy_order_collateral_token=market.quote.symbol,
                sell_order_collateral_token=market.base.symbol,
                supports_limit_orders=True,
                supports_market_orders=False,
            )
            for market in markets.values()
        ]

    async def _update_trading_fees(self):
        self._trade_fees = await self._build_data_source().get_trade_fees()

    async def stop_network(self):
        await super().stop_network()
        # Tx lookups and halt cancels are restarted from the saved state by the next status update.
        self._order_batcher.stop()
        tasks = list(self._tx_lookup_tasks.values()) + list(self._halt_cancel_tasks.values())
        for task in tasks + list(self._tx_fetch_tasks.values()) + list(self._deferred_cancel_tasks.values()):
            task.cancel()
        self._tx_lookup_tasks.clear()
        self._halt_cancel_tasks.clear()
        self._deferred_cancel_tasks.clear()
        self._tx_fetch_tasks.clear()

    async def _status_polling_loop_fetch_updates(self):
        await super()._status_polling_loop_fetch_updates()
        await self._update_halted_denoms_if_due()
        self._cancel_open_orders_on_halted_pairs()
        self._retry_deferred_cancels()

    async def _update_halted_denoms_if_due(self):
        if self._halted_denoms_loaded and time.time() - self._last_halted_denoms_refresh < CONSTANTS.HALTED_DENOMS_REFRESH_INTERVAL:
            return
        try:
            await self._refresh_halted_denoms()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.logger().warning(f"Failed to update Beezee halted denoms. Keeping the last known set. Error: {error}")

    async def _refresh_halted_denoms(self):
        await self._read_halted_denoms()
        await self._apply_halt_transitions()

    async def _read_halted_denoms(self):
        self._halted_denoms = await self._build_data_source().get_halted_denoms()
        self._refused_halted_market_ids.clear()
        self._halted_denoms_loaded = True
        self._last_halted_denoms_refresh = time.time()

    async def _mark_market_halted_by_chain(self, market: BeezeeMarket):
        # The refusal does not name the denom: re-read the halted set, and if the node still does not list
        # this market's denoms, treat the market itself as halted until the next refresh.
        try:
            await self._read_halted_denoms()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.logger().warning(f"Failed to re-read Beezee halted denoms after a halted refusal. Error: {error}")
        if self._halt_reason(market) is None:
            self._refused_halted_market_ids.add(market.market_id)
        await self._apply_halt_transitions()

    def _halt_reason(self, market: BeezeeMarket) -> Optional[str]:
        for denom in (market.base.denom, market.quote.denom):
            if denom in self._halted_denoms:
                return f"denom {denom}"
        if market.market_id in self._refused_halted_market_ids:
            return "denom unknown, refused by the chain"
        return None

    async def _apply_halt_transitions(self):
        data_source = self._build_data_source()
        trading_pairs = set(self._trading_pairs or []) | {order.trading_pair for order in self.in_flight_orders.values()}
        halted_now: Dict[str, str] = {}
        for trading_pair in trading_pairs:
            try:
                market = await data_source.get_market_by_trading_pair(trading_pair)
            except KeyError:
                continue
            reason = self._halt_reason(market)
            if reason is not None:
                halted_now[trading_pair] = reason
        for trading_pair, reason in halted_now.items():
            if trading_pair not in self._halted_trading_pairs:
                self.logger().warning(
                    f"Beezee market {trading_pair} is halted ({reason}). New orders are refused; "
                    f"cancelling its open orders."
                )
        for trading_pair in self._halted_trading_pairs:
            if trading_pair not in halted_now:
                self.logger().info(f"Beezee market {trading_pair} is no longer halted. Trading is allowed again.")
        self._halted_trading_pairs = halted_now
        self._cancel_open_orders_on_halted_pairs()

    def _cancel_open_orders_on_halted_pairs(self):
        """Keeps cancelling the open orders on halted pairs until they are gone.

        Called on every halted-denoms refresh and status poll. A cancel can be deferred (exchange order id not
        resolved yet) or fail, and a create queued before the halt still executes, so one attempt is not enough.
        An order with a cancel task running or a cancel tx awaiting confirmation is skipped.
        """
        for order in list(self.in_flight_orders.values()):
            if order.trading_pair not in self._halted_trading_pairs or order.is_done or order.is_pending_cancel_confirmation:
                continue
            running = self._halt_cancel_tasks.get(order.client_order_id)
            if running is not None and not running.done():
                continue
            self._halt_cancel_tasks[order.client_order_id] = safe_ensure_future(
                self._execute_cancel(order.trading_pair, order.client_order_id)
            )
        for client_order_id in [key for key, task in self._halt_cancel_tasks.items() if task.done()]:
            if client_order_id not in self.in_flight_orders:
                del self._halt_cancel_tasks[client_order_id]

    async def _user_stream_event_listener(self):
        async for _event in self._iter_user_event_queue():
            pass

    async def _update_balances(self):
        data_source = self._build_data_source()
        balances = await data_source.get_balances()
        dust_balances = await data_source.get_user_dust()
        totals = defaultdict(Decimal)
        available = defaultdict(Decimal)
        for balance in balances:
            metadata = data_source._metadata_by_denom.get(balance["denom"]) or await data_source.get_denom_metadata(balance["denom"])
            token = token_from_metadata(balance["denom"], metadata)
            amount = Decimal(balance["amount"]) * token.quantum
            totals[token.symbol] += amount
            available[token.symbol] += amount
        for dust in dust_balances:
            metadata = data_source._metadata_by_denom.get(dust["denom"]) or await data_source.get_denom_metadata(dust["denom"])
            token = token_from_metadata(dust["denom"], metadata)
            totals[token.symbol] += Decimal(dust["amount"]) * token.quantum
        self._account_balances = dict(totals)
        self._account_available_balances = dict(available)

    # === Order tracking from chain events ===
    #
    # Every create and cancel tx is looked up after its broadcast to read the queue `message_id` of its message.
    # Blocks are then read one by one from RPC `/block_results`: the EndBlock queue emits the outcome of each message
    # (OrderExecutedEvent, OrderSavedEvent, OrderCanceledEvent) tagged with that `message_id`, and a resting order's
    # later fills carry its order id. Processing a block twice is harmless: fills are deduplicated by trade id.

    @property
    def tracking_states(self) -> Dict[str, Any]:
        states = super().tracking_states
        states[CONSTANTS.TRACKING_STATE_KEY] = {
            "last_processed_height": self._last_processed_height,
            "creates": {client_order_id: message.to_json() for client_order_id, message in self._queued_creates.items()},
            "cancels": {client_order_id: message.to_json() for client_order_id, message in self._queued_cancels.items()},
            "fills_counted_through": dict(self._fills_counted_through),
        }
        return states

    def restore_tracking_states(self, saved_states: Dict[str, Any]):
        saved_states = dict(saved_states or {})
        block_state = saved_states.pop(CONSTANTS.TRACKING_STATE_KEY, None) or {}
        super().restore_tracking_states(saved_states)
        self._last_processed_height = block_state.get("last_processed_height")
        saved_creates = block_state.get("creates") or {}
        saved_cancels = block_state.get("cancels") or {}
        self._fills_counted_through = dict(block_state.get("fills_counted_through") or {})
        for order in self.in_flight_orders.values():
            client_order_id = order.client_order_id
            exchange_order_id = order.exchange_order_id or ""
            if client_order_id in saved_creates:
                # Created but not yet saved by the chain: resume from the create tx.
                self._queued_creates[client_order_id] = QueuedOrderMessage.from_json(saved_creates[client_order_id])
            elif len(exchange_order_id) == CONSTANTS.TX_HASH_LENGTH:
                # A state saved without the tx details: the create message is matched by its values.
                self._queued_creates[client_order_id] = QueuedOrderMessage(
                    client_order_id=client_order_id, tx_hash=exchange_order_id
                )
            elif len(exchange_order_id) == CONSTANTS.ORDER_ID_LENGTH:
                if client_order_id in saved_cancels:
                    self._queued_cancels[client_order_id] = QueuedOrderMessage.from_json(saved_cancels[client_order_id])
                self._orders_to_verify.add(client_order_id)
            elif not exchange_order_id:
                # Stopped while the create was on its way, before its tx hash was known.
                self._restored_without_tx.add(client_order_id)

    def _queued_messages(self, is_cancel: bool) -> Dict[str, QueuedOrderMessage]:
        return self._queued_cancels if is_cancel else self._queued_creates

    def _queue_message(self, message: QueuedOrderMessage, is_cancel: bool):
        self._queued_messages(is_cancel)[message.client_order_id] = message
        self._start_tx_lookup(message, is_cancel)

    def _start_tx_lookup(self, message: QueuedOrderMessage, is_cancel: bool):
        key = (is_cancel, message.client_order_id)
        task = self._tx_lookup_tasks.get(key)
        if task is None or task.done():
            self._tx_lookup_tasks[key] = safe_ensure_future(self._look_up_tx(message, is_cancel))

    async def _look_up_tx(self, message: QueuedOrderMessage, is_cancel: bool):
        try:
            for delay in CONSTANTS.TX_LOOKUP_DELAYS:
                await self._sleep(delay)
                if self._queued_messages(is_cancel).get(message.client_order_id) is not message:
                    return
                # None only when the node answers "not found": a failed request raises and is retried later.
                tx = await self._fetch_tx(message.tx_hash)
                tx_response = (tx or {}).get("tx_response")
                if tx_response:
                    await self._on_tx_found(message, is_cancel, tx_response)
                    return
            await self._on_tx_not_found(message, is_cancel)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.logger().warning(
                f"Failed to follow Beezee tx {message.tx_hash} (client_order_id={message.client_order_id}). "
                f"Retrying on the next status update. Error: {error}"
            )

    async def _fetch_tx(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        # The lookups of a batch's messages run in step, so they share the request in flight for their tx.
        task = self._tx_fetch_tasks.get(tx_hash)
        if task is None:
            # Its error goes to the lookups awaiting it, which log and retry it.
            task = asyncio.ensure_future(self._build_data_source().get_tx(tx_hash))
            self._tx_fetch_tasks[tx_hash] = task
            task.add_done_callback(lambda done: self._forget_tx_fetch(tx_hash, done))
        return await asyncio.shield(task)

    def _forget_tx_fetch(self, tx_hash: str, task: asyncio.Task):
        if self._tx_fetch_tasks.get(tx_hash) is task:
            del self._tx_fetch_tasks[tx_hash]
        if not task.cancelled():
            task.exception()  # retrieved here too, for when every lookup awaiting it was cancelled

    async def _on_tx_not_found(self, message: QueuedOrderMessage, is_cancel: bool):
        waited = int(sum(CONSTANTS.TX_LOOKUP_DELAYS))
        if is_cancel:
            self._queued_cancels.pop(message.client_order_id, None)
            order = self._order_tracker.fetch_tracked_order(message.client_order_id)
            if order is not None:
                self.logger().warning(
                    f"Beezee cancel tx {message.tx_hash} was not found within {waited} s. The order stays open. "
                    f"client_order_id={message.client_order_id}"
                )
                self._update_order_state(order, self._open_state(order))
        else:
            await self._settle_create_from_book(message, f"its tx was not found within {waited} s")

    async def _on_tx_found(self, message: QueuedOrderMessage, is_cancel: bool, tx_response: Dict[str, Any]):
        order = self._order_tracker.fetch_tracked_order(message.client_order_id)
        if order is None:
            self._queued_messages(is_cancel).pop(message.client_order_id, None)
            return
        code = int(tx_response.get("code", 0))
        if code != 0:
            self._queued_messages(is_cancel).pop(message.client_order_id, None)
            raw_log = tx_response.get("raw_log")
            if is_cancel:
                self.logger().warning(
                    f"Beezee cancel tx {message.tx_hash} failed with code {code}: {raw_log}. The order stays open. "
                    f"client_order_id={message.client_order_id}"
                )
                self._update_order_state(order, self._open_state(order))
                return
            if is_denom_halted_error(code, tx_response.get("codespace"), raw_log):
                market = await self._build_data_source().get_market_by_trading_pair(order.trading_pair)
                await self._mark_market_halted_by_chain(market)
            self.logger().warning(
                f"Beezee create tx {message.tx_hash} failed with code {code}: {raw_log}. "
                f"client_order_id={message.client_order_id}"
            )
            self._update_order_state(order, OrderState.FAILED)
            return

        events = tx_tradebin_events(tx_response)
        if is_cancel:
            event = next(
                (
                    event for event in events
                    if event.type == CONSTANTS.ORDER_CANCEL_MESSAGE_EVENT
                    and event.get("creator") == self._account_address
                    and event.get("orderId") == message.order_id
                ),
                None,
            )
        else:
            event = await self._create_message_event(message, order, events)
        if event is None or not event.get("message_id"):
            self._queued_messages(is_cancel).pop(message.client_order_id, None)
            kind = "cancel" if is_cancel else "create"
            self.logger().warning(
                f"Beezee {kind} tx {message.tx_hash} has no matching order message event. "
                f"client_order_id={message.client_order_id}"
            )
            self._update_order_state(order, self._open_state(order) if is_cancel else OrderState.FAILED)
            return
        # Under the lock, so a pass already past the tx height cannot take this message in mid-way.
        async with self._block_processing_lock:
            message.tx_height = int(tx_response["height"])
            message.message_id = event.get("message_id")
            message.next_height = message.tx_height
        await self._process_blocks()

    async def _create_message_event(
        self, message: QueuedOrderMessage, order: InFlightOrder, events: List[TradebinEvent]
    ) -> Optional[TradebinEvent]:
        # The tx emits one OrderCreateMessageEvent per create message, in message order.
        create_events = [
            event for event in events
            if event.type == CONSTANTS.ORDER_CREATE_MESSAGE_EVENT and event.get("creator") == self._account_address
        ]
        if message.msg_index is not None:
            return create_events[message.msg_index] if message.msg_index < len(create_events) else None
        market = await self._build_data_source().get_market_by_trading_pair(order.trading_pair)
        claimed = {
            other.message_id for other in self._queued_creates.values()
            if other is not message and other.tx_hash == message.tx_hash and other.message_id
        }
        for index, event in enumerate(create_events):
            if (
                event.get("message_id") not in claimed
                and event.get("market_id") == market.market_id
                and event.get("order_type") == self._side(order.trade_type)
                and chain_amount_to_display(event.get("amount"), market.base) == order.amount
                and chain_price_to_display(event.get("price"), market) == order.price
            ):
                message.msg_index = index
                return event
        return None

    def _on_new_block(self):
        if self.is_trading_required and not self._block_processing_lock.locked():
            safe_ensure_future(self._update_order_status())

    async def _update_order_status(self):
        # Order states and fills come from the chain's order events, read block by block (see above).
        try:
            await self._process_blocks()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.logger().warning(f"Failed to read Beezee order events. Retrying on the next update. Error: {error}")

    async def _process_blocks(self):
        async with self._block_processing_lock:
            for is_cancel in (False, True):
                for message in list(self._queued_messages(is_cancel).values()):
                    if message.tx_height is None:
                        self._start_tx_lookup(message, is_cancel)
            data_source = self._build_data_source()
            latest_height = await data_source.get_latest_block_height()
            nothing_to_follow = not (
                self.in_flight_orders or self._queued_creates or self._queued_cancels or self._orders_to_verify
            )
            if self._last_processed_height is None or nothing_to_follow:
                self._last_processed_height = latest_height
            elif latest_height - self._last_processed_height > CONSTANTS.MAX_BLOCK_CATCH_UP:
                self.logger().warning(
                    f"Beezee fills were not read for {latest_height - self._last_processed_height} blocks "
                    f"(more than {CONSTANTS.MAX_BLOCK_CATCH_UP}). Reconciling resting orders from the chain instead."
                )
                await self._reconcile_resting_orders(latest_height)
                self._last_processed_height = latest_height
            # Never read further back than the catch-up limit: older messages are settled from the order book.
            floor_height = latest_height - CONSTANTS.MAX_BLOCK_CATCH_UP
            behind = f"its blocks are more than {CONSTANTS.MAX_BLOCK_CATCH_UP} behind the chain"
            for message in list(self._queued_creates.values()):
                if message.tx_height is not None and message.next_height <= floor_height:
                    await self._settle_create_from_book(message, behind, latest_height)
            for message in list(self._queued_cancels.values()):
                if message.tx_height is not None and message.next_height <= floor_height:
                    await self._finish_cancel_without_outcome(message, behind)
            start_height = self._last_processed_height + 1
            for message in list(self._queued_creates.values()) + list(self._queued_cancels.values()):
                if message.tx_height is not None:
                    start_height = min(start_height, message.next_height)
            for height in range(start_height, latest_height + 1):
                events = await self._read_block_events(height)
                if events is None:
                    await self._skip_unreadable_block(height, latest_height)
                    events = []
                await self._apply_block_events(height, events)
                self._last_processed_height = max(self._last_processed_height, height)
            await self._expire_queued_messages()
            await self._resolve_restored_without_tx(latest_height)
            await self._verify_restored_orders()
            self._retry_deferred_cancels()

    async def _read_block_events(self, height: int) -> Optional[List[TradebinEvent]]:
        """The block's tradebin events, or None once the block failed to load MAX_BLOCK_READ_ATTEMPTS times."""
        try:
            events = await self._block_events(height)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            failures = self._block_read_failures.get(height, 0) + 1
            if failures < CONSTANTS.MAX_BLOCK_READ_ATTEMPTS:
                self._block_read_failures[height] = failures
                raise
            self._block_read_failures.pop(height, None)
            self.logger().warning(
                f"Beezee block {height} could not be read {failures} times ({error}). Skipping it and settling the "
                f"orders it could affect from the chain state."
            )
            return None
        self._block_read_failures.pop(height, None)
        return events

    async def _skip_unreadable_block(self, height: int, latest_height: int):
        reason = f"block {height} could not be read"
        for message in [message for message in self._queued_creates.values() if message.in_window(height)]:
            await self._settle_create_from_book(message, reason, latest_height)
        for message in [message for message in self._queued_cancels.values() if message.in_window(height)]:
            await self._finish_cancel_without_outcome(message, reason)
        # The chain state right after the skipped block has the resting orders' exact fills through it; the next
        # blocks are then read as usual. A node that pruned that state too is read at the latest height instead.
        await self._reconcile_resting_orders(height, fallback_height=latest_height)

    async def _block_events(self, height: int) -> List[TradebinEvent]:
        events = self._block_events_cache.get(height)
        if events is None:
            events = block_tradebin_events(await self._build_data_source().get_block_results(height))
            self._block_events_cache[height] = events
            oldest_kept = height - CONSTANTS.BLOCK_EVENTS_CACHE_SIZE
            for cached_height in [cached for cached in self._block_events_cache if cached <= oldest_kept]:
                del self._block_events_cache[cached_height]
        return events

    async def _apply_block_events(self, height: int, events: List[TradebinEvent]):
        creates = [message for message in self._queued_creates.values() if message.in_window(height)]
        cancels = [message for message in self._queued_cancels.values() if message.in_window(height)]
        if events and self._account_address is not None:
            data_source = self._build_data_source()
            markets = await data_source.get_all_markets()
            market_ids = (await data_source.get_symbol_map()).inverse
            resting = {
                order.exchange_order_id: order
                for order in self._order_tracker.all_fillable_orders.values()
                if len(order.exchange_order_id or "") == CONSTANTS.ORDER_ID_LENGTH
            }
            # client order id of a create → the id of its saved order, or None when nothing rested.
            outcomes: Dict[str, Optional[str]] = {}
            canceled: Set[str] = set()
            maker_filled: Set[str] = set()
            superseded: List[QueuedOrderMessage] = []
            for index, event in enumerate(events):
                if event.type in (CONSTANTS.ORDER_CREATE_MESSAGE_EVENT, CONSTANTS.ORDER_CANCEL_MESSAGE_EVENT):
                    # A message id is reissued only once the queue is empty, so a later tx queuing the same id
                    # proves our message was already processed. v8.2.0 emits nothing for a refunded create or a
                    # cancel of an order that is gone, so that message has no outcome left to wait for.
                    for message in creates + cancels:
                        if message.message_id == event.get("message_id") and height > message.tx_height:
                            superseded.append(message)
                    creates = [message for message in creates if message not in superseded]
                    cancels = [message for message in cancels if message not in superseded]
                elif event.type == CONSTANTS.ORDER_EXECUTED_EVENT:
                    market = markets.get(event.get("market_id"))
                    if market is None:
                        continue
                    # The event's order_type is the side of the resting (maker) order.
                    resting_order = resting.get(event.get("id"))
                    if event.get("maker") == self._account_address and resting_order is not None:
                        self._process_fill(resting_order, market, event, height, f"{height}-{index}-maker", is_taker=False)
                        maker_filled.add(resting_order.client_order_id)
                    if event.get("taker") == self._account_address:
                        message = self._match_create(creates, market_ids, event, self._opposite_side(event.get("order_type")))
                        if message is not None:
                            order = self._order_tracker.fetch_order(message.client_order_id)
                            paid_taker_fee = self._rested_before_tx(event.get("id"), message.tx_height, height, events[:index])
                            self._process_fill(
                                order, market, event, height, f"{height}-{index}-taker", is_taker=True,
                                paid_taker_fee=paid_taker_fee,
                            )
                            outcomes.setdefault(message.client_order_id, None)
                elif event.type == CONSTANTS.ORDER_SAVED_EVENT and event.get("owner") == self._account_address:
                    message = self._match_create(creates, market_ids, event, event.get("order_type"))
                    if message is not None:
                        order = self._order_tracker.fetch_order(message.client_order_id)
                        order.update_exchange_order_id(event.get("id"))
                        resting[event.get("id")] = order
                        outcomes[message.client_order_id] = event.get("id")
                elif event.type == CONSTANTS.ORDER_CANCELED_EVENT and event.get("owner") == self._account_address:
                    message = next(
                        (
                            message for message in cancels
                            if message.message_id == event.get("message_id") and message.order_id == event.get("id")
                        ),
                        None,
                    )
                    if message is not None:
                        canceled.add(message.client_order_id)
                        continue
                    # An order id is never reused, so a cancel of one of our resting orders settles it even without
                    # a queued cancel: one whose state was saved before the cancel tx, or one cancelled outside the bot.
                    resting_order = resting.get(event.get("id"))
                    if (
                        resting_order is not None
                        and market_ids.get(resting_order.trading_pair) == event.get("market_id")
                    ):
                        canceled.add(resting_order.client_order_id)
            for client_order_id in set(outcomes) | canceled | maker_filled:
                self._settle_order(client_order_id, height, outcomes, canceled)
            for message in superseded:
                if message.client_order_id in outcomes or message.client_order_id in canceled:
                    continue
                if self._queued_creates.get(message.client_order_id) is message:
                    await self._finish_create_without_outcome(message, f"its message id was reused at height {height}")
                elif self._queued_cancels.get(message.client_order_id) is message:
                    await self._finish_cancel_without_outcome(message, f"its message id was reused at height {height}")
        for message in creates + cancels:
            if message.next_height == height:
                message.next_height = height + 1

    def _match_create(
        self,
        creates: List[QueuedOrderMessage],
        market_ids: Dict[str, str],
        event: TradebinEvent,
        side: str,
    ) -> Optional[QueuedOrderMessage]:
        candidates = []
        for message in creates:
            order = self._order_tracker.fetch_order(message.client_order_id)
            if (
                order is not None
                and message.message_id == event.get("message_id")
                and market_ids.get(order.trading_pair) == event.get("market_id")
                and self._side(order.trade_type) == side
            ):
                candidates.append(message)
        # A message id is reused only after its previous holder was processed, so the latest tx owns it.
        return max(candidates, key=lambda message: message.tx_height, default=None)

    def _settle_order(
        self,
        client_order_id: str,
        height: int,
        outcomes: Dict[str, Optional[str]],
        canceled: Set[str],
    ):
        order = self._order_tracker.fetch_order(client_order_id)
        create = self._queued_creates.pop(client_order_id, None) if client_order_id in outcomes else None
        if client_order_id in canceled:
            self._queued_cancels.pop(client_order_id, None)
        if order is None:
            return
        if order.executed_amount_base >= order.amount:
            self._queued_cancels.pop(client_order_id, None)
            state = OrderState.FILLED
        elif client_order_id in canceled:
            state = OrderState.CANCELED
        elif create is not None and outcomes[client_order_id] is None:
            self.logger().info(
                f"Beezee order {client_order_id} filled {order.executed_amount_base} of {order.amount} at height "
                f"{height} and the chain refunded the rest. tx_hash={create.tx_hash}, message_id={create.message_id}"
            )
            state = OrderState.CANCELED
        elif client_order_id in self._queued_cancels:
            return
        else:
            state = self._open_state(order)
        if create is not None:
            self.logger().info(
                f"Beezee order {client_order_id} processed at height {height}. exchange_order_id="
                f"{order.exchange_order_id}, tx_hash={create.tx_hash}, message_id={create.message_id}, state={state.name}"
            )
        self._update_order_state(order, state)

    def _rested_before_tx(self, order_id: str, tx_height: int, height: int, events_before: List[TradebinEvent]) -> bool:
        """Whether the crossed order already rested on the book when our create tx ran. Tradebin picks the fee at tx
        time: taker only when an opposite order rests at that price, so crossing an order saved in the tx block or
        later (both legs of a same-block self-fill) costs the maker fee. A block missing from the cache counts as
        rested, so the fee is never under-reported."""
        def saved(events: List[TradebinEvent]) -> bool:
            return any(event.type == CONSTANTS.ORDER_SAVED_EVENT and event.get("id") == order_id for event in events)

        if saved(events_before):
            return False
        for earlier_height in range(tx_height, height):
            events = self._block_events_cache.get(earlier_height)
            if events is None:
                return True
            if saved(events):
                return False
        return True

    def _process_fill(
        self, order: InFlightOrder, market: BeezeeMarket, event: TradebinEvent, height: int, trade_id: str, is_taker: bool,
        paid_taker_fee: Optional[bool] = None,
    ):
        if height <= self._fills_counted_through.get(order.client_order_id, -1):
            return
        amount = chain_amount_to_display(event.get("amount"), market.base)
        price = chain_price_to_display(event.get("price"), market)
        self._order_tracker.process_trade_update(
            TradeUpdate(
                trade_id=trade_id,
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
                trading_pair=order.trading_pair,
                fill_timestamp=self.current_timestamp,
                fill_price=price,
                fill_base_amount=amount,
                fill_quote_amount=amount * price,
                fee=self._fill_fee(order, market, amount, price, is_taker if paid_taker_fee is None else paid_taker_fee),
                is_taker=is_taker,
            )
        )

    def _fill_fee(self, order: InFlightOrder, market: BeezeeMarket, amount: Decimal, price: Decimal, is_taker: bool):
        """The tradebin fee is a flat amount charged once per create message, at tx time, so it goes on the order's
        first fill only. The chain charges the taker fee when an opposite order rests at that exact price, else the
        maker fee; a first fill crossing an order that rested before the create tx counts as taker, any other as
        maker."""
        if order.order_fills:
            return AddedToCostTradeFee(flat_fees=[])
        return self.get_fee(
            market.base.symbol, market.quote.symbol, order.order_type, order.trade_type, amount, price,
            is_maker=not is_taker,
        )

    async def _expire_queued_messages(self):
        window = f"no outcome within {CONSTANTS.ORDER_OUTCOME_BLOCK_WINDOW} blocks of its tx"
        for message in list(self._queued_creates.values()):
            if message.tx_height is not None and message.next_height > message.last_height:
                await self._finish_create_without_outcome(message, window)
        for message in list(self._queued_cancels.values()):
            if message.tx_height is not None and message.next_height > message.last_height:
                await self._finish_cancel_without_outcome(message, window)

    async def _finish_create_without_outcome(self, message: QueuedOrderMessage, reason: str):
        self._queued_creates.pop(message.client_order_id, None)
        self.logger().warning(
            f"Beezee order {message.client_order_id} failed: {reason}. tx_hash={message.tx_hash}, "
            f"message_id={message.message_id}, tx_height={message.tx_height}"
        )
        order = self._order_tracker.fetch_tracked_order(message.client_order_id)
        if order is not None:
            self._update_order_state(order, OrderState.FAILED)

    async def _settle_create_from_book(self, message: QueuedOrderMessage, reason: str, height: Optional[int] = None):
        """Settles a create whose outcome cannot be read from blocks: it rests if the book holds an order of ours with
        its market, side and price and its amount or less that no tracked order claims (the difference is booked as
        one fill), else it fails."""
        self._queued_creates.pop(message.client_order_id, None)
        order = self._order_tracker.fetch_tracked_order(message.client_order_id)
        if order is None:
            return
        order_id, remaining = await self._unclaimed_order_on_book(order, partly_filled=True)
        if order_id is None:
            await self._finish_create_without_outcome(message, reason)
            return
        self.logger().warning(
            f"Beezee order {message.client_order_id}: {reason}, but a matching order of this account rests on the "
            f"book. Taking it as this order. exchange_order_id={order_id}, tx_hash={message.tx_hash}"
        )
        await self._take_book_order(order, order_id, remaining, height)

    async def _unclaimed_order_on_book(
        self, order: InFlightOrder, partly_filled: bool = False
    ) -> Tuple[Optional[str], Decimal]:
        """An order of ours on the book with the order's side, price and amount that no tracked order claims, and its
        remaining amount. With `partly_filled`, a smaller remaining amount matches too when no full one is found."""
        data_source = self._build_data_source()
        market = await data_source.get_market_by_trading_pair(order.trading_pair)
        side = self._side(order.trade_type)
        claimed = {tracked.exchange_order_id for tracked in self._order_tracker.all_fillable_orders.values()}
        partial: Tuple[Optional[str], Decimal] = (None, Decimal("0"))
        for open_order in await data_source.get_user_market_orders(market.market_id):
            order_id = open_order.get("id")
            if order_id in claimed or open_order.get("order_type", side) != side:
                continue
            chain_order = await data_source.get_market_order(market.market_id, side, order_id)
            if chain_order is None or chain_price_to_display(chain_order["price"], market) != order.price:
                continue
            remaining = chain_amount_to_display(chain_order["amount"], market.base)
            if remaining == order.amount:
                return order_id, remaining
            if partly_filled and Decimal("0") < remaining < order.amount and partial[0] is None:
                partial = (order_id, remaining)
        return partial

    async def _resolve_restored_without_tx(self, height: int):
        """A restored order that has neither a tx nor an order id rests if the book holds an unclaimed order of ours
        with its side and price and its amount or less (the difference is booked as one fill), else it fails. It stays
        pending while a queued create of the same pair, side, price and amount is unsettled, since that create may own
        the book order, and until a book read succeeds."""
        for client_order_id in list(self._restored_without_tx):
            order = self._order_tracker.fetch_tracked_order(client_order_id)
            if order is None or order.exchange_order_id:
                self._restored_without_tx.discard(client_order_id)
                continue
            if any(self._is_same_order(order, message) for message in self._queued_creates.values()):
                continue
            order_id, remaining = await self._unclaimed_order_on_book(order, partly_filled=True)
            self._restored_without_tx.discard(client_order_id)
            if order_id is None:
                self.logger().warning(
                    f"Beezee order {client_order_id} was saved before its create tx was known and no matching order of "
                    f"this account rests on the book. Marking it failed."
                )
                self._update_order_state(order, OrderState.FAILED)
                continue
            self.logger().warning(
                f"Beezee order {client_order_id} was saved before its create tx was known. A matching order of this "
                f"account rests on the book: taking it as this order. exchange_order_id={order_id}"
            )
            await self._take_book_order(order, order_id, remaining, height)

    async def _take_book_order(
        self, order: InFlightOrder, order_id: str, remaining: Decimal, height: Optional[int] = None
    ):
        """The order rests as the book order `order_id`; what the book order lost by `height` (the latest height when
        not given) is booked as one fill."""
        order.update_exchange_order_id(order_id)
        missed_fill = order.amount - remaining - order.executed_amount_base
        if missed_fill > Decimal("0"):
            data_source = self._build_data_source()
            if height is None:
                height = await data_source.get_latest_block_height()
            market = await data_source.get_market_by_trading_pair(order.trading_pair)
            # Its fills so far are in the book amount, so block fills up to this height must not count again.
            self._fills_counted_through[order.client_order_id] = height
            self._order_tracker.process_trade_update(
                TradeUpdate(
                    trade_id=f"reconcile-{height}",
                    client_order_id=order.client_order_id,
                    exchange_order_id=order_id,
                    trading_pair=order.trading_pair,
                    fill_timestamp=self.current_timestamp,
                    fill_price=order.price,
                    fill_base_amount=missed_fill,
                    fill_quote_amount=missed_fill * order.price,
                    fee=self._fill_fee(order, market, missed_fill, order.price, is_taker=False),
                    is_taker=False,
                )
            )
        self._update_order_state(order, self._open_state(order))

    def _is_same_order(self, order: InFlightOrder, message: QueuedOrderMessage) -> bool:
        other = self._order_tracker.fetch_tracked_order(message.client_order_id)
        return other is not None and (other.trading_pair, other.trade_type, other.price, other.amount) == (
            order.trading_pair, order.trade_type, order.price, order.amount
        )

    async def _finish_cancel_without_outcome(self, message: QueuedOrderMessage, reason: str):
        self._queued_cancels.pop(message.client_order_id, None)
        order = self._order_tracker.fetch_tracked_order(message.client_order_id)
        if order is None or order.is_done:
            return
        if await self._is_on_book(order):
            self.logger().warning(
                f"Beezee cancel of order {message.client_order_id} did not take effect ({reason}) and the order is "
                f"still on the book. tx_hash={message.tx_hash}, message_id={message.message_id}"
            )
            self._update_order_state(order, self._open_state(order))
        else:
            # Filled before the cancel executed: the fills came from the block events.
            self._update_order_state(order, self._off_book_state(order))

    async def _verify_restored_orders(self):
        for client_order_id in list(self._orders_to_verify):
            self._orders_to_verify.discard(client_order_id)
            order = self._order_tracker.fetch_tracked_order(client_order_id)
            if order is None:
                continue
            self._order_tracker.process_order_update(await self._request_order_status(order))

    async def _reconcile_resting_orders(self, height: int, fallback_height: Optional[int] = None):
        """Takes the resting orders' fills up to `height` from the chain state, for when their block events are lost.

        An order still on the book gets the amount it lost as one fill at the order price; its block fills up to
        `height` are then ignored. An order gone from the book has an unknown history (filled, or cancelled from
        elsewhere), so no fill is invented: it is checked against the book and ends CANCELED unless its fills cover it.
        """
        data_source = self._build_data_source()
        for order in list(self.in_flight_orders.values()):
            if len(order.exchange_order_id or "") != CONSTANTS.ORDER_ID_LENGTH:
                continue
            market = await data_source.get_market_by_trading_pair(order.trading_pair)
            side = self._side(order.trade_type)
            read_height = height
            try:
                chain_order = await data_source.get_market_order(
                    market.market_id, side, order.exchange_order_id, height=height
                )
            except Exception:
                if fallback_height is None:
                    raise
                read_height = fallback_height
                chain_order = await data_source.get_market_order(
                    market.market_id, side, order.exchange_order_id, height=fallback_height
                )
            self._orders_to_verify.add(order.client_order_id)
            if chain_order is None:
                # No fill comes from the chain state, so block fills still read after this point keep counting.
                if order.executed_amount_base < order.amount:
                    self.logger().warning(
                        f"Beezee order {order.client_order_id} left the book while its fills were not read. "
                        f"Its fills are unknown, so it is closed as cancelled without a fill."
                    )
                continue
            self._fills_counted_through[order.client_order_id] = read_height
            missed_fill = order.amount - chain_amount_to_display(chain_order["amount"], market.base) - order.executed_amount_base
            if missed_fill <= Decimal("0"):
                continue
            self.logger().warning(
                f"Beezee order {order.client_order_id} lost {missed_fill} while fills were not read. "
                f"Recording it as one fill at the order price."
            )
            self._order_tracker.process_trade_update(
                TradeUpdate(
                    trade_id=f"reconcile-{read_height}",
                    client_order_id=order.client_order_id,
                    exchange_order_id=order.exchange_order_id,
                    trading_pair=order.trading_pair,
                    fill_timestamp=self.current_timestamp,
                    fill_price=order.price,
                    fill_base_amount=missed_fill,
                    fill_quote_amount=missed_fill * order.price,
                    fee=self._fill_fee(order, market, missed_fill, order.price, is_taker=False),
                    is_taker=False,
                )
            )

    async def _is_on_book(self, order: InFlightOrder) -> bool:
        data_source = self._build_data_source()
        market = await data_source.get_market_by_trading_pair(order.trading_pair)
        open_orders = await data_source.get_user_market_orders(market.market_id)
        return any(open_order.get("id") == order.exchange_order_id for open_order in open_orders)

    def _update_order_state(self, order: InFlightOrder, state: OrderState):
        update_task = self._order_tracker.process_order_update(
            OrderUpdate(
                trading_pair=order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=state,
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
            )
        )
        update_task.add_done_callback(self._report_order_progress)

    @staticmethod
    def _open_state(order: InFlightOrder) -> OrderState:
        return OrderState.PARTIALLY_FILLED if order.executed_amount_base > Decimal("0") else OrderState.OPEN

    @staticmethod
    def _off_book_state(order: InFlightOrder) -> OrderState:
        return OrderState.FILLED if order.executed_amount_base >= order.amount else OrderState.CANCELED

    @staticmethod
    def _side(trade_type: TradeType) -> str:
        return CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL

    @staticmethod
    def _opposite_side(side: str) -> str:
        return CONSTANTS.SIDE_SELL if side == CONSTANTS.SIDE_BUY else CONSTANTS.SIDE_BUY

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        # Fills come from the block events (see _apply_block_events), never from the id-less market history.
        return []

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        state = tracked_order.current_state
        if len(tracked_order.exchange_order_id or "") == CONSTANTS.ORDER_ID_LENGTH:
            if await self._is_on_book(tracked_order):
                state = (
                    OrderState.PENDING_CANCEL
                    if tracked_order.client_order_id in self._queued_cancels
                    else self._open_state(tracked_order)
                )
            else:
                state = self._off_book_state(tracked_order)
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=state,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id,
        )

    async def _initialize_trading_pair_symbol_map(self):
        try:
            symbol_map = await self._build_data_source().get_symbol_map()
            self._set_trading_pair_symbol_map(symbol_map)
        except Exception:
            self.logger().exception("There was an error requesting exchange info.")

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        # The async symbol map initialization above is used instead because Beezee pair metadata
        # requires denom lookups in addition to the raw `all_markets` payload.
        return None
