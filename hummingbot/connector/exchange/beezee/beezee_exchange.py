import asyncio
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
        return self.tx_height is not None and self.next_height <= height <= self.last_height

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
        self._halted_denoms: Set[str] = set()
        self._halted_denoms_loaded = False
        self._last_halted_denoms_refresh = 0.0
        # Markets the chain refused as halted although the last halted-denoms read did not list their denoms.
        self._refused_halted_market_ids: Set[str] = set()
        self._halted_trading_pairs: Dict[str, str] = {}
        self._queued_creates: Dict[str, QueuedOrderMessage] = {}
        self._queued_cancels: Dict[str, QueuedOrderMessage] = {}
        self._tx_lookup_tasks: Dict[Tuple[bool, str], asyncio.Task] = {}
        self._last_processed_height: Optional[int] = None
        self._block_events_cache: Dict[int, List[TradebinEvent]] = {}
        self._block_processing_lock = asyncio.Lock()
        # Resting orders restored from a saved state, checked against UserMarketOrders once fills are caught up.
        self._orders_to_verify: Set[str] = set()
        self._last_status_log_timestamp = 0.0

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
        try:
            response = await data_source.broadcast_create_order(
                market=market,
                order_type=side,
                amount=amount,
                price=price,
                memo=f"hbot:{order_id}",
            )
        except BeezeeTxRefusedError as error:
            if error.is_denom_halted:
                await self._mark_market_halted_by_chain(market)
            raise IOError(f"Beezee order create failed: {error.chain_message}") from error
        tx_hash = response["tx_response"]["txhash"]
        # The tx hash stays the order's exchange id until the chain saves the order and gives it an id.
        self._queue_message(QueuedOrderMessage(client_order_id=order_id, tx_hash=tx_hash, msg_index=0), is_cancel=False)
        self.logger().info(
            f"Beezee create order transaction submitted. client_order_id={order_id}, "
            f"market={market.market_id}, side={side}, tx_hash={tx_hash}"
        )
        return tx_hash, self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        if self._signer is None:
            raise ValueError("Beezee wallet mode is required for cancels.")
        data_source = self._build_data_source()
        market = await data_source.get_market_by_trading_pair(tracked_order.trading_pair)
        exchange_order_id = tracked_order.exchange_order_id
        if not exchange_order_id or len(exchange_order_id) != CONSTANTS.ORDER_ID_LENGTH:
            self.logger().debug(
                f"Deferring Beezee cancel until the chain saves the order. client_order_id={order_id}"
            )
            return False
        if order_id in self._queued_cancels:
            return True

        try:
            response = await data_source.broadcast_cancel_order(
                market_id=market.market_id,
                order_type=CONSTANTS.SIDE_BUY if tracked_order.trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL,
                order_id=exchange_order_id,
                memo=f"hbot-cancel:{order_id}",
            )
        except BeezeeTxRefusedError as error:
            raise IOError(f"Beezee cancel failed: {error.chain_message}") from error
        tx_hash = response["tx_response"]["txhash"]
        self._queue_message(
            QueuedOrderMessage(client_order_id=order_id, tx_hash=tx_hash, order_id=exchange_order_id),
            is_cancel=True,
        )
        self.logger().info(
            f"Beezee cancel order transaction submitted. client_order_id={order_id}, "
            f"exchange_order_id={exchange_order_id}, tx_hash={tx_hash}"
        )
        return True

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        data_source = self._build_data_source()
        markets = await data_source.get_all_markets(refresh=True)
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

    async def _status_polling_loop_fetch_updates(self):
        await super()._status_polling_loop_fetch_updates()
        await self._update_halted_denoms_if_due()

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
            if trading_pair in self._halted_trading_pairs:
                continue
            open_orders = [
                order for order in self.in_flight_orders.values()
                if order.trading_pair == trading_pair and not order.is_done
            ]
            self.logger().warning(
                f"Beezee market {trading_pair} is halted ({reason}). New orders are refused; "
                f"cancelling {len(open_orders)} open order(s)."
            )
            for order in open_orders:
                self.cancel(trading_pair, order.client_order_id)
        for trading_pair in self._halted_trading_pairs:
            if trading_pair not in halted_now:
                self.logger().info(f"Beezee market {trading_pair} is no longer halted. Trading is allowed again.")
        self._halted_trading_pairs = halted_now

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
        }
        return states

    def restore_tracking_states(self, saved_states: Dict[str, Any]):
        saved_states = dict(saved_states or {})
        block_state = saved_states.pop(CONSTANTS.TRACKING_STATE_KEY, None) or {}
        super().restore_tracking_states(saved_states)
        self._last_processed_height = block_state.get("last_processed_height")
        saved_creates = block_state.get("creates") or {}
        saved_cancels = block_state.get("cancels") or {}
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
        data_source = self._build_data_source()
        try:
            for delay in CONSTANTS.TX_LOOKUP_DELAYS:
                await self._sleep(delay)
                if self._queued_messages(is_cancel).get(message.client_order_id) is not message:
                    return
                tx = await data_source.get_tx(message.tx_hash)
                tx_response = (tx or {}).get("tx_response")
                if tx_response:
                    await self._on_tx_found(message, is_cancel, tx_response)
                    return
            self._on_tx_not_found(message, is_cancel)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.logger().warning(
                f"Failed to follow Beezee tx {message.tx_hash} (client_order_id={message.client_order_id}). "
                f"Retrying on the next status update. Error: {error}"
            )

    def _on_tx_not_found(self, message: QueuedOrderMessage, is_cancel: bool):
        self._queued_messages(is_cancel).pop(message.client_order_id, None)
        order = self._order_tracker.fetch_tracked_order(message.client_order_id)
        if order is None:
            return
        waited = int(sum(CONSTANTS.TX_LOOKUP_DELAYS))
        if is_cancel:
            self.logger().warning(
                f"Beezee cancel tx {message.tx_hash} was not found within {waited} s. The order stays open. "
                f"client_order_id={message.client_order_id}"
            )
            self._update_order_state(order, self._open_state(order))
        else:
            self.logger().warning(
                f"Beezee create tx {message.tx_hash} was not found within {waited} s. Marking the order failed. "
                f"client_order_id={message.client_order_id}"
            )
            self._update_order_state(order, OrderState.FAILED)

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
                self._last_processed_height = latest_height
                await self._reconcile_resting_orders(latest_height)
            start_height = self._last_processed_height + 1
            for message in list(self._queued_creates.values()) + list(self._queued_cancels.values()):
                if message.tx_height is not None:
                    start_height = min(start_height, message.next_height)
            for height in range(start_height, latest_height + 1):
                await self._apply_block_events(height, await self._block_events(height))
                self._last_processed_height = max(self._last_processed_height, height)
            await self._expire_queued_messages()
            await self._verify_restored_orders()

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
                        self._process_fill(resting_order, market, event, f"{height}-{index}-maker", is_taker=False)
                        maker_filled.add(resting_order.client_order_id)
                    if event.get("taker") == self._account_address:
                        message = self._match_create(creates, market_ids, event, self._opposite_side(event.get("order_type")))
                        if message is not None:
                            order = self._order_tracker.fetch_order(message.client_order_id)
                            self._process_fill(order, market, event, f"{height}-{index}-taker", is_taker=True)
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
            if message.next_height <= height:
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

    def _process_fill(self, order: InFlightOrder, market: BeezeeMarket, event: TradebinEvent, trade_id: str, is_taker: bool):
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
                fee=self.get_fee(
                    market.base.symbol, market.quote.symbol, order.order_type, order.trade_type, amount, price,
                    is_maker=not is_taker,
                ),
                is_taker=is_taker,
            )
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

    async def _reconcile_resting_orders(self, height: int):
        data_source = self._build_data_source()
        for order in list(self.in_flight_orders.values()):
            if len(order.exchange_order_id or "") != CONSTANTS.ORDER_ID_LENGTH:
                continue
            market = await data_source.get_market_by_trading_pair(order.trading_pair)
            remaining = Decimal("0")
            if await self._is_on_book(order):
                chain_order = await data_source.get_market_order(
                    market.market_id, self._side(order.trade_type), order.exchange_order_id
                )
                if chain_order is None:
                    continue
                remaining = chain_amount_to_display(chain_order["amount"], market.base)
            missed_fill = order.amount - remaining - order.executed_amount_base
            if missed_fill > Decimal("0"):
                self.logger().warning(
                    f"Beezee order {order.client_order_id} lost {missed_fill} while fills were not read. "
                    f"Recording it as one fill at the order price."
                )
                self._order_tracker.process_trade_update(
                    TradeUpdate(
                        trade_id=f"reconcile-{height}",
                        client_order_id=order.client_order_id,
                        exchange_order_id=order.exchange_order_id,
                        trading_pair=order.trading_pair,
                        fill_timestamp=self.current_timestamp,
                        fill_price=order.price,
                        fill_base_amount=missed_fill,
                        fill_quote_amount=missed_fill * order.price,
                        fee=self.get_fee(
                            market.base.symbol, market.quote.symbol, order.order_type, order.trade_type,
                            missed_fill, order.price, is_maker=True,
                        ),
                        is_taker=False,
                    )
                )
            self._orders_to_verify.add(order.client_order_id)

    async def _is_on_book(self, order: InFlightOrder) -> bool:
        data_source = self._build_data_source()
        market = await data_source.get_market_by_trading_pair(order.trading_pair)
        open_orders = await data_source.get_user_market_orders(market.market_id)
        return any(open_order.get("id") == order.exchange_order_id for open_order in open_orders)

    def _update_order_state(self, order: InFlightOrder, state: OrderState):
        self._order_tracker.process_order_update(
            OrderUpdate(
                trading_pair=order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=state,
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
            )
        )

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
