import asyncio
import time
from collections import defaultdict
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
from hummingbot.connector.exchange.beezee.beezee_signer import BeezeeSigner
from hummingbot.connector.exchange.beezee.beezee_utils import (
    BeezeeConfigMap,
    BeezeeMarket,
    chain_amount_to_display,
    chain_price_to_display,
    display_amount_to_chain,
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
        self._halt_cancel_tasks: Dict[str, asyncio.Task] = {}
        self._creation_tx_hashes: Dict[str, str] = {}
        self._cancel_tx_hashes: Dict[str, str] = {}
        self._pre_create_order_ids: Dict[str, Set[str]] = {}
        self._create_order_specs: Dict[str, Tuple[str, str, str, str]] = {}
        self._processed_trade_ids_by_order: Dict[str, Set[str]] = defaultdict(set)
        self._last_unresolved_order_log_timestamp: Dict[str, float] = {}
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
        existing_orders = await data_source.get_user_market_orders(market.market_id)
        self._pre_create_order_ids[order_id] = {order["id"] for order in existing_orders if "id" in order}
        self._create_order_specs[order_id] = (
            market.market_id,
            side,
            display_amount_to_chain(amount, market.base),
            chain_price,
        )
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
        self._creation_tx_hashes[order_id] = tx_hash
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
            status_update = await self._request_order_status(tracked_order)
            exchange_order_id = status_update.exchange_order_id
        if not exchange_order_id or len(exchange_order_id) != CONSTANTS.ORDER_ID_LENGTH:
            self.logger().debug(
                f"Deferring Beezee cancel until the exchange order id is resolved. client_order_id={order_id}"
            )
            return False

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
        self._cancel_tx_hashes[order_id] = tx_hash
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
        self._cancel_open_orders_on_halted_pairs()

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

    async def _update_orders_fills(self, orders: List[InFlightOrder]):
        grouped_orders: Dict[str, List[InFlightOrder]] = defaultdict(list)
        for order in orders:
            grouped_orders[order.trading_pair].append(order)
        for trading_pair, trading_pair_orders in grouped_orders.items():
            market = await self._build_data_source().get_market_by_trading_pair(trading_pair)
            history = await self._build_data_source().get_market_history(market.market_id, limit=CONSTANTS.DEFAULT_HISTORY_LIMIT)
            trade_updates = self._allocate_trade_updates(trading_pair_orders, market, history)
            for trade_update in trade_updates:
                self._order_tracker.process_trade_update(trade_update)

    def _allocate_trade_updates(self, orders: List[InFlightOrder], market: BeezeeMarket, history: List[Dict[str, Any]]) -> List[TradeUpdate]:
        if self._account_address is None:
            return []
        updates: List[TradeUpdate] = []
        buckets: Dict[Tuple[str, str], List[InFlightOrder]] = defaultdict(list)
        for order in sorted(orders, key=lambda item: (item.creation_timestamp, item.client_order_id)):
            side = CONSTANTS.SIDE_BUY if order.trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
            buckets[(side, format(order.price, "f"))].append(order)
        for (side, display_price), bucket_orders in buckets.items():
            first_creation_timestamp = bucket_orders[0].creation_timestamp
            matching_trades = [
                trade
                for trade in history
                if float(trade["executed_at"]) >= first_creation_timestamp
                and format(chain_price_to_display(trade["price"], market), "f") == display_price
                and (
                    (trade.get("maker") == self._account_address and trade.get("order_type") == side)
                    or (trade.get("taker") == self._account_address and trade.get("order_type") != side)
                )
            ]
            matching_trades.sort(key=lambda trade: (int(trade["executed_at"]), self._build_data_source().fingerprint_history_trade(trade)))
            order_index = 0
            for trade in matching_trades:
                trade_id = self._build_data_source().fingerprint_history_trade(trade)
                remaining_trade_amount = chain_amount_to_display(trade["amount"], market.base)
                while remaining_trade_amount > Decimal("0") and order_index < len(bucket_orders):
                    order = bucket_orders[order_index]
                    synthetic_trade_id = f"{trade_id}-{order.client_order_id}"
                    if synthetic_trade_id in self._processed_trade_ids_by_order[order.client_order_id]:
                        break
                    remaining_order_amount = order.amount - order.executed_amount_base
                    if remaining_order_amount <= Decimal("0"):
                        order_index += 1
                        continue
                    fill_amount = min(remaining_trade_amount, remaining_order_amount)
                    price = chain_price_to_display(trade["price"], market)
                    updates.append(
                        TradeUpdate(
                            trade_id=synthetic_trade_id,
                            client_order_id=order.client_order_id,
                            exchange_order_id=order.exchange_order_id or self._creation_tx_hashes.get(order.client_order_id, order.client_order_id),
                            trading_pair=order.trading_pair,
                            fill_timestamp=float(trade["executed_at"]),
                            fill_price=price,
                            fill_base_amount=fill_amount,
                            fill_quote_amount=fill_amount * price,
                            fee=self.get_fee(market.base.symbol, market.quote.symbol, order.order_type, order.trade_type, fill_amount, price, is_maker=trade.get("maker") == self._account_address),
                            is_taker=trade.get("taker") == self._account_address,
                        )
                    )
                    self._processed_trade_ids_by_order[order.client_order_id].add(synthetic_trade_id)
                    remaining_trade_amount -= fill_amount
                    if fill_amount >= remaining_order_amount:
                        order_index += 1
        return updates

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        market = await self._build_data_source().get_market_by_trading_pair(order.trading_pair)
        history = await self._build_data_source().get_market_history(market.market_id, limit=CONSTANTS.DEFAULT_HISTORY_LIMIT)
        return self._allocate_trade_updates([order], market, history)

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        data_source = self._build_data_source()
        market = await data_source.get_market_by_trading_pair(tracked_order.trading_pair)
        side = CONSTANTS.SIDE_BUY if tracked_order.trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        open_refs = await data_source.get_user_market_orders(market.market_id)

        if tracked_order.exchange_order_id and len(tracked_order.exchange_order_id) == CONSTANTS.ORDER_ID_LENGTH:
            if any(ref["id"] == tracked_order.exchange_order_id for ref in open_refs):
                if tracked_order.is_pending_cancel_confirmation:
                    state = OrderState.PENDING_CANCEL
                else:
                    state = OrderState.PARTIALLY_FILLED if tracked_order.executed_amount_base > Decimal("0") else OrderState.OPEN
                return OrderUpdate(
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=self.current_timestamp,
                    new_state=state,
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=tracked_order.exchange_order_id,
                )

        create_spec = self._create_order_specs.get(tracked_order.client_order_id)
        recovered_after_restart = False
        if create_spec is None and len(tracked_order.exchange_order_id or "") == CONSTANTS.TX_HASH_LENGTH:
            creation_tx = await data_source.get_tx(tracked_order.exchange_order_id)
            tx_response = (creation_tx or {}).get("tx_response")
            creation_tx_code = (
                int(tx_response["code"])
                if isinstance(tx_response, dict) and "code" in tx_response
                else None
            )
            if creation_tx_code == 0:
                create_spec = (
                    market.market_id,
                    side,
                    display_amount_to_chain(tracked_order.amount, market.base),
                    display_price_to_chain(tracked_order.price, market),
                )
                recovered_after_restart = True
        if create_spec is not None:
            expected_market_id, expected_side, expected_amount, expected_price = create_spec
            pre_create_ids = self._pre_create_order_ids.get(tracked_order.client_order_id, set())
            claimed_exchange_ids = {order.exchange_order_id for order in self.in_flight_orders.values() if order.exchange_order_id}
            matching_candidate_ids: List[str] = []
            candidate_ids = data_source.candidate_order_ids(open_refs, expected_market_id, expected_side)
            for candidate_id in candidate_ids:
                if candidate_id in pre_create_ids or candidate_id in claimed_exchange_ids:
                    continue
                candidate_order = await data_source.get_market_order(expected_market_id, expected_side, candidate_id)
                if candidate_order is None:
                    continue
                try:
                    matches = (
                        Decimal(str(candidate_order.get("amount"))) == Decimal(expected_amount)
                        and Decimal(str(candidate_order.get("price"))) == Decimal(expected_price)
                    )
                    if recovered_after_restart:
                        created_at = Decimal(str(candidate_order["created_at"]))
                        order_created_at = Decimal(str(tracked_order.creation_timestamp))
                        matches = (
                            matches
                            and abs(created_at - order_created_at) <= CONSTANTS.ORDER_ID_RECOVERY_TIME_TOLERANCE
                        )
                except Exception:
                    matches = False
                if matches:
                    matching_candidate_ids.append(candidate_id)
            if len(matching_candidate_ids) == 1:
                candidate_id = matching_candidate_ids[0]
                self._pre_create_order_ids.pop(tracked_order.client_order_id, None)
                self._create_order_specs.pop(tracked_order.client_order_id, None)
                if recovered_after_restart:
                    self.logger().info(
                        f"Recovered Beezee exchange order id after restart. client_order_id={tracked_order.client_order_id}, "
                        f"exchange_order_id={candidate_id}"
                    )
                state = OrderState.PARTIALLY_FILLED if tracked_order.executed_amount_base > Decimal("0") else OrderState.OPEN
                return OrderUpdate(
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=self.current_timestamp,
                    new_state=state,
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=candidate_id,
                )
            self._log_unresolved_order_id(
                client_order_id=tracked_order.client_order_id,
                expected_market_id=expected_market_id,
                expected_side=expected_side,
                expected_amount=expected_amount,
                expected_price=expected_price,
                candidate_ids=candidate_ids,
                matching_candidate_ids=matching_candidate_ids,
            )

        cancel_tx_hash = self._cancel_tx_hashes.get(tracked_order.client_order_id)
        creation_tx_hash = self._creation_tx_hashes.get(tracked_order.client_order_id)
        if creation_tx_hash is None and len(tracked_order.exchange_order_id or "") == CONSTANTS.TX_HASH_LENGTH:
            creation_tx_hash = tracked_order.exchange_order_id
        tx_hash = cancel_tx_hash or creation_tx_hash
        tx = await data_source.get_tx(tx_hash) if tx_hash else None
        tx_code = int((tx.get("tx_response") or {}).get("code", 0)) if tx is not None else None
        if tx is not None and tx_hash == creation_tx_hash and tx_code not in (None, 0):
            tx_response = tx.get("tx_response") or {}
            if is_denom_halted_error(tx_code, tx_response.get("codespace"), tx_response.get("raw_log")):
                await self._mark_market_halted_by_chain(market)
        if creation_tx_hash is not None and tx_code not in (None, 0):
            new_state = OrderState.FAILED
        elif cancel_tx_hash is not None and tx_code not in (None, 0):
            new_state = OrderState.PARTIALLY_FILLED if tracked_order.executed_amount_base > Decimal("0") else OrderState.OPEN
        elif tracked_order.executed_amount_base >= tracked_order.amount:
            new_state = OrderState.FILLED
        elif tracked_order.is_pending_cancel_confirmation:
            new_state = OrderState.CANCELED if cancel_tx_hash is not None and tx_code == 0 else OrderState.PENDING_CANCEL
        elif creation_tx_hash is not None and tx_code == 0:
            # A create tx can be confirmed after the order has immediately filled.
            # Preserve the existing lifecycle state until fill polling determines the terminal state.
            new_state = OrderState.OPEN if tracked_order.current_state == OrderState.PENDING_CREATE else tracked_order.current_state
        elif tracked_order.executed_amount_base > Decimal("0"):
            new_state = OrderState.PARTIALLY_FILLED
        else:
            new_state = OrderState.PENDING_CREATE if tx_hash else OrderState.FAILED
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=new_state,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=tracked_order.exchange_order_id or tx_hash,
        )

    def _log_unresolved_order_id(
        self,
        client_order_id: str,
        expected_market_id: str,
        expected_side: str,
        expected_amount: str,
        expected_price: str,
        candidate_ids: List[str],
        matching_candidate_ids: List[str],
    ):
        now = time.time()
        last_log_timestamp = self._last_unresolved_order_log_timestamp.get(client_order_id, 0.0)
        if now - last_log_timestamp >= 30.0:
            self.logger().warning(
                f"Beezee exchange order id is unresolved. client_order_id={client_order_id}, "
                f"market={expected_market_id}, side={expected_side}, amount={expected_amount}, "
                f"price={expected_price}, candidates={candidate_ids}, matches={matching_candidate_ids}"
            )
            self._last_unresolved_order_log_timestamp[client_order_id] = now

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
