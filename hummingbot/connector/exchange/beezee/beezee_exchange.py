import asyncio
import json
import re
import time
from collections import OrderedDict, defaultdict
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS, beezee_web_utils as web_utils
from hummingbot.connector.exchange.beezee.beezee_amm import (
    BeezeePool,
    BeezeePoolTracker,
    BeezeeSwapError,
    BeezeeSwapQuote,
    BeezeeSwapRequest,
    BeezeeSwapResult,
    apply_swap,
    min_output_after_slippage,
    quote_swap as pool_quote_swap,
    swap_fee_amount,
    swap_output,
)
from hummingbot.connector.exchange.beezee.beezee_api_data_source import (
    BeezeeAPIDataSource,
    BeezeeTxRefusedError,
    is_denom_halted_error,
)
from hummingbot.connector.exchange.beezee.beezee_api_order_book_data_source import BeezeeAPIOrderBookDataSource
from hummingbot.connector.exchange.beezee.beezee_api_user_stream_data_source import BeezeeAPIUserStreamDataSource
from hummingbot.connector.exchange.beezee.beezee_auth import BeezeeAuth
from hummingbot.connector.exchange.beezee.beezee_events import TradebinEvent, block_tradebin_events, tx_tradebin_events
from hummingbot.connector.exchange.beezee.beezee_fills import (
    BeezeeFillEntry,
    BeezeeFillError,
    levels_to_take as book_levels_to_take,
    prepare_fill_entries,
    resting_side,
)
from hummingbot.connector.exchange.beezee.beezee_order_batcher import (
    BatchedOrderMessage,
    BeezeeCreateWithdrawnError,
    BeezeeOrderBatcher,
)
from hummingbot.connector.exchange.beezee.beezee_signer import BeezeeSigner
from hummingbot.connector.exchange.beezee.beezee_utils import (
    VENUE_BOOK,
    VENUE_POOL,
    BeezeeConfigMap,
    BeezeeMarket,
    BeezeePair,
    BeezeeVenueError,
    chain_amount_to_display,
    chain_price_to_display,
    display_price_to_chain,
    minimum_order_size_for_price,
    private_key_from_account_mode,
    token_from_metadata,
)
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import get_new_client_order_id
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
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
    # Fills only: the client order id of the first entry of its MsgFillOrders. A fill never rests: it settles once its
    # queue message is processed, filled in part or in full and refunded for the rest.
    fill_group: Optional[str] = None

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


@dataclass
class PendingSwap:
    """A swap sent (or about to be sent) in a MsgMultiSwap, followed until its tx is found.

    Swaps settle inside their tx: the tx result holds one SwapEvent per message, in message order.
    """

    client_order_id: str
    trading_pair: str
    side: str
    pool_id: str
    input_denom: str
    output_denom: str
    # The pool fee rate the swap was quoted with: the SwapEvent carries the full input, not the fee.
    pool_fee: str
    msg_index: int = 0
    tx_hash: Optional[str] = None

    def to_json(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "PendingSwap":
        return cls(**{key: data.get(key) for key in cls.__dataclass_fields__ if key in data})


@dataclass
class PreparedSwap:
    """A swap checked up front, with its message, waiting to be tracked and sent."""

    request: BeezeeSwapRequest
    market: BeezeePair
    quote: BeezeeSwapQuote
    min_output_amount: int
    message: bytes
    pending: PendingSwap


@dataclass
class PreparedFill:
    """A MsgFillOrders checked up front, with its entries' tracked orders, waiting to be sent."""

    market: BeezeeMarket
    client_order_ids: List[str]
    message: bytes


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
        self._pool_tracker: Optional[BeezeePoolTracker] = None
        self._pool_refresh_task: Optional[asyncio.Task] = None
        self._trade_fees: Dict[str, Optional[TokenAmount]] = {}
        # The order-book markets by pair name, and every pair (market, pool or both) with its venues.
        self._markets_by_trading_pair: Dict[str, BeezeeMarket] = {}
        self._pairs_by_trading_pair: Dict[str, BeezeePair] = {}
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
        # Swaps by client order id, from their send until their tx result is read.
        self._pending_swaps: Dict[str, PendingSwap] = {}
        # One task per swap tx: sending it, then reading its result (keyed by the first swap's client order id).
        self._swap_tasks: Dict[str, asyncio.Task] = {}
        self._swap_results: "OrderedDict[str, BeezeeSwapResult]" = OrderedDict()
        self._new_blocks_seen = 0
        # Swaps restored from a saved state, followed again on the next status update.
        self._swaps_to_resume: Set[str] = set()
        # Fill entries' client order id -> the first entry of their MsgFillOrders (see "Taker fills").
        self._fill_groups: Dict[str, str] = {}
        self._fill_tasks: Dict[str, asyncio.Task] = {}
        self._balance_refresh_task: Optional[asyncio.Task] = None
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
        status["pools_initialized"] = self._pools_initialized()
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

    def _pools_initialized(self) -> bool:
        """False until the first pool refresh completes, when a traded pair has a liquidity pool: the pools are read in
        the background on each new block, and the first tick after ready must find them (a script would read a missing
        pool as a pair without one). Pairs without a pool never wait."""
        traded = set(self._trading_pairs or [])
        if not any(pair.has_pool for name, pair in self._pairs_by_trading_pair.items() if name in traded):
            return True
        return self._pool_tracker is not None and self._pool_tracker.loaded

    def gas_fees_paid(self) -> Decimal:
        """Gas fees of every tx this connector broadcast since it started, in the native token (BZE)."""
        return self._build_data_source().gas_fees_paid

    @property
    def new_blocks_seen(self) -> int:
        """How many NewBlock notifications arrived since the connector started. Scripts count blocks with it (e.g. a
        cooldown in blocks); it does not advance while the block websocket is down and the book is polled."""
        return self._new_blocks_seen

    def minimum_order_amount(self, trading_pair: str, price: Decimal) -> Decimal:
        """The chain's minimum order size at `price`, in base display units: ceil(1 / chain price) x 2 base units.
        Zero while the market is not loaded yet (the trading rules load it)."""
        market = self._markets_by_trading_pair.get(trading_pair)
        if market is None:
            return Decimal("0")
        return minimum_order_size_for_price(display_price_to_chain(price, market), market)

    def supported_order_types(self):
        return [OrderType.LIMIT]

    # === Venues ===
    #
    # On chain an order-book market and a liquidity pool are unrelated: a pair can have either or both. Order-book
    # calls (orders, cancels, fills, the book) need the market; pool calls (pool reads, quotes, swaps) need the pool.

    def trading_pair_venues(self, trading_pair: str) -> Tuple[str, ...]:
        """The pair's venues, "book" and/or "pool", in that order. Empty while the pairs are not loaded (the trading
        rules load them) or for an unknown pair. A pool created after the pairs were loaded counts once the pool
        tracker has read it."""
        pair = self._pairs_by_trading_pair.get(trading_pair)
        has_book = pair is not None and pair.has_book
        has_pool = (pair is not None and pair.has_pool) or self._build_pool_tracker().get_pool(trading_pair) is not None
        return tuple(venue for venue, present in ((VENUE_BOOK, has_book), (VENUE_POOL, has_pool)) if present)

    def _is_pool_only(self, trading_pair: str) -> bool:
        return self.trading_pair_venues(trading_pair) == (VENUE_POOL,)

    def _check_book_venue(self, trading_pair: str, action: str):
        """Raises BeezeeVenueError for a known pair with no order-book market, before anything is tracked or sent."""
        pair = self._pairs_by_trading_pair.get(trading_pair)
        if pair is not None and not pair.has_book:
            raise BeezeeVenueError(
                f"Beezee pair {trading_pair} has no order-book market, only a liquidity pool: {action} refused. Use the "
                f"pool calls (quote_swap, amm_swap) on it."
            )

    def buy(self, trading_pair: str, amount: Decimal, order_type=OrderType.LIMIT, price: Decimal = s_decimal_NaN, **kwargs) -> str:
        self._check_book_venue(trading_pair, "a buy order")
        return super().buy(trading_pair, amount, order_type, price, **kwargs)

    def sell(self, trading_pair: str, amount: Decimal, order_type=OrderType.LIMIT, price: Decimal = s_decimal_NaN, **kwargs) -> str:
        self._check_book_venue(trading_pair, "a sell order")
        return super().sell(trading_pair, amount, order_type, price, **kwargs)

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

    def _build_pool_tracker(self) -> BeezeePoolTracker:
        if self._pool_tracker is None:
            self._pool_tracker = BeezeePoolTracker(
                source=self._build_data_source(),
                trading_pairs=lambda: list(self._trading_pairs or []),
                halted_denoms=lambda: self._halted_denoms,
            )
        return self._pool_tracker

    # === Liquidity pools (read only) ===

    def get_pool(self, trading_pair: str) -> Optional[BeezeePool]:
        """The trading pair's liquidity pool as of the last block, with its halted flag, or None."""
        return self._build_pool_tracker().get_pool(trading_pair)

    def get_pool_price(self, trading_pair: str) -> Optional[Decimal]:
        """The pool price in the units of the pair's order prices (quote per base, display units), or None."""
        return self._build_pool_tracker().get_pool_price(trading_pair)

    def get_pool_reserves(self, trading_pair: str) -> Optional[Tuple[Decimal, Decimal]]:
        """The pool's (base, quote) reserves in display units of the pair, as of the last block, or None."""
        return self._build_pool_tracker().get_pool_reserves(trading_pair)

    def quote_swap(self, trading_pair: str, side: str, amount: Decimal) -> BeezeeSwapQuote:
        """What the pool pays for `amount` of the input token: quote on a "buy", base on a "sell"."""
        return self._build_pool_tracker().quote_swap(trading_pair, side, amount)

    def quote_input_for_output(self, trading_pair: str, side: str, amount: Decimal) -> BeezeeSwapQuote:
        """The swap that receives at least `amount` of the output token: base on a "buy", quote on a "sell"."""
        return self._build_pool_tracker().quote_input_for_output(trading_pair, side, amount)

    # === Prices ===
    #
    # The upstream getters answer NaN for an empty side of the book, and the rate oracle (behind `history`, the trade
    # monitor and the fee conversion) raises on an ordered comparison with NaN. These getters never answer NaN:
    # - a pool-only pair (its book is always empty) answers every price type with its pool price (a pool has no
    #   spread);
    # - a pair with an order-book market answers the book price when the book has it (that side, both sides for the
    #   mid, a trade for the last trade price), else its pool price when it has a pool. The pool price is in the
    #   pair's orientation, whichever way the pool is oriented. The mid of a one-sided book is the pool price, never the
    #   one side left: on a thin market that side can be far from the fair price;
    # - None when there is neither. The rate oracle and the performance metrics skip None (they fall back to the trade
    #   price).

    def get_price_by_type(self, trading_pair: str, price_type: PriceType) -> Optional[Decimal]:
        if self._is_pool_only(trading_pair):
            return self.get_pool_price(trading_pair)
        return self._book_or_pool_price(trading_pair, super().get_price_by_type(trading_pair, price_type))

    def get_price(self, trading_pair: str, is_buy: bool) -> Optional[Decimal]:
        if self._is_pool_only(trading_pair):
            return self.get_pool_price(trading_pair)
        return self._book_or_pool_price(trading_pair, super().get_price(trading_pair, is_buy))

    def get_mid_price(self, trading_pair: str) -> Optional[Decimal]:
        return self.get_price_by_type(trading_pair, PriceType.MidPrice)

    def _book_or_pool_price(self, trading_pair: str, book_price: Optional[Decimal]) -> Optional[Decimal]:
        if book_price is not None and not Decimal(book_price).is_nan():
            return book_price
        return self.get_pool_price(trading_pair)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """The pool price of a pool-only pair, else the market's last trade; NaN when there is none."""
        prices = await self._orderbook_ds.get_last_traded_prices([trading_pair])
        return prices[trading_pair]

    # === Pool swaps ===
    #
    # A swap is a MsgMultiSwap on the pair's single pool. It settles inside its tx (no order queue), so it is tracked
    # as a market order that fills at once: PENDING_CREATE until the tx is accepted, OPEN (exchange id = tx hash) until
    # the tx is found, then one taker fill and FILLED, or FAILED with the chain's error. Nothing is retried. Swap txs
    # go through the same simulate/sign/broadcast path as the order batches, which hands out the account sequences
    # one tx at a time.

    def amm_swap(self, trading_pair: str, side: str, amount: Decimal, slippage_pct: Decimal) -> str:
        """Swaps `amount` of the input token (display units: quote on a "buy", base on a "sell") on the pair's pool.

        The chain refuses the swap when it would pay out less than the quote minus `slippage_pct` percent. Returns the
        client order id; the outcome arrives as order events (OrderFilled + completed, or OrderFailure). Raises
        BeezeeSwapError, sending nothing, when the swap is refused up front: no pool, a halted pool, an amount the
        pool cannot swap, a zero output after slippage, or a balance too low.
        """
        return self.amm_swaps([BeezeeSwapRequest(trading_pair, side, Decimal(amount), Decimal(slippage_pct))])[0]

    def amm_swaps(self, swaps: List[BeezeeSwapRequest]) -> List[str]:
        """Several swaps in one tx (one MsgMultiSwap each), executed in list order, e.g. both legs of a round trip.

        Each swap is quoted on the pool as the swaps before it in the list leave it. Returns one client order id per
        swap. Refused up front as a whole (BeezeeSwapError, nothing sent) when any swap is; on chain the tx fails as a
        whole too, so every swap of it fails with the same error.
        """
        if self._signer is None:
            raise BeezeeSwapError("Beezee wallet mode is required for swaps.")
        if not swaps:
            return []
        prepared = self._prepare_swaps(swaps)
        client_order_ids = self._track_swaps(prepared)
        key = client_order_ids[0]
        self._swap_tasks[key] = safe_ensure_future(self._send_swaps(key, prepared))
        return client_order_ids

    def _track_swaps(self, prepared: List[PreparedSwap]) -> List[str]:
        for swap in prepared:
            base_amount = swap.quote.output if swap.request.side == CONSTANTS.SIDE_BUY else swap.quote.input
            self.start_tracking_order(
                order_id=swap.pending.client_order_id,
                exchange_order_id=None,
                trading_pair=swap.request.trading_pair,
                trade_type=TradeType.BUY if swap.request.side == CONSTANTS.SIDE_BUY else TradeType.SELL,
                price=swap.quote.effective_price,
                amount=base_amount,
                order_type=OrderType.MARKET,
            )
            self._pending_swaps[swap.pending.client_order_id] = swap.pending
        return [swap.pending.client_order_id for swap in prepared]

    def get_swap_result(self, client_order_id: str) -> Optional[BeezeeSwapResult]:
        """What a settled swap put in and got out (one of the last SWAP_RESULTS_KEPT), or None."""
        return self._swap_results.get(client_order_id)

    def _prepare_swaps(self, requests: List[BeezeeSwapRequest]) -> List[PreparedSwap]:
        tracker = self._build_pool_tracker()
        data_source = self._build_data_source()
        # Pools as the swaps before in the list leave them, and the balances they leave (display units by symbol).
        pools: Dict[str, BeezeePool] = {}
        balances: Dict[str, Decimal] = {}
        taker_fee = self._trade_fees.get("taker")
        prepared: List[PreparedSwap] = []
        for index, request in enumerate(requests):
            if request.side not in (CONSTANTS.SIDE_BUY, CONSTANTS.SIDE_SELL):
                raise BeezeeSwapError(f"side must be '{CONSTANTS.SIDE_BUY}' or '{CONSTANTS.SIDE_SELL}', got {request.side!r}")
            market = tracker.pair(request.trading_pair)
            pool = tracker.get_pool(request.trading_pair)
            if market is None or pool is None:
                raise BeezeeSwapError(f"no liquidity pool for {request.trading_pair}")
            halt_reason = self._halt_reason(market)
            if pool.halted or halt_reason is not None:
                raise BeezeeSwapError(f"pool {pool.pool_id} is halted ({halt_reason or 'halted denom'})")
            pool = pools.get(pool.pool_id, pool)
            quote = pool_quote_swap(pool, market, request.side, request.amount)
            if not quote.ok:
                raise BeezeeSwapError(f"{request.trading_pair} {request.side} of {request.amount}: {quote.error}")
            min_output_amount = min_output_after_slippage(quote.output_amount, request.slippage_pct)
            if min_output_amount <= 0:
                raise BeezeeSwapError(
                    f"{request.trading_pair} {request.side} of {request.amount}: the output after "
                    f"{request.slippage_pct}% slippage rounds to 0"
                )
            input_token, output_token = (market.quote, market.base) if request.side == CONSTANTS.SIDE_BUY else (market.base, market.quote)
            for symbol in (input_token.symbol, output_token.symbol) + ((taker_fee.token,) if taker_fee else ()):
                balances.setdefault(symbol, self.get_available_balance(symbol))
            balances[input_token.symbol] -= quote.input
            if taker_fee is not None:
                balances[taker_fee.token] -= taker_fee.amount
            spent = dict.fromkeys([input_token.symbol] + ([taker_fee.token] if taker_fee else []))
            short = [symbol for symbol in spent if balances[symbol] < 0]
            if short:
                needed = f"{quote.input} {input_token.symbol}" + (f" + {taker_fee.amount} {taker_fee.token} taker fee" if taker_fee else "")
                raise BeezeeSwapError(
                    f"{request.trading_pair} {request.side}: balance too low for {needed} "
                    f"(short of {', '.join(f'{-balances[symbol]} {symbol}' for symbol in short)})"
                )
            # A later swap may spend this one's output, but only what the chain guarantees.
            balances[output_token.symbol] += chain_amount_to_display(min_output_amount, output_token)
            pools[pool.pool_id] = apply_swap(pool, swap_output(pool, quote.input_denom, quote.input_amount))
            client_order_id = get_new_client_order_id(
                is_buy=request.side == CONSTANTS.SIDE_BUY,
                trading_pair=request.trading_pair,
                hbot_order_id_prefix=self.client_order_id_prefix,
                max_id_len=self.client_order_id_max_length,
            )
            prepared.append(PreparedSwap(
                request=request,
                market=market,
                quote=quote,
                min_output_amount=min_output_amount,
                message=data_source.multi_swap_message(
                    pool.pool_id, quote.input_denom, quote.input_amount, quote.output_denom, min_output_amount
                ),
                pending=PendingSwap(
                    client_order_id=client_order_id,
                    trading_pair=request.trading_pair,
                    side=request.side,
                    pool_id=pool.pool_id,
                    input_denom=quote.input_denom,
                    output_denom=quote.output_denom,
                    pool_fee=format(pool.fee, "f"),
                    msg_index=index,
                ),
            ))
        return prepared

    async def _send_swaps(self, key: str, swaps: List[PreparedSwap], fill: Optional[PreparedFill] = None):
        """Sends the swaps in one tx, after the fill's MsgFillOrders when there is one (fill_orders_and_swap)."""
        try:
            if fill is not None:
                memo = f"{CONSTANTS.FILL_SWAP_MEMO}:{fill.client_order_ids[0]}"
            else:
                memo = f"hbot-swap:{swaps[0].pending.client_order_id}" if len(swaps) == 1 else CONSTANTS.SWAP_BATCH_MEMO
            for swap in swaps:
                self.logger().info(
                    f"Beezee swap {swap.pending.client_order_id}: {swap.request.side} {swap.quote.input} "
                    f"{swap.quote.input_denom} on pool {swap.pending.pool_id}, quoted {swap.quote.output}, "
                    f"min_output {swap.min_output_amount} {swap.quote.output_denom}"
                )
            try:
                response = await self._build_data_source().simulate_sign_and_broadcast(
                    ([fill.message] if fill is not None else []) + [swap.message for swap in swaps], memo=memo
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if isinstance(error, BeezeeTxRefusedError):
                    if error.is_denom_halted:
                        markets = {self._halt_key(swap.market): swap.market for swap in swaps}
                        if fill is not None:
                            markets[fill.market.market_id] = fill.market
                        for market in markets.values():
                            await self._mark_market_halted_by_chain(market)
                    message = error.chain_message
                else:
                    message = str(error)
                for client_order_id in fill.client_order_ids if fill is not None else []:
                    self._fail_tracked_order(client_order_id, f"Beezee fill failed: {message}", error, "BeezeeFillFailed")
                for swap in swaps:
                    self._fail_swap(swap.pending.client_order_id, f"Beezee swap failed: {message}", error)
                return
            tx_hash = response["tx_response"]["txhash"]
            if fill is not None:
                # The fill's entries are the tx's first OrderCreateMessageEvents: the swaps emit none.
                self._register_fill_messages(fill.client_order_ids, tx_hash, first_index=0)
            self.logger().info(f"Beezee swap tx {tx_hash} submitted with {len(swaps)} swap(s).")
            for swap in swaps:
                swap.pending.tx_hash = tx_hash
                order = self._order_tracker.fetch_tracked_order(swap.pending.client_order_id)
                if order is not None:
                    order.update_exchange_order_id(tx_hash)
                    self._update_order_state(order, OrderState.OPEN)
            await self._follow_swap_tx(tx_hash)
        finally:
            if self._swap_tasks.get(key) is asyncio.current_task():
                del self._swap_tasks[key]

    async def _follow_swap_tx(self, tx_hash: str):
        for delay in CONSTANTS.TX_LOOKUP_DELAYS:
            await self._sleep(delay)
            if not self._swaps_of_tx(tx_hash):
                return
            try:
                tx = await self._fetch_tx(tx_hash)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.logger().debug(f"Beezee swap tx {tx_hash} lookup failed, retrying: {error}")
                continue
            tx_response = (tx or {}).get("tx_response")
            if tx_response:
                await self._settle_swaps(tx_hash, tx_response)
                return
        waited = int(sum(CONSTANTS.TX_LOOKUP_DELAYS))
        for swap in self._swaps_of_tx(tx_hash):
            self._fail_swap(
                swap.client_order_id,
                f"Beezee swap tx {tx_hash} was not found within {waited} s. It may still land: the balances are read "
                f"again from the chain.",
            )
        safe_ensure_future(self._update_balances())

    def _swaps_of_tx(self, tx_hash: str) -> List[PendingSwap]:
        return sorted(
            (swap for swap in self._pending_swaps.values() if swap.tx_hash == tx_hash), key=lambda swap: swap.msg_index
        )

    async def _settle_swaps(self, tx_hash: str, tx_response: Dict[str, Any]):
        swaps = self._swaps_of_tx(tx_hash)
        code = int(tx_response.get("code", 0))
        if code != 0:
            raw_log = tx_response.get("raw_log") or ""
            if is_denom_halted_error(code, tx_response.get("codespace"), raw_log):
                data_source = self._build_data_source()
                for trading_pair in {swap.trading_pair for swap in swaps}:
                    await self._mark_market_halted_by_chain(await data_source.get_pair(trading_pair))
            for swap in swaps:
                self._fail_swap(swap.client_order_id, f"Beezee swap tx {tx_hash} failed with code {code}: {raw_log}")
            return
        swap_events = [
            event for event in tx_tradebin_events(tx_response)
            if event.type == CONSTANTS.SWAP_EVENT and event.get("creator") == self._account_address
        ]
        for swap in swaps:
            event = swap_events[swap.msg_index] if swap.msg_index < len(swap_events) else None
            coins = self._swap_event_coins(event) if event is not None else None
            if coins is None or event.get("pool_id") != swap.pool_id or coins[0][0] != swap.input_denom or coins[1][0] != swap.output_denom:
                self._fail_swap(
                    swap.client_order_id,
                    f"Beezee swap tx {tx_hash} has no matching SwapEvent for message {swap.msg_index}. The balances "
                    f"are read again from the chain.",
                )
                safe_ensure_future(self._update_balances())
                continue
            await self._process_swap_fill(swap, tx_hash, coins[0][1], coins[1][1])
        # The pool kept part of the fee, so its new reserves are read from the chain, never predicted.
        await self._refresh_pools()

    @staticmethod
    def _swap_event_coins(event: TradebinEvent) -> Optional[Tuple[Tuple[str, int], Tuple[str, int]]]:
        """((input denom, amount), (output denom, amount)) of a SwapEvent, or None when it cannot be read."""
        try:
            coins = tuple(json.loads(event.get(key)) for key in ("in", "out"))
            return tuple((coin["denom"], int(coin["amount"])) for coin in coins)
        except (TypeError, ValueError, KeyError):
            return None

    async def _process_swap_fill(self, swap: PendingSwap, tx_hash: str, input_amount: int, output_amount: int):
        order = self._order_tracker.fetch_tracked_order(swap.client_order_id)
        self._pending_swaps.pop(swap.client_order_id, None)
        market = await self._build_data_source().get_pair(swap.trading_pair)
        is_buy = swap.side == CONSTANTS.SIDE_BUY
        input_token, output_token = (market.quote, market.base) if is_buy else (market.base, market.quote)
        pool_fee_amount = swap_fee_amount(Decimal(swap.pool_fee), input_amount)
        # Reported before the pool fee, which is booked as a fee in the input token: what the pool swapped is the
        # input after its fee, so the price is the pool's own and the fee is not counted twice.
        swapped = chain_amount_to_display(input_amount - pool_fee_amount, input_token)
        received = chain_amount_to_display(output_amount, output_token)
        base_amount, quote_amount = (received, swapped) if is_buy else (swapped, received)
        price = quote_amount / base_amount
        pool_fee = chain_amount_to_display(pool_fee_amount, input_token)
        taker_fee = self._trade_fees.get("taker")
        flat_fees = ([TokenAmount(input_token.symbol, pool_fee)] if pool_fee_amount > 0 else []) + ([taker_fee] if taker_fee else [])
        result = BeezeeSwapResult(
            client_order_id=swap.client_order_id, trading_pair=swap.trading_pair, side=swap.side, tx_hash=tx_hash,
            input_denom=swap.input_denom, output_denom=swap.output_denom,
            input=chain_amount_to_display(input_amount, input_token), output=received, pool_fee=pool_fee, price=price,
            input_amount=input_amount, output_amount=output_amount, pool_fee_amount=pool_fee_amount,
        )
        self._swap_results[swap.client_order_id] = result
        while len(self._swap_results) > CONSTANTS.SWAP_RESULTS_KEPT:
            self._swap_results.popitem(last=False)
        # The swap's effect on the balances is known exactly from its event; the next balance poll confirms it.
        for balances in (self._account_balances, self._account_available_balances):
            balances[input_token.symbol] = balances.get(input_token.symbol, Decimal("0")) - result.input
            balances[output_token.symbol] = balances.get(output_token.symbol, Decimal("0")) + received
            if taker_fee is not None:
                balances[taker_fee.token] = balances.get(taker_fee.token, Decimal("0")) - taker_fee.amount
        self.logger().info(
            f"Beezee swap {swap.client_order_id} settled in tx {tx_hash}: {result.input} {input_token.symbol} -> "
            f"{received} {output_token.symbol} at {price} (pool fee {pool_fee} {input_token.symbol})."
        )
        if order is None:
            return
        # The base amount is only known now on a buy (and is the input after the pool fee on a sell).
        order.amount = base_amount
        self._order_tracker.process_trade_update(
            TradeUpdate(
                trade_id=f"{tx_hash}-{swap.msg_index}",
                client_order_id=swap.client_order_id,
                exchange_order_id=tx_hash,
                trading_pair=swap.trading_pair,
                fill_timestamp=self.current_timestamp,
                fill_price=price,
                fill_base_amount=base_amount,
                fill_quote_amount=quote_amount,
                fee=AddedToCostTradeFee(flat_fees=flat_fees),
                is_taker=True,
            )
        )
        self._update_order_state(order, OrderState.FILLED)

    def _fail_swap(self, client_order_id: str, message: str, error: Optional[Exception] = None):
        self._pending_swaps.pop(client_order_id, None)
        self.logger().warning(f"Beezee swap {client_order_id} failed: {message}")
        self._fail_tracked_order(client_order_id, message, error, "BeezeeSwapFailed")

    def _fail_tracked_order(self, client_order_id: str, message: str, error: Optional[Exception], error_type: str):
        order = self._order_tracker.fetch_tracked_order(client_order_id)
        if order is None:
            return
        update_task = self._order_tracker.process_order_update(
            OrderUpdate(
                trading_pair=order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=OrderState.FAILED,
                client_order_id=client_order_id,
                exchange_order_id=order.exchange_order_id,
                misc_updates={
                    "error_message": message,
                    "error_type": error.__class__.__name__ if error is not None else error_type,
                },
            )
        )
        update_task.add_done_callback(self._report_order_progress)

    def _resume_swaps(self):
        """Follows the swaps restored from a saved state again: those with a tx hash from their tx; the others were
        stopped before their tx was accepted, so whether they were sent is unknown and they fail."""
        for client_order_id in list(self._swaps_to_resume):
            self._swaps_to_resume.discard(client_order_id)
            swap = self._pending_swaps.get(client_order_id)
            if swap is None:
                continue
            if not swap.tx_hash:
                self._fail_swap(
                    client_order_id,
                    "the connector stopped before its tx was accepted, so whether it was sent is unknown. The balances "
                    "are read again from the chain.",
                )
                continue
            if swap.tx_hash not in self._swap_tasks:
                self._swap_tasks[swap.tx_hash] = safe_ensure_future(self._resume_swap_tx(swap.tx_hash))

    async def _resume_swap_tx(self, tx_hash: str):
        try:
            await self._follow_swap_tx(tx_hash)
        finally:
            if self._swap_tasks.get(tx_hash) is asyncio.current_task():
                del self._swap_tasks[tx_hash]

    def _schedule_pool_refresh(self):
        if not self._trading_pairs or (self._pool_refresh_task is not None and not self._pool_refresh_task.done()):
            return
        self._pool_refresh_task = safe_ensure_future(self._refresh_pools())

    async def _refresh_pools(self):
        try:
            await self._update_halted_denoms_if_due()
            await self._build_pool_tracker().refresh()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.logger().warning(f"Failed to refresh Beezee liquidity pools. Keeping the last known reserves. Error: {error}")

    # === Taker fills ===
    #
    # A MsgFillOrders takes resting orders at exactly the given prices and never leaves an order resting. Each entry is
    # tracked as an in-flight LIMIT order of its own (a taker, exchange id = its tx hash) and settled by its queue
    # message id like a create (see "Order tracking from chain events"): its OrderExecutedEvents are its fills, and
    # once its message is processed, the rest was refunded. It then ends FILLED, or CANCELED with the fills it got.
    # The taker fee is paid once per message, so it is booked on the first fill of any of its entries.

    def levels_to_take(
        self, trading_pair: str, side: str, max_amount: Decimal, worst_price: Decimal
    ) -> List[Tuple[Decimal, Decimal]]:
        """(price, base amount) levels, display units and best first, to take from the last order book read for
        `side` ("buy" takes asks, "sell" takes bids): at most `max_amount` base, no price worse than `worst_price`.
        Ready to pass to `fill_orders`. Empty while no book was read yet."""
        market = self._fill_market(trading_pair)
        book = self._orderbook_ds.chain_levels(trading_pair) or {}
        levels = book.get("asks" if side == CONSTANTS.SIDE_BUY else "bids") or []
        try:
            return book_levels_to_take(levels, market, side, Decimal(max_amount), Decimal(worst_price))
        except ArithmeticError as error:
            raise BeezeeFillError(f"invalid max_amount {max_amount} or worst_price {worst_price}: {error}")

    def fill_orders(self, trading_pair: str, side: str, levels: Sequence[Tuple[Decimal, Decimal]]) -> List[str]:
        """Takes resting orders at exactly each (price, base amount) level, in display units, in one MsgFillOrders.

        `side` is ours: "buy" takes resting sells, "sell" takes resting buys. Levels below the chain minimum amount at
        their price are dropped (logged). Returns one client order id per entry sent, in entry order. Each entry
        settles as a taker order: an OrderFilled per match, then completed, or cancelled for the part the chain
        refunded. Nothing ever rests. The message goes through the order batcher, so it may share a tx with creates
        and cancels. Raises BeezeeFillError, sending nothing, when the fill is refused up front: no wallet, an unknown
        pair, a halted market, an invalid level, more than 50 entries, or every level below the minimum.
        """
        fill = self._prepare_fill(trading_pair, side, levels)
        key = fill.client_order_ids[0]
        item = BatchedOrderMessage(
            client_order_id=key,
            is_cancel=False,
            message=fill.message,
            market=fill.market,
            memo=f"{CONSTANTS.FILL_MEMO}:{key}",
            fill_client_order_ids=fill.client_order_ids,
        )
        self._fill_tasks[key] = safe_ensure_future(self._submit_fill(item))
        return fill.client_order_ids

    def fill_orders_and_swap(
        self,
        trading_pair: str,
        side: str,
        levels: Sequence[Tuple[Decimal, Decimal]],
        swaps: Sequence[BeezeeSwapRequest],
    ) -> Tuple[List[str], List[str]]:
        """`fill_orders` and `amm_swaps` in ONE tx: the MsgFillOrders first, then one MsgMultiSwap per swap.

        The tx succeeds or fails as a whole. The swaps settle in the tx itself; the fill settles in its EndBlock and
        may be refunded in part, which the swaps do not follow. Not batched with other messages. Returns (fill client
        order ids, swap client order ids). Raises BeezeeFillError or BeezeeSwapError, sending nothing.
        """
        if not swaps:
            raise BeezeeFillError("no swaps given: use fill_orders")
        if self._signer is None:
            raise BeezeeFillError("Beezee wallet mode is required for fills.")
        # Both are checked before anything is tracked, so a refusal leaves no order behind.
        market, entries, message = self._check_fill(trading_pair, side, levels)
        prepared = self._prepare_swaps(list(swaps))
        fill = PreparedFill(market=market, client_order_ids=self._track_fill(market, side, entries), message=message)
        swap_ids = self._track_swaps(prepared)
        key = fill.client_order_ids[0]
        self._swap_tasks[key] = safe_ensure_future(self._send_swaps(key, prepared, fill=fill))
        return fill.client_order_ids, swap_ids

    def _fill_market(self, trading_pair: str) -> BeezeeMarket:
        market = self._markets_by_trading_pair.get(trading_pair)
        if market is None:
            if trading_pair in self._pairs_by_trading_pair:
                raise BeezeeFillError(
                    f"Beezee pair {trading_pair} has no order-book market, only a liquidity pool: there is no book to "
                    f"take orders from"
                )
            raise BeezeeFillError(f"unknown Beezee trading pair {trading_pair}")
        return market

    def _check_fill(
        self, trading_pair: str, side: str, levels: Sequence[Tuple[Decimal, Decimal]]
    ) -> Tuple[BeezeeMarket, List[BeezeeFillEntry], bytes]:
        if self._signer is None:
            raise BeezeeFillError("Beezee wallet mode is required for fills.")
        market = self._fill_market(trading_pair)
        halt_reason = self._halt_reason(market)
        if halt_reason is not None:
            raise BeezeeFillError(f"Beezee market {market.market_id} is halted ({halt_reason})")
        entries, dropped = prepare_fill_entries(market, side, levels)
        for price, amount in dropped:
            self.logger().info(
                f"Beezee fill on {trading_pair}: dropping the level {amount.normalize():f} at {price.normalize():f}, "
                f"below the chain minimum amount."
            )
        message = self._build_data_source().fill_orders_message(
            market, resting_side(side), [(entry.chain_price, entry.chain_amount) for entry in entries]
        )
        return market, entries, message

    def _prepare_fill(self, trading_pair: str, side: str, levels: Sequence[Tuple[Decimal, Decimal]]) -> PreparedFill:
        market, entries, message = self._check_fill(trading_pair, side, levels)
        return PreparedFill(market=market, client_order_ids=self._track_fill(market, side, entries), message=message)

    def _track_fill(self, market: BeezeeMarket, side: str, entries: List[BeezeeFillEntry]) -> List[str]:
        # Groups whose entries have all settled are no longer needed for the fee or for cancels.
        active_groups = {group for client_order_id, group in self._fill_groups.items() if client_order_id in self.in_flight_orders}
        self._fill_groups = {key: group for key, group in self._fill_groups.items() if group in active_groups}
        is_buy = side == CONSTANTS.SIDE_BUY
        client_order_ids: List[str] = []
        for entry in entries:
            client_order_id = get_new_client_order_id(
                is_buy=is_buy,
                trading_pair=market.trading_pair,
                hbot_order_id_prefix=self.client_order_id_prefix,
                max_id_len=self.client_order_id_max_length,
            )
            self.start_tracking_order(
                order_id=client_order_id,
                exchange_order_id=None,
                trading_pair=market.trading_pair,
                trade_type=TradeType.BUY if is_buy else TradeType.SELL,
                price=entry.price,
                amount=entry.amount,
                order_type=OrderType.LIMIT,
            )
            client_order_ids.append(client_order_id)
        for client_order_id in client_order_ids:
            self._fill_groups[client_order_id] = client_order_ids[0]
        self.logger().info(
            f"Beezee fill {client_order_ids[0]}: {side} on {market.trading_pair} taking "
            + ", ".join(f"{entry.amount.normalize():f} at {entry.price.normalize():f}" for entry in entries)
        )
        return client_order_ids

    async def _submit_fill(self, item: BatchedOrderMessage):
        try:
            await self._order_batcher.submit(item)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            for client_order_id in item.fill_client_order_ids:
                self._fail_tracked_order(client_order_id, str(error), error, "BeezeeFillFailed")
        finally:
            if self._fill_tasks.get(item.client_order_id) is asyncio.current_task():
                del self._fill_tasks[item.client_order_id]

    def _register_fill_messages(self, client_order_ids: List[str], tx_hash: str, first_index: int):
        """The fill's entries are in the tx: each is followed by its OrderCreateMessageEvent, the i-th entry being the
        tx's (first_index + i)-th one of this account."""
        for offset, client_order_id in enumerate(client_order_ids):
            order = self._order_tracker.fetch_tracked_order(client_order_id)
            if order is not None:
                order.update_exchange_order_id(tx_hash)
                self._update_order_state(order, OrderState.OPEN)
            self._queue_message(
                QueuedOrderMessage(
                    client_order_id=client_order_id, tx_hash=tx_hash, msg_index=first_index + offset,
                    fill_group=client_order_ids[0],
                ),
                is_cancel=False,
            )
        self.logger().info(
            f"Beezee fill transaction submitted. client_order_ids={', '.join(client_order_ids)}, tx_hash={tx_hash}"
        )

    def _is_fill(self, client_order_id: str) -> bool:
        return client_order_id in self._fill_groups

    def _refresh_balances_soon(self):
        """Re-reads the balances once: a settled fill got back the escrow it did not spend."""
        if self._balance_refresh_task is None or self._balance_refresh_task.done():
            self._balance_refresh_task = safe_ensure_future(self._update_balances())

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(throttler=self._throttler, auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return BeezeeAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs or [],
            data_source=self._build_data_source(),
            api_factory=self._web_assistants_factory,
            websocket_endpoint=self._websocket_endpoint,
            block_listener=self._on_new_block,
            pool_price_provider=self.get_pool_price,
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
        if order_id in self._pending_swaps:
            self.logger().info(f"Beezee swap {order_id} settles in its own tx and cannot be cancelled.")
            return False
        if self._is_fill(order_id):
            self.logger().info(f"Beezee fill {order_id} settles in EndBlock and cannot be cancelled.")
            return False
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
        for order in orders_to_create:
            self._check_book_venue(order.trading_pair, "an order")
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
            if item.fill_client_order_ids:
                self._register_fill_messages(item.fill_client_order_ids, tx_hash, first_index=create_index)
                create_index += len(item.fill_client_order_ids)
                item.resolve(tx_hash)
                continue
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
        # A fill does not scan the queue (its extra gas grows with its own entries, which the simulation counts).
        return sum(
            queued_ahead[item.market.trading_pair] * data_source.queued_message_gas(item.market.market_id)
            for item in items
            if not item.is_cancel and not item.fill_client_order_ids and item.market.trading_pair in queued_ahead
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
            kind = "cancel" if item.is_cancel else "fill" if item.fill_client_order_ids else "order create"
            rejection = IOError(f"Beezee {kind} failed: {message}")
            rejection.__cause__ = error
            item.reject(rejection)

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """One rule per pair. A market's pair gets the order-book rules. A pool-only pair has no order-book rules: its
        rule only carries the tokens' amount steps (limit orders unsupported); what a swap can take comes from the
        pool math (quote_swap refuses an amount too small to swap)."""
        data_source = self._build_data_source()
        pairs = await data_source.get_all_pairs(refresh=True)
        self._pairs_by_trading_pair = dict(pairs)
        self._markets_by_trading_pair = {name: pair.market for name, pair in pairs.items() if pair.market is not None}
        self._trade_fees = await data_source.get_trade_fees()
        return [
            TradingRule(
                trading_pair=name,
                min_order_size=pair.base.quantum,
                min_price_increment=pair.min_price_increment,
                min_base_amount_increment=pair.base.quantum,
                min_quote_amount_increment=pair.quote.quantum,
                min_notional_size=pair.quote.quantum * Decimal("2") if pair.has_book else Decimal("0"),
                buy_order_collateral_token=pair.quote.symbol,
                sell_order_collateral_token=pair.base.symbol,
                supports_limit_orders=pair.has_book,
                supports_market_orders=False,
            )
            for name, pair in pairs.items()
        ]

    async def _update_trading_fees(self):
        self._trade_fees = await self._build_data_source().get_trade_fees()

    async def stop_network(self):
        await super().stop_network()
        # Tx lookups and halt cancels are restarted from the saved state by the next status update.
        self._order_batcher.stop()
        if self._pool_refresh_task is not None:
            self._pool_refresh_task.cancel()
            self._pool_refresh_task = None
        tasks = list(self._tx_lookup_tasks.values()) + list(self._halt_cancel_tasks.values())
        tasks += list(self._swap_tasks.values())
        if self._balance_refresh_task is not None:
            tasks.append(self._balance_refresh_task)
            self._balance_refresh_task = None
        for task in tasks + list(self._tx_fetch_tasks.values()) + list(self._deferred_cancel_tasks.values()):
            task.cancel()
        self._swap_tasks.clear()
        # The swaps not settled yet are followed again from their tx, like after a restart.
        self._swaps_to_resume.update(self._pending_swaps)
        self._tx_lookup_tasks.clear()
        self._halt_cancel_tasks.clear()
        self._deferred_cancel_tasks.clear()
        self._tx_fetch_tasks.clear()

    async def _status_polling_loop_fetch_updates(self):
        await super()._status_polling_loop_fetch_updates()
        await self._update_halted_denoms_if_due()
        self._cancel_open_orders_on_halted_pairs()
        self._retry_deferred_cancels()
        self._resume_swaps()

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

    async def _mark_market_halted_by_chain(self, market: Union[BeezeeMarket, BeezeePair]):
        # The refusal does not name the denom: re-read the halted set, and if the node still does not list
        # this market's denoms, treat the market itself as halted until the next refresh.
        try:
            await self._read_halted_denoms()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.logger().warning(f"Failed to re-read Beezee halted denoms after a halted refusal. Error: {error}")
        if self._halt_reason(market) is None:
            self._refused_halted_market_ids.add(self._halt_key(market))
        await self._apply_halt_transitions()

    @staticmethod
    def _halt_key(market: Union[BeezeeMarket, BeezeePair]) -> str:
        """A market's id, or for a pool-only pair its pool id: what a chain refusal marks halted."""
        return market.market_id or market.pool_id

    def _halt_reason(self, market: Union[BeezeeMarket, BeezeePair]) -> Optional[str]:
        for denom in (market.base.denom, market.quote.denom):
            if denom in self._halted_denoms:
                return f"denom {denom}"
        if self._halt_key(market) in self._refused_halted_market_ids:
            return "denom unknown, refused by the chain"
        return None

    async def _apply_halt_transitions(self):
        data_source = self._build_data_source()
        trading_pairs = set(self._trading_pairs or []) | {order.trading_pair for order in self.in_flight_orders.values()}
        halted_now: Dict[str, str] = {}
        for trading_pair in trading_pairs:
            try:
                market = await data_source.get_market_by_trading_pair(trading_pair)
            except BeezeeVenueError:
                # A pool-only pair: its pool's denoms decide.
                market = await data_source.get_pair(trading_pair)
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
            if (
                order.trading_pair not in self._halted_trading_pairs
                or order.is_done
                or order.is_pending_cancel_confirmation
                or order.client_order_id in self._pending_swaps
                or self._is_fill(order.client_order_id)
            ):
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
            "swaps": {client_order_id: swap.to_json() for client_order_id, swap in self._pending_swaps.items()},
            "fills": {
                client_order_id: group for client_order_id, group in self._fill_groups.items()
                if client_order_id in self.in_flight_orders
            },
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
        saved_swaps = block_state.get("swaps") or {}
        self._fill_groups = {
            client_order_id: group for client_order_id, group in (block_state.get("fills") or {}).items()
            if client_order_id in self.in_flight_orders
        }
        for order in self.in_flight_orders.values():
            client_order_id = order.client_order_id
            exchange_order_id = order.exchange_order_id or ""
            if client_order_id in saved_swaps:
                # A swap settles in its tx: its outcome is read from that tx, never from the order book.
                self._pending_swaps[client_order_id] = PendingSwap.from_json(saved_swaps[client_order_id])
                self._swaps_to_resume.add(client_order_id)
            elif client_order_id in saved_creates:
                # Created but not yet saved by the chain: resume from the create tx.
                self._queued_creates[client_order_id] = QueuedOrderMessage.from_json(saved_creates[client_order_id])
            elif len(exchange_order_id) == CONSTANTS.TX_HASH_LENGTH:
                # A state saved without the tx details: the create message is matched by its values.
                self._queued_creates[client_order_id] = QueuedOrderMessage(
                    client_order_id=client_order_id, tx_hash=exchange_order_id,
                    fill_group=self._fill_groups.get(client_order_id),
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
        self._new_blocks_seen += 1
        self._schedule_pool_refresh()
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
                elif event.type == CONSTANTS.ORDER_MESSAGE_REFUNDED_EVENT and event.get("owner") == self._account_address:
                    # Chain v8.3.0+: the message was processed and this part of it returned, so nothing (more) rests.
                    message = self._match_create(creates, market_ids, event, event.get("order_type"))
                    if message is not None:
                        self.logger().info(
                            f"Beezee order {message.client_order_id}: the chain refunded {event.get('amount')} base "
                            f"units ({event.get('reason')}) at height {height}. message_id={message.message_id}"
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
                if message.fill_group and self._queued_creates.get(message.client_order_id) is message:
                    # A fill never rests: processed means settled, with the fills read so far.
                    self._settle_order(message.client_order_id, height, {message.client_order_id: None}, set())
                elif self._queued_creates.get(message.client_order_id) is message:
                    await self._finish_create_without_outcome(message, f"its message id was reused at height {height}")
                elif self._queued_cancels.get(message.client_order_id) is message:
                    await self._finish_cancel_without_outcome(message, f"its message id was reused at height {height}")
        await self._settle_processed_fills(height, creates)
        for message in creates + cancels:
            if message.next_height == height:
                message.next_height = height + 1

    async def _settle_processed_fills(self, height: int, creates: List[QueuedOrderMessage]):
        """Settles the fills whose queue message is gone from the state committed at `height`: processed, with no
        outcome event left to come. Chain v8.2.0 emits nothing for the refunded part of a fill, so a fill matching
        nothing has no event at all; v8.3.0 emits OrderMessageRefundedEvent and settles before this is needed."""
        data_source = self._build_data_source()
        for message in creates:
            if not message.fill_group or self._queued_creates.get(message.client_order_id) is not message:
                continue
            order = self._order_tracker.fetch_tracked_order(message.client_order_id)
            if order is None:
                continue
            try:
                market = await data_source.get_market_by_trading_pair(order.trading_pair)
                pending = await data_source.is_queue_message_pending(market.market_id, message.message_id, height)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.logger().debug(f"Beezee queue read for fill {message.client_order_id} at {height} failed: {error}")
                continue
            if pending is False:
                self._settle_order(message.client_order_id, height, {message.client_order_id: None}, set())

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
        if create is not None and create.fill_group:
            self._refresh_balances_soon()
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
        group = self._fill_groups.get(order.client_order_id)
        if group is not None:
            # A MsgFillOrders pays the taker fee once, for all of its entries.
            entries = [self._order_tracker.fetch_order(key) for key, other in self._fill_groups.items() if other == group]
            if any(entry is not None and entry.order_fills for entry in entries):
                return AddedToCostTradeFee(flat_fees=[])
            is_taker = True
        elif order.order_fills:
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
        if message.fill_group:
            # Its escrow may have come back meanwhile; the fills read so far stay booked.
            self._refresh_balances_soon()
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
        if message.fill_group:
            # A fill never rests, so the book cannot tell its outcome.
            await self._finish_create_without_outcome(message, reason)
            return
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
            if self._is_fill(client_order_id):
                self._restored_without_tx.discard(client_order_id)
                self.logger().warning(
                    f"Beezee fill {client_order_id} was saved before its tx was known, so whether it was sent is "
                    f"unknown. Marking it failed; the balances are read again from the chain."
                )
                self._update_order_state(order, OrderState.FAILED)
                self._refresh_balances_soon()
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
