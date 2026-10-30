from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple

from pydantic import Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange.beezee.beezee_amm import BeezeeSwapError
from hummingbot.connector.exchange.beezee.beezee_pool_price_delegate import BeezeePoolPriceDelegate
from hummingbot.connector.exchange.beezee.beezee_utils import VENUE_BOOK, describe_venues, venue_refusal
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.strategy_v2.controllers.market_making_controller_base import (
    MarketMakingControllerBase,
    MarketMakingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import ExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType

POOL = "pool"
BOOK_MID = "book mid"
# A pending hedge also goes out after this many seconds when no NewBlock arrives (block websocket down).
FALLBACK_SECONDS_PER_BLOCK = 10


class BeezeePoolAnchoredPMMConfig(MarketMakingControllerConfigBase):
    """pmm_simple's spreads, levels, amounts and refresh, quoted around the pair's liquidity pool price. Spreads are
    fractions as upstream (0.01 = 1%); the fields ending in `_pct` are percents (0.2 = 0.2%)."""
    controller_name: str = "beezee_pool_anchored_pmm"
    connector_name: str = Field(default="beezee", json_schema_extra={
        "prompt": "Enter the connector name (beezee): ", "prompt_on_new": True})
    trading_pair: str = Field(default="BZE-USDC", json_schema_extra={
        "prompt": "Enter the trading pair (it needs a liquidity pool): ", "prompt_on_new": True})
    leverage: int = Field(default=1)
    # The BeeZee book takes limit orders only: the stop loss, the time limit and the position rebalance all close or
    # buy with market orders, so they are off unless set.
    stop_loss: Optional[Decimal] = Field(default=None, gt=0, json_schema_extra={"is_updatable": True})
    time_limit: Optional[int] = Field(default=None, gt=0, json_schema_extra={"is_updatable": True})
    skip_rebalance: bool = Field(default=True)
    # Every quote is at least pool fee + this far from the pool price, so a pool arbitrageur cannot profit from our
    # quote alone.
    min_spread_over_pool_fee_pct: Decimal = Field(default=Decimal("0.1"), ge=0, json_schema_extra={
        "prompt": "Enter the margin over the pool fee every quote keeps from the pool price, in % (e.g. 0.1): ",
        "prompt_on_new": True, "is_updatable": True})
    # Requote as soon as the reference moved this far from where the quotes were placed, instead of waiting for
    # executor_refresh_time.
    requote_threshold_pct: Decimal = Field(default=Decimal("0.2"), gt=0, json_schema_extra={
        "prompt": "Enter the pool move that triggers a requote, in % (e.g. 0.2): ",
        "prompt_on_new": True, "is_updatable": True})
    # Swap every profitable fill back on the pool, so the inventory stays flat and the spread is kept.
    hedge_fills_on_pool: bool = Field(default=False, json_schema_extra={
        "prompt": "Hedge fills back on the pool? (true/false): ", "prompt_on_new": True})
    # The chain refuses a hedge swap that would pay out more than this % less than quoted.
    hedge_slippage_pct: Decimal = Field(default=Decimal("0.5"), ge=0, lt=100, json_schema_extra={"is_updatable": True})
    # A fill is hedged only when its distance from the pool price covers the pool fee, the tradebin taker fee and gas
    # of the hedge (in % of the fill's value) and this margin.
    min_hedge_profit_pct: Decimal = Field(default=Decimal("0.1"), ge=0, json_schema_extra={"is_updatable": True})
    # Gas of one hedge swap tx in BZE, counted in the hedge check.
    estimated_gas_bze: Decimal = Field(default=Decimal("0.005"), ge=0, json_schema_extra={"is_updatable": True})


@dataclass
class HedgeFill:
    """A fill of one of our book orders, display units. `side` is ours on the book."""
    side: TradeType
    amount: Decimal
    price: Decimal
    block: int
    timestamp: float


@dataclass
class PendingHedge:
    swap_id: str
    side: str
    base_amount: Decimal
    fills: int


class BeezeePoolAnchoredPMM(MarketMakingControllerBase):
    """
    Market making on a BeeZee order book around the pair's liquidity pool price instead of the book's own mid, which a
    thin book makes easy to push. Spreads, levels, amounts and refresh are pmm_simple's; every quote also keeps at
    least the pool fee + `min_spread_over_pool_fee_pct` from the pool price, and the quotes are replaced as soon as the
    pool moves more than `requote_threshold_pct`. Without a pool the book mid is the reference (a warning in status).
    A pool and an order-book market are separate venues on chain: a pair with no market is refused (nothing to quote
    on), a pair with a market and no pool falls back to the book mid.

    With `hedge_fills_on_pool`, each of our fills is swapped back on the pool in the opposite direction when its edge
    over the pool price pays for the pool fee, the taker fee, the gas and `min_hedge_profit_pct`. The fills of one
    block go out as one swap; a failed hedge is logged and the inventory kept, never retried. A halted pair cancels
    every quote and stops quoting until the controller is restarted.
    """

    def __init__(self, config: BeezeePoolAnchoredPMMConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config
        self.base, self.quote = config.trading_pair.split("-", 1)
        self._started_at = self.market_data_provider.time()
        self._delegate: Optional[BeezeePoolPriceDelegate] = None
        self._reference_by_executor: Dict[str, Decimal] = {}
        self._warned_source: Optional[str] = None
        self._halted: Optional[str] = None
        self._refused: Optional[str] = None
        self._halt_stopped: Set[str] = set()
        # Hedge mode: what each executor had filled when last seen (base, quote), fills waiting for their block to end,
        # the swap in flight, and counters for status.
        self._seen_fills: Dict[str, Tuple[Decimal, Decimal]] = {}
        self._pending_fills: List[HedgeFill] = []
        self._hedge: Optional[PendingHedge] = None
        self._hedge_refused: Optional[str] = None
        self._hedges_sent = 0
        self._hedges_failed = 0
        self._unhedged_base = Decimal("0")
        self._last_hedge_note: Optional[str] = None

    @property
    def connector(self) -> ConnectorBase:
        return self.market_data_provider.get_connector(self.config.connector_name)

    @property
    def delegate(self) -> BeezeePoolPriceDelegate:
        if self._delegate is None:
            self._delegate = BeezeePoolPriceDelegate(self.connector, self.config.trading_pair)
        return self._delegate

    # === Reference price ===

    async def update_processed_data(self):
        pair = self.config.trading_pair
        if not self._check_venue():
            self.processed_data = {"reference_price": None, "spread_multiplier": Decimal("1"), "price_source": None,
                                   "pool_fee": Decimal("0"), "min_spread": Decimal("0")}
            return
        pool = self.connector.get_pool(pair)
        if pool is not None and pool.halted:
            self._set_halted(f"the pool {pool.pool_id} or the market of {pair} is halted")
        pool_price = self.delegate.get_price_by_type(PriceType.MidPrice)
        if pool is not None and not pool_price.is_nan():
            reference, source, pool_fee = pool_price, POOL, pool.fee
        else:
            why = f"{pair} has no liquidity pool" if pool is None else f"the pool {pool.pool_id} is empty"
            self._warn_source(f"{why}: quoting around the order-book mid instead")
            reference = self._book_mid()
            source, pool_fee = BOOK_MID, Decimal("0")
        if source == POOL and self._warned_source is not None:
            self._warned_source = None
            self.logger().info(f"{pair}: the pool price is back, quoting around it.")
        self.processed_data = {
            "reference_price": reference,
            "spread_multiplier": Decimal("1"),
            "price_source": source,
            "pool_fee": pool_fee,
            "min_spread": pool_fee + self.config.min_spread_over_pool_fee_pct / 100,
        }
        if self.config.hedge_fills_on_pool:
            self.process_hedges(pool_price if source == POOL else None)

    def _check_venue(self) -> bool:
        """The pair needs an order-book market to quote on; refused once, with the reason, when it has none. Waits
        while the connector has not loaded the pairs' venues yet."""
        if self._refused is not None:
            return False
        venues = tuple(self.connector.trading_pair_venues(self.config.trading_pair))
        if not venues:
            return False
        self._refused = venue_refusal(self.connector, self.config.trading_pair, (VENUE_BOOK,), "pool-anchored PMM")
        if self._refused is not None:
            self.logger().error(f"Pool-anchored PMM refuses to start: {self._refused}.")
            return False
        return True

    def _book_mid(self) -> Optional[Decimal]:
        try:
            mid = Decimal(str(self.market_data_provider.get_price_by_type(
                self.config.connector_name, self.config.trading_pair, PriceType.MidPrice)))
        except EnvironmentError:
            return None
        return None if mid.is_nan() or mid <= 0 else mid

    def _warn_source(self, warning: str):
        if warning != self._warned_source:
            self._warned_source = warning
            self.logger().warning(f"Pool-anchored PMM: {warning}.")

    def _set_halted(self, why: str):
        if self._halted is None:
            self._halted = why
            self.logger().warning(
                f"Pool-anchored PMM stops on {self.config.trading_pair}: {why}. Cancelling every quote; restart the "
                f"controller once the halt is lifted.")

    # === Quotes ===

    def determine_executor_actions(self) -> List[ExecutorAction]:
        if self._halted is not None:
            return self._halt_stop_actions()
        return super().determine_executor_actions()

    def create_actions_proposal(self) -> List[ExecutorAction]:
        if self.processed_data.get("reference_price") is None:
            return []
        return super().create_actions_proposal()

    def _halt_stop_actions(self) -> List[ExecutorAction]:
        """Exit only: every active executor is stopped once, keeping whatever it filled (no market close)."""
        actions = []
        for executor in self.filter_executors(executors=self.executors_info, filter_func=lambda x: x.is_active):
            if executor.id not in self._halt_stopped:
                self._halt_stopped.add(executor.id)
                actions.append(StopExecutorAction(controller_id=self.config.id, executor_id=executor.id,
                                                  keep_position=True))
        return actions

    def get_price_and_amount(self, level_id: str) -> Tuple[Decimal, Decimal]:
        """pmm_simple's price, with the spread raised to the floor (pool fee + margin) where it is below it."""
        level = self.get_level_from_level_id(level_id)
        trade_type = self.get_trade_type_from_level_id(level_id)
        spreads, amounts_quote = self.config.get_spreads_and_amounts_in_quote(trade_type)
        reference_price = Decimal(self.processed_data["reference_price"])
        spread = Decimal(str(spreads[int(level)])) * Decimal(self.processed_data["spread_multiplier"])
        spread = max(spread, self.processed_data["min_spread"])
        side_multiplier = Decimal("-1") if trade_type == TradeType.BUY else Decimal("1")
        order_price = reference_price * (1 + side_multiplier * spread)
        return order_price, Decimal(str(amounts_quote[int(level)])) / order_price

    def get_executor_config(self, level_id: str, price: Decimal, amount: Decimal):
        trade_type = self.get_trade_type_from_level_id(level_id)
        if self.config.hedge_fills_on_pool:
            # A filled quote is hedged on the pool, so nothing may close it again on the book: a plain limit order.
            config = OrderExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                side=trade_type,
                amount=amount,
                price=price,
                execution_strategy=ExecutionStrategy.LIMIT,
                level_id=level_id,
                controller_id=self.config.id,
            )
        else:
            config = PositionExecutorConfig(
                timestamp=self.market_data_provider.time(),
                level_id=level_id,
                connector_name=self.config.connector_name,
                trading_pair=self.config.trading_pair,
                entry_price=price,
                amount=amount,
                triple_barrier_config=self.config.triple_barrier_config,
                leverage=self.config.leverage,
                side=trade_type,
            )
        self._reference_by_executor[config.id] = Decimal(self.processed_data["reference_price"])
        return config

    def get_levels_to_execute(self) -> List[str]:
        """A level waits `cooldown_time` after a stop loss (as upstream) and, in hedge mode, after a fill."""
        cooling = (CloseType.STOP_LOSS, CloseType.POSITION_HOLD) if self.config.hedge_fills_on_pool else \
            (CloseType.STOP_LOSS,)
        now = self.market_data_provider.time()
        working_levels = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda x: x.is_active or (x.close_type in cooling and now - x.close_timestamp < self.config.cooldown_time),
        )
        return self.get_not_active_levels_ids([executor.custom_info["level_id"] for executor in working_levels])

    def executors_to_refresh(self) -> List[ExecutorAction]:
        # A hedge-mode order executor cannot unwind a partial fill: keep it (it is hedged like any fill).
        return [StopExecutorAction(controller_id=action.controller_id, executor_id=action.executor_id,
                                   keep_position=self.config.hedge_fills_on_pool)
                for action in super().executors_to_refresh()]

    def executors_to_early_stop(self) -> List[ExecutorAction]:
        """Requote: the quotes not trading yet whose reference moved more than `requote_threshold_pct` since."""
        reference = self.processed_data.get("reference_price")
        if reference is None:
            return []
        threshold = self.config.requote_threshold_pct / 100
        for executor in self.executors_info:
            if not executor.is_active:
                self._reference_by_executor.pop(executor.id, None)
        actions = []
        for executor in self.filter_executors(executors=self.executors_info,
                                              filter_func=lambda x: x.is_active and not x.is_trading):
            placed_at = self._reference_by_executor.get(executor.id)
            if placed_at is not None and abs(reference - placed_at) / placed_at > threshold:
                # Stopped once: the executor stays active until its cancel is confirmed.
                del self._reference_by_executor[executor.id]
                actions.append(StopExecutorAction(controller_id=self.config.id, executor_id=executor.id,
                                                  keep_position=self.config.hedge_fills_on_pool))
        return actions

    # === Hedge mode ===

    def process_hedges(self, pool_price: Optional[Decimal]):
        """Settles the hedge in flight, collects new fills and sends the last block's fills as one swap."""
        self._check_hedge_in_flight()
        self._collect_fills()
        if not self._pending_fills or self._hedge is not None:
            return
        if self._halted is not None or pool_price is None:
            why = self._halted or "no pool price"
            self._drop_pending(f"not hedged ({why}), kept in the inventory")
            return
        connector = self.connector
        first = self._pending_fills[0]
        block_over = connector.new_blocks_seen > max(fill.block for fill in self._pending_fills)
        timed_out = self.market_data_provider.time() - first.timestamp >= FALLBACK_SECONDS_PER_BLOCK
        if block_over or timed_out:
            self._send_hedge(pool_price)

    def _collect_fills(self):
        """New fills of our quotes since the last look, from the executors' executed amounts."""
        block = self.connector.new_blocks_seen
        now = self.market_data_provider.time()
        for executor in self.executors_info:
            if executor.config.timestamp is None or executor.config.timestamp < self._started_at:
                continue  # an executor from before this start: its fills are not ours to hedge
            if executor.config.type != "order_executor":
                continue
            base = Decimal(str(executor.custom_info.get("executed_amount_base") or 0))
            quote = base * Decimal(str(executor.custom_info.get("average_executed_price") or 0))
            seen_base, seen_quote = self._seen_fills.get(executor.id, (Decimal("0"), Decimal("0")))
            if base <= seen_base:
                continue
            self._seen_fills[executor.id] = (base, quote)
            amount = base - seen_base
            price = (quote - seen_quote) / amount
            if price <= 0:
                continue
            self._pending_fills.append(HedgeFill(side=executor.config.side, amount=amount, price=price, block=block,
                                                 timestamp=now))

    def hedge_edge_pct(self, fill: HedgeFill, pool_price: Decimal) -> Decimal:
        """How far the fill is on the right side of the pool price, in %: bought below it or sold above it."""
        if fill.side == TradeType.BUY:
            return (pool_price - fill.price) / pool_price * 100
        return (fill.price - pool_price) / pool_price * 100

    def hedge_cost_pct(self, fill: HedgeFill, pool_price: Decimal) -> Optional[Decimal]:
        """Pool fee + the hedge's taker fee and gas (in % of the fill's value) + `min_hedge_profit_pct`, or None when
        the fees cannot be counted in the pair's terms (no BZE side)."""
        fixed_quote = self.fixed_cost_quote(pool_price)
        if fixed_quote is None:
            return None
        pool_fee = Decimal(self.processed_data.get("pool_fee") or 0)
        return pool_fee * 100 + fixed_quote / (fill.amount * fill.price) * 100 + self.config.min_hedge_profit_pct

    def fixed_cost_quote(self, pool_price: Decimal) -> Optional[Decimal]:
        """The hedge swap's tradebin taker fee + `estimated_gas_bze`, paid in BZE, in the pair's quote: 1:1 when BZE
        is the quote, at the pool price when it is the base, None when BZE is neither."""
        fee = self.connector.get_fee(self.base, self.quote, OrderType.LIMIT, TradeType.BUY, Decimal("0"), Decimal("0"),
                                     is_maker=False)
        flat = fee.flat_fees
        token = flat[0].token if flat else "BZE"
        amount = sum((item.amount for item in flat), Decimal("0")) + self.config.estimated_gas_bze
        if token == self.quote:
            return amount
        if token == self.base:
            return amount * pool_price
        return None

    def _send_hedge(self, pool_price: Decimal):
        fills, self._pending_fills = self._pending_fills, []
        hedged: List[HedgeFill] = []
        for fill in fills:
            cost = self.hedge_cost_pct(fill, pool_price)
            if cost is None:
                self._refuse_hedging()
                self._keep(fill, "fees not countable without a BZE side")
                continue
            edge = self.hedge_edge_pct(fill, pool_price)
            if edge < cost:
                self._keep(fill, f"edge {edge:.3f}% < {cost:.3f}% (pool fee + taker fee + gas + margin)")
                continue
            hedged.append(fill)
        bought = sum((fill.amount for fill in hedged if fill.side == TradeType.BUY), Decimal("0"))
        sold = sum((fill.amount for fill in hedged if fill.side == TradeType.SELL), Decimal("0"))
        net = bought - sold
        if not hedged or net == 0:
            return
        side = "sell" if net > 0 else "buy"
        amount = abs(net)
        pair = self.config.trading_pair
        quote = self.connector.quote_swap(pair, "sell", amount) if side == "sell" else \
            self.connector.quote_input_for_output(pair, "buy", amount)
        if not quote.ok:
            self._hedge_failed(net, f"refused: {quote.error}")
            return
        # The per-fill check counts the pool fee only: the batch must also pay for the swap's price impact.
        fills_quote = sum((fill.amount * fill.price * (1 if fill.side == TradeType.SELL else -1) for fill in hedged),
                          Decimal("0"))
        swap_quote = quote.output if side == "sell" else -quote.input
        traded = sum((fill.amount * fill.price for fill in hedged), Decimal("0"))
        profit = fills_quote + swap_quote - self.fixed_cost_quote(pool_price)
        if profit < traded * self.config.min_hedge_profit_pct / 100:
            for fill in hedged:
                self._keep(fill, f"the hedge swap would net {profit:.6g} {self.quote} after the pool price impact, "
                                 f"below min_hedge_profit_pct")
            return
        swap_amount = amount if side == "sell" else quote.input
        try:
            swap_id = self.connector.amm_swap(pair, side, swap_amount, self.config.hedge_slippage_pct)
        except BeezeeSwapError as error:
            self._hedge_failed(net, f"refused: {error}")
            return
        self._hedge = PendingHedge(swap_id=swap_id, side=side, base_amount=amount, fills=len(hedged))
        self._hedges_sent += 1
        self.logger().info(
            f"Hedging {len(hedged)} fill(s) on the pool: {side} {amount.normalize():f} {self.base}, expected net "
            f"{profit:.6g} {self.quote} (slippage {self.config.hedge_slippage_pct}%).")

    def _check_hedge_in_flight(self):
        hedge = self._hedge
        if hedge is None:
            return
        connector = self.connector
        result = connector.get_swap_result(hedge.swap_id)
        if result is not None:
            self._hedge = None
            self._last_hedge_note = (
                f"{hedge.side} {result.input.normalize():f} -> {result.output.normalize():f} for {hedge.fills} fill(s)")
            self.logger().info(f"Hedge swap settled: {self._last_hedge_note}.")
        elif hedge.swap_id not in connector.in_flight_orders:
            self._hedge = None
            delta = hedge.base_amount if hedge.side == "sell" else -hedge.base_amount
            self._hedge_failed(delta, "the swap failed on chain")

    def _hedge_failed(self, base_delta: Decimal, why: str):
        """A failed hedge leaves the fills in the inventory; it is not tried again."""
        self._hedges_failed += 1
        self._unhedged_base += base_delta
        self._last_hedge_note = f"failed ({why}), {base_delta.normalize():f} {self.base} kept"
        self.logger().warning(f"Hedge swap {why}: {base_delta.normalize():f} {self.base} stays in the inventory.")
        if "halted" in why.lower():
            self._set_halted(why)

    def _keep(self, fill: HedgeFill, why: str):
        delta = fill.amount if fill.side == TradeType.BUY else -fill.amount
        self._unhedged_base += delta
        self.logger().info(f"Fill of {fill.amount.normalize():f} {self.base} at {fill.price:.10g} not hedged: {why}.")

    def _drop_pending(self, why: str):
        for fill in self._pending_fills:
            self._keep(fill, why)
        self._pending_fills = []

    def _refuse_hedging(self):
        if self._hedge_refused is None:
            self._hedge_refused = (
                f"{self.config.trading_pair} has no BZE side: the hedge fees are paid in BZE and cannot be counted in "
                f"the pair's terms, so fills are not hedged")
            self.logger().error(f"Pool-anchored PMM: {self._hedge_refused}.")

    # === Status ===

    def to_format_status(self) -> List[str]:
        if self._refused is not None:
            return [f"Pool-anchored PMM {self.config.trading_pair}", f"  Refused: {self._refused}"]
        data = self.processed_data
        reference = data.get("reference_price")
        venues = describe_venues(self.connector.trading_pair_venues(self.config.trading_pair))
        lines = [
            f"Pool-anchored PMM {self.config.trading_pair}{' (HALTED: ' + self._halted + ')' if self._halted else ''}"
            f"   venues: {venues}",
            f"  Reference: {data.get('price_source', '-')} {f'{reference:.10g}' if reference is not None else '-'}   "
            f"pool fee {Decimal(data.get('pool_fee') or 0) * 100:.3f}%   "
            f"spread floor {Decimal(data.get('min_spread') or 0) * 100:.3f}%",
        ]
        if self._warned_source is not None:
            lines.append(f"  WARNING: {self._warned_source}")
        if self.config.hedge_fills_on_pool:
            lines.append(
                f"  Hedge: {self._hedges_sent} sent, {self._hedges_failed} failed, "
                f"unhedged {self._unhedged_base.normalize():f} {self.base}, "
                f"{len(self._pending_fills)} fill(s) waiting{', one in flight' if self._hedge else ''}")
            if self._last_hedge_note:
                lines.append(f"  Last hedge: {self._last_hedge_note}")
            if self._hedge_refused:
                lines.append(f"  WARNING: {self._hedge_refused}")
        return lines

    def get_custom_info(self) -> dict:
        reference = self.processed_data.get("reference_price")
        return {
            "price_source": self.processed_data.get("price_source"),
            "reference_price": str(reference) if reference is not None else None,
            "halted": self._halted is not None,
            "refused": self._refused,
            "hedges_sent": self._hedges_sent,
            "hedges_failed": self._hedges_failed,
            "unhedged_base": str(self._unhedged_base),
        }
