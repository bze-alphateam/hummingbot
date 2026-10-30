import logging
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_FLOOR, Decimal
from typing import Callable, Dict, List, Optional, Set, Tuple

from pydantic import Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange.beezee.beezee_amm import BeezeeSwapError, BeezeeSwapRequest
from hummingbot.connector.exchange.beezee.beezee_fills import BeezeeFillError
from hummingbot.core.data_type.common import MarketDict, OrderType, TradeType
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
)
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase

# Opportunity A buys on the book and sells on the pool; B buys on the pool and sells on the book.
BOOK_TO_POOL = "book_to_pool"
POOL_TO_BOOK = "pool_to_book"
# The cooldown also ends after this many seconds per block when no NewBlock arrives (block websocket down).
FALLBACK_SECONDS_PER_BLOCK = 10


class BeezeePoolBookArbConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    controllers_config: List[str] = []
    exchange: str = Field("beezee")
    trading_pair: str = Field("BZE-USDC")
    # Profit an arb must keep after every fee, in % of what it spends. Each book level must also beat the pool by it.
    min_profit_pct: Decimal = Field(Decimal("0.3"), ge=0)
    # Largest arb, in base units.
    max_trade_base: Decimal = Field(Decimal("1000"), gt=0)
    # Balances the script never spends.
    reserve_base: Decimal = Field(Decimal("0"), ge=0)
    reserve_quote: Decimal = Field(Decimal("0"), ge=0)
    # The swap is refused by the chain when it would pay out more than this % less than quoted.
    slippage_pct: Decimal = Field(Decimal("0.5"), ge=0, lt=100)
    # A fill refunded in part leaves the inventory off by the refunded amount: swap it back on the pool once, when
    # that costs at most max_rebalance_cost_pct % of its value.
    rebalance_on_partial: bool = Field(True)
    max_rebalance_cost_pct: Decimal = Field(Decimal("1"), ge=0)
    # Blocks to wait after an arb or a rebalance settled, so the book and the pool show it.
    cooldown_blocks: int = Field(2, ge=0)
    # The script checks on every new block, and at least this often.
    check_interval_seconds: int = Field(10, gt=0)
    # Gas of one fill + swap tx, in BZE, counted in the profit before sending (the real gas is in the status).
    estimated_gas_bze: Decimal = Field(Decimal("0.005"), ge=0)

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets[self.exchange] = markets.get(self.exchange, set()) | {self.trading_pair}
        return markets


@dataclass
class ArbPlan:
    """One arb, display units. `fill_side` is ours on the book, `swap_side` ours on the pool. `book_quote` is what the
    book levels cost (A) or pay (B), `pool_quote` what the pool pays (A) or costs (B). `swap_amount` is the swap's
    input: base on A, quote on B. `fixed_cost` is both taker fees + the gas estimate, in quote."""
    direction: str
    fill_side: str
    swap_side: str
    levels: List[Tuple[Decimal, Decimal]]
    base_amount: Decimal
    book_quote: Decimal
    pool_quote: Decimal
    swap_amount: Decimal
    pool_fee_quote: Decimal
    fixed_cost: Decimal

    @property
    def spent(self) -> Decimal:
        return self.book_quote if self.direction == BOOK_TO_POOL else self.pool_quote

    @property
    def net_profit(self) -> Decimal:
        received = self.pool_quote if self.direction == BOOK_TO_POOL else self.book_quote
        return received - self.spent - self.fixed_cost

    @property
    def net_profit_pct(self) -> Decimal:
        return self.net_profit / self.spent * 100


@dataclass
class PendingArb:
    plan: ArbPlan
    fill_ids: List[str]
    swap_id: str
    gas_before: Decimal
    filled_base: Decimal = Decimal("0")
    filled_quote: Decimal = Decimal("0")
    done: Set[str] = field(default_factory=set)
    swap_failure: Optional[str] = None

    @property
    def order_ids(self) -> Set[str]:
        return set(self.fill_ids) | {self.swap_id}


@dataclass
class Imbalance:
    """Base the inventory is off by after a partly refunded fill: < 0 when the arb sold more base than it bought."""
    base_delta: Decimal
    attempted: bool = False
    note: str = "open"


@dataclass
class PendingRebalance:
    imbalance: Imbalance
    swap_id: str
    gas_before: Decimal
    done: bool = False


def fmt(amount: Decimal) -> str:
    """An amount without trailing zeros or an exponent: 400.000000 -> 400."""
    return f"{amount.normalize():f}"


def utc_day(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")


class BeezeePoolBookArb(StrategyV2Base):
    """
    Arbitrage between one BeeZee pair's liquidity pool and its own order book. When the book sells below the pool
    price, it takes those asks and sells the same base on the pool (A); when the book buys above it, it buys the base
    on the pool and sells it into those bids (B). Both legs go out in one tx (MsgFillOrders, then MsgMultiSwap), sized
    to the largest amount whose every part still earns `min_profit_pct` after the pool fee, and sent only when the
    whole arb keeps it after both tradebin taker fees and the gas. Both legs spend inventory already held: the fill
    escrows its funds in the tx, but only executes at the end of the block.
    """

    def __init__(self, connectors: Dict[str, ConnectorBase], config: BeezeePoolBookArbConfig):
        super().__init__(connectors, config)
        self.config = config
        self.base, self.quote = config.trading_pair.split("-", 1)
        self._native: Optional[str] = None
        self._refused: Optional[str] = None
        self._halted = False
        self._last_block_seen: Optional[int] = None
        self._next_check_timestamp = 0.0
        self._arb: Optional[PendingArb] = None
        self._rebalance: Optional[PendingRebalance] = None
        self._imbalances: List[Imbalance] = []
        self._cooldown_block: Optional[int] = None
        self._cooldown_timestamp: Optional[float] = None
        self._day: Optional[str] = None
        self._trades_today = 0
        self._profit_today = Decimal("0")
        self._tradebin_fees_today = Decimal("0")
        self._pool_fees_today = Decimal("0")
        self._gas_baseline = Decimal("0")
        self._last_skip_reason: Optional[str] = None
        self._last_gap: Optional[str] = None

    @property
    def connector(self) -> ConnectorBase:
        return self.connectors[self.config.exchange]

    @property
    def pair(self) -> str:
        return self.config.trading_pair

    # === Clock ===

    def on_tick(self):
        self._roll_day()
        if self._refused is not None or not self._check_pair():
            return
        blocks = self.connector.new_blocks_seen
        if blocks == self._last_block_seen and self.current_timestamp < self._next_check_timestamp:
            return
        self._last_block_seen = blocks
        self._next_check_timestamp = self.current_timestamp + self.config.check_interval_seconds
        skip_reason = self._run_cycle()
        if skip_reason != self._last_skip_reason and skip_reason is not None:
            self.log_with_clock(logging.INFO, f"Pool/book arb skipped: {skip_reason}")
        self._last_skip_reason = skip_reason

    def _check_pair(self) -> bool:
        """Tradebin fees and gas are paid in the native token (BZE), so one side of the pair must be it."""
        if self._native is not None:
            return True
        taker = self._taker_fee_token()
        native = taker[0] if taker is not None else "BZE"
        if native not in (self.base, self.quote):
            self._refused = (
                f"{self.pair} has no {native} side: the fees and the gas are paid in {native} and cannot be counted "
                f"in the pair's terms. Pick a pair with {native} as base or quote."
            )
            self.log_with_clock(logging.ERROR, f"Pool/book arb refuses to start: {self._refused}")
            return False
        self._native = native
        return True

    def _roll_day(self):
        day = utc_day(self.current_timestamp)
        if day == self._day:
            return
        self._day = day
        self._trades_today = 0
        self._profit_today = Decimal("0")
        self._tradebin_fees_today = Decimal("0")
        self._pool_fees_today = Decimal("0")
        self._gas_baseline = self.connector.gas_fees_paid()

    # === One cycle ===

    def _run_cycle(self) -> Optional[str]:
        """Rebalances an open imbalance or sends one arb. Returns why it did nothing, or None."""
        if self._arb is not None:
            return "waiting for the last arb to settle"
        if self._rebalance is not None:
            return "waiting for the rebalance swap to settle"
        pool = self.connector.get_pool(self.pair)
        if pool is None:
            return f"{self.pair} has no liquidity pool"
        if pool.halted:
            self._set_halted(f"the pool {pool.pool_id} or the market of {self.pair} is halted")
            return "market or pool halted"
        pool_price = self.connector.get_pool_price(self.pair)
        if pool_price is None:
            return "the pool is empty"
        cooldown = self._cooldown_left()
        if cooldown is not None:
            return cooldown
        imbalance = next((item for item in self._imbalances if not item.attempted), None)
        if imbalance is not None:
            return self._try_rebalance(imbalance, pool_price)
        return self._try_arb(pool_price)

    def _set_halted(self, why: str):
        if not self._halted:
            self._halted = True
            self.log_with_clock(logging.WARNING, f"Pool/book arb stops trading {self.pair}: {why}.")

    def _cooldown_left(self) -> Optional[str]:
        if self._cooldown_block is None or self.config.cooldown_blocks == 0:
            return None
        blocks = self.connector.new_blocks_seen - self._cooldown_block
        elapsed = self.current_timestamp - self._cooldown_timestamp
        if blocks >= self.config.cooldown_blocks or elapsed >= self.config.cooldown_blocks * FALLBACK_SECONDS_PER_BLOCK:
            self._cooldown_block = None
            return None
        return f"cooldown, {blocks}/{self.config.cooldown_blocks} blocks since the last trade"

    def _start_cooldown(self):
        self._cooldown_block = self.connector.new_blocks_seen
        self._cooldown_timestamp = self.current_timestamp

    # === Sizing ===

    def _try_arb(self, pool_price: Decimal) -> Optional[str]:
        fixed_cost = self.to_quote(2 * self._taker_fee() + self.config.estimated_gas_bze, pool_price)
        reasons: List[str] = []
        plans: List[ArbPlan] = []
        for direction in (BOOK_TO_POOL, POOL_TO_BOOK):
            plan = self.plan_arb(direction, pool_price, fixed_cost)
            if isinstance(plan, ArbPlan):
                plans.append(plan)
            else:
                reasons.append(plan)
        if not plans:
            return "; ".join(reasons)
        plan = max(plans, key=lambda item: item.net_profit)
        return self._send_arb(plan)

    def plan_arb(self, direction: str, pool_price: Decimal, fixed_cost: Decimal):
        """The largest profitable arb in `direction`, or why there is none. Book levels are taken best first, each as
        far as the part taken from it still beats the pool by `min_profit_pct` (pool price impact and fee included),
        within `max_trade_base` and the balances above the reserves. The whole arb must then keep `min_profit_pct`
        of what it spends after `fixed_cost` (both taker fees + gas, in quote)."""
        book_to_pool = direction == BOOK_TO_POOL
        fill_side, swap_side = ("buy", "sell") if book_to_pool else ("sell", "buy")
        native_cost = 2 * self._taker_fee() + self.config.estimated_gas_bze
        base_cap = self._spendable(self.base, self.config.reserve_base, native_cost)
        quote_cap = self._spendable(self.quote, self.config.reserve_quote, native_cost)
        if base_cap <= 0:
            return f"no {self.base} above the reserve"
        if quote_cap <= 0:
            return f"no {self.quote} above the reserve"
        max_base = min(self.config.max_trade_base, base_cap)
        try:
            levels = self.connector.levels_to_take(self.pair, fill_side, max_base, pool_price)
        except BeezeeFillError as error:
            return f"cannot read the book levels: {error}"
        if not levels:
            return f"no {'ask below' if book_to_pool else 'bid above'} the pool price {pool_price:.10g}"
        self._last_gap = self._gap_text(direction, levels[0][0], pool_price, fixed_cost)
        margin = 1 + self.config.min_profit_pct / 100
        pool_value = self._pool_value(swap_side)
        step = self.connector.trading_rules[self.pair].min_base_amount_increment
        taken: List[Tuple[Decimal, Decimal]] = []
        cum, book_quote = Decimal("0"), Decimal("0")
        why_not = f"gap at {levels[0][0]} vs pool {pool_price:.10g} is below the pool fee + min_profit_pct"
        for price, amount in levels:
            before = pool_value(cum)
            if before is None:
                break

            def profitable(part: Decimal) -> bool:
                after = pool_value(cum + part)
                if after is None:
                    return False
                if book_to_pool:
                    return after - before >= price * part * margin and book_quote + price * part <= quote_cap
                return price * part >= (after - before) * margin and after <= quote_cap

            part = self._largest(amount, step, profitable)
            if part <= 0:
                break
            if part < self.connector.minimum_order_amount(self.pair, price):
                why_not = f"the profitable part ({part}) of the level at {price} is below the chain minimum"
                break
            taken.append((price, part))
            cum += part
            book_quote += price * part
            if part < amount:
                break
        if not taken:
            return why_not
        quote = self.connector.quote_swap(self.pair, "sell", cum) if book_to_pool else \
            self.connector.quote_input_for_output(self.pair, "buy", cum)
        plan = ArbPlan(
            direction=direction,
            fill_side=fill_side,
            swap_side=swap_side,
            levels=taken,
            base_amount=cum,
            book_quote=book_quote,
            pool_quote=quote.output if book_to_pool else quote.input,
            swap_amount=cum if book_to_pool else quote.input,
            pool_fee_quote=quote.fee * pool_price if book_to_pool else quote.fee,
            fixed_cost=fixed_cost,
        )
        if plan.net_profit_pct < self.config.min_profit_pct:
            return (
                f"best arb of {cum} {self.base} nets {plan.net_profit:.6g} {self.quote} ({plan.net_profit_pct:.3f}%) "
                f"after fees, below min_profit_pct {self.config.min_profit_pct}%"
            )
        return plan

    def _pool_value(self, swap_side: str) -> Callable[[Decimal], Optional[Decimal]]:
        """Quote the pool pays for selling x base ("sell"), or costs for buying x base ("buy"). None when the pool
        cannot swap x."""
        cache: Dict[Decimal, Optional[Decimal]] = {Decimal("0"): Decimal("0")}

        def value(amount: Decimal) -> Optional[Decimal]:
            if amount not in cache:
                if swap_side == "sell":
                    quote = self.connector.quote_swap(self.pair, "sell", amount)
                    cache[amount] = quote.output if quote.ok else None
                else:
                    quote = self.connector.quote_input_for_output(self.pair, "buy", amount)
                    cache[amount] = quote.input if quote.ok else None
            return cache[amount]

        return value

    @staticmethod
    def _largest(amount: Decimal, step: Decimal, ok: Callable[[Decimal], bool]) -> Decimal:
        """The largest multiple of `step` up to `amount` that is `ok`, 0 if none. `ok` must hold up to some size and
        not beyond it (the pool pays less per base the more it swaps)."""
        steps = int((amount / step).to_integral_value(rounding=ROUND_FLOOR))
        if steps <= 0:
            return Decimal("0")
        if ok(steps * step):
            return steps * step
        low, high = 0, steps
        while high - low > 1:
            middle = (low + high) // 2
            if ok(middle * step):
                low = middle
            else:
                high = middle
        return low * step

    def _spendable(self, token: str, reserve: Decimal, native_cost: Decimal) -> Decimal:
        spendable = self.connector.get_available_balance(token) - reserve
        if token == self._native:
            spendable -= native_cost
        return spendable

    def _gap_text(self, direction: str, best_price: Decimal, pool_price: Decimal, fixed_cost: Decimal) -> str:
        if direction == BOOK_TO_POOL:
            gross = (pool_price - best_price) / best_price * 100
            side = f"best ask {best_price:.10g} < pool {pool_price:.10g}"
        else:
            gross = (best_price - pool_price) / pool_price * 100
            side = f"best bid {best_price:.10g} > pool {pool_price:.10g}"
        return f"{side}: {gross:.3f}% before the pool fee, fixed fees {fixed_cost:.6g} {self.quote} per arb"

    # === Sending ===

    def _send_arb(self, plan: ArbPlan) -> Optional[str]:
        gas_before = self.connector.gas_fees_paid()
        request = BeezeeSwapRequest(self.pair, plan.swap_side, plan.swap_amount, self.config.slippage_pct)
        try:
            fill_ids, swap_ids = self.connector.fill_orders_and_swap(self.pair, plan.fill_side, plan.levels, [request])
        except (BeezeeFillError, BeezeeSwapError) as error:
            if "halted" in str(error).lower():
                self._set_halted(str(error))
                return "market or pool halted"
            return f"arb refused up front: {error}"
        self._arb = PendingArb(plan=plan, fill_ids=list(fill_ids), swap_id=swap_ids[0], gas_before=gas_before)
        if self._halted:
            # A halt (seen on the pool or in a refusal) is over once an arb is accepted again.
            self._halted = False
            self.log_with_clock(logging.INFO, f"{self.pair} is no longer halted: arbitrage resumes.")
        verb = "buys on the book, sells on the pool" if plan.direction == BOOK_TO_POOL else \
            "buys on the pool, sells on the book"
        self.log_with_clock(
            logging.INFO,
            f"Arb sent ({verb}): {fmt(plan.base_amount)} {self.base} over {len(plan.levels)} level(s), "
            f"expected net {plan.net_profit:.6g} {self.quote} ({plan.net_profit_pct:.3f}%).",
        )
        return None

    def _try_rebalance(self, imbalance: Imbalance, pool_price: Decimal) -> Optional[str]:
        """One attempt per imbalance, whatever its outcome: kept when switched off, refused or too costly."""
        imbalance.attempted = True
        amount = abs(imbalance.base_delta)
        if not self.config.rebalance_on_partial:
            imbalance.note = "kept (rebalance_on_partial is off)"
            return f"imbalance of {fmt(imbalance.base_delta)} {self.base} kept"
        fixed = self.to_quote(self._taker_fee() + self.config.estimated_gas_bze, pool_price)
        value = amount * pool_price
        if imbalance.base_delta < 0:
            quote = self.connector.quote_input_for_output(self.pair, "buy", amount)
            request = BeezeeSwapRequest(self.pair, "buy", quote.input, self.config.slippage_pct) if quote.ok else None
            cost = quote.input - value + fixed if quote.ok else None
        else:
            quote = self.connector.quote_swap(self.pair, "sell", amount)
            request = BeezeeSwapRequest(self.pair, "sell", amount, self.config.slippage_pct) if quote.ok else None
            cost = value - quote.output + fixed if quote.ok else None
        if request is None:
            imbalance.note = f"kept (the pool cannot swap it: {quote.error})"
            return f"imbalance of {fmt(imbalance.base_delta)} {self.base} kept"
        cost_pct = cost / value * 100
        if cost_pct > self.config.max_rebalance_cost_pct:
            imbalance.note = f"kept (rebalancing costs {cost_pct:.3f}% > {self.config.max_rebalance_cost_pct}%)"
            return f"imbalance of {fmt(imbalance.base_delta)} {self.base} kept, rebalancing costs {cost_pct:.3f}%"
        gas_before = self.connector.gas_fees_paid()
        try:
            swap_id = self.connector.amm_swap(self.pair, request.side, request.amount, request.slippage_pct)
        except BeezeeSwapError as error:
            imbalance.note = f"kept (rebalance refused: {error})"
            return f"imbalance of {fmt(imbalance.base_delta)} {self.base} kept, rebalance refused"
        self._rebalance = PendingRebalance(imbalance=imbalance, swap_id=swap_id, gas_before=gas_before)
        imbalance.note = "rebalancing"
        self.log_with_clock(
            logging.INFO,
            f"Rebalancing {fmt(imbalance.base_delta)} {self.base} with a pool {request.side} (cost {cost_pct:.3f}%).",
        )
        return None

    # === Outcomes ===

    def did_fill_order(self, event: OrderFilledEvent):
        arb = self._arb
        if arb is not None and event.order_id in arb.fill_ids:
            arb.filled_base += event.amount
            arb.filled_quote += event.amount * event.price

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        self._order_done(event.order_id)

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        self._order_done(event.order_id)

    def did_cancel_order(self, event: OrderCancelledEvent):
        # A fill entry ends cancelled when the chain refunded part of it.
        self._order_done(event.order_id)

    def did_fail_order(self, event: MarketOrderFailureEvent):
        self._order_done(event.order_id, event.error_message or "failed")

    def _order_done(self, order_id: str, failure: Optional[str] = None):
        arb = self._arb
        if arb is not None and order_id in arb.order_ids:
            arb.done.add(order_id)
            if order_id == arb.swap_id and failure is not None:
                arb.swap_failure = failure
            if arb.done >= arb.order_ids:
                self._arb = None
                self._settle_arb(arb)
        rebalance = self._rebalance
        if rebalance is not None and order_id == rebalance.swap_id:
            self._rebalance = None
            self._settle_rebalance(rebalance, failure)

    def _settle_arb(self, arb: PendingArb):
        self._roll_day()
        self._start_cooldown()
        plan = arb.plan
        if arb.swap_failure is not None:
            # Both legs share one tx, so nothing traded.
            self.log_with_clock(logging.WARNING, f"Arb tx failed, nothing traded: {arb.swap_failure}")
            return
        result = self.connector.get_swap_result(arb.swap_id)
        swap_in = result.input if result is not None else plan.swap_amount
        swap_out = result.output if result is not None else (plan.pool_quote if plan.direction == BOOK_TO_POOL else plan.base_amount)
        if plan.direction == BOOK_TO_POOL:
            quote_flow, base_flow = swap_out - arb.filled_quote, arb.filled_base - swap_in
        else:
            quote_flow, base_flow = arb.filled_quote - swap_in, swap_out - arb.filled_base
        pool_price = self.connector.get_pool_price(self.pair) or (plan.book_quote / plan.base_amount)
        if result is not None:
            self._pool_fees_today += result.pool_fee * (pool_price if plan.direction == BOOK_TO_POOL else 1)
        profit = self._book_trade(quote_flow, base_flow, 2 * self._taker_fee(), arb.gas_before, pool_price)
        refunded = plan.base_amount - arb.filled_base
        if refunded > 0:
            # The swap already moved the full amount: the inventory is off by what the fill did not get.
            delta = -refunded if plan.direction == BOOK_TO_POOL else refunded
            self._imbalances.append(Imbalance(base_delta=delta))
            self.log_with_clock(
                logging.WARNING,
                f"Arb fill refunded {fmt(refunded)} of {fmt(plan.base_amount)} {self.base}: inventory off by {fmt(delta)} "
                f"{self.base}.",
            )
        self.log_with_clock(
            logging.INFO,
            f"Arb settled: {fmt(arb.filled_base)} {self.base} filled on the book, profit {profit:.6g} {self.quote} "
            f"(expected {plan.net_profit:.6g}).",
        )

    def _settle_rebalance(self, rebalance: PendingRebalance, failure: Optional[str]):
        self._roll_day()
        self._start_cooldown()
        imbalance = rebalance.imbalance
        if failure is not None:
            imbalance.note = f"kept (rebalance failed: {failure})"
            self.log_with_clock(logging.WARNING, f"Rebalance swap failed, imbalance kept: {failure}")
            return
        result = self.connector.get_swap_result(rebalance.swap_id)
        pool_price = self.connector.get_pool_price(self.pair) or Decimal("0")
        if result is not None:
            buy = result.side == "buy"
            quote_flow = -result.input if buy else result.output
            base_flow = result.output if buy else -result.input
            self._book_trade(quote_flow, base_flow, self._taker_fee(), rebalance.gas_before, pool_price)
            self._pool_fees_today += result.pool_fee * (1 if buy else pool_price)
        self._imbalances.remove(imbalance)
        self.log_with_clock(logging.INFO, f"Imbalance of {fmt(imbalance.base_delta)} {self.base} rebalanced on the pool.")

    def _book_trade(self, quote_flow: Decimal, base_flow: Decimal, tradebin_fees: Decimal, gas_before: Decimal,
                    pool_price: Decimal) -> Decimal:
        """Counts one landed tx; its profit is the quote it made plus the base it made valued at the pool price, less
        its tradebin fees and gas (in BZE, converted)."""
        gas = self.connector.gas_fees_paid() - gas_before
        profit = quote_flow + base_flow * pool_price - self.to_quote(tradebin_fees + gas, pool_price)
        self._trades_today += 1
        self._profit_today += profit
        self._tradebin_fees_today += tradebin_fees
        return profit

    # === Fees ===

    def _taker_fee_token(self) -> Optional[Tuple[str, Decimal]]:
        fee = self.connector.get_fee(self.base, self.quote, OrderType.LIMIT, TradeType.BUY, Decimal("0"), Decimal("0"),
                                     is_maker=False)
        flat = fee.flat_fees
        return (flat[0].token, sum((item.amount for item in flat), Decimal("0"))) if flat else None

    def _taker_fee(self) -> Decimal:
        taker = self._taker_fee_token()
        return taker[1] if taker is not None else Decimal("0")

    def to_quote(self, native_amount: Decimal, pool_price: Decimal) -> Decimal:
        """A BZE amount in the pair's quote: 1:1 when BZE is the quote, at the pair's own price when it is the base."""
        return native_amount if self._native == self.quote else native_amount * pool_price

    # === Status ===

    @property
    def gas_today(self) -> Decimal:
        return self.connector.gas_fees_paid() - self._gas_baseline

    @property
    def open_imbalance(self) -> Decimal:
        return sum((item.base_delta for item in self._imbalances), Decimal("0"))

    def format_status(self) -> str:
        if self._refused is not None:
            return f"  Refused: {self._refused}"
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        pool_price = self.connector.get_pool_price(self.pair)
        bid, ask = self._best_bid_ask()
        lines = [
            f"  Pair: {self.pair}{' (HALTED)' if self._halted else ''}",
            f"  Pool price: {f'{pool_price:.10g}' if pool_price is not None else '-'}   "
            f"best bid: {f'{bid:.10g}' if bid is not None else '-'}   best ask: {f'{ask:.10g}' if ask is not None else '-'}",
            f"  Gap: {self._last_gap or '-'}",
            f"  Today (UTC {self._day}): {self._trades_today} trade(s), profit {self._profit_today:.6g} {self.quote}, "
            f"tradebin fees {fmt(self._tradebin_fees_today)} {self._native or 'BZE'}, gas {fmt(self.gas_today)} "
            f"{self._native or 'BZE'}, pool fees {self._pool_fees_today:.6g} {self.quote}",
            f"  Open imbalance: {fmt(self.open_imbalance)} {self.base}",
        ]
        lines += [f"    {fmt(item.base_delta)} {self.base}: {item.note}" for item in self._imbalances]
        if self._arb is not None:
            lines.append(f"  In flight: arb of {fmt(self._arb.plan.base_amount)} {self.base}")
        if self._last_skip_reason:
            lines.append(f"  Last check skipped: {self._last_skip_reason}")
        return "\n".join(lines)

    def _best_bid_ask(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        order_book = self.connector.get_order_book(self.pair)
        prices: List[Optional[Decimal]] = []
        for is_buy in (False, True):
            try:
                price = order_book.get_price(is_buy)
            except EnvironmentError:
                price = math.nan
            prices.append(None if math.isnan(price) or price <= 0 else Decimal(str(price)))
        return prices[0], prices[1]
