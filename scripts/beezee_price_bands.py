import logging
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_FLOOR, Decimal
from typing import Callable, Dict, List, Literal, Optional, Set, Tuple

from pydantic import Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange.beezee.beezee_amm import BeezeeSwapError
from hummingbot.connector.exchange.beezee.beezee_fills import BeezeeFillError
from hummingbot.connector.exchange.beezee.beezee_pool_price_delegate import BeezeePoolPriceDelegate
from hummingbot.connector.exchange.beezee.beezee_utils import VENUE_BOOK, VENUE_POOL, describe_venues, venue_refusal
from hummingbot.core.data_type.common import MarketDict, OrderType, PriceType, TradeType
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
)
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase

BUY = "buy"
SELL = "sell"
POOL = "pool"
BOOK = "book"
BOOK_MID = "book_mid"
BEST = "best"
TOTAL = "total"
DAILY = "daily"
# The cooldown also ends after this many seconds per block when no NewBlock arrives (block websocket down).
FALLBACK_SECONDS_PER_BLOCK = 10


class BeezeePriceBandsConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    controllers_config: List[str] = []
    exchange: str = Field("beezee")
    trading_pair: str = Field("TOKEN-BZE")
    # Reference price: the pair's pool, or the order-book mid. Empty = the pool when the pair has one, else the book mid.
    # A pool and an order-book market are separate venues: a pair may have either or both.
    price_source: Optional[Literal["pool", "book_mid"]] = Field(None)
    # The band, in quote per base (the pair's own order-price units). Below the floor the script buys base, above the
    # ceiling it sells base, inside it does nothing.
    floor_price: Decimal = Field(gt=0)
    ceiling_price: Decimal = Field(gt=0)
    # Where a trade goes: the pool only, or the cheaper of the pool and the book (never split across both). "best" uses
    # whatever venue the pair has and compares only when it has both.
    venue: Literal["pool", "best"] = Field("pool")
    # Largest buy, in quote spent, and largest sell, in base sold.
    max_trade_quote: Decimal = Field(Decimal("100"), gt=0)
    max_trade_base: Decimal = Field(Decimal("100"), gt=0)
    # A trade's own price impact (effective price against the venue's price before it, pool fee included), in %.
    max_impact_pct: Decimal = Field(Decimal("1"), gt=0)
    # Balances the script never spends.
    keep_quote_reserve: Decimal = Field(Decimal("0"), ge=0)
    keep_base_reserve: Decimal = Field(Decimal("0"), ge=0)
    # Quote all buys may spend and base all sells may sell, over the whole run or per UTC day.
    buy_budget_quote: Decimal = Field(Decimal("0"), ge=0)
    sell_budget_base: Decimal = Field(Decimal("0"), ge=0)
    budget_period: Literal["total", "daily"] = Field("daily")
    # A pool swap is refused by the chain when it would pay out more than this % less than quoted.
    slippage_pct: Decimal = Field(Decimal("0.5"), ge=0, lt=100)
    # Blocks to wait after a trade settled, so the pool and the book show it.
    cooldown_blocks: int = Field(3, ge=0)
    # The script checks on every new block, and at least this often.
    check_interval_seconds: int = Field(10, gt=0)
    # Gas of one trade tx, in BZE, kept free on the BZE balance and counted when comparing venues.
    estimated_gas_bze: Decimal = Field(Decimal("0.005"), ge=0)

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets[self.exchange] = markets.get(self.exchange, set()) | {self.trading_pair}
        return markets


@dataclass
class TradePlan:
    """One trade on one venue, display units. `amount` is its input: quote on a buy, base on a sell. `base` and
    `quote` are what it moves (quote spent on a buy, received on a sell). `levels` are the book levels taken, empty
    on the pool. `limit` names the cap that sized it."""
    venue: str
    side: str
    amount: Decimal
    base: Decimal
    quote: Decimal
    impact_pct: Decimal
    limit: str
    levels: List[Tuple[Decimal, Decimal]] = field(default_factory=list)

    @property
    def price(self) -> Decimal:
        return self.quote / self.base


@dataclass
class PendingTrade:
    plan: TradePlan
    order_ids: List[str]
    gas_before: Decimal
    filled_base: Decimal = Decimal("0")
    filled_quote: Decimal = Decimal("0")
    done: Set[str] = field(default_factory=set)
    failure: Optional[str] = None


@dataclass
class SideTotals:
    trades: int = 0
    base: Decimal = Decimal("0")
    quote: Decimal = Decimal("0")

    @property
    def average_price(self) -> Optional[Decimal]:
        return self.quote / self.base if self.base > 0 else None


def fmt(amount: Decimal) -> str:
    """An amount without trailing zeros or an exponent: 400.000000 -> 400."""
    return f"{amount.normalize():f}"


def utc_day(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")


def floor_to(amount: Decimal, step: Decimal) -> Decimal:
    return (amount / step).to_integral_value(rounding=ROUND_FLOOR) * step


def largest(amount: Decimal, step: Decimal, ok: Callable[[Decimal], bool]) -> Decimal:
    """The largest multiple of `step` up to `amount` that is `ok`, 0 if none. `ok` must hold up to some size and not
    beyond it (a larger trade never has a smaller impact)."""
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


class BeezeePriceBands(StrategyV2Base):
    """
    Price support for one BeeZee pair: buys base when the reference price is below `floor_price`, sells base when it
    is above `ceiling_price`, does nothing inside the band. A trade is sized to bring the venue's price back to the
    band edge and no further, capped per trade (`max_trade_quote` / `max_trade_base`), by its own price impact
    (`max_impact_pct`), by the period's budget and by the balance above the kept reserve. The pair is traded in its
    own terms: a TOKEN/BZE band is set in BZE and nothing is routed through other pools. One trade is in flight at a
    time, followed by `cooldown_blocks` blocks before the next.
    """

    def __init__(self, connectors: Dict[str, ConnectorBase], config: BeezeePriceBandsConfig):
        super().__init__(connectors, config)
        self.config = config
        self.base, self.quote = config.trading_pair.split("-", 1)
        self._native: Optional[str] = None
        self._started = False
        self._refused: Optional[str] = None
        self._halted: Optional[str] = None
        self._delegate: Optional[BeezeePoolPriceDelegate] = None
        self._last_block_seen: Optional[int] = None
        self._next_check_timestamp = 0.0
        self._pending: Optional[PendingTrade] = None
        self._cooldown_block: Optional[int] = None
        self._cooldown_timestamp: Optional[float] = None
        self._day: Optional[str] = None
        self._bought_quote = Decimal("0")
        self._sold_base = Decimal("0")
        self._buys_today = SideTotals()
        self._sells_today = SideTotals()
        self._tradebin_fees_today = Decimal("0")
        self._pool_fees_quote_today = Decimal("0")
        self._gas_baseline = Decimal("0")
        self._last_reference: Optional[Tuple[Decimal, str]] = None
        self._last_skip_reason: Optional[str] = None

    @property
    def connector(self) -> ConnectorBase:
        return self.connectors[self.config.exchange]

    @property
    def pair(self) -> str:
        return self.config.trading_pair

    @property
    def delegate(self) -> BeezeePoolPriceDelegate:
        if self._delegate is None:
            self._delegate = BeezeePoolPriceDelegate(self.connector, self.pair)
        return self._delegate

    # === Clock ===

    def on_tick(self):
        self._roll_day()
        if self._refused is not None or self._halted is not None or not self._check_start():
            return
        blocks = self.connector.new_blocks_seen
        if blocks == self._last_block_seen and self.current_timestamp < self._next_check_timestamp:
            return
        self._last_block_seen = blocks
        self._next_check_timestamp = self.current_timestamp + self.config.check_interval_seconds
        skip_reason = self._run_check()
        if skip_reason != self._last_skip_reason and skip_reason is not None:
            self.log_with_clock(logging.INFO, f"Price bands: {skip_reason}")
        self._last_skip_reason = skip_reason

    def _check_start(self) -> bool:
        """Refuses a band whose floor is not below its ceiling, a pair without the venue the config needs (a pool for
        `venue: pool` or `price_source: pool`, a market for `price_source: book_mid`), a reserve above the balance, and
        a pair without the native token (BZE) on one side, since the fees and the gas are paid in it."""
        if self._started:
            return True
        taker = self._taker_fee_token()
        native = taker[0] if taker is not None else "BZE"
        config = self.config
        missing_venue = self._venue_refusal()
        if config.floor_price >= config.ceiling_price:
            reason = f"floor_price {config.floor_price} is not below ceiling_price {config.ceiling_price}"
        elif missing_venue is not None:
            reason = missing_venue
        elif native not in (self.base, self.quote):
            reason = (
                f"{self.pair} has no {native} side: the fees and the gas are paid in {native} and cannot be counted "
                f"in the pair's terms. Pick a pair with {native} as base or quote."
            )
        else:
            reason = next((
                f"keep_{name}_reserve {fmt(reserve)} is larger than the {token} balance {fmt(balance)}"
                for name, token, reserve in (("base", self.base, config.keep_base_reserve),
                                             ("quote", self.quote, config.keep_quote_reserve))
                for balance in (self.connector.get_balance(token),)
                if reserve > balance
            ), None)
        if reason is not None:
            self._refused = reason
            self.log_with_clock(logging.ERROR, f"Price bands refuses to start: {reason}")
            return False
        self._native = native
        self._started = True
        return True

    def _venue_refusal(self) -> Optional[str]:
        config = self.config
        checks = []
        if config.venue == POOL:
            checks.append(((VENUE_POOL,), "price bands with venue: pool"))
        if config.price_source == POOL:
            checks.append(((VENUE_POOL,), "price bands with price_source: pool"))
        elif config.price_source == BOOK_MID:
            checks.append(((VENUE_BOOK,), "price bands with price_source: book_mid"))
        for needed, user in checks:
            refusal = venue_refusal(self.connector, self.pair, needed, user)
            if refusal is not None:
                return refusal
        if not self.venues:
            return f"{self.pair} has neither a liquidity pool nor an order-book market"
        return None

    @property
    def venues(self) -> Tuple[str, ...]:
        return tuple(self.connector.trading_pair_venues(self.pair))

    def _roll_day(self):
        day = utc_day(self.current_timestamp)
        if day == self._day:
            return
        self._day = day
        self._buys_today = SideTotals()
        self._sells_today = SideTotals()
        self._tradebin_fees_today = Decimal("0")
        self._pool_fees_quote_today = Decimal("0")
        self._gas_baseline = self.connector.gas_fees_paid()
        if self.config.budget_period == DAILY:
            self._bought_quote = Decimal("0")
            self._sold_base = Decimal("0")

    # === One check ===

    def _run_check(self) -> Optional[str]:
        """Sends one trade when the price is outside the band. Returns why it did not, or None."""
        if self._pending is not None:
            return "waiting for the last trade to settle"
        pool = self.connector.get_pool(self.pair)
        if pool is not None and pool.halted:
            self._set_halted(f"the pool {pool.pool_id} of {self.pair} is halted")
            return "pair halted"
        reference = self.reference_price()
        self._last_reference = reference
        if reference is None:
            return "no reference price (empty pool or book)"
        price, _ = reference
        if self.config.floor_price <= price <= self.config.ceiling_price:
            return None
        side = BUY if price < self.config.floor_price else SELL
        cooldown = self._cooldown_left()
        if cooldown is not None:
            return cooldown
        plan = self.plan_trade(side)
        if isinstance(plan, str):
            return plan
        return self._send(plan)

    def reference_price(self) -> Optional[Tuple[Decimal, str]]:
        """(price, source): the pool price through the pool price delegate, or the order-book mid. By default the pool
        when the pair has one, else the book mid."""
        source = self.config.price_source or (POOL if VENUE_POOL in self.venues else BOOK_MID)
        if source == POOL:
            price = self.delegate.get_price_by_type(PriceType.MidPrice)
            return None if price.is_nan() else (price, POOL)
        bid, ask = self._best_bid_ask()
        return None if bid is None or ask is None else ((bid + ask) / 2, BOOK_MID)

    def _set_halted(self, why: str):
        if self._halted is None:
            self._halted = why
            self.log_with_clock(logging.WARNING, f"Price bands stops trading {self.pair}: {why}. Restart it once the "
                                                 f"halt is lifted.")

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

    def plan_trade(self, side: str):
        """The trade that pushes the price back towards the band on `side`, or why there is none. Its input is
        capped at the per-trade maximum, the budget left in the period and the balance above the kept reserve
        (less the fees when the input is BZE). The venue then cuts it at the band edge and at `max_impact_pct`."""
        buy = side == BUY
        config = self.config
        token = self.quote if buy else self.base
        budget_left = config.buy_budget_quote - self._bought_quote if buy else config.sell_budget_base - self._sold_base
        if budget_left <= 0:
            period = "today" if config.budget_period == DAILY else "for this run"
            return f"{'buy' if buy else 'sell'} budget spent {period}"
        native_cost = self._taker_fee() + config.estimated_gas_bze
        if self._native != token and self.connector.get_available_balance(self._native) - self._native_reserve() < native_cost:
            return f"not enough {self._native} above the reserve for the fees ({fmt(native_cost)})"
        reserve = config.keep_quote_reserve if buy else config.keep_base_reserve
        spendable = self.connector.get_available_balance(token) - reserve - (native_cost if token == self._native else 0)
        caps = [
            (config.max_trade_quote if buy else config.max_trade_base, "max trade"),
            (budget_left, "budget"),
            (spendable, "balance above the reserve"),
        ]
        cap, limit = min(caps, key=lambda item: item[0])
        if cap <= 0:
            return f"no {token} above the reserve"
        plans: List[TradePlan] = []
        reasons: List[str] = []
        venues = [POOL] if config.venue == POOL else [venue for venue in (POOL, BOOK) if venue in self.venues]
        for venue in venues:
            plan = self._plan_pool(side, cap, limit) if venue == POOL else self._plan_book(side, cap, limit)
            if isinstance(plan, TradePlan):
                plans.append(plan)
            else:
                reasons.append(f"{venue}: {plan}")
        if not plans:
            return "; ".join(reasons)
        return min(plans, key=self._cost_per_base) if buy else max(plans, key=self._proceeds_per_base)

    def _plan_pool(self, side: str, cap: Decimal, limit: str):
        buy = side == BUY
        pool = self.connector.get_pool(self.pair)
        reserves = self.connector.get_pool_reserves(self.pair)
        spot = self.connector.get_pool_price(self.pair)
        if pool is None:
            return f"{self.pair} has no liquidity pool"
        if reserves is None or spot is None or reserves[0] <= 0 or reserves[1] <= 0:
            return "the pool is empty"
        to_band = self._pool_input_to_band(side, reserves)
        if isinstance(to_band, str):
            return to_band
        if to_band < cap:
            cap, limit = to_band, "band edge"
        rules = self.connector.trading_rules[self.pair]
        step = rules.min_quote_amount_increment if buy else rules.min_base_amount_increment
        quotes: Dict[Decimal, Optional[Tuple[Decimal, Decimal]]] = {}

        def impact(amount: Decimal) -> Optional[Tuple[Decimal, Decimal]]:
            """(output, impact %) of swapping `amount`, None when the pool cannot."""
            if amount not in quotes:
                quote = self.connector.quote_swap(self.pair, side, amount)
                if not quote.ok or quote.output is None or quote.output <= 0:
                    quotes[amount] = None
                else:
                    effective = amount / quote.output if buy else quote.output / amount
                    worse_by = effective - spot if buy else spot - effective
                    quotes[amount] = (quote.output, worse_by / spot * 100)
            return quotes[amount]

        def within(amount: Decimal) -> bool:
            result = impact(amount)
            return result is not None and result[1] <= self.config.max_impact_pct

        amount = floor_to(cap, step)
        if amount <= 0:
            return f"{limit} rounds to zero"
        if not within(amount):
            amount = largest(amount, step, within)
            limit = "impact cap"
            if amount <= 0:
                return f"impact cap: even the smallest swap moves the pool more than {self.config.max_impact_pct}%"
        output, impact_pct = impact(amount)
        base, quote = (output, amount) if buy else (amount, output)
        return TradePlan(venue=POOL, side=side, amount=amount, base=base, quote=quote, impact_pct=impact_pct, limit=limit)

    def _pool_input_to_band(self, side: str, reserves: Tuple[Decimal, Decimal]):
        """The input that brings the pool price to the floor (buy) or the ceiling (sell), from the constant product:
        a buy takes base out until reserve_quote / reserve_base = floor, a sell takes quote out until it equals the
        ceiling. The input is the connector's `quote_input_for_output` for that output."""
        base_reserve, quote_reserve = reserves
        product = base_reserve * quote_reserve
        if side == BUY:
            output = base_reserve - (product / self.config.floor_price).sqrt()
        else:
            output = quote_reserve - (product * self.config.ceiling_price).sqrt()
        edge = "floor" if side == BUY else "ceiling"
        if output <= 0:
            return f"the pool price is already at the {edge}"
        quote = self.connector.quote_input_for_output(self.pair, side, output)
        if not quote.ok:
            return f"the pool cannot reach the {edge}: {quote.error}"
        return quote.input

    def _plan_book(self, side: str, cap: Decimal, limit: str):
        """Takes the book levels best first, no worse than the band edge (asks up to the floor on a buy, bids down to
        the ceiling on a sell), each only as far as the whole trade stays within `cap` (its input) and its average
        price within `max_impact_pct` of the best level."""
        buy = side == BUY
        bid, ask = self._best_bid_ask()
        best = ask if buy else bid
        if best is None:
            return f"no {'asks' if buy else 'bids'} on the book"
        edge = self.config.floor_price if buy else self.config.ceiling_price
        max_base = cap / best if buy else cap
        try:
            levels = self.connector.levels_to_take(self.pair, side, max_base, edge)
        except BeezeeFillError as error:
            return f"cannot read the book levels: {error}"
        if not levels:
            return f"no {'ask at or below the floor' if buy else 'bid at or above the ceiling'} {edge}"
        first = levels[0][0]
        limit_price = first * (1 + self.config.max_impact_pct / 100) if buy else \
            first * (1 - self.config.max_impact_pct / 100)
        step = self.connector.trading_rules[self.pair].min_base_amount_increment
        taken: List[Tuple[Decimal, Decimal]] = []
        cum_base, cum_quote = Decimal("0"), Decimal("0")
        cut = False
        for price, amount in levels:

            def within_cap(part: Decimal) -> bool:
                return cum_quote + price * part <= cap if buy else cum_base + part <= cap

            def ok(part: Decimal) -> bool:
                average = (cum_quote + price * part) / (cum_base + part)
                return within_cap(part) and (average <= limit_price if buy else average >= limit_price)

            part = largest(amount, step, ok)
            cut = part < amount
            if cut and within_cap(amount):
                limit = "impact cap"
            if part <= 0 or part < self.connector.minimum_order_amount(self.pair, price):
                break
            taken.append((price, part))
            cum_base += part
            cum_quote += price * part
            if cut:
                break
        if not taken:
            return f"no book level within the {limit} and above the chain minimum"
        if not cut and (cum_quote < cap if buy else cum_base < cap):
            # Every level up to the band edge was taken in full.
            limit = "band edge"
        average = cum_quote / cum_base
        impact_pct = ((average - first) if buy else (first - average)) / first * 100
        return TradePlan(venue=BOOK, side=side, amount=cum_quote if buy else cum_base, base=cum_base, quote=cum_quote,
                         impact_pct=impact_pct, limit=limit, levels=taken)

    def _fixed_cost_quote(self, price: Decimal) -> Decimal:
        """One taker fee + the gas estimate, in quote: both venues pay them once per trade."""
        return self.to_quote(self._taker_fee() + self.config.estimated_gas_bze, price)

    def _cost_per_base(self, plan: TradePlan) -> Decimal:
        return (plan.quote + self._fixed_cost_quote(plan.price)) / plan.base

    def _proceeds_per_base(self, plan: TradePlan) -> Decimal:
        return (plan.quote - self._fixed_cost_quote(plan.price)) / plan.base

    def _native_reserve(self) -> Decimal:
        return self.config.keep_quote_reserve if self._native == self.quote else self.config.keep_base_reserve

    # === Sending ===

    def _send(self, plan: TradePlan) -> Optional[str]:
        gas_before = self.connector.gas_fees_paid()
        try:
            if plan.venue == POOL:
                order_ids = [self.connector.amm_swap(self.pair, plan.side, plan.amount, self.config.slippage_pct)]
            else:
                order_ids = self.connector.fill_orders(self.pair, plan.side, plan.levels)
        except (BeezeeSwapError, BeezeeFillError) as error:
            if "halted" in str(error).lower():
                self._set_halted(str(error))
                return "pair halted"
            return f"trade refused up front: {error}"
        self._pending = PendingTrade(plan=plan, order_ids=list(order_ids), gas_before=gas_before)
        verb = "Buying" if plan.side == BUY else "Selling"
        venue = "on the pool" if plan.venue == POOL else f"on the book ({len(plan.levels)} level(s))"
        self.log_with_clock(
            logging.INFO,
            f"{verb} {fmt(plan.base)} {self.base} for {plan.quote:.10g} {self.quote} {venue} at {plan.price:.10g}, "
            f"impact {plan.impact_pct:.3f}%, sized by {plan.limit}.",
        )
        return None

    # === Outcomes ===

    def did_fill_order(self, event: OrderFilledEvent):
        pending = self._pending
        if pending is not None and pending.plan.venue == BOOK and event.order_id in pending.order_ids:
            pending.filled_base += event.amount
            pending.filled_quote += event.amount * event.price

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
        pending = self._pending
        if pending is None or order_id not in pending.order_ids:
            return
        pending.done.add(order_id)
        if failure is not None:
            pending.failure = failure
        if pending.done >= set(pending.order_ids):
            self._pending = None
            self._settle(pending)

    def _settle(self, pending: PendingTrade):
        self._roll_day()
        self._start_cooldown()
        plan = pending.plan
        if pending.failure is not None:
            if "halted" in pending.failure.lower():
                self._set_halted(pending.failure)
            if plan.venue == POOL or pending.filled_base <= 0:
                # One swap message: a failure means nothing was swapped.
                self.log_with_clock(logging.WARNING, f"Price bands trade failed, nothing traded: {pending.failure}")
                return
            # The book entries settle one by one: the ones that filled before another failed still traded.
            self.log_with_clock(
                logging.WARNING,
                f"Price bands {plan.side} on the book partly failed ({pending.failure}): booking the "
                f"{fmt(pending.filled_base)} {self.base} it filled.",
            )
        if plan.venue == POOL:
            result = self.connector.get_swap_result(pending.order_ids[0])
            if result is None:
                base, quote, pool_fee_quote = plan.base, plan.quote, Decimal("0")
                self.log_with_clock(logging.WARNING, "Swap settled without a result: booked as quoted.")
            else:
                base, quote = (result.output, result.input) if plan.side == BUY else (result.input, result.output)
                pool_fee_quote = result.pool_fee if plan.side == BUY else result.pool_fee * result.price
        else:
            base, quote, pool_fee_quote = pending.filled_base, pending.filled_quote, Decimal("0")
        totals = self._buys_today if plan.side == BUY else self._sells_today
        totals.trades += 1
        totals.base += base
        totals.quote += quote
        if plan.side == BUY:
            self._bought_quote += quote
        else:
            self._sold_base += base
        # The tradebin taker fee is paid once per message, even by a fill that matched nothing.
        self._tradebin_fees_today += self._taker_fee()
        self._pool_fees_quote_today += pool_fee_quote
        self.log_with_clock(
            logging.INFO,
            f"Price bands {plan.side} settled: {fmt(base)} {self.base} for {quote:.10g} {self.quote}. Budget left: "
            f"buy {fmt(self.buy_budget_left)} {self.quote}, sell {fmt(self.sell_budget_left)} {self.base}.",
        )

    # === Fees ===

    def _taker_fee_token(self) -> Optional[Tuple[str, Decimal]]:
        fee = self.connector.get_fee(self.base, self.quote, OrderType.LIMIT, TradeType.BUY, Decimal("0"), Decimal("0"),
                                     is_maker=False)
        flat = fee.flat_fees
        return (flat[0].token, sum((item.amount for item in flat), Decimal("0"))) if flat else None

    def _taker_fee(self) -> Decimal:
        taker = self._taker_fee_token()
        return taker[1] if taker is not None else Decimal("0")

    def to_quote(self, native_amount: Decimal, price: Decimal) -> Decimal:
        """A BZE amount in the pair's quote: 1:1 when BZE is the quote, at the pair's own price when it is the base."""
        return native_amount if self._native == self.quote else native_amount * price

    # === Status ===

    @property
    def buy_budget_left(self) -> Decimal:
        return max(self.config.buy_budget_quote - self._bought_quote, Decimal("0"))

    @property
    def sell_budget_left(self) -> Decimal:
        return max(self.config.sell_budget_base - self._sold_base, Decimal("0"))

    @property
    def gas_today(self) -> Decimal:
        return self.connector.gas_fees_paid() - self._gas_baseline

    def format_status(self) -> str:
        if self._refused is not None:
            return f"  Refused: {self._refused}"
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        config = self.config
        native = self._native or "BZE"
        reference = self.reference_price()
        if reference is None:
            where = "-"
        else:
            price, source = reference
            position = "below the floor" if price < config.floor_price else \
                "above the ceiling" if price > config.ceiling_price else "inside the band"
            where = f"{price:.10g} ({source}), {position}"
        period = "today (UTC)" if config.budget_period == DAILY else "this run"
        lines = [
            f"  Pair: {self.pair}{f' (HALTED: {self._halted})' if self._halted else ''}   venue: {config.venue}   "
            f"venues: {describe_venues(self.venues)}",
            f"  Reference price: {where}",
            f"  Band: floor {fmt(config.floor_price)} / ceiling {fmt(config.ceiling_price)} {self.quote}",
            f"  Buy budget {period}: used {fmt(self._bought_quote)} / {fmt(config.buy_budget_quote)} {self.quote}, "
            f"left {fmt(self.buy_budget_left)}",
            f"  Sell budget {period}: used {fmt(self._sold_base)} / {fmt(config.sell_budget_base)} {self.base}, "
            f"left {fmt(self.sell_budget_left)}",
            f"  Reserves kept: {fmt(config.keep_base_reserve)} {self.base}, {fmt(config.keep_quote_reserve)} {self.quote}",
        ]
        for name, totals in (("Buys", self._buys_today), ("Sells", self._sells_today)):
            average = totals.average_price
            lines.append(
                f"  {name} today: {totals.trades}, {fmt(totals.base)} {self.base} for {totals.quote:.10g} {self.quote}"
                f"{f', average {average:.10g}' if average is not None else ''}"
            )
        lines.append(
            f"  Fees today: tradebin {fmt(self._tradebin_fees_today)} {native}, gas {fmt(self.gas_today)} {native}, "
            f"pool {self._pool_fees_quote_today:.6g} {self.quote}"
        )
        if self._pending is not None:
            plan = self._pending.plan
            lines.append(f"  In flight: {plan.side} of {fmt(plan.base)} {self.base} on the {plan.venue}")
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
