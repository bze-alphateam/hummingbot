import logging
import math
import os
from collections import Counter
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Callable, Dict, List, Literal, Optional, Set, Tuple

from pydantic import Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange.beezee.beezee_amm import BeezeeSwapError
from hummingbot.connector.exchange.beezee.beezee_fills import BeezeeFillError
from hummingbot.connector.exchange.beezee.beezee_pool_price_delegate import BeezeePoolPriceDelegate
from hummingbot.connector.exchange.beezee.beezee_utils import VENUE_POOL, describe_venues, venue_refusal
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
# A slice never trades more than this many even slices, however much catch-up is owed.
MAX_SLICE_MULTIPLE = Decimal("2")


class BeezeeTreasuryTwapConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    controllers_config: List[str] = []
    exchange: str = Field("beezee")
    trading_pair: str = Field("TOKEN-BZE")
    # "buy" buys the base back with the quote, "sell" sells the base down for the quote.
    side: Literal["buy", "sell"] = Field("buy")
    # The base amount to buy or sell over the run.
    total_amount: Decimal = Field(gt=0)
    # The run lasts this long; one slice is due every `slice_interval_seconds` within it.
    duration_hours: Decimal = Field(gt=0)
    slice_interval_seconds: int = Field(300, gt=0)
    # A slice's own price impact (its average price against the venue's price before it, pool fee included), in %.
    max_impact_pct: Decimal = Field(Decimal("0.5"), gt=0)
    # Never buy above / sell below this price (quote per base, fees not included). Empty = no limit.
    limit_price: Optional[Decimal] = Field(None, gt=0)
    # Where a slice goes: the pool, taking the book, or the cheaper of both (never split across both). A pool and an
    # order-book market are separate venues: "best" uses whatever venue the pair has and compares only when it has both.
    venue: Literal["pool", "book", "best"] = Field("best")
    # A pool swap is refused by the chain when it would pay out more than this % less than quoted.
    slippage_pct: Decimal = Field(Decimal("0.5"), ge=0, lt=100)
    # Balances the script never spends.
    keep_quote_reserve: Decimal = Field(Decimal("0"), ge=0)
    keep_base_reserve: Decimal = Field(Decimal("0"), ge=0)
    # true: what earlier slices did not trade (impact cap, limit price, ...) is spread over the slices left, each at
    # most twice the even slice. false: it is left over and reported at the end.
    catch_up: bool = Field(True)
    # Gas of one trade tx, in BZE, kept free on the BZE balance and counted when comparing venues.
    estimated_gas_bze: Decimal = Field(Decimal("0.005"), ge=0)

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets[self.exchange] = markets.get(self.exchange, set()) | {self.trading_pair}
        return markets


@dataclass
class SlicePlan:
    """One slice on one venue, display units. `amount` is its input: quote on a buy, base on a sell. `base` and
    `quote` are what it moves (quote spent on a buy, received on a sell). `levels` are the book levels taken, empty
    on the pool. `limit` names what sized it."""
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
class PendingSlice:
    plan: SlicePlan
    order_ids: List[str]
    filled_base: Decimal = Decimal("0")
    filled_quote: Decimal = Decimal("0")
    done: Set[str] = field(default_factory=set)
    failure: Optional[str] = None


def fmt(amount: Decimal) -> str:
    """An amount without trailing zeros or an exponent: 400.000000 -> 400."""
    return f"{amount.normalize():f}"


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


class BeezeeTreasuryTwap(StrategyV2Base):
    """
    Treasury TWAP for one BeeZee pair: buys back (`side: buy`) or sells down (`side: sell`) `total_amount` of the base
    over `duration_hours`, one slice every `slice_interval_seconds`. Each slice is the amount left over the slices
    left, cut to its own price impact (`max_impact_pct`), `limit_price` and the balance above the kept reserve, and
    goes whole to the cheaper venue (pool or book) under `venue: best`. A slice that cannot trade within those limits
    waits for the next interval instead of trading bigger. The pair is traded in its own terms: nothing is routed
    through other pools. At completion, at the deadline or on a halted pair the script stops and logs a summary;
    whatever is left over is reported, never forced through.
    """

    def __init__(self, connectors: Dict[str, ConnectorBase], config: BeezeeTreasuryTwapConfig):
        super().__init__(connectors, config)
        self.config = config
        self.base, self.quote = config.trading_pair.split("-", 1)
        self._native: Optional[str] = None
        self._started = False
        self._refused: Optional[str] = None
        self._finished: Optional[str] = None
        self._delegate: Optional[BeezeePoolPriceDelegate] = None
        self._start_timestamp = 0.0
        self._next_slice = 0
        self._pending: Optional[PendingSlice] = None
        self._done_base = Decimal("0")
        self._done_quote = Decimal("0")
        # Base the slices did not trade (skipped or cut): made up later only with catch_up.
        self._forgone_base = Decimal("0")
        self._slices_traded: Counter = Counter()
        self._skips: Counter = Counter()
        self._last_skip: Optional[str] = None
        self._reference_sum = Decimal("0")
        self._reference_samples = 0
        self._tradebin_fees = Decimal("0")
        self._pool_fees_quote = Decimal("0")
        self._gas_baseline = Decimal("0")

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

    @property
    def slice_count(self) -> int:
        seconds = self.config.duration_hours * 3600 / self.config.slice_interval_seconds
        return max(int(seconds.to_integral_value(rounding=ROUND_CEILING)), 1)

    @property
    def even_slice(self) -> Decimal:
        return self.config.total_amount / self.slice_count

    @property
    def deadline(self) -> float:
        return self._start_timestamp + float(self.config.duration_hours * 3600)

    @property
    def remaining(self) -> Decimal:
        return max(self.config.total_amount - self._done_base, Decimal("0"))

    # === Clock ===

    def on_tick(self):
        if self._refused is not None or self._finished is not None or not self._check_start():
            return
        if self._pending is not None:
            return
        if self.remaining < self._base_step():
            self._finish("completed")
            return
        if self.current_timestamp >= self.deadline:
            self._finish("deadline reached")
            return
        index = int((self.current_timestamp - self._start_timestamp) // self.config.slice_interval_seconds)
        if index < self._next_slice:
            return
        for missed in range(self._next_slice, index):
            self._skip(self.slice_size(missed), "the slice before was still settling")
        self._next_slice = index + 1
        self._run_slice(index)

    def _check_start(self) -> bool:
        """Refuses a pair without the configured venue (a pool for `venue: pool`, a market for `venue: book`), a reserve
        above the balance and a pair without the native token (BZE) on one side, since the fees and the gas are paid
        in it."""
        if self._started:
            return True
        taker = self._taker_fee_token()
        native = taker[0] if taker is not None else "BZE"
        config = self.config
        if config.venue != BEST:
            missing_venue = venue_refusal(self.connector, self.pair, (config.venue,), f"treasury TWAP with venue: {config.venue}")
        elif not self.venues:
            missing_venue = f"{self.pair} has neither a liquidity pool nor an order-book market"
        else:
            missing_venue = None
        if missing_venue is not None:
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
            self.log_with_clock(logging.ERROR, f"Treasury TWAP refuses to start: {reason}")
            return False
        self._native = native
        self._started = True
        self._start_timestamp = self.current_timestamp
        self._gas_baseline = self.connector.gas_fees_paid()
        self.log_with_clock(
            logging.INFO,
            f"Treasury TWAP: {config.side} {fmt(config.total_amount)} {self.base} over {fmt(config.duration_hours)} h "
            f"in {self.slice_count} slices of about {self.even_slice:.10g} {self.base}.",
        )
        return True

    @property
    def venues(self) -> Tuple[str, ...]:
        return tuple(self.connector.trading_pair_venues(self.pair))

    # === One slice ===

    def slice_size(self, index: int) -> Decimal:
        """Slice `index`'s base amount: what is left to trade over the slices left, at most twice the even slice.
        Without catch_up, what earlier slices did not trade is not part of what is left."""
        left = self.remaining if self.config.catch_up else self.remaining - self._forgone_base
        slices_left = max(self.slice_count - index, 1)
        size = min(left / slices_left, self.even_slice * MAX_SLICE_MULTIPLE, self.remaining)
        return max(floor_to(size, self._base_step()), Decimal("0"))

    def _run_slice(self, index: int):
        pool = self.connector.get_pool(self.pair)
        if pool is not None and pool.halted:
            self._finish(f"the pool {pool.pool_id} of {self.pair} is halted")
            return
        reference = self.reference_price()
        if reference is not None:
            self._reference_sum += reference[0]
            self._reference_samples += 1
        size = self.slice_size(index)
        if size <= 0:
            self._skip(Decimal("0"), "nothing left to trade (catch_up is off)")
            return
        plan = self.plan_slice(size)
        if isinstance(plan, str):
            self._skip(size, plan)
            return
        refusal = self._send(plan, index)
        if self._finished is not None:
            return
        if refusal is not None:
            self._skip(size, refusal)
        elif plan.base < size:
            self._forgone_base += size - plan.base

    def reference_price(self) -> Optional[Tuple[Decimal, str]]:
        """(price, source): the pool price through the pool price delegate when the pair has a pool, the order-book
        mid otherwise."""
        if VENUE_POOL in self.venues:
            price = self.delegate.get_price_by_type(PriceType.MidPrice)
            return None if price.is_nan() else (price, POOL)
        bid, ask = self._best_bid_ask()
        return None if bid is None or ask is None else ((bid + ask) / 2, BOOK_MID)

    def _skip(self, size: Decimal, reason: str):
        self._forgone_base += size
        self._skips[reason] += 1
        self._last_skip = reason
        self.log_with_clock(logging.INFO, f"Treasury TWAP skips a slice of {fmt(size)} {self.base}: {reason}")

    # === Sizing ===

    def plan_slice(self, size: Decimal):
        """The slice of at most `size` base on the configured venue, or why there is none. It is cut to the balance
        above the kept reserve (less the fees when the input is BZE), `limit_price` and `max_impact_pct`."""
        buy = self.config.side == BUY
        token = self.quote if buy else self.base
        native_cost = self._taker_fee() + self.config.estimated_gas_bze
        if self._native != token and self.connector.get_available_balance(self._native) - self._native_reserve() < native_cost:
            return f"not enough {self._native} above the reserve for the fees ({fmt(native_cost)})"
        reserve = self.config.keep_quote_reserve if buy else self.config.keep_base_reserve
        spendable = self.connector.get_available_balance(token) - reserve - (native_cost if token == self._native else 0)
        if spendable <= 0:
            return f"no {token} above the reserve"
        plans: List[SlicePlan] = []
        reasons: List[str] = []
        venues = [self.config.venue] if self.config.venue != BEST else [
            venue for venue in (POOL, BOOK) if venue in self.venues
        ]
        for venue in venues:
            plan = self._plan_pool(size, spendable) if venue == POOL else self._plan_book(size, spendable)
            if isinstance(plan, SlicePlan):
                plans.append(plan)
            else:
                reasons.append(f"{venue}: {plan}")
        if not plans:
            return "; ".join(reasons)
        return min(plans, key=self._cost_per_base) if buy else max(plans, key=self._proceeds_per_base)

    def _plan_pool(self, size: Decimal, spendable: Decimal):
        buy = self.config.side == BUY
        side = self.config.side
        reserves = self.connector.get_pool_reserves(self.pair)
        spot = self.connector.get_pool_price(self.pair)
        if self.connector.get_pool(self.pair) is None:
            return f"{self.pair} has no liquidity pool"
        if reserves is None or spot is None or reserves[0] <= 0 or reserves[1] <= 0:
            return "the pool is empty"
        rules = self.connector.trading_rules[self.pair]
        step = rules.min_quote_amount_increment if buy else rules.min_base_amount_increment
        limit_price = self.config.limit_price
        if limit_price is not None and (spot > limit_price if buy else spot < limit_price):
            return f"limit price: the pool price {spot:.10g} is past {fmt(limit_price)}"
        if buy:
            # The quote that buys `size` base; the input of a buy is quote.
            needed = self.connector.quote_input_for_output(self.pair, side, size)
            if not needed.ok:
                return f"the pool cannot pay {fmt(size)} {self.base}: {needed.error}"
            cap, limit = needed.input, "slice"
        else:
            cap, limit = size, "slice"
        if spendable < cap:
            cap, limit = spendable, "balance above the reserve"
        quotes: Dict[Decimal, Optional[Tuple[Decimal, Decimal, Decimal]]] = {}

        def quoted(amount: Decimal) -> Optional[Tuple[Decimal, Decimal, Decimal]]:
            """(output, effective price, impact %) of swapping `amount`, None when the pool cannot."""
            if amount not in quotes:
                quote = self.connector.quote_swap(self.pair, side, amount)
                if not quote.ok or quote.output is None or quote.output <= 0:
                    quotes[amount] = None
                else:
                    effective = amount / quote.output if buy else quote.output / amount
                    worse_by = effective - spot if buy else spot - effective
                    quotes[amount] = (quote.output, effective, worse_by / spot * 100)
            return quotes[amount]

        def within_impact(amount: Decimal) -> bool:
            result = quoted(amount)
            return result is not None and result[2] <= self.config.max_impact_pct

        def within_limit(amount: Decimal) -> bool:
            result = quoted(amount)
            return result is not None and (limit_price is None
                                           or (result[1] <= limit_price if buy else result[1] >= limit_price))

        amount = floor_to(cap, step)
        if amount <= 0:
            return f"the {limit} rounds to zero"
        for ok, name in ((within_impact, "impact cap"), (within_limit, "limit price")):
            if not ok(amount):
                amount = largest(amount, step, ok)
                limit = name
                if amount <= 0:
                    if name == "impact cap":
                        return f"impact cap: even the smallest swap moves the pool more than {self.config.max_impact_pct}%"
                    return f"limit price: even the smallest swap is past {fmt(limit_price)}"
        output, _, impact_pct = quoted(amount)
        base, quote = (output, amount) if buy else (amount, output)
        return SlicePlan(venue=POOL, side=side, amount=amount, base=base, quote=quote, impact_pct=impact_pct, limit=limit)

    def _plan_book(self, size: Decimal, spendable: Decimal):
        """Takes the book levels best first up to `size` base, none priced more than `max_impact_pct` from the best
        level (so the average is within it too) nor past `limit_price`, and on a buy no more quote than `spendable`.
        Levels below the chain minimum are left out."""
        buy = self.config.side == BUY
        side = self.config.side
        bid, ask = self._best_bid_ask()
        best = ask if buy else bid
        if best is None:
            return f"no {'asks' if buy else 'bids'} on the book"
        limit_price = self.config.limit_price
        if limit_price is not None and (best > limit_price if buy else best < limit_price):
            return f"limit price: the best {'ask' if buy else 'bid'} {best:.10g} is past {fmt(limit_price)}"
        impact_edge = best * (1 + self.config.max_impact_pct / 100) if buy else \
            best * (1 - self.config.max_impact_pct / 100)
        edge, edge_name = impact_edge, "impact cap"
        if limit_price is not None and (limit_price < impact_edge if buy else limit_price > impact_edge):
            edge, edge_name = limit_price, "limit price"
        step = self._base_step()
        wanted, limit = size, "slice"
        if not buy and spendable < size:
            wanted, limit = floor_to(spendable, step), "balance above the reserve"
        try:
            levels = self.connector.levels_to_take(self.pair, side, wanted, edge)
        except BeezeeFillError as error:
            return f"cannot read the book levels: {error}"
        taken: List[Tuple[Decimal, Decimal]] = []
        cum_base, cum_quote = Decimal("0"), Decimal("0")
        for price, amount in levels:
            if buy and cum_quote + price * amount > spendable:
                amount, limit = floor_to((spendable - cum_quote) / price, step), "balance above the reserve"
            if amount <= 0 or amount < self.connector.minimum_order_amount(self.pair, price):
                break
            taken.append((price, amount))
            cum_base += amount
            cum_quote += price * amount
        if not taken:
            return f"no book level within the {edge_name} and above the chain minimum"
        if cum_base < wanted and limit == "slice":
            # The book ran out of levels within the edge.
            limit = edge_name
        average = cum_quote / cum_base
        impact_pct = ((average - best) if buy else (best - average)) / best * 100
        return SlicePlan(venue=BOOK, side=side, amount=cum_quote if buy else cum_base, base=cum_base, quote=cum_quote,
                         impact_pct=impact_pct, limit=limit, levels=taken)

    def _fixed_cost_quote(self, price: Decimal) -> Decimal:
        """One taker fee + the gas estimate, in quote: both venues pay them once per slice."""
        return self.to_quote(self._taker_fee() + self.config.estimated_gas_bze, price)

    def _cost_per_base(self, plan: SlicePlan) -> Decimal:
        return (plan.quote + self._fixed_cost_quote(plan.price)) / plan.base

    def _proceeds_per_base(self, plan: SlicePlan) -> Decimal:
        return (plan.quote - self._fixed_cost_quote(plan.price)) / plan.base

    def _native_reserve(self) -> Decimal:
        return self.config.keep_quote_reserve if self._native == self.quote else self.config.keep_base_reserve

    def _base_step(self) -> Decimal:
        return self.connector.trading_rules[self.pair].min_base_amount_increment

    # === Sending ===

    def _send(self, plan: SlicePlan, index: int) -> Optional[str]:
        """Sends the slice. Returns why the connector refused it up front (nothing sent), or None."""
        try:
            if plan.venue == POOL:
                order_ids = [self.connector.amm_swap(self.pair, plan.side, plan.amount, self.config.slippage_pct)]
            else:
                order_ids = self.connector.fill_orders(self.pair, plan.side, plan.levels)
        except (BeezeeSwapError, BeezeeFillError) as error:
            if "halted" in str(error).lower():
                self._finish(str(error))
            return f"refused up front: {error}"
        self._pending = PendingSlice(plan=plan, order_ids=list(order_ids))
        verb = "buying" if plan.side == BUY else "selling"
        venue = "on the pool" if plan.venue == POOL else f"on the book ({len(plan.levels)} level(s))"
        self.log_with_clock(
            logging.INFO,
            f"Treasury TWAP slice {index + 1}/{self.slice_count}: {verb} {fmt(plan.base)} {self.base} for "
            f"{plan.quote:.10g} {self.quote} {venue} at {plan.price:.10g}, impact {plan.impact_pct:.3f}%, sized by "
            f"{plan.limit}.",
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

    def _settle(self, pending: PendingSlice):
        """Books what the slice traded. Whatever it did not (a refund, a failed entry, a failed swap) stays in what is
        left to trade."""
        plan = pending.plan
        if pending.failure is not None:
            halted = "halted" in pending.failure.lower()
            if plan.venue == POOL or pending.filled_base <= 0:
                # One swap message: a failure means nothing was swapped.
                self.log_with_clock(logging.WARNING, f"Treasury TWAP slice failed, nothing traded: {pending.failure}")
                if halted:
                    self._finish(pending.failure)
                return
            # The book entries settle one by one: the ones that filled before another failed still traded.
            self.log_with_clock(
                logging.WARNING,
                f"Treasury TWAP {plan.side} on the book partly failed ({pending.failure}): booking the "
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
        self._done_base += base
        self._done_quote += quote
        self._slices_traded[plan.venue] += 1
        # The tradebin taker fee is paid once per message, even by a fill that matched nothing.
        self._tradebin_fees += self._taker_fee()
        self._pool_fees_quote += pool_fee_quote
        self.log_with_clock(
            logging.INFO,
            f"Treasury TWAP slice settled: {fmt(base)} {self.base} for {quote:.10g} {self.quote} on the {plan.venue}. "
            f"Done {fmt(self._done_base)} / {fmt(self.config.total_amount)} {self.base}.",
        )
        if pending.failure is not None and "halted" in pending.failure.lower():
            self._finish(pending.failure)

    def _finish(self, why: str):
        if self._finished is not None:
            return
        self._finished = why
        self.log_with_clock(logging.INFO, f"Treasury TWAP stopped: {why}.\n{self.summary()}")

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
    def average_price(self) -> Optional[Decimal]:
        return self._done_quote / self._done_base if self._done_base > 0 else None

    @property
    def reference_twap(self) -> Optional[Decimal]:
        """The mean of the reference price sampled at every slice: slices are evenly spaced, so it is time-weighted."""
        return self._reference_sum / self._reference_samples if self._reference_samples else None

    @property
    def gas_paid(self) -> Decimal:
        return self.connector.gas_fees_paid() - self._gas_baseline

    def summary(self) -> str:
        config = self.config
        native = self._native or "BZE"
        average, twap = self.average_price, self.reference_twap
        if average is None:
            execution = "nothing traded"
        else:
            execution = f"average {average:.10g} {self.quote}"
            if twap is not None:
                execution += f" vs reference TWAP {twap:.10g} ({(average - twap) / twap * 100:+.3f}%)"
        venues = ", ".join(f"{venue} {count}" for venue, count in sorted(self._slices_traded.items())) or "none"
        skips = "; ".join(f"{reason} ×{count}" for reason, count in self._skips.most_common()) or "none"
        return "\n".join([
            f"  Pair: {self.pair}   side: {config.side}   venue: {config.venue}   catch_up: {config.catch_up}   "
            f"venues: {describe_venues(self.venues)}",
            f"  Done: {fmt(self._done_base)} / {fmt(config.total_amount)} {self.base} "
            f"({self._done_base / config.total_amount * 100:.2f}%) for {self._done_quote:.10g} {self.quote}, "
            f"left {fmt(self.remaining)} {self.base}",
            f"  Execution: {execution}",
            f"  Slices traded: {venues}; skipped: {sum(self._skips.values())} ({skips})",
            f"  Fees: tradebin {fmt(self._tradebin_fees)} {native}, gas {fmt(self.gas_paid)} {native}, "
            f"pool {self._pool_fees_quote:.6g} {self.quote}",
            f"  Reserves kept: {fmt(config.keep_base_reserve)} {self.base}, {fmt(config.keep_quote_reserve)} {self.quote}",
        ])

    def format_status(self) -> str:
        if self._refused is not None:
            return f"  Refused: {self._refused}"
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        if not self._started:
            return "  Not started yet."
        duration = float(self.config.duration_hours * 3600)
        elapsed = min(max(self.current_timestamp - self._start_timestamp, 0.0), duration)
        lines = [self.summary(), f"  Time: {elapsed / duration * 100:.1f}% elapsed, slice {min(self._next_slice, self.slice_count)}"
                                 f"/{self.slice_count} sent or skipped"]
        if self._finished is not None:
            lines.insert(0, f"  STOPPED: {self._finished}")
        elif self._pending is not None:
            plan = self._pending.plan
            lines.append(f"  In flight: {plan.side} of {fmt(plan.base)} {self.base} on the {plan.venue}")
        elif self._next_slice < self.slice_count:
            size = self.slice_size(self._next_slice)
            plan = self.plan_slice(size) if size > 0 else "nothing left to trade"
            venue = f"{plan.venue}, {fmt(plan.base)} {self.base} after the caps" if isinstance(plan, SlicePlan) else \
                f"would skip: {plan}"
            lines.append(f"  Next slice: {fmt(size)} {self.base} ({venue})")
        if self._last_skip:
            lines.append(f"  Last skip: {self._last_skip}")
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
