import logging
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_FLOOR, Decimal
from typing import Dict, List, Optional, Set, Tuple

from pydantic import Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange.beezee.beezee_amm import BeezeeSwapError, BeezeeSwapRequest, BeezeeSwapResult
from hummingbot.connector.exchange.beezee.beezee_utils import VENUE_POOL, describe_venues, venue_refusal
from hummingbot.core.data_type.common import MarketDict, OrderType, TradeType
from hummingbot.core.event.events import BuyOrderCompletedEvent, MarketOrderFailureEvent, SellOrderCompletedEvent
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase

SECONDS_PER_DAY = 86400
BUY = "buy"
SELL = "sell"


class BeezeePoolVolumeConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    controllers_config: List[str] = []
    exchange: str = Field("beezee")
    trading_pair: str = Field("BZE-USDC")
    # Volume to print per UTC day on the pair's pool, in quote units. Both legs of a round trip count.
    daily_volume_quote: Decimal = Field(Decimal("1000"), gt=0)
    cycle_seconds: int = Field(300, gt=0)
    # Bounds of one leg, in quote units (the first leg's input, valued at the pool price when it is base).
    min_swap_amount: Decimal = Field(Decimal("0"), ge=0)
    max_swap_amount: Decimal = Field(Decimal("1000000"), gt=0)
    # The first leg spends at most this % of the pool's reserve of the token it puts in.
    max_reserve_share_pct: Decimal = Field(Decimal("0.5"), gt=0, le=100)
    # Balances the script never spends.
    reserve_base: Decimal = Field(Decimal("0"), ge=0)
    reserve_quote: Decimal = Field(Decimal("0"), ge=0)
    # Each leg is refused by the chain when it would pay out more than this % less than quoted.
    slippage_pct: Decimal = Field(Decimal("0.5"), ge=0, lt=100)
    # Leg 2 swaps back this % less than leg 1's quoted output (and never more than leg 1's minimum output).
    safety_pct: Decimal = Field(Decimal("0.5"), ge=0, lt=100)
    # Pool fees + tradebin taker fees + gas per UTC day, in BZE. Reaching it stops the script until UTC midnight.
    max_daily_fee_spend: Decimal = Field(Decimal("100"), gt=0)
    # A cycle is skipped when the pool price moved more than this % since the last cycle.
    max_price_move_pct: Decimal = Field(Decimal("2"), gt=0)
    # Gas of one round-trip tx, in BZE, kept free on the BZE balance (the real gas is in the status).
    estimated_gas_bze: Decimal = Field(Decimal("0.005"), ge=0)

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets[self.exchange] = markets.get(self.exchange, set()) | {self.trading_pair}
        return markets


@dataclass
class RoundTripPlan:
    """Both legs of one round trip, display units. Leg 1 puts `first_input` of its token in and is quoted
    `first_output`; leg 2 puts `second_input` of that output back. `leg_quote` is leg 1 in quote, `reserve_share_pct`
    its share of the pool's reserve of the input token."""
    first_side: str
    first_input: Decimal
    first_output: Decimal
    second_input: Decimal
    leg_quote: Decimal
    reserve_share_pct: Decimal

    @property
    def second_side(self) -> str:
        return SELL if self.first_side == BUY else BUY


@dataclass
class PendingRoundTrip:
    plan: RoundTripPlan
    swap_ids: List[str]
    done: Set[str] = field(default_factory=set)
    failure: Optional[str] = None


def fmt(amount: Decimal) -> str:
    """An amount without trailing zeros or an exponent: 400.000000 -> 400."""
    return f"{amount.normalize():f}"


def utc_day(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")


def floor_to(amount: Decimal, step: Decimal) -> Decimal:
    return (amount / step).to_integral_value(rounding=ROUND_FLOOR) * step


class BeezeePoolVolume(StrategyV2Base):
    """
    Prints a daily volume on one BeeZee pair's liquidity pool with round trips: every `cycle_seconds` it swaps one
    token of the pair for the other and back again, both legs in one tx (two MsgMultiSwap). The pair is traded in its
    own terms on its own pool, nothing else. Leg 1 is sized from the volume left today over the cycles left, capped at
    a share of the pool's reserves and at the balance above the reserves; leg 2 swaps leg 1's output back. The token
    that goes first alternates every cycle, so the small leftover of leg 1 does not pile up on one side. Apart from
    the pool fees, the tradebin taker fees and the gas, the inventory stays where it was.
    """

    def __init__(self, connectors: Dict[str, ConnectorBase], config: BeezeePoolVolumeConfig):
        super().__init__(connectors, config)
        self.config = config
        self.base, self.quote = config.trading_pair.split("-", 1)
        self._native: Optional[str] = None
        self._refused: Optional[str] = None
        self._halted = False
        self._next_cycle_timestamp = 0.0
        self._buy_first = True
        self._last_price: Optional[Decimal] = None
        self._pending: Optional[PendingRoundTrip] = None
        self._day: Optional[str] = None
        self._volume_today = Decimal("0")
        self._round_trips_today = 0
        self._pool_fees_quote_today = Decimal("0")
        self._pool_fees_native_today = Decimal("0")
        self._tradebin_fees_today = Decimal("0")
        self._gas_baseline = Decimal("0")
        self._base_drift_today = Decimal("0")
        self._quote_drift_today = Decimal("0")
        self._last_skip_reason: Optional[str] = None

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
        if self.current_timestamp < self._next_cycle_timestamp:
            return
        self._next_cycle_timestamp = self.current_timestamp + self.config.cycle_seconds
        skip_reason = self._run_cycle()
        if skip_reason != self._last_skip_reason and skip_reason is not None:
            self.log_with_clock(logging.INFO, f"Pool volume cycle skipped: {skip_reason}")
        self._last_skip_reason = skip_reason

    def _check_pair(self) -> bool:
        """The pair needs a liquidity pool (an order-book market is not needed). Tradebin fees and gas are paid in the
        native token (BZE), so one side of the pair must be it."""
        if self._native is not None:
            return True
        taker = self._taker_fee_token()
        native = taker[0] if taker is not None else "BZE"
        no_pool = venue_refusal(self.connector, self.pair, (VENUE_POOL,), "pool volume")
        if no_pool is not None:
            self._refused = no_pool
        elif native not in (self.base, self.quote):
            self._refused = (
                f"{self.pair} has no {native} side: the fees and the gas are paid in {native} and cannot be counted "
                f"in the pair's terms. Pick a pair with {native} as base or quote."
            )
        if self._refused is not None:
            self.log_with_clock(logging.ERROR, f"Pool volume refuses to start: {self._refused}")
            return False
        self._native = native
        return True

    def _roll_day(self):
        day = utc_day(self.current_timestamp)
        if day == self._day:
            return
        self._day = day
        self._volume_today = Decimal("0")
        self._round_trips_today = 0
        self._pool_fees_quote_today = Decimal("0")
        self._pool_fees_native_today = Decimal("0")
        self._tradebin_fees_today = Decimal("0")
        self._gas_baseline = self.connector.gas_fees_paid()
        self._base_drift_today = Decimal("0")
        self._quote_drift_today = Decimal("0")

    # === One cycle ===

    def _run_cycle(self) -> Optional[str]:
        """Sends one round trip. Returns why it did not, or None."""
        if self._pending is not None:
            return "waiting for the last round trip to settle"
        if self.spend_today >= self.config.max_daily_fee_spend:
            return f"daily fee cap reached ({self.spend_today:.6f} >= {self.config.max_daily_fee_spend} BZE)"
        remaining = self.remaining_quote
        if remaining <= 0:
            return "daily volume target reached, idle until UTC midnight"
        pool = self.connector.get_pool(self.pair)
        if pool is None:
            return f"{self.pair} has no liquidity pool"
        if pool.halted:
            self._set_halted(f"the pool {pool.pool_id} of {self.pair} is halted")
            return "pool halted"
        price = self.connector.get_pool_price(self.pair)
        reserves = self.connector.get_pool_reserves(self.pair)
        if price is None or reserves is None:
            return "the pool is empty"
        last_price, self._last_price = self._last_price, price
        if last_price is not None:
            move_pct = abs(price - last_price) / last_price * 100
            if move_pct > self.config.max_price_move_pct:
                return (
                    f"pool price moved {move_pct:.3f}% since the last cycle ({last_price:.10g} -> {price:.10g}, "
                    f"max {self.config.max_price_move_pct}%)"
                )
        size = self.swap_size(remaining)
        first = BUY if self._buy_first else SELL
        reasons: List[str] = []
        for side in (first, SELL if first == BUY else BUY):
            plan = self.plan_round_trip(side, size, price, reserves)
            if isinstance(plan, RoundTripPlan):
                return self._send(plan)
            reasons.append(plan)
        return "; ".join(reasons)

    def _set_halted(self, why: str):
        if not self._halted:
            self._halted = True
            self.log_with_clock(logging.WARNING, f"Pool volume stops trading {self.pair}: {why}.")

    # === Sizing ===

    def cycles_left_today(self) -> int:
        seconds_left = SECONDS_PER_DAY - (int(self.current_timestamp) % SECONDS_PER_DAY)
        return max(1, math.ceil(seconds_left / self.config.cycle_seconds))

    def swap_size(self, remaining_quote: Decimal) -> Decimal:
        """One leg in quote units: the volume left today over the cycles left, halved since both legs count, clamped
        to [min_swap_amount, max_swap_amount]."""
        size = remaining_quote / self.cycles_left_today() / 2
        return min(max(size, self.config.min_swap_amount), self.config.max_swap_amount)

    def plan_round_trip(self, first_side: str, size_quote: Decimal, price: Decimal,
                        reserves: Tuple[Decimal, Decimal]):
        """The round trip that starts with a `first_side` swap of `size_quote` (in quote), or why there is none. Leg 1
        is capped at `max_reserve_share_pct` of the pool's reserve of its input token and at the balance above the
        configured reserve (less the fees on the BZE side). Leg 2 puts leg 1's quoted output less `safety_pct` back,
        never more than leg 1's minimum output, so it never spends what the wallet held before the tx."""
        buy = first_side == BUY
        token, reserve, pool_reserve = (self.quote, self.config.reserve_quote, reserves[1]) if buy else \
            (self.base, self.config.reserve_base, reserves[0])
        native_cost = 2 * self._taker_fee() + self.config.estimated_gas_bze
        if self._native not in (token, None):
            native_reserve = self.config.reserve_quote if self._native == self.quote else self.config.reserve_base
            if self.connector.get_available_balance(self._native) - native_reserve < native_cost:
                return f"not enough {self._native} above the reserve for the fees ({fmt(native_cost)})"
        wanted = size_quote if buy else size_quote / price
        share_cap = pool_reserve * self.config.max_reserve_share_pct / 100
        spendable = self.connector.get_available_balance(token) - reserve - (native_cost if token == self._native else 0)
        rules = self.connector.trading_rules[self.pair]
        step_in, step_out = (rules.min_quote_amount_increment, rules.min_base_amount_increment) if buy else \
            (rules.min_base_amount_increment, rules.min_quote_amount_increment)
        amount = floor_to(min(wanted, share_cap, spendable), step_in)
        leg_quote = amount if buy else amount * price
        if amount <= 0 or leg_quote < self.config.min_swap_amount:
            limit = "the balance above the reserve" if spendable <= min(wanted, share_cap) else \
                f"{self.config.max_reserve_share_pct}% of the pool's {token} reserve"
            return f"{token} first: {limit} allows {fmt(max(amount, Decimal('0')))} {token}, too little for a leg"
        first = self.connector.quote_swap(self.pair, first_side, amount)
        if not first.ok:
            return f"{token} first: the pool cannot swap {fmt(amount)} {token}: {first.error}"
        keep = max(self.config.safety_pct, self.config.slippage_pct)
        second_input = floor_to(first.output * (1 - keep / 100), step_out)
        if second_input <= 0:
            return f"{token} first: leg 2 rounds to zero"
        return RoundTripPlan(
            first_side=first_side,
            first_input=amount,
            first_output=first.output,
            second_input=second_input,
            leg_quote=leg_quote,
            reserve_share_pct=amount / pool_reserve * 100,
        )

    # === Sending ===

    def _send(self, plan: RoundTripPlan) -> Optional[str]:
        requests = [
            BeezeeSwapRequest(self.pair, plan.first_side, plan.first_input, self.config.slippage_pct),
            BeezeeSwapRequest(self.pair, plan.second_side, plan.second_input, self.config.slippage_pct),
        ]
        try:
            swap_ids = self.connector.amm_swaps(requests)
        except BeezeeSwapError as error:
            if "halted" in str(error).lower():
                self._set_halted(str(error))
                return "pool halted"
            return f"round trip refused up front: {error}"
        self._pending = PendingRoundTrip(plan=plan, swap_ids=list(swap_ids))
        # The next cycle starts from the other token.
        self._buy_first = plan.first_side != BUY
        if self._halted:
            # A halt (seen on the pool or in a refusal) is over once a round trip is accepted again.
            self._halted = False
            self.log_with_clock(logging.INFO, f"{self.pair} is no longer halted: pool volume resumes.")
        token_in, token_out = (self.quote, self.base) if plan.first_side == BUY else (self.base, self.quote)
        self.log_with_clock(
            logging.INFO,
            f"Round trip sent in one tx: {fmt(plan.first_input)} {token_in} -> {fmt(plan.first_output)} {token_out}, "
            f"then {fmt(plan.second_input)} {token_out} back ({plan.reserve_share_pct:.3f}% of the {token_in} reserve). "
            f"Remaining today: {fmt(self.remaining_quote)} {self.quote}.",
        )
        return None

    # === Outcomes ===

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        self._leg_done(event.order_id)

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        self._leg_done(event.order_id)

    def did_fail_order(self, event: MarketOrderFailureEvent):
        self._leg_done(event.order_id, event.error_message or "failed")

    def _leg_done(self, order_id: str, failure: Optional[str] = None):
        pending = self._pending
        if pending is None or order_id not in pending.swap_ids:
            return
        pending.done.add(order_id)
        if failure is not None:
            pending.failure = failure
        if pending.done >= set(pending.swap_ids):
            self._pending = None
            self._settle(pending)

    def _settle(self, pending: PendingRoundTrip):
        self._roll_day()
        if pending.failure is not None:
            # Both legs share one tx: nothing swapped, only the gas (if the tx landed) is spent.
            self.log_with_clock(logging.WARNING, f"Round trip tx failed, nothing swapped: {pending.failure}")
            return
        results = [self.connector.get_swap_result(swap_id) for swap_id in pending.swap_ids]
        settled = [result for result in results if result is not None]
        for result in settled:
            self._book_leg(result)
        # Tradebin charges its taker fee once per message.
        self._tradebin_fees_today += self._taker_fee() * len(pending.swap_ids)
        self._round_trips_today += 1
        if len(settled) < len(results):
            self.log_with_clock(logging.WARNING, "Round trip settled without a swap result: its volume is not counted.")
        self.log_with_clock(
            logging.INFO,
            f"Round trip settled: volume today {fmt(self._volume_today)} / {fmt(self.config.daily_volume_quote)} "
            f"{self.quote}, spend {self.spend_today:.6f} / {self.config.max_daily_fee_spend} BZE.",
        )

    def _book_leg(self, result: BeezeeSwapResult):
        """Volume, pool fee and inventory change of one settled leg. The volume is the leg's quote side: what left the
        wallet on a buy, what arrived on a sell."""
        if result.side == BUY:
            quote_amount = result.input
            pool_fee_quote = result.pool_fee
            self._quote_drift_today -= result.input
            self._base_drift_today += result.output
        else:
            quote_amount = result.output
            pool_fee_quote = result.pool_fee * result.price
            self._base_drift_today -= result.input
            self._quote_drift_today += result.output
        self._volume_today += quote_amount
        self._pool_fees_quote_today += pool_fee_quote
        self._pool_fees_native_today += self.to_native(pool_fee_quote, result.price)

    # === Fees ===

    def _taker_fee_token(self) -> Optional[Tuple[str, Decimal]]:
        fee = self.connector.get_fee(self.base, self.quote, OrderType.LIMIT, TradeType.BUY, Decimal("0"), Decimal("0"),
                                     is_maker=False)
        flat = fee.flat_fees
        return (flat[0].token, sum((item.amount for item in flat), Decimal("0"))) if flat else None

    def _taker_fee(self) -> Decimal:
        taker = self._taker_fee_token()
        return taker[1] if taker is not None else Decimal("0")

    def to_native(self, quote_amount: Decimal, price: Decimal) -> Decimal:
        """A quote amount in BZE: 1:1 when BZE is the quote, at the pair's own price when it is the base."""
        return quote_amount if self._native == self.quote else quote_amount / price

    # === Accounting ===

    @property
    def remaining_quote(self) -> Decimal:
        return self.config.daily_volume_quote - self._volume_today

    @property
    def gas_today(self) -> Decimal:
        return self.connector.gas_fees_paid() - self._gas_baseline

    @property
    def spend_today(self) -> Decimal:
        """Pool fees + tradebin taker fees + gas today, in BZE."""
        return self._pool_fees_native_today + self._tradebin_fees_today + self.gas_today

    # === Status ===

    def format_status(self) -> str:
        if self._refused is not None:
            return f"  Refused: {self._refused}"
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        native = self._native or "BZE"
        price = self.connector.get_pool_price(self.pair)
        reserves = self.connector.get_pool_reserves(self.pair)
        volume = self._volume_today
        cost_per_1000 = self.spend_today / volume * 1000 if volume > 0 else None
        if reserves is not None and reserves[1] > 0:
            next_size = min(self.swap_size(max(self.remaining_quote, Decimal("0"))),
                            reserves[1] * self.config.max_reserve_share_pct / 100)
            next_swap = f"{next_size:.6g} {self.quote} per leg ({next_size / reserves[1] * 100:.3f}% of the reserves)"
        else:
            next_swap = "-"
        drift_value = self._quote_drift_today + self._base_drift_today * price if price is not None else None
        lines = [
            f"  Pair: {self.pair}{' (HALTED)' if self._halted else ''}   venues: "
            f"{describe_venues(self.connector.trading_pair_venues(self.pair))}   day (UTC): {self._day}",
            f"  Target / done / remaining: {fmt(self.config.daily_volume_quote)} / {fmt(volume)} / "
            f"{fmt(max(self.remaining_quote, Decimal('0')))} {self.quote}",
            f"  Next swap: {next_swap}",
            f"  Pool price: {f'{price:.10g}' if price is not None else '-'}",
            f"  Cost today ({self._round_trips_today} round trip(s)): pool fees {self._pool_fees_quote_today:.6g} "
            f"{self.quote} ({self._pool_fees_native_today:.6g} {native}) + taker fees {fmt(self._tradebin_fees_today)} "
            f"{native} + gas {fmt(self.gas_today)} {native} = {self.spend_today:.6f} / "
            f"{self.config.max_daily_fee_spend} {native}",
            f"  Cost per 1,000 {self.quote} of volume: "
            f"{f'{cost_per_1000:.6f} {native}' if cost_per_1000 is not None else '-'}",
            f"  Inventory drift today (swaps only): {fmt(self._base_drift_today)} {self.base}, "
            f"{fmt(self._quote_drift_today)} {self.quote}"
            f"{f' = {drift_value:.6g} {self.quote}' if drift_value is not None else ''}",
        ]
        if self._pending is not None:
            plan = self._pending.plan
            lines.append(f"  In flight: round trip of {plan.leg_quote:.6g} {self.quote} per leg")
        if self._last_skip_reason:
            lines.append(f"  Last cycle skipped: {self._last_skip_reason}")
        return "\n".join(lines)
