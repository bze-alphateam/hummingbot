"""Taker fills (MsgFillOrders): taking resting orders off the order book at exactly their prices.

A fill never rests an order. The tx escrows the funds of every entry and pays the tradebin taker fee once per
message; each entry is then a queue message of its own, executed in EndBlock against the resting orders at exactly
its price only, and whatever does not match is refunded. An entry below the chain minimum at its price is skipped by
the tx without a trace, so such entries are dropped here before sending (and the message ids of the others stay in
step with the entries).
"""

from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal
from typing import Dict, List, Sequence, Tuple

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_utils import (
    BeezeeMarket,
    chain_amount_to_display,
    chain_price_to_display,
    display_price_to_chain,
    minimum_order_size_for_price,
)


class BeezeeFillError(ValueError):
    """The fill is refused up front: nothing was sent."""


@dataclass(frozen=True)
class BeezeeFillEntry:
    """One MsgFillOrders entry: display price and base amount, and the same in chain units."""

    price: Decimal
    amount: Decimal
    chain_price: str
    chain_amount: int


def resting_side(side: str) -> str:
    """The MsgFillOrders `order_type` for our `side`: buying takes resting sells, selling takes resting buys."""
    if side == CONSTANTS.SIDE_BUY:
        return CONSTANTS.SIDE_SELL
    if side == CONSTANTS.SIDE_SELL:
        return CONSTANTS.SIDE_BUY
    raise BeezeeFillError(f"side must be '{CONSTANTS.SIDE_BUY}' or '{CONSTANTS.SIDE_SELL}', got {side!r}")


def floor_to_base_units(amount: Decimal, market: BeezeeMarket) -> int:
    return int((Decimal(amount) / market.base.quantum).to_integral_value(rounding=ROUND_FLOOR))


def prepare_fill_entries(
    market: BeezeeMarket, side: str, levels: Sequence[Tuple[Decimal, Decimal]]
) -> Tuple[List[BeezeeFillEntry], List[Tuple[Decimal, Decimal]]]:
    """The entries to send for (display price, base amount) levels, and the levels dropped as below the chain minimum.

    Amounts are rounded down to whole base units; levels at the same price are merged (the chain refuses duplicate
    prices). Raises BeezeeFillError for an invalid side, price or amount, more entries than one message takes, or when
    every level is below the minimum (the chain would refuse the tx).
    """
    resting_side(side)
    if not levels:
        raise BeezeeFillError("no levels to fill")
    merged: Dict[Decimal, Tuple[Decimal, int]] = {}
    for raw_price, raw_amount in levels:
        price, amount = Decimal(raw_price), Decimal(raw_amount)
        if not price.is_finite() or price <= 0 or not amount.is_finite() or amount <= 0:
            raise BeezeeFillError(f"invalid level ({raw_price}, {raw_amount}): price and amount must be positive")
        chain_price = Decimal(display_price_to_chain(price, market))
        if chain_price <= CONSTANTS.MIN_CHAIN_PRICE:
            raise BeezeeFillError(f"price {price} is at or below the chain minimum price")
        if -chain_price.normalize().as_tuple().exponent > 18:
            raise BeezeeFillError(f"price {price} has more decimals than a chain price can hold")
        previous = merged.get(chain_price, (price, 0))
        merged[chain_price] = (previous[0], previous[1] + floor_to_base_units(amount, market))
    entries: List[BeezeeFillEntry] = []
    dropped: List[Tuple[Decimal, Decimal]] = []
    for chain_price, (price, chain_amount) in merged.items():
        amount = chain_amount_to_display(chain_amount, market.base)
        if amount < minimum_order_size_for_price(chain_price, market):
            dropped.append((price, amount))
            continue
        entries.append(BeezeeFillEntry(
            price=price, amount=amount, chain_price=format(chain_price.normalize(), "f"), chain_amount=chain_amount,
        ))
    if not entries:
        minimums = ", ".join(f"{minimum_order_size_for_price(Decimal(display_price_to_chain(price, market)), market)} at {price}" for price, _ in dropped)
        raise BeezeeFillError(
            f"every level is below the chain minimum amount ({minimums} {market.base.symbol}); nothing to send"
        )
    if len(entries) > CONSTANTS.MAX_FILL_ORDERS:
        raise BeezeeFillError(f"{len(entries)} levels: one fill takes at most {CONSTANTS.MAX_FILL_ORDERS}")
    return entries, dropped


def levels_to_take(
    chain_levels: Sequence[Tuple[str, str]],
    market: BeezeeMarket,
    side: str,
    max_amount: Decimal,
    worst_price: Decimal,
) -> List[Tuple[Decimal, Decimal]]:
    """The (display price, base amount) levels to take, best first, from the opposite side's book levels in chain units
    (best first), up to `max_amount` base and no worse than `worst_price`.

    Stops at the first level whose part would be below the chain minimum, and at MAX_FILL_ORDERS levels. The chain
    may still execute a little less at a level when taking part of a resting order would leave it below the minimum;
    that part is refunded.
    """
    resting_side(side)
    remaining = floor_to_base_units(max_amount, market)
    taken: List[Tuple[Decimal, Decimal]] = []
    for raw_price, raw_amount in chain_levels:
        if remaining <= 0 or len(taken) >= CONSTANTS.MAX_FILL_ORDERS:
            break
        price = chain_price_to_display(raw_price, market)
        if (price > worst_price) if side == CONSTANTS.SIDE_BUY else (price < worst_price):
            break
        take = min(int(Decimal(raw_amount)), remaining)
        amount = chain_amount_to_display(take, market.base)
        if take <= 0 or amount < minimum_order_size_for_price(raw_price, market):
            break
        taken.append((price, amount))
        remaining -= take
    return taken
