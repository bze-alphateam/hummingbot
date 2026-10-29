"""Tradebin liquidity pools: the pool model, the chain's swap math, and a tracker that keeps the pools of the traded
pairs fresh.

The math mirrors chain v8.2.0 (`x/tradebin/keeper/service_amm.go`) to the micro-unit. The chain computes with
`math.LegacyDec`: an integer scaled by 10^18, where `Mul` and `Quo` round half to even at the 18th decimal and
`TruncateInt` / `Ceil` drop the decimals. The functions below do the same on Python integers, with no I/O.
"""
import asyncio
import logging
from dataclasses import dataclass, replace
from decimal import Decimal, localcontext
from typing import Any, Callable, Dict, List, Optional, Protocol, Set, Tuple

from hummingbot.connector.exchange.beezee.beezee_utils import (
    BeezeeMarket,
    chain_amount_to_display,
    chain_price_to_display,
    display_amount_to_chain,
)

# LegacyDec precision: 18 decimals.
DEC_PRECISION = 10 ** 18
_HALF_PRECISION = DEC_PRECISION // 2
# Digits for prices derived from reserves (Int strings can be far longer than the default 28 digits).
PRICE_PRECISION_DIGITS = 60

SIDE_BUY = "buy"
SIDE_SELL = "sell"


class BeezeeSwapError(ValueError):
    """The chain would refuse this swap. The message is the chain's own wording where it has one."""


# === LegacyDec arithmetic on scaled integers (positive values only, as in every swap computation) ===


def dec_from_str(value: str) -> int:
    """A LegacyDec string (e.g. the pool fee "0.001000000000000000") as its scaled integer, exactly."""
    scaled = Decimal(value).scaleb(18)
    if scaled != scaled.to_integral_value():
        raise ValueError(f"{value} has more than 18 decimals")
    return int(scaled)


def dec_from_int(value: int) -> int:
    return value * DEC_PRECISION


def _chop_precision_and_round(value: int) -> int:
    """Divides by 10^18 rounding half to even, as LegacyDec's chopPrecisionAndRound."""
    quotient, remainder = divmod(value, DEC_PRECISION)
    if remainder < _HALF_PRECISION:
        return quotient
    if remainder > _HALF_PRECISION:
        return quotient + 1
    return quotient if quotient % 2 == 0 else quotient + 1


def dec_mul(a: int, b: int) -> int:
    return _chop_precision_and_round(a * b)


def dec_quo(a: int, b: int) -> int:
    # LegacyDec.Quo: a * 10^36 / b (big.Int Quo truncates; both are positive here), then chop and round.
    return _chop_precision_and_round(a * DEC_PRECISION * DEC_PRECISION // b)


def dec_truncate_int(a: int) -> int:
    return a // DEC_PRECISION


def dec_ceil_int(a: int) -> int:
    quotient, remainder = divmod(a, DEC_PRECISION)
    return quotient + 1 if remainder > 0 else quotient


# === Pool model ===


def pool_id_for(denom_a: str, denom_b: str) -> str:
    """The chain's pool id for a pair: the two denoms sorted, joined by "_" (keeper.CreatePoolId)."""
    base, quote = sorted((denom_a, denom_b))
    return f"{base}_{quote}"


@dataclass(frozen=True)
class BeezeePool:
    pool_id: str
    base: str
    quote: str
    lp_denom: str
    fee: Decimal
    fee_dest: Dict[str, Decimal]
    reserve_base: int
    reserve_quote: int
    stable: bool = False
    halted: bool = False

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "BeezeePool":
        """Builds a pool from a tradebin REST pool (snake_case on chain v8.2.0, camelCase accepted too)."""
        fee_dest = data.get("fee_dest") or data.get("feeDest") or {}
        return cls(
            pool_id=data["id"],
            base=data["base"],
            quote=data["quote"],
            lp_denom=data.get("lp_denom") or data.get("lpDenom") or "",
            fee=Decimal(str(data.get("fee") or "0")),
            fee_dest={key: Decimal(str(value)) for key, value in fee_dest.items()},
            reserve_base=int(data.get("reserve_base") or data.get("reserveBase") or 0),
            reserve_quote=int(data.get("reserve_quote") or data.get("reserveQuote") or 0),
            stable=bool(data.get("stable", False)),
        )

    @property
    def fee_dec(self) -> int:
        return dec_from_str(format(self.fee, "f"))

    @property
    def is_empty(self) -> bool:
        return self.reserve_base <= 0 or self.reserve_quote <= 0

    def has_denom(self, denom: str) -> bool:
        return denom in (self.base, self.quote)

    def other_denom(self, denom: str) -> str:
        return self.quote if denom == self.base else self.base

    def reserves_for(self, denom: str) -> Tuple[int, int]:
        """(reserve of `denom`, reserve of the other denom), as the chain's GetReservesCoinsByDenom."""
        if denom == self.base:
            return self.reserve_base, self.reserve_quote
        return self.reserve_quote, self.reserve_base

    def spot_price(self, base_denom: str) -> Optional[Decimal]:
        """Micro-units of the other denom per micro-unit of `base_denom`, or None for an empty pool."""
        if not self.has_denom(base_denom) or self.is_empty:
            return None
        base_reserve, quote_reserve = self.reserves_for(base_denom)
        with localcontext() as context:
            context.prec = PRICE_PRECISION_DIGITS
            return Decimal(quote_reserve) / Decimal(base_reserve)


# === Swap math (chain v8.2.0) ===


@dataclass(frozen=True)
class BeezeePoolSwap:
    """One swap through one pool, in chain base units."""
    input_denom: str
    output_denom: str
    input_amount: int
    fee_amount: int
    output_amount: int


def calculate_swap_input_and_fee(pool: BeezeePool, amount: int) -> Tuple[int, int]:
    """(input after the fee, fee): keeper.calculateSwapInputAndFee, the pool fee is taken from the input."""
    fee_amount = dec_truncate_int(dec_mul(dec_from_int(amount), pool.fee_dec))
    return amount - fee_amount, fee_amount


def swap_output(pool: BeezeePool, input_denom: str, amount: int) -> BeezeePoolSwap:
    """What the chain pays out for `amount` of `input_denom`: keeper.swapTokens on the pool's current reserves.

    Raises BeezeeSwapError where the chain would fail the swap. The halted check comes first, as on chain.
    """
    if pool.halted:
        raise BeezeeSwapError(f"pool {pool.pool_id} is halted")
    if not pool.has_denom(input_denom):
        raise BeezeeSwapError(f"denom {input_denom} does not exist in pool {pool.pool_id}")
    if amount <= 0:
        raise BeezeeSwapError("input amount must be positive")
    real_input, fee_amount = calculate_swap_input_and_fee(pool, amount)
    if pool.fee > 0 and fee_amount <= 0:
        raise BeezeeSwapError("amount is too low to be traded")
    input_reserve, output_reserve = pool.reserves_for(input_denom)
    product = output_reserve * real_input
    quotient = input_reserve + real_input
    if quotient <= 0 or product <= 0:
        raise BeezeeSwapError("non positive product or quotient on swap tokens")
    output_amount = dec_truncate_int(dec_quo(dec_from_int(product), dec_from_int(quotient)))
    if output_amount <= 0:
        raise BeezeeSwapError("non positive output on swap tokens")
    return BeezeePoolSwap(input_denom, pool.other_denom(input_denom), amount, fee_amount, output_amount)


def input_for_output(pool: BeezeePool, output_denom: str, amount: int) -> int:
    """The input needed to receive `amount` of `output_denom`: keeper.CalculateOptimalInputForOutput.

    Rounded up, so swapping the result yields at least `amount`. Raises BeezeeSwapError like the chain.
    """
    if pool.halted:
        raise BeezeeSwapError(f"pool {pool.pool_id} is halted")
    if not pool.has_denom(output_denom):
        raise BeezeeSwapError(f"denom {output_denom} does not exist in pool {pool.pool_id}")
    if amount <= 0:
        raise BeezeeSwapError("output amount must be positive")
    output_reserve, input_reserve = pool.reserves_for(output_denom)
    if output_reserve <= 0 or input_reserve <= 0:
        raise BeezeeSwapError(f"pool {pool.pool_id} has insufficient liquidity")
    if amount >= output_reserve:
        raise BeezeeSwapError(f"desired output {amount} exceeds available reserve {output_reserve}")
    numerator = dec_mul(dec_from_int(amount), dec_from_int(input_reserve))
    real_input = dec_quo(numerator, dec_from_int(output_reserve - amount))
    fee_multiplier = DEC_PRECISION - pool.fee_dec
    if fee_multiplier <= 0:
        raise BeezeeSwapError("fee is too high, cannot calculate required input")
    required = dec_ceil_int(dec_quo(real_input, fee_multiplier))
    if required <= 0:
        raise BeezeeSwapError("calculated input amount is not positive")
    return required


# === Quotes in Hummingbot terms ===


@dataclass(frozen=True)
class BeezeeSwapQuote:
    """A swap on a trading pair's pool, in display units.

    `side` is the Hummingbot side: "buy" spends quote for base, "sell" spends base for quote. `fee` is the pool fee,
    in the input token. `effective_price` is quote per base (order-book units, fee included) and `price_impact_pct`
    how much worse it is than the pool price before the swap, in percent (the pool fee included). The tradebin taker
    fee a swap tx also pays (a flat amount per tx) is not part of the quote.

    When `ok` is False nothing else is set: `halted` tells a halted pool apart, `error` says why.
    """
    trading_pair: str
    side: str
    input: Optional[Decimal] = None
    output: Optional[Decimal] = None
    fee: Optional[Decimal] = None
    effective_price: Optional[Decimal] = None
    price_impact_pct: Optional[Decimal] = None
    input_denom: Optional[str] = None
    output_denom: Optional[str] = None
    input_amount: Optional[int] = None
    output_amount: Optional[int] = None
    fee_amount: Optional[int] = None
    halted: bool = False
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


def _side_denoms(market: BeezeeMarket, side: str):
    if side == SIDE_BUY:
        return market.quote, market.base
    if side == SIDE_SELL:
        return market.base, market.quote
    raise ValueError(f"side must be '{SIDE_BUY}' or '{SIDE_SELL}', got {side!r}")


def pool_price(pool: BeezeePool, market: BeezeeMarket) -> Optional[Decimal]:
    """The pool price in the market's orientation and display units (quote per base), like the order prices.

    The pool's base may be the market's quote (pool ids are sorted): the reserves are read by denom, so the price
    comes out inverted where it must be.
    """
    chain_price = pool.spot_price(market.base.denom)
    if chain_price is None:
        return None
    with localcontext() as context:
        context.prec = PRICE_PRECISION_DIGITS
        return chain_price_to_display(chain_price, market)


def build_swap_quote(pool: BeezeePool, market: BeezeeMarket, side: str, input_amount: int, output_amount: Optional[int] = None) -> BeezeeSwapQuote:
    """Quotes swapping `input_amount` chain units on `side`. With `output_amount`, the quote is reported for that
    target output (the input was computed for it) while the effective numbers use what the chain would pay out."""
    input_token, output_token = _side_denoms(market, side)
    base_quote = {"trading_pair": market.trading_pair, "side": side, "input_denom": input_token.denom, "output_denom": output_token.denom}
    try:
        swap = swap_output(pool, input_token.denom, input_amount)
    except BeezeeSwapError as error:
        return BeezeeSwapQuote(**base_quote, halted=pool.halted, error=str(error))
    with localcontext() as context:
        context.prec = PRICE_PRECISION_DIGITS
        input_display = chain_amount_to_display(swap.input_amount, input_token)
        output_display = chain_amount_to_display(swap.output_amount, output_token)
        if side == SIDE_BUY:
            effective_price = input_display / output_display
        else:
            effective_price = output_display / input_display
        spot = pool_price(pool, market)
        impact = None
        if spot is not None and spot > 0:
            worse_by = effective_price - spot if side == SIDE_BUY else spot - effective_price
            impact = worse_by / spot * Decimal(100)
    return BeezeeSwapQuote(
        **base_quote,
        input=input_display,
        output=output_display,
        fee=chain_amount_to_display(swap.fee_amount, input_token),
        effective_price=effective_price,
        price_impact_pct=impact,
        input_amount=swap.input_amount,
        output_amount=swap.output_amount,
        fee_amount=swap.fee_amount,
    )


def quote_swap(pool: BeezeePool, market: BeezeeMarket, side: str, amount: Decimal) -> BeezeeSwapQuote:
    """Swapping `amount` of the input token (display units): quote on a buy, base on a sell."""
    input_token, _ = _side_denoms(market, side)
    return build_swap_quote(pool, market, side, int(display_amount_to_chain(amount, input_token)))


def quote_input_for_output(pool: BeezeePool, market: BeezeeMarket, side: str, amount: Decimal) -> BeezeeSwapQuote:
    """The swap that receives at least `amount` of the output token (display units): base on a buy, quote on a
    sell. `input` is the chain's CalculateOptimalInputForOutput; `output` is what that input really buys."""
    input_token, output_token = _side_denoms(market, side)
    try:
        required = input_for_output(pool, output_token.denom, int(display_amount_to_chain(amount, output_token)))
    except BeezeeSwapError as error:
        return BeezeeSwapQuote(
            trading_pair=market.trading_pair, side=side, input_denom=input_token.denom,
            output_denom=output_token.denom, halted=pool.halted, error=str(error),
        )
    return build_swap_quote(pool, market, side, required)


# === Tracker ===


class BeezeePoolSource(Protocol):
    """What the tracker reads pools through (the connector's data source)."""

    async def get_all_liquidity_pools(self) -> List[Dict[str, Any]]:
        ...

    async def get_liquidity_pool(self, pool_id: str) -> Optional[Dict[str, Any]]:
        ...

    async def get_market_by_trading_pair(self, trading_pair: str) -> BeezeeMarket:
        ...


class BeezeePoolTracker:
    """Keeps the pools of the traded pairs and answers pool prices and quotes from memory.

    The first refresh reads every pool in one paged request; later refreshes (one per new block) read only the pools
    of the traded pairs. A pool is halted when either of its denoms is in `halted_denoms()`, checked at read time,
    so a halt takes effect with the connector's halted-denom cache, without a pool refresh.
    """

    def __init__(self, source: BeezeePoolSource, trading_pairs: Callable[[], List[str]], halted_denoms: Callable[[], Set[str]]):
        self._source = source
        self._trading_pairs = trading_pairs
        self._halted_denoms = halted_denoms
        self._pools_by_id: Dict[str, BeezeePool] = {}
        self._markets: Dict[str, BeezeeMarket] = {}
        self._loaded = False
        self._refresh_lock = asyncio.Lock()

    @classmethod
    def logger(cls):
        return logging.getLogger(__name__)

    @property
    def loaded(self) -> bool:
        return self._loaded

    @property
    def refreshing(self) -> bool:
        return self._refresh_lock.locked()

    async def refresh(self):
        """Reloads the reserves of the traded pairs' pools (all pools on the first call). Errors propagate; the
        pools keep their last known reserves."""
        async with self._refresh_lock:
            for trading_pair in self._trading_pairs():
                if trading_pair not in self._markets:
                    try:
                        self._markets[trading_pair] = await self._source.get_market_by_trading_pair(trading_pair)
                    except KeyError:
                        continue
            if not self._loaded:
                pools = [BeezeePool.from_data(data) for data in await self._source.get_all_liquidity_pools()]
                self._pools_by_id = {pool.pool_id: pool for pool in pools}
                self._loaded = True
                return
            for market in self._markets.values():
                pool_id = pool_id_for(market.base.denom, market.quote.denom)
                data = await self._source.get_liquidity_pool(pool_id)
                if data is None:
                    self._pools_by_id.pop(pool_id, None)
                else:
                    self._pools_by_id[pool_id] = BeezeePool.from_data(data)

    def market(self, trading_pair: str) -> Optional[BeezeeMarket]:
        return self._markets.get(trading_pair)

    def get_pool(self, trading_pair: str) -> Optional[BeezeePool]:
        """The pair's pool with its halted flag, or None when the pair is unknown or has no pool."""
        market = self._markets.get(trading_pair)
        if market is None:
            return None
        pool = self._pools_by_id.get(pool_id_for(market.base.denom, market.quote.denom))
        if pool is None:
            return None
        halted = self._halted_denoms()
        return replace(pool, halted=pool.base in halted or pool.quote in halted)

    def get_pool_price(self, trading_pair: str) -> Optional[Decimal]:
        """The pool price in display units, quote per base of the trading pair, or None (no pool, empty pool)."""
        pool = self.get_pool(trading_pair)
        return None if pool is None else pool_price(pool, self._markets[trading_pair])

    def quote_swap(self, trading_pair: str, side: str, amount: Decimal) -> BeezeeSwapQuote:
        pool = self.get_pool(trading_pair)
        if pool is None:
            return BeezeeSwapQuote(trading_pair=trading_pair, side=side, error=f"no liquidity pool for {trading_pair}")
        return quote_swap(pool, self._markets[trading_pair], side, amount)

    def quote_input_for_output(self, trading_pair: str, side: str, amount: Decimal) -> BeezeeSwapQuote:
        pool = self.get_pool(trading_pair)
        if pool is None:
            return BeezeeSwapQuote(trading_pair=trading_pair, side=side, error=f"no liquidity pool for {trading_pair}")
        return quote_input_for_output(pool, self._markets[trading_pair], side, amount)
