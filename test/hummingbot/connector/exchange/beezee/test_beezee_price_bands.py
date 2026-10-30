import unittest
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import yaml

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange.beezee.beezee_amm import BeezeeSwapError, BeezeeSwapQuote, BeezeeSwapResult
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.clock import Clock
from hummingbot.core.clock_mode import ClockMode
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    MarketEvent,
    MarketOrderFailureEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
)
from scripts.beezee_price_bands import BeezeePriceBands, BeezeePriceBandsConfig

STEP = Decimal("0.000001")
TAKER_FEE = Decimal("0.1")
GAS = Decimal("0.005")
# One taker fee + the gas estimate, in BZE.
FEES_BZE = TAKER_FEE + GAS
# 08:00 UTC.
START = 1_800_000_000.0
NEXT_MIDNIGHT = 1_800_057_600.0
EXAMPLE = Path(__file__).resolve().parents[5] / "conf" / "scripts" / "beezee_price_bands_token_bze.yml.example"


def floor_step(amount: Decimal) -> Decimal:
    return (amount / STEP).to_integral_value(rounding=ROUND_FLOOR) * STEP


@dataclass
class FakePool:
    pool_id: str
    reserve_base: Decimal
    reserve_quote: Decimal
    fee: Decimal = Decimal("0.003")
    halted: bool = False


class FakeOrderBook:
    def __init__(self, connector: "FakeBandsConnector"):
        self._connector = connector

    def get_price(self, is_buy: bool) -> float:
        levels = self._connector.asks if is_buy else self._connector.bids
        if not levels:
            raise EnvironmentError("Order book is empty - no price quote is possible.")
        return float(levels[0][0])


class FakeBandsConnector(ConnectorBase):
    """The parts of the BeeZee connector the script uses: a constant-product pool (fee from the input, like the chain),
    a book of (price, amount) levels, swaps and fills. Events are triggered on it like the real one does."""

    def __init__(self, base: str, quote: str):
        super().__init__()
        self.pair = f"{base}-{quote}"
        self.pool: Optional[FakePool] = FakePool(f"{base}_{quote}", Decimal("1000000"), Decimal("1000000"))
        self.asks: List[Tuple[Decimal, Decimal]] = []
        self.bids: List[Tuple[Decimal, Decimal]] = []
        self.balances: Dict[str, Decimal] = {base: Decimal("1000000"), quote: Decimal("1000000")}
        self.taker_fee: Optional[TokenAmount] = TokenAmount("BZE", TAKER_FEE)
        self.gas_paid = Decimal("0")
        self.new_blocks_seen = 0
        self.rules = {self.pair: TradingRule(self.pair, min_base_amount_increment=STEP, min_quote_amount_increment=STEP)}
        self.swaps: List[Tuple[str, Decimal, Decimal]] = []
        self.swap_ids: List[str] = []
        self.fills: List[Tuple[str, List[Tuple[Decimal, Decimal]]]] = []
        self.fill_ids: List[List[str]] = []
        self.swap_results: Dict[str, BeezeeSwapResult] = {}
        self.swap_error: Optional[Exception] = None
        self._next_id = 0

    @property
    def name(self) -> str:
        return "beezee"

    @property
    def ready(self) -> bool:
        return True

    @property
    def trading_rules(self):
        return self.rules

    @property
    def in_flight_orders(self):
        return {}

    def get_order_book(self, trading_pair: str):
        return FakeOrderBook(self)

    def get_pool(self, trading_pair: str):
        return self.pool

    def get_pool_price(self, trading_pair: str) -> Optional[Decimal]:
        if self.pool is None or self.pool.reserve_base <= 0:
            return None
        return self.pool.reserve_quote / self.pool.reserve_base

    def get_pool_reserves(self, trading_pair: str) -> Optional[Tuple[Decimal, Decimal]]:
        if self.pool is None:
            return None
        return self.pool.reserve_base, self.pool.reserve_quote

    def _reserves(self, side: str) -> Tuple[Decimal, Decimal]:
        """(input reserve, output reserve)."""
        pool = self.pool
        return (pool.reserve_quote, pool.reserve_base) if side == "buy" else (pool.reserve_base, pool.reserve_quote)

    def quote_swap(self, trading_pair: str, side: str, amount: Decimal) -> BeezeeSwapQuote:
        reserve_in, reserve_out = self._reserves(side)
        fee = amount * self.pool.fee
        net = amount - fee
        output = reserve_out * net / (reserve_in + net)
        return BeezeeSwapQuote(trading_pair, side, input=amount, output=output, fee=fee)

    def quote_input_for_output(self, trading_pair: str, side: str, amount: Decimal) -> BeezeeSwapQuote:
        reserve_in, reserve_out = self._reserves(side)
        if amount >= reserve_out:
            return BeezeeSwapQuote(trading_pair, side, error="the pool cannot pay that much")
        net = reserve_in * amount / (reserve_out - amount)
        total = net / (1 - self.pool.fee)
        return BeezeeSwapQuote(trading_pair, side, input=total, output=amount, fee=total - net)

    def levels_to_take(self, trading_pair: str, side: str, max_amount: Decimal, worst_price: Decimal):
        book = self.asks if side == "buy" else self.bids
        levels, left = [], max_amount
        for price, amount in book:
            if (side == "buy" and price > worst_price) or (side == "sell" and price < worst_price) or left <= 0:
                break
            take = min(amount, floor_step(left))
            levels.append((price, take))
            left -= take
        return levels

    def minimum_order_amount(self, trading_pair: str, price: Decimal) -> Decimal:
        return Decimal("0")

    def get_balance(self, currency: str) -> Decimal:
        return self.balances.get(currency, Decimal("0"))

    def get_available_balance(self, currency: str) -> Decimal:
        return self.balances.get(currency, Decimal("0"))

    def gas_fees_paid(self) -> Decimal:
        return self.gas_paid

    def get_fee(self, base, quote, order_type, order_side, amount, price=Decimal("NaN"), is_maker=None):
        return AddedToCostTradeFee(flat_fees=[self.taker_fee] if self.taker_fee is not None else [])

    def _new_id(self, prefix: str) -> str:
        self._next_id += 1
        return f"{prefix}-{self._next_id}"

    def amm_swap(self, trading_pair: str, side: str, amount: Decimal, slippage_pct: Decimal) -> str:
        if self.swap_error is not None:
            raise self.swap_error
        swap_id = self._new_id(f"swap-{side}")
        self.swaps.append((side, amount, slippage_pct))
        self.swap_ids.append(swap_id)
        return swap_id

    def fill_orders(self, trading_pair: str, side: str, levels) -> List[str]:
        ids = [self._new_id(f"fill-{side}") for _ in levels]
        self.fills.append((side, list(levels)))
        self.fill_ids.append(ids)
        return ids

    def get_swap_result(self, client_order_id: str) -> Optional[BeezeeSwapResult]:
        return self.swap_results.get(client_order_id)


class BeezeePriceBandsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # BZE as the quote: the band and the fees are in BZE.
        self.connector = FakeBandsConnector("TOKEN", "BZE")

    def _strategy(self, start: float = START, **settings) -> BeezeePriceBands:
        values = {
            "trading_pair": self.connector.pair,
            "floor_price": Decimal("0.9"),
            "ceiling_price": Decimal("1.2"),
            "max_trade_quote": Decimal("1000000"),
            "max_trade_base": Decimal("1000000"),
            "max_impact_pct": Decimal("50"),
            "buy_budget_quote": Decimal("1000000"),
            "sell_budget_base": Decimal("1000000"),
            "estimated_gas_bze": GAS,
        }
        values.update(settings)
        config = BeezeePriceBandsConfig(**values)
        with patch("asyncio.create_task", return_value=MagicMock()), \
                patch("hummingbot.strategy.strategy_v2_base.StrategyV2Base.listen_to_executor_actions", return_value=AsyncMock()), \
                patch("hummingbot.strategy.strategy_v2_base._get_executor_orchestrator_class"), \
                patch("hummingbot.strategy.strategy_v2_base.MarketDataProvider"):
            strategy = BeezeePriceBands({"beezee": self.connector}, config)
        # The clock's first tick only finds the connector ready; the first check runs on the next one.
        self.clock = Clock(ClockMode.BACKTEST, 1, start - 2, start + 86400)
        self.clock.add_iterator(strategy)
        self.now = start - 2
        return strategy

    def _run(self, seconds: int = 1):
        self.now += seconds
        self.clock.backtest_til(self.now)

    def _new_blocks(self, count: int = 1):
        self.connector.new_blocks_seen += count
        self._run()

    def _set_pool_price(self, price: Decimal):
        self.connector.pool.reserve_base = Decimal("1000000")
        self.connector.pool.reserve_quote = Decimal("1000000") * price

    # --- events, as the connector reports them ---

    def _complete(self, order_id: str, is_buy: bool):
        base, quote = self.connector.pair.split("-")
        if is_buy:
            self.connector.trigger_event(MarketEvent.BuyOrderCompleted, BuyOrderCompletedEvent(
                self.now, order_id, base, quote, Decimal("0"), Decimal("0"), OrderType.MARKET))
        else:
            self.connector.trigger_event(MarketEvent.SellOrderCompleted, SellOrderCompletedEvent(
                self.now, order_id, base, quote, Decimal("0"), Decimal("0"), OrderType.MARKET))

    def _settle_last_swap(self):
        side, amount, _ = self.connector.swaps[-1]
        swap_id = self.connector.swap_ids[-1]
        quote = self.connector.quote_swap(self.connector.pair, side, amount)
        self.connector.swap_results[swap_id] = BeezeeSwapResult(
            client_order_id=swap_id, trading_pair=self.connector.pair, side=side, tx_hash="AB" * 32,
            input_denom="in", output_denom="out", input=amount, output=quote.output, pool_fee=quote.fee,
            price=Decimal("1"), input_amount=0, output_amount=0, pool_fee_amount=0,
        )
        self._complete(swap_id, side == "buy")

    def _settle_last_fill(self):
        side, levels = self.connector.fills[-1]
        for order_id, (price, amount) in zip(self.connector.fill_ids[-1], levels):
            self.connector.trigger_event(MarketEvent.OrderFilled, OrderFilledEvent(
                timestamp=self.now, order_id=order_id, trading_pair=self.connector.pair,
                trade_type=TradeType.BUY if side == "buy" else TradeType.SELL, order_type=OrderType.LIMIT,
                price=price, amount=amount, trade_fee=AddedToCostTradeFee(flat_fees=[]),
            ))
            self._complete(order_id, side == "buy")

    def _impact(self, side: str, amount: Decimal) -> Decimal:
        spot = self.connector.get_pool_price(self.connector.pair)
        output = self.connector.quote_swap(self.connector.pair, side, amount).output
        effective = amount / output if side == "buy" else output / amount
        return ((effective - spot) if side == "buy" else (spot - effective)) / spot * 100

    # === Inside the band ===

    async def test_no_trade_inside_the_band(self):
        strategy = self._strategy()
        self._run(2)
        self._new_blocks()
        self.assertEqual([], self.connector.swaps)
        self.assertIsNone(strategy._last_skip_reason)
        self.assertIn("inside the band", strategy.format_status())

    # === Below the floor ===

    async def test_below_the_floor_buys_back_to_the_floor(self):
        self._set_pool_price(Decimal("0.8"))
        product = Decimal("1000000") * Decimal("800000")
        out = Decimal("1000000") - (product / Decimal("0.9")).sqrt()
        needed = self.connector.quote_input_for_output("TOKEN-BZE", "buy", out).input
        self._strategy(slippage_pct=Decimal("0.7"))
        self._run(2)
        self.assertEqual([("buy", floor_step(needed), Decimal("0.7"))], self.connector.swaps)
        # The swap lands the pool price on the floor (the fee left in the pool keeps it a hair above).
        output = self.connector.quote_swap("TOKEN-BZE", "buy", floor_step(needed)).output
        after = (Decimal("800000") + floor_step(needed) * Decimal("0.997")) / (Decimal("1000000") - output)
        self.assertAlmostEqual(Decimal("0.9"), after, delta=Decimal("0.0001"))

    async def test_buy_is_capped_by_max_trade_quote(self):
        self._set_pool_price(Decimal("0.8"))
        self._strategy(max_trade_quote=Decimal("1000"))
        self._run(2)
        self.assertEqual("buy", self.connector.swaps[0][0])
        self.assertEqual(Decimal("1000"), self.connector.swaps[0][1])

    async def test_buy_is_capped_by_the_impact(self):
        self._set_pool_price(Decimal("0.8"))
        strategy = self._strategy(max_impact_pct=Decimal("1"))
        self._run(2)
        amount = self.connector.swaps[0][1]
        self.assertLessEqual(self._impact("buy", amount), Decimal("1"))
        self.assertGreater(self._impact("buy", amount + STEP), Decimal("1"))
        self.assertEqual("impact cap", strategy._pending.plan.limit)

    async def test_an_impact_cap_below_the_pool_fee_trades_nothing(self):
        self._set_pool_price(Decimal("0.8"))
        strategy = self._strategy(max_impact_pct=Decimal("0.2"))
        self._run(2)
        self.assertEqual([], self.connector.swaps)
        self.assertIn("impact cap", strategy._last_skip_reason)

    async def test_buy_is_capped_by_the_budget(self):
        self._set_pool_price(Decimal("0.8"))
        self._strategy(buy_budget_quote=Decimal("500"))
        self._run(2)
        self.assertEqual(Decimal("500"), self.connector.swaps[0][1])

    async def test_buy_never_touches_the_quote_reserve(self):
        self._set_pool_price(Decimal("0.8"))
        self.connector.balances["BZE"] = Decimal("10000")
        self._strategy(keep_quote_reserve=Decimal("9800"))
        self._run(2)
        amount = self.connector.swaps[0][1]
        # The taker fee and the gas come out of the BZE above the reserve too.
        self.assertEqual(floor_step(Decimal("200") - FEES_BZE), amount)
        self.assertGreaterEqual(Decimal("10000") - amount - FEES_BZE, Decimal("9800"))

    async def test_nothing_above_the_reserve_trades_nothing(self):
        self._set_pool_price(Decimal("0.8"))
        self.connector.balances["BZE"] = Decimal("100")
        strategy = self._strategy(keep_quote_reserve=Decimal("100"))
        self._run(2)
        self.assertEqual([], self.connector.swaps)
        self.assertEqual("no BZE above the reserve", strategy._last_skip_reason)

    # === Above the ceiling ===

    async def test_above_the_ceiling_sells_back_to_the_ceiling(self):
        self._set_pool_price(Decimal("1.5"))
        product = Decimal("1000000") * Decimal("1500000")
        out = Decimal("1500000") - (product * Decimal("1.2")).sqrt()
        needed = self.connector.quote_input_for_output("TOKEN-BZE", "sell", out).input
        self._strategy()
        self._run(2)
        self.assertEqual([("sell", floor_step(needed), Decimal("0.5"))], self.connector.swaps)

    async def test_sell_is_capped_by_max_trade_base_budget_impact_and_reserve(self):
        self._set_pool_price(Decimal("1.5"))
        self._strategy(max_trade_base=Decimal("700"))
        self._run(2)
        self.assertEqual(Decimal("700"), self.connector.swaps[-1][1])

        self.connector.swaps.clear()
        self.connector.swap_ids.clear()
        self._strategy(sell_budget_base=Decimal("300"))
        self._run(2)
        self.assertEqual(Decimal("300"), self.connector.swaps[-1][1])

        self.connector.swaps.clear()
        self.connector.swap_ids.clear()
        self._strategy(max_impact_pct=Decimal("1"))
        self._run(2)
        amount = self.connector.swaps[-1][1]
        self.assertLessEqual(self._impact("sell", amount), Decimal("1"))
        self.assertGreater(self._impact("sell", amount + STEP), Decimal("1"))

        self.connector.swaps.clear()
        self.connector.swap_ids.clear()
        self.connector.balances["TOKEN"] = Decimal("5000")
        self._strategy(keep_base_reserve=Decimal("4900"))
        self._run(2)
        # TOKEN is not BZE: no fee comes out of it, the reserve is the whole limit.
        self.assertEqual(Decimal("100"), self.connector.swaps[-1][1])

    async def test_a_sell_needs_bze_above_its_reserve_for_the_fees(self):
        self._set_pool_price(Decimal("1.5"))
        self.connector.balances["BZE"] = Decimal("50.1")
        strategy = self._strategy(keep_quote_reserve=Decimal("50"))
        self._run(2)
        self.assertEqual([], self.connector.swaps)
        self.assertIn("not enough BZE above the reserve for the fees", strategy._last_skip_reason)

    # === Budgets ===

    async def test_daily_budget_is_spent_then_starts_again_at_utc_midnight(self):
        self._set_pool_price(Decimal("0.8"))
        strategy = self._strategy(start=NEXT_MIDNIGHT - 30, buy_budget_quote=Decimal("500"), cooldown_blocks=0)
        self._run(2)
        self.assertEqual(Decimal("500"), self.connector.swaps[-1][1])
        self._settle_last_swap()
        self._new_blocks()
        self.assertEqual(1, len(self.connector.swaps))
        self.assertEqual("buy budget spent today", strategy._last_skip_reason)
        self.assertIn("Buy budget today (UTC): used 500 / 500 BZE, left 0", strategy.format_status())
        self._run(40)
        self.assertEqual(2, len(self.connector.swaps))
        self.assertEqual(Decimal("500"), self.connector.swaps[-1][1])

    async def test_total_budget_does_not_start_again_at_midnight(self):
        self._set_pool_price(Decimal("0.8"))
        strategy = self._strategy(start=NEXT_MIDNIGHT - 30, buy_budget_quote=Decimal("500"), budget_period="total",
                                  cooldown_blocks=0)
        self._run(2)
        self._settle_last_swap()
        self._run(40)
        self.assertEqual(1, len(self.connector.swaps))
        self.assertEqual("buy budget spent for this run", strategy._last_skip_reason)

    async def test_the_budget_counts_what_the_trades_spent(self):
        self._set_pool_price(Decimal("0.8"))
        strategy = self._strategy(buy_budget_quote=Decimal("1500"), max_trade_quote=Decimal("1000"), cooldown_blocks=0)
        self._run(2)
        self._settle_last_swap()
        self._new_blocks()
        self.assertEqual([Decimal("1000"), Decimal("500")], [amount for _, amount, _ in self.connector.swaps])
        self._settle_last_swap()
        self.assertEqual(Decimal("0"), strategy.buy_budget_left)
        self.assertEqual(2, strategy._buys_today.trades)
        self.assertEqual(2 * TAKER_FEE, strategy._tradebin_fees_today)

    # === Venue ===

    async def test_best_venue_takes_the_book_when_it_is_cheaper(self):
        self._set_pool_price(Decimal("0.8"))
        self.connector.asks = [(Decimal("0.75"), Decimal("2000")), (Decimal("0.95"), Decimal("1000"))]
        strategy = self._strategy(venue="best", max_trade_quote=Decimal("1000"))
        self._run(2)
        self.assertEqual([], self.connector.swaps)
        # The ask above the floor is never taken; 1000 BZE buys 1333.333333 TOKEN at 0.75.
        self.assertEqual([("buy", [(Decimal("0.75"), floor_step(Decimal("1000") / Decimal("0.75")))])],
                         self.connector.fills)
        self._settle_last_fill()
        self.assertIsNone(strategy._pending)
        self.assertEqual(floor_step(Decimal("1000") / Decimal("0.75")), strategy._buys_today.base)

    async def test_best_venue_takes_the_pool_when_it_is_cheaper(self):
        self._set_pool_price(Decimal("0.8"))
        self.connector.asks = [(Decimal("0.85"), Decimal("2000"))]
        self._strategy(venue="best", max_trade_quote=Decimal("1000"))
        self._run(2)
        self.assertEqual([], self.connector.fills)
        self.assertEqual([("buy", Decimal("1000"), Decimal("0.5"))], self.connector.swaps)

    async def test_pool_venue_never_takes_the_book(self):
        self._set_pool_price(Decimal("0.8"))
        self.connector.asks = [(Decimal("0.5"), Decimal("2000"))]
        self._strategy(max_trade_quote=Decimal("1000"))
        self._run(2)
        self.assertEqual([], self.connector.fills)
        self.assertEqual(1, len(self.connector.swaps))

    async def test_book_walk_stops_at_the_impact_cap(self):
        self.connector.pool = None
        self.connector.bids = [(Decimal("1.5"), Decimal("100")), (Decimal("1.3"), Decimal("1000"))]
        self.connector.asks = [(Decimal("1.6"), Decimal("100"))]
        strategy = self._strategy(venue="best", max_impact_pct=Decimal("5"))
        self._run(2)
        side, levels = self.connector.fills[-1]
        self.assertEqual("sell", side)
        base = sum((amount for _, amount in levels), Decimal("0"))
        quote = sum((price * amount for price, amount in levels), Decimal("0"))
        # Average at least 1.5 x 0.95 = 1.425: 100 at 1.5 + x at 1.3 with x ≤ 60.
        self.assertGreaterEqual(quote / base, Decimal("1.425"))
        self.assertEqual(Decimal("160"), base)
        self.assertEqual("impact cap", strategy._pending.plan.limit)

    # === Cooldown ===

    async def test_cooldown_then_a_repeat_trade(self):
        self._set_pool_price(Decimal("0.8"))
        strategy = self._strategy(max_trade_quote=Decimal("1000"), cooldown_blocks=2)
        self._run(2)
        self._settle_last_swap()
        self._new_blocks()
        self.assertEqual(1, len(self.connector.swaps))
        self.assertEqual("cooldown, 1/2 blocks since the last trade", strategy._last_skip_reason)
        self._new_blocks()
        self.assertEqual(2, len(self.connector.swaps))

    async def test_cooldown_ends_on_time_without_block_notifications(self):
        self._set_pool_price(Decimal("0.8"))
        self._strategy(max_trade_quote=Decimal("1000"), cooldown_blocks=2, check_interval_seconds=5)
        self._run(2)
        self._settle_last_swap()
        self._run(10)
        self.assertEqual(1, len(self.connector.swaps))
        self._run(15)
        self.assertEqual(2, len(self.connector.swaps))

    async def test_a_failed_trade_spends_no_budget(self):
        self._set_pool_price(Decimal("0.8"))
        strategy = self._strategy(buy_budget_quote=Decimal("500"))
        self._run(2)
        self.connector.trigger_event(MarketEvent.OrderFailure, MarketOrderFailureEvent(
            self.now, self.connector.swap_ids[-1], OrderType.MARKET, "the resulted amount is too low"))
        self.assertIsNone(strategy._pending)
        self.assertEqual(Decimal("500"), strategy.buy_budget_left)

    async def test_a_book_trade_with_a_failed_entry_books_what_the_others_filled(self):
        self._set_pool_price(Decimal("0.8"))
        self.connector.asks = [(Decimal("0.75"), Decimal("100")), (Decimal("0.78"), Decimal("100"))]
        strategy = self._strategy(venue="best", max_trade_quote=Decimal("1000"), buy_budget_quote=Decimal("1000"))
        self._run(2)
        self.assertEqual([("buy", [(Decimal("0.75"), Decimal("100")), (Decimal("0.78"), Decimal("100"))])],
                         self.connector.fills)
        first, second = self.connector.fill_ids[-1]
        self.connector.trigger_event(MarketEvent.OrderFilled, OrderFilledEvent(
            timestamp=self.now, order_id=first, trading_pair=self.connector.pair, trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT, price=Decimal("0.75"), amount=Decimal("100"),
            trade_fee=AddedToCostTradeFee(flat_fees=[]),
        ))
        self._complete(first, True)
        self.connector.trigger_event(MarketEvent.OrderFailure, MarketOrderFailureEvent(
            self.now, second, OrderType.LIMIT, "no outcome within 20 blocks"))
        self.assertIsNone(strategy._pending)
        self.assertEqual(Decimal("75"), strategy._bought_quote)
        self.assertEqual(Decimal("925"), strategy.buy_budget_left)
        self.assertEqual(1, strategy._buys_today.trades)
        self.assertEqual(Decimal("100"), strategy._buys_today.base)
        self.assertEqual(TAKER_FEE, strategy._tradebin_fees_today)

    # === Sanity ===

    async def test_a_floor_not_below_the_ceiling_is_refused(self):
        strategy = self._strategy(floor_price=Decimal("1.2"), ceiling_price=Decimal("1.2"))
        self._run(2)
        self.assertEqual([], self.connector.swaps)
        self.assertIn("is not below ceiling_price", strategy.format_status())

    async def test_a_reserve_above_the_balance_is_refused(self):
        self._set_pool_price(Decimal("0.8"))
        self.connector.balances["TOKEN"] = Decimal("10")
        strategy = self._strategy(keep_base_reserve=Decimal("11"))
        self._run(2)
        self.assertEqual([], self.connector.swaps)
        self.assertIn("keep_base_reserve 11 is larger than the TOKEN balance 10", strategy.format_status())

    async def test_a_pair_without_bze_is_refused(self):
        self.connector = FakeBandsConnector("TOKEN", "USDC")
        self._set_pool_price(Decimal("0.8"))
        strategy = self._strategy()
        self._run(2)
        self.assertEqual([], self.connector.swaps)
        self.assertIn("TOKEN-USDC has no BZE side", strategy.format_status())

    async def test_an_unknown_venue_is_refused_by_the_config(self):
        with self.assertRaises(ValueError):
            BeezeePriceBandsConfig(floor_price=Decimal("1"), ceiling_price=Decimal("2"), venue="cex")

    async def test_a_halted_pool_stops_the_script(self):
        self._set_pool_price(Decimal("0.8"))
        self.connector.pool.halted = True
        strategy = self._strategy()
        self._run(2)
        self.connector.pool.halted = False
        self._new_blocks()
        self.assertEqual([], self.connector.swaps)
        self.assertIn("HALTED", strategy.format_status())

    async def test_a_halted_refusal_stops_the_script(self):
        self._set_pool_price(Decimal("0.8"))
        self.connector.swap_error = BeezeeSwapError("market TOKEN-BZE is halted")
        strategy = self._strategy()
        self._run(2)
        self.connector.swap_error = None
        self._new_blocks()
        self.assertEqual([], self.connector.swaps)
        self.assertEqual("market TOKEN-BZE is halted", strategy._halted)

    # === Reference price ===

    async def test_book_mid_reference(self):
        # The pool sits below the floor but the book mid is inside the band: nothing to do.
        self._set_pool_price(Decimal("0.8"))
        self.connector.bids = [(Decimal("0.95"), Decimal("10"))]
        self.connector.asks = [(Decimal("1.05"), Decimal("10"))]
        self._strategy(price_source="book_mid")
        self._run(2)
        self.assertEqual([], self.connector.swaps)

    async def test_no_pool_defaults_to_the_book_mid(self):
        self.connector.pool = None
        self.connector.bids = [(Decimal("0.7"), Decimal("10"))]
        self.connector.asks = [(Decimal("0.8"), Decimal("10"))]
        strategy = self._strategy()
        self._run(2)
        self.assertEqual("pool: TOKEN-BZE has no liquidity pool", strategy._last_skip_reason)
        self.assertEqual((Decimal("0.75"), "book_mid"), strategy.reference_price())

    # === Status and example ===

    async def test_status_shows_the_band_budgets_reserves_and_trades(self):
        self._set_pool_price(Decimal("0.8"))
        strategy = self._strategy(max_trade_quote=Decimal("1000"), keep_base_reserve=Decimal("5"),
                                  keep_quote_reserve=Decimal("7"))
        self._run(2)
        self._settle_last_swap()
        status = strategy.format_status()
        self.assertIn("Reference price: 0.8 (pool), below the floor", status)
        self.assertIn("Band: floor 0.9 / ceiling 1.2 BZE", status)
        self.assertIn("Reserves kept: 5 TOKEN, 7 BZE", status)
        self.assertIn("Buys today: 1,", status)
        self.assertIn("average", status)
        self.assertIn("Fees today: tradebin 0.1 BZE", status)

    def test_example_config_loads(self):
        config = BeezeePriceBandsConfig(**yaml.safe_load(EXAMPLE.read_text()))
        self.assertEqual("beezee", config.exchange)
        self.assertEqual("TOKEN-BZE", config.trading_pair)
        self.assertEqual("beezee_price_bands.py", config.script_file_name)
        self.assertLess(config.floor_price, config.ceiling_price)
        self.assertEqual("pool", config.venue)
        self.assertEqual("daily", config.budget_period)
