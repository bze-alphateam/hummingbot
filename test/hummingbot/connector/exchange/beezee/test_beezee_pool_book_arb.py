import unittest
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import yaml

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange.beezee.beezee_amm import (
    BeezeeSwapError,
    BeezeeSwapQuote,
    BeezeeSwapRequest,
    BeezeeSwapResult,
)
from hummingbot.connector.exchange.beezee.beezee_fills import BeezeeFillError
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.clock import Clock
from hummingbot.core.clock_mode import ClockMode
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    MarketEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
)
from scripts.beezee_pool_book_arb import BOOK_TO_POOL, POOL_TO_BOOK, BeezeePoolBookArb, BeezeePoolBookArbConfig

BASE_STEP = Decimal("0.000001")
TAKER_FEE = Decimal("0.1")
GAS = Decimal("0.005")
# Two taker fees + the gas estimate, in BZE.
FIXED_BZE = 2 * TAKER_FEE + GAS
START = 1_800_000_000.0
EXAMPLE = Path(__file__).resolve().parents[5] / "conf" / "scripts" / "beezee_pool_book_arb_mainnet.yml.example"


@dataclass
class FakePool:
    pool_id: str
    reserve_base: Decimal
    reserve_quote: Decimal
    fee: Decimal = Decimal("0.003")
    halted: bool = False


class FakeOrderBook:
    def __init__(self, connector: "FakeArbConnector"):
        self._connector = connector

    def get_price(self, is_buy: bool) -> float:
        levels = self._connector.asks if is_buy else self._connector.bids
        if not levels:
            raise EnvironmentError("Order book is empty - no price quote is possible.")
        return float(levels[0][0])


class FakeArbConnector(ConnectorBase):
    """The parts of the BeeZee connector the script uses: a constant-product pool (fee from the input, like the chain)
    and a book of (price, amount) levels. Events are triggered on it like the real one does."""

    def __init__(self, base: str, quote: str):
        super().__init__()
        self.pair = f"{base}-{quote}"
        self.pool: Optional[FakePool] = FakePool(f"{base}_{quote}", Decimal("1000000"), Decimal("1000000"))
        self.asks: List[Tuple[Decimal, Decimal]] = []
        self.bids: List[Tuple[Decimal, Decimal]] = []
        self.balances: Dict[str, Decimal] = {base: Decimal("1000000"), quote: Decimal("1000000")}
        self.taker_fee: Optional[TokenAmount] = TokenAmount("BZE", TAKER_FEE)
        self.gas_paid = Decimal("0")
        self.chain_minimum = Decimal("0")
        self.new_blocks_seen = 0
        self.rules = {self.pair: TradingRule(self.pair, min_base_amount_increment=BASE_STEP)}
        self.sent: List[Tuple[str, List[Tuple[Decimal, Decimal]], BeezeeSwapRequest]] = []
        self.sent_ids: List[Tuple[List[str], str]] = []
        self.swaps: List[Tuple[str, Decimal, Decimal]] = []
        self.swap_results: Dict[str, BeezeeSwapResult] = {}
        self.fill_error: Optional[Exception] = None
        self.swap_error: Optional[Exception] = None
        self.venues: Tuple[str, ...] = ("book", "pool")
        self._next_id = 0

    @property
    def name(self) -> str:
        return "beezee"

    def trading_pair_venues(self, trading_pair: str) -> Tuple[str, ...]:
        return self.venues

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

    def quote_swap(self, trading_pair: str, side: str, amount: Decimal) -> BeezeeSwapQuote:
        pool = self.pool
        if side == "sell":
            reserve_in, reserve_out = pool.reserve_base, pool.reserve_quote
        else:
            reserve_in, reserve_out = pool.reserve_quote, pool.reserve_base
        fee = amount * pool.fee
        net = amount - fee
        output = reserve_out * net / (reserve_in + net)
        return BeezeeSwapQuote(trading_pair, side, input=amount, output=output, fee=fee)

    def quote_input_for_output(self, trading_pair: str, side: str, amount: Decimal) -> BeezeeSwapQuote:
        pool = self.pool
        if side == "buy":
            reserve_in, reserve_out = pool.reserve_quote, pool.reserve_base
        else:
            reserve_in, reserve_out = pool.reserve_base, pool.reserve_quote
        if amount >= reserve_out:
            return BeezeeSwapQuote(trading_pair, side, error="the pool cannot pay that much")
        net = reserve_in * amount / (reserve_out - amount)
        total = net / (1 - pool.fee)
        return BeezeeSwapQuote(trading_pair, side, input=total, output=amount, fee=total - net)

    def levels_to_take(self, trading_pair: str, side: str, max_amount: Decimal, worst_price: Decimal):
        book = self.asks if side == "buy" else self.bids
        levels, left = [], max_amount
        for price, amount in book:
            if (side == "buy" and price > worst_price) or (side == "sell" and price < worst_price) or left <= 0:
                break
            take = min(amount, left)
            levels.append((price, take))
            left -= take
        return levels

    def minimum_order_amount(self, trading_pair: str, price: Decimal) -> Decimal:
        return self.chain_minimum

    def get_available_balance(self, currency: str) -> Decimal:
        return self.balances.get(currency, Decimal("0"))

    def gas_fees_paid(self) -> Decimal:
        return self.gas_paid

    def get_fee(self, base, quote, order_type, order_side, amount, price=Decimal("NaN"), is_maker=None):
        return AddedToCostTradeFee(flat_fees=[self.taker_fee] if self.taker_fee is not None else [])

    def _new_id(self, prefix: str) -> str:
        self._next_id += 1
        return f"{prefix}-{self._next_id}"

    def fill_orders_and_swap(self, trading_pair, side, levels, swaps):
        if self.fill_error is not None:
            raise self.fill_error
        [request] = swaps
        fill_ids = [self._new_id(f"fill-{side}") for _ in levels]
        swap_id = self._new_id(f"swap-{request.side}")
        self.sent.append((side, list(levels), request))
        self.sent_ids.append((fill_ids, swap_id))
        return fill_ids, [swap_id]

    def amm_swap(self, trading_pair: str, side: str, amount: Decimal, slippage_pct: Decimal) -> str:
        if self.swap_error is not None:
            raise self.swap_error
        swap_id = self._new_id(f"rebalance-{side}")
        self.swaps.append((side, amount, slippage_pct))
        return swap_id

    def get_swap_result(self, client_order_id: str) -> Optional[BeezeeSwapResult]:
        return self.swap_results.get(client_order_id)


class BeezeePoolBookArbTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # BZE as the quote: fees count 1:1.
        self.connector = FakeArbConnector("TOKEN", "BZE")

    def _strategy(self, **settings) -> BeezeePoolBookArb:
        values = {"trading_pair": self.connector.pair, "estimated_gas_bze": GAS}
        values.update(settings)
        config = BeezeePoolBookArbConfig(**values)
        with patch("asyncio.create_task", return_value=MagicMock()), \
                patch("hummingbot.strategy.strategy_v2_base.StrategyV2Base.listen_to_executor_actions", return_value=AsyncMock()), \
                patch("hummingbot.strategy.strategy_v2_base._get_executor_orchestrator_class"), \
                patch("hummingbot.strategy.strategy_v2_base.MarketDataProvider"):
            strategy = BeezeePoolBookArb({"beezee": self.connector}, config)
        # The clock's first tick only finds the connector ready; the first check runs on the next one.
        self.clock = Clock(ClockMode.BACKTEST, 1, START - 2, START + 86400)
        self.clock.add_iterator(strategy)
        self.now = START - 2
        return strategy

    def _run(self, seconds: int = 1):
        self.now += seconds
        self.clock.backtest_til(self.now)

    def _new_block(self):
        self.connector.new_blocks_seen += 1
        self._run()

    # --- events, as the connector reports them ---

    def _fill(self, order_id: str, amount: Decimal, price: Decimal, is_buy: bool):
        self.connector.trigger_event(MarketEvent.OrderFilled, OrderFilledEvent(
            timestamp=self.now, order_id=order_id, trading_pair=self.connector.pair,
            trade_type=TradeType.BUY if is_buy else TradeType.SELL, order_type=OrderType.LIMIT,
            price=price, amount=amount, trade_fee=AddedToCostTradeFee(flat_fees=[]),
        ))

    def _complete(self, order_id: str, is_buy: bool):
        base, quote = self.connector.pair.split("-")
        if is_buy:
            self.connector.trigger_event(MarketEvent.BuyOrderCompleted, BuyOrderCompletedEvent(
                self.now, order_id, base, quote, Decimal("0"), Decimal("0"), OrderType.LIMIT))
        else:
            self.connector.trigger_event(MarketEvent.SellOrderCompleted, SellOrderCompletedEvent(
                self.now, order_id, base, quote, Decimal("0"), Decimal("0"), OrderType.LIMIT))

    def _cancel(self, order_id: str):
        self.connector.trigger_event(MarketEvent.OrderCancelled, OrderCancelledEvent(self.now, order_id))

    def _failure(self, order_id: str, message: str):
        self.connector.trigger_event(MarketEvent.OrderFailure, MarketOrderFailureEvent(
            self.now, order_id, OrderType.MARKET, message))

    def _settle_swap(self, swap_id: str, side: str, swap_in: Decimal, swap_out: Decimal, pool_fee=Decimal("0")):
        self.connector.swap_results[swap_id] = BeezeeSwapResult(
            client_order_id=swap_id, trading_pair=self.connector.pair, side=side, tx_hash="AB" * 32,
            input_denom="in", output_denom="out", input=swap_in, output=swap_out, pool_fee=pool_fee, price=Decimal("1"),
            input_amount=0, output_amount=0, pool_fee_amount=0,
        )
        self._fill(swap_id, swap_out if side == "buy" else swap_in, Decimal("1"), side == "buy")
        self._complete(swap_id, side == "buy")

    def _settle_last_arb(self, filled: Optional[Decimal] = None):
        """Settles the last arb as the chain would: the swap in the tx, then each fill entry, refunded down to
        `filled` base in total when given."""
        side, levels, request = self.connector.sent[-1]
        fill_ids, swap_id = self.connector.sent_ids[-1]
        total = sum((amount for _, amount in levels), Decimal("0"))
        if request.side == "sell":
            swap_out = self.connector.quote_swap(self.connector.pair, "sell", request.amount).output
            self._settle_swap(swap_id, "sell", request.amount, swap_out)
        else:
            self._settle_swap(swap_id, "buy", request.amount, total)
        left = total if filled is None else filled
        for order_id, (price, amount) in zip(fill_ids, levels):
            taken = min(amount, left)
            left -= taken
            if taken > 0:
                self._fill(order_id, taken, price, side == "buy")
            if taken == amount:
                self._complete(order_id, side == "buy")
            else:
                self._cancel(order_id)

    def _status(self, strategy: BeezeePoolBookArb) -> str:
        return strategy.format_status()

    # === Profitability ===

    async def test_no_trade_when_the_gap_is_within_the_pool_fee(self):
        self.connector.asks = [(Decimal("0.998"), Decimal("100"))]
        strategy = self._strategy()
        self._run(2)
        self.assertEqual([], self.connector.sent)
        self.assertIn("below the pool fee + min_profit_pct", strategy._last_skip_reason)

    async def test_no_trade_when_the_fixed_fees_eat_the_gap(self):
        # 1 TOKEN at 0.99: the pool pays about 0.997, i.e. 0.007 BZE gross, less than 0.205 BZE of fees.
        self.connector.asks = [(Decimal("0.99"), Decimal("1"))]
        strategy = self._strategy()
        self._run(2)
        self.assertEqual([], self.connector.sent)
        self.assertIn("after fees, below min_profit_pct", strategy._last_skip_reason)

    async def test_no_trade_without_a_book_level_across_the_pool_price(self):
        self.connector.asks = [(Decimal("1.01"), Decimal("100"))]
        self.connector.bids = [(Decimal("0.99"), Decimal("100"))]
        strategy = self._strategy()
        self._run(2)
        self.assertEqual([], self.connector.sent)
        self.assertIn("no ask below the pool price", strategy._last_skip_reason)
        self.assertIn("no bid above the pool price", strategy._last_skip_reason)

    async def test_book_to_pool_takes_the_asks_and_sells_the_base_on_the_pool_in_one_tx(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        self._strategy(slippage_pct=Decimal("0.7"))
        self._run(2)
        [(side, levels, request)] = self.connector.sent
        self.assertEqual("buy", side)
        self.assertEqual([(Decimal("0.98"), Decimal("1000"))], levels)
        self.assertEqual(BeezeeSwapRequest("TOKEN-BZE", "sell", Decimal("1000"), Decimal("0.7")), request)

    async def test_pool_to_book_buys_on_the_pool_and_sells_into_the_bids(self):
        self.connector.bids = [(Decimal("1.02"), Decimal("1000"))]
        self._strategy()
        self._run(2)
        [(side, levels, request)] = self.connector.sent
        self.assertEqual("sell", side)
        self.assertEqual([(Decimal("1.02"), Decimal("1000"))], levels)
        self.assertEqual("buy", request.side)
        # The swap's input is the quote that buys exactly the base sold into the bids.
        quote = self.connector.quote_input_for_output("TOKEN-BZE", "buy", Decimal("1000"))
        self.assertEqual(quote.input, request.amount)

    async def test_profit_subtracts_two_taker_fees_and_the_gas_with_bze_as_quote(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        strategy = self._strategy()
        self._run(2)
        plan = strategy.plan_arb(BOOK_TO_POOL, Decimal("1"), strategy.to_quote(FIXED_BZE, Decimal("1")))
        output = self.connector.quote_swap("TOKEN-BZE", "sell", Decimal("1000")).output
        self.assertEqual(FIXED_BZE, plan.fixed_cost)
        self.assertEqual(output - Decimal("980") - FIXED_BZE, plan.net_profit)
        # The pool fee is inside the pool's output; it is reported, not subtracted twice.
        self.assertEqual(Decimal("3") * Decimal("1"), plan.pool_fee_quote)

    async def test_profit_converts_the_fees_at_the_pool_price_with_bze_as_base(self):
        self.connector = FakeArbConnector("BZE", "USDC")
        # Pool price 0.5 USDC per BZE.
        self.connector.pool.reserve_quote = Decimal("500000")
        self.connector.bids = [(Decimal("0.52"), Decimal("1000"))]
        strategy = self._strategy()
        self._run(2)
        [(side, _, request)] = self.connector.sent
        self.assertEqual(("sell", "buy"), (side, request.side))
        fixed = strategy.to_quote(FIXED_BZE, Decimal("0.5"))
        self.assertEqual(FIXED_BZE * Decimal("0.5"), fixed)
        plan = strategy.plan_arb(POOL_TO_BOOK, Decimal("0.5"), fixed)
        cost = self.connector.quote_input_for_output("BZE-USDC", "buy", Decimal("1000")).input
        self.assertEqual(Decimal("520") - cost - fixed, plan.net_profit)

    async def test_a_pair_missing_a_venue_is_refused_once_naming_it(self):
        cases = {
            ("pool",): "TOKEN-BZE has no order-book market (venues: pool only); pool/book arb needs a liquidity pool "
                       "and an order-book market",
            ("book",): "TOKEN-BZE has no liquidity pool (venues: order book only); pool/book arb needs a liquidity pool "
                       "and an order-book market",
        }
        for venues, reason in cases.items():
            self.connector = FakeArbConnector("TOKEN", "BZE")
            self.connector.venues = venues
            self.connector.asks = [(Decimal("0.9"), Decimal("1000"))]
            strategy = self._strategy()
            with patch.object(strategy, "log_with_clock") as log:
                self._run(30)
                self._run(30)
            self.assertEqual([], self.connector.sent)
            self.assertEqual(1, sum("refuses to start" in str(call) for call in log.call_args_list))
            self.assertIn(f"Refused: {reason}", self._status(strategy))

    async def test_status_shows_the_venues(self):
        strategy = self._strategy()
        self._run(2)
        self.assertIn("venues: order book + pool", self._status(strategy))

    async def test_a_pair_without_bze_is_refused(self):
        self.connector = FakeArbConnector("TOKEN", "USDC")
        self.connector.asks = [(Decimal("0.9"), Decimal("1000"))]
        strategy = self._strategy()
        self._run(30)
        self.assertEqual([], self.connector.sent)
        self.assertIn("has no BZE side", self._status(strategy))

    # === Sizing ===

    async def test_sizing_stops_at_the_profitable_depth(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000")), (Decimal("0.99"), Decimal("100000"))]
        self._strategy(max_trade_base=Decimal("1000000"))
        self._run(2)
        [(_, levels, request)] = self.connector.sent
        self.assertEqual((Decimal("0.98"), Decimal("1000")), levels[0])
        price, part = levels[1]
        self.assertEqual(Decimal("0.99"), price)
        self.assertTrue(Decimal("0") < part < Decimal("100000"))
        self.assertEqual(Decimal("1000") + part, request.amount)
        # The second level is taken as far as the pool still pays its price + min_profit_pct for the part taken, and
        # not one base step further.
        margin = Decimal("1.003")
        before = self._pool_output(Decimal("1000"))
        self.assertGreaterEqual(self._pool_output(Decimal("1000") + part) - before, price * part * margin)
        larger = part + BASE_STEP
        self.assertLess(self._pool_output(Decimal("1000") + larger) - before, price * larger * margin)

    def _pool_output(self, base_amount: Decimal) -> Decimal:
        return self.connector.quote_swap(self.connector.pair, "sell", base_amount).output

    async def test_sizing_stops_at_max_trade_base(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        self._strategy(max_trade_base=Decimal("500"))
        self._run(2)
        [(_, levels, request)] = self.connector.sent
        self.assertEqual([(Decimal("0.98"), Decimal("500"))], levels)
        self.assertEqual(Decimal("500"), request.amount)

    async def test_sizing_keeps_the_base_reserve(self):
        self.connector.balances["TOKEN"] = Decimal("300")
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        self._strategy(reserve_base=Decimal("100"))
        self._run(2)
        [(_, levels, _)] = self.connector.sent
        self.assertEqual([(Decimal("0.98"), Decimal("200"))], levels)

    async def test_sizing_keeps_the_quote_reserve_and_the_bze_for_fees(self):
        # 50 BZE spendable after the 10 BZE reserve and the arb's own fees: at most 51.020408 TOKEN at 0.98.
        self.connector.balances["BZE"] = Decimal("60") + FIXED_BZE
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        self._strategy(reserve_quote=Decimal("10"))
        self._run(2)
        [(_, levels, _)] = self.connector.sent
        expected = (Decimal("50") / Decimal("0.98") / BASE_STEP).to_integral_value(rounding=ROUND_FLOOR) * BASE_STEP
        self.assertEqual([(Decimal("0.98"), expected)], levels)

    async def test_a_cut_level_below_the_chain_minimum_is_dropped(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000")), (Decimal("0.99"), Decimal("100000"))]
        self.connector.chain_minimum = Decimal("5000")
        strategy = self._strategy()
        self._run(2)
        self.assertEqual([], self.connector.sent)
        self.assertIn("of the level at 0.98 is below the chain minimum", strategy._last_skip_reason)

    # === One arb at a time, cooldown ===

    async def test_waits_for_the_arb_to_settle_then_for_the_cooldown(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        strategy = self._strategy(cooldown_blocks=2)
        self._run(2)
        self.assertEqual(1, len(self.connector.sent))
        self._new_block()
        self.assertEqual(1, len(self.connector.sent))
        self.assertEqual("waiting for the last arb to settle", strategy._last_skip_reason)

        self._settle_last_arb()
        self._new_block()
        self.assertEqual(1, len(self.connector.sent))
        self.assertIn("cooldown", strategy._last_skip_reason)
        self._new_block()
        self.assertEqual(2, len(self.connector.sent))

    async def test_cooldown_ends_on_time_without_block_notifications(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        self._strategy(cooldown_blocks=2, check_interval_seconds=5)
        self._run(2)
        self._settle_last_arb()
        self._run(15)
        self.assertEqual(1, len(self.connector.sent))
        self._run(10)
        self.assertEqual(2, len(self.connector.sent))

    async def test_a_failed_arb_tx_trades_nothing_and_leaves_no_imbalance(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        strategy = self._strategy()
        self._run(2)
        fill_ids, swap_id = self.connector.sent_ids[-1]
        for order_id in [swap_id] + fill_ids:
            self._failure(order_id, "the resulted amount is too low")
        self.assertIsNone(strategy._arb)
        self.assertEqual([], strategy._imbalances)
        self.assertEqual(0, strategy._trades_today)

    async def test_status_shows_prices_profit_and_the_skip_reason(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        self.connector.bids = [(Decimal("0.97"), Decimal("10"))]
        strategy = self._strategy()
        self._run(2)
        self.connector.gas_paid = Decimal("0.004")
        self._settle_last_arb()
        status = self._status(strategy)
        self.assertIn("Pool price: 1", status)
        self.assertIn("best bid: 0.97", status)
        self.assertIn("best ask: 0.98", status)
        self.assertIn("1 trade(s)", status)
        self.assertIn("tradebin fees 0.2 BZE, gas 0.004 BZE", status)
        output = self.connector.quote_swap("TOKEN-BZE", "sell", Decimal("1000")).output
        self.assertEqual(output - Decimal("980") - Decimal("0.2") - Decimal("0.004"), strategy._profit_today)
        self.assertIn("Open imbalance: 0 TOKEN", status)
        self.assertIn("Gap: best ask 0.98 < pool 1", status)

    # === Partial fills ===

    async def test_a_partly_refunded_fill_is_rebalanced_once_on_the_pool(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        strategy = self._strategy(cooldown_blocks=0, max_rebalance_cost_pct=Decimal("1"))
        self._run(2)
        # The swap sold 1000 TOKEN, the fill only got 600: 400 TOKEN short.
        self._settle_last_arb(filled=Decimal("600"))
        self.assertEqual(Decimal("-400"), strategy.open_imbalance)
        self.assertIn("Open imbalance: -400 TOKEN", self._status(strategy))

        self.connector.asks = []
        self._new_block()
        [(side, amount, _)] = self.connector.swaps
        self.assertEqual("buy", side)
        self.assertEqual(self.connector.quote_input_for_output("TOKEN-BZE", "buy", Decimal("400")).input, amount)

        swap_id = f"rebalance-buy-{self.connector._next_id}"
        self._settle_swap(swap_id, "buy", amount, Decimal("400"))
        self.assertEqual(Decimal("0"), strategy.open_imbalance)
        self._new_block()
        self._new_block()
        self.assertEqual(1, len(self.connector.swaps))

    async def test_a_refunded_sell_fill_is_rebalanced_by_selling_on_the_pool(self):
        self.connector.bids = [(Decimal("1.02"), Decimal("1000"))]
        strategy = self._strategy(cooldown_blocks=0)
        self._run(2)
        self._settle_last_arb(filled=Decimal("250"))
        self.assertEqual(Decimal("750"), strategy.open_imbalance)
        self.connector.bids = []
        self._new_block()
        self.assertEqual([("sell", Decimal("750"), Decimal("0.5"))], self.connector.swaps)

    async def test_an_imbalance_too_costly_to_rebalance_is_kept_and_never_retried(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        strategy = self._strategy(cooldown_blocks=0, max_rebalance_cost_pct=Decimal("0.01"))
        self._run(2)
        self._settle_last_arb(filled=Decimal("600"))
        self.connector.asks = []
        for _ in range(3):
            self._new_block()
        self.assertEqual([], self.connector.swaps)
        self.assertEqual(Decimal("-400"), strategy.open_imbalance)
        self.assertIn("kept (rebalancing costs", self._status(strategy))

    async def test_an_imbalance_is_kept_when_rebalancing_is_off(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        strategy = self._strategy(cooldown_blocks=0, rebalance_on_partial=False)
        self._run(2)
        self._settle_last_arb(filled=Decimal("600"))
        self.connector.asks = []
        self._new_block()
        self._new_block()
        self.assertEqual([], self.connector.swaps)
        self.assertIn("kept (rebalance_on_partial is off)", self._status(strategy))

    async def test_a_failed_rebalance_is_not_retried(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        strategy = self._strategy(cooldown_blocks=0)
        self._run(2)
        self._settle_last_arb(filled=Decimal("600"))
        self.connector.asks = []
        self._new_block()
        self._failure(f"rebalance-buy-{self.connector._next_id}", "the resulted amount is too low")
        for _ in range(3):
            self._new_block()
        self.assertEqual(1, len(self.connector.swaps))
        self.assertEqual(Decimal("-400"), strategy.open_imbalance)
        self.assertIn("kept (rebalance failed", self._status(strategy))

    async def test_a_rebalance_refused_up_front_is_not_retried(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        strategy = self._strategy(cooldown_blocks=0)
        self._run(2)
        self._settle_last_arb(filled=Decimal("600"))
        self.connector.asks = []
        self.connector.swap_error = BeezeeSwapError("not enough BZE")
        self._new_block()
        self._new_block()
        self.assertIn("kept (rebalance refused", self._status(strategy))
        self.assertTrue(strategy._imbalances[0].attempted)

    # === Halts ===

    async def test_a_halted_pool_stops_trading_until_it_is_lifted(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        self.connector.pool.halted = True
        strategy = self._strategy()
        self._run(2)
        self._new_block()
        self.assertEqual([], self.connector.sent)
        self.assertEqual("market or pool halted", strategy._last_skip_reason)
        self.assertIn("(HALTED)", self._status(strategy))

        self.connector.pool.halted = False
        self._new_block()
        self.assertEqual(1, len(self.connector.sent))

    async def test_a_halted_refusal_stops_trading(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        self.connector.fill_error = BeezeeFillError("Beezee market TOKEN/BZE is halted (denom utoken)")
        strategy = self._strategy()
        self._run(2)
        self.assertEqual("market or pool halted", strategy._last_skip_reason)
        self.assertTrue(strategy._halted)

    async def test_other_refusals_are_reported_as_the_skip_reason(self):
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        self.connector.fill_error = BeezeeSwapError("not enough BZE for the swap")
        strategy = self._strategy()
        self._run(2)
        self.assertEqual("arb refused up front: not enough BZE for the swap", strategy._last_skip_reason)
        self.assertFalse(strategy._halted)

    async def test_a_pool_not_loaded_yet_waits_without_a_skip(self):
        # Right after a start the pair has the pool venue but the connector has not read the pool yet.
        self.connector.asks = [(Decimal("0.98"), Decimal("1000"))]
        pool, self.connector.pool = self.connector.pool, None
        strategy = self._strategy(slippage_pct=Decimal("0.7"))
        self._run(2)
        self.assertIsNone(strategy._last_skip_reason)
        self.assertEqual([], self.connector.sent)
        self.connector.pool = pool
        self._run(1)
        self.assertEqual(1, len(self.connector.sent))

    # === Example ===

    async def test_example_config_loads(self):
        config = BeezeePoolBookArbConfig(**yaml.safe_load(EXAMPLE.read_text()))
        self.assertEqual("beezee", config.exchange)
        self.assertEqual("BZE-ERC20:0XA00C59FF5A080D2B954D0C75E46E22A0C371235A", config.trading_pair)
        self.assertEqual("beezee_pool_book_arb.py", config.script_file_name)
        self.assertTrue(config.rebalance_on_partial)
        self.assertEqual(2, config.cooldown_blocks)
