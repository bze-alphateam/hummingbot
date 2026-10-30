import unittest
from decimal import Decimal
from pathlib import Path
from test.hummingbot.connector.exchange.beezee.test_beezee_price_bands import STEP, TAKER_FEE, FakeBandsConnector
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import yaml

from hummingbot.connector.exchange.beezee.beezee_amm import BeezeeSwapError, BeezeeSwapResult
from hummingbot.core.clock import Clock
from hummingbot.core.clock_mode import ClockMode
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    MarketEvent,
    MarketOrderFailureEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
)
from scripts.beezee_treasury_twap import BeezeeTreasuryTwap, BeezeeTreasuryTwapConfig

GAS = Decimal("0.005")
START = 1_800_000_000.0
INTERVAL = 300
EXAMPLE = Path(__file__).resolve().parents[5] / "conf" / "scripts" / "beezee_treasury_twap_token_bze.yml.example"


class BeezeeTreasuryTwapTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # BZE as the quote: the prices and the fees are in BZE.
        self.connector = FakeBandsConnector("TOKEN", "BZE")

    def _strategy(self, **settings) -> BeezeeTreasuryTwap:
        # 1200 TOKEN over one hour: 12 slices of 100, five minutes apart.
        values = {
            "trading_pair": self.connector.pair,
            "side": "sell",
            "total_amount": Decimal("1200"),
            "duration_hours": Decimal("1"),
            "slice_interval_seconds": INTERVAL,
            "max_impact_pct": Decimal("5"),
            "venue": "pool",
            "estimated_gas_bze": GAS,
        }
        values.update(settings)
        config = BeezeeTreasuryTwapConfig(**values)
        with patch("asyncio.create_task", return_value=MagicMock()), \
                patch("hummingbot.strategy.strategy_v2_base.StrategyV2Base.listen_to_executor_actions", return_value=AsyncMock()), \
                patch("hummingbot.strategy.strategy_v2_base._get_executor_orchestrator_class"), \
                patch("hummingbot.strategy.strategy_v2_base.MarketDataProvider"):
            strategy = BeezeeTreasuryTwap({"beezee": self.connector}, config)
        # The clock's first tick only finds the connector ready; the first slice goes on the next one.
        self.clock = Clock(ClockMode.BACKTEST, 1, START - 2, START + 86400)
        self.clock.add_iterator(strategy)
        self.now = START - 2
        self.strategy = strategy
        return strategy

    def _run(self, seconds: int = 1):
        self.now += seconds
        self.clock.backtest_til(self.now)

    def _start(self) -> BeezeeTreasuryTwap:
        """Runs until the first slice is due and sent (or skipped)."""
        self._run(2)
        return self.strategy

    def _next_slice(self):
        """Runs to the next slice time."""
        self._run(INTERVAL)

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

    def _fill(self, order_id: str, side: str, price: Decimal, amount: Decimal):
        self.connector.trigger_event(MarketEvent.OrderFilled, OrderFilledEvent(
            timestamp=self.now, order_id=order_id, trading_pair=self.connector.pair,
            trade_type=TradeType.BUY if side == "buy" else TradeType.SELL, order_type=OrderType.LIMIT,
            price=price, amount=amount, trade_fee=AddedToCostTradeFee(flat_fees=[]),
        ))

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
            self._fill(order_id, side, price, amount)
            self._complete(order_id, side == "buy")

    def _impact(self, side: str, amount: Decimal) -> Decimal:
        spot = self.connector.get_pool_price(self.connector.pair)
        output = self.connector.quote_swap(self.connector.pair, side, amount).output
        effective = amount / output if side == "buy" else output / amount
        return ((effective - spot) if side == "buy" else (spot - effective)) / spot * 100

    def _swapped(self):
        return [amount for _, amount, _ in self.connector.swaps]

    # === Slicing ===

    async def test_even_slices_every_interval(self):
        strategy = self._strategy()
        self._start()
        self.assertEqual(12, strategy.slice_count)
        self.assertEqual([Decimal("100")], self._swapped())
        self._settle_last_swap()
        self._run(INTERVAL - 10)
        self.assertEqual(1, len(self.connector.swaps))
        self._run(10)
        self.assertEqual([Decimal("100"), Decimal("100")], self._swapped())
        self._settle_last_swap()
        self.assertEqual(Decimal("200"), strategy._done_base)
        self.assertEqual(Decimal("1000"), strategy.remaining)

    async def test_the_run_completes_and_stops(self):
        strategy = self._strategy(total_amount=Decimal("1000"))
        self._start()
        for _ in range(12):
            self._settle_last_swap()
            self._next_slice()
        self.assertEqual(12, len(self.connector.swaps))
        # 1000 / 12 = 83.333333 per slice; each slice takes what is left over the slices left, so the rounding dust
        # moves into the later slices and the last one closes the total.
        self.assertEqual(Decimal("83.333333"), self.connector.swaps[0][1])
        self.assertEqual(Decimal("83.333334"), self.connector.swaps[-1][1])
        self.assertEqual(Decimal("1000"), strategy._done_base)
        self.assertEqual("completed", strategy._finished)
        self.assertIn("STOPPED: completed", strategy.format_status())

    async def test_catch_up_spreads_skipped_slices_up_to_twice_the_even_slice(self):
        # Below the limit price for the first slices, then back above it.
        self._set_pool_price(Decimal("0.8"))
        strategy = self._strategy(limit_price=Decimal("0.9"))
        self._start()
        for _ in range(5):
            self._next_slice()
        self.assertEqual([], self.connector.swaps)
        self.assertEqual(6, sum(strategy._skips.values()))
        self._set_pool_price(Decimal("1"))
        self._next_slice()
        # 1200 left over 6 slices = 200: exactly the 2x cap.
        self.assertEqual([Decimal("200")], self._swapped())
        self._settle_last_swap()

        self._set_pool_price(Decimal("0.8"))
        self.connector.swaps.clear()
        self.connector.swap_ids.clear()
        self._strategy(limit_price=Decimal("0.9"))
        self._start()
        for _ in range(7):
            self._next_slice()
        self._set_pool_price(Decimal("1"))
        self._next_slice()
        # 1200 left over 4 slices = 300, capped at 2 x 100.
        self.assertEqual([Decimal("200")], self._swapped())

    async def test_without_catch_up_skipped_slices_are_left_over(self):
        self._set_pool_price(Decimal("0.8"))
        strategy = self._strategy(limit_price=Decimal("0.9"), catch_up=False)
        self._start()
        for _ in range(7):
            self._next_slice()
        self._set_pool_price(Decimal("1"))
        self._next_slice()
        self.assertEqual([Decimal("100")], self._swapped())
        self.assertEqual(Decimal("800"), strategy._forgone_base)

    # === Impact cap ===

    async def test_the_impact_cap_shrinks_the_slice(self):
        strategy = self._strategy(total_amount=Decimal("120000"), max_impact_pct=Decimal("0.5"))
        self._start()
        amount = self.connector.swaps[0][1]
        self.assertLess(amount, Decimal("10000"))
        self.assertLessEqual(self._impact("sell", amount), Decimal("0.5"))
        self.assertGreater(self._impact("sell", amount + STEP), Decimal("0.5"))
        self.assertEqual("impact cap", strategy._pending.plan.limit)
        # The cut part waits for the next slices (catch_up) and is not traded bigger now.
        self.assertEqual(Decimal("10000") - amount, strategy._forgone_base)

    async def test_a_slice_whose_minimum_breaks_the_impact_cap_is_skipped(self):
        # The pool fee alone (0.3%) is above the cap.
        strategy = self._strategy(max_impact_pct=Decimal("0.2"))
        self._start()
        self.assertEqual([], self.connector.swaps)
        self.assertIn("impact cap: even the smallest swap", strategy._last_skip)
        self.assertEqual(Decimal("100"), strategy._forgone_base)

    # === Limit price ===

    async def test_limit_price_skips_a_sell_below_it_and_a_buy_above_it(self):
        self._set_pool_price(Decimal("0.8"))
        strategy = self._strategy(limit_price=Decimal("0.9"))
        self._start()
        self.assertEqual([], self.connector.swaps)
        self.assertIn("limit price", strategy._last_skip)

        self._set_pool_price(Decimal("1.2"))
        strategy = self._strategy(side="buy", limit_price=Decimal("1"))
        self._start()
        self.assertEqual([], self.connector.swaps)
        self.assertIn("limit price: the pool price 1.2 is past 1", strategy._last_skip)

    async def test_limit_price_cuts_the_slice(self):
        strategy = self._strategy(total_amount=Decimal("120000"), limit_price=Decimal("0.99"))
        self._start()
        amount = self.connector.swaps[0][1]
        output = self.connector.quote_swap(self.connector.pair, "sell", amount).output
        self.assertGreaterEqual(output / amount, Decimal("0.99"))
        more = self.connector.quote_swap(self.connector.pair, "sell", amount + STEP).output
        self.assertLess(more / (amount + STEP), Decimal("0.99"))
        self.assertEqual("limit price", strategy._pending.plan.limit)

    async def test_the_book_never_takes_a_level_past_the_limit_price(self):
        self.connector.bids = [(Decimal("1.05"), Decimal("40")), (Decimal("1.0"), Decimal("1000"))]
        self._strategy(venue="book", limit_price=Decimal("1.02"))
        self._start()
        self.assertEqual([("sell", [(Decimal("1.05"), Decimal("40"))])], self.connector.fills)

    # === Venue ===

    async def test_best_takes_the_book_when_it_pays_more(self):
        self.connector.bids = [(Decimal("1.05"), Decimal("1000"))]
        strategy = self._strategy(venue="best")
        self._start()
        self.assertEqual([], self.connector.swaps)
        self.assertEqual([("sell", [(Decimal("1.05"), Decimal("100"))])], self.connector.fills)
        self._settle_last_fill()
        self.assertEqual(Decimal("100"), strategy._done_base)
        self.assertEqual(Decimal("105"), strategy._done_quote)
        self.assertEqual(1, strategy._slices_traded["book"])

    async def test_best_takes_the_pool_when_it_pays_more(self):
        self.connector.bids = [(Decimal("0.95"), Decimal("1000"))]
        self._strategy(venue="best")
        self._start()
        self.assertEqual([], self.connector.fills)
        self.assertEqual([Decimal("100")], self._swapped())

    async def test_best_buy_takes_the_cheaper_venue_both_ways(self):
        self.connector.asks = [(Decimal("0.95"), Decimal("1000"))]
        self._strategy(side="buy", venue="best")
        self._start()
        self.assertEqual([("buy", [(Decimal("0.95"), Decimal("100"))])], self.connector.fills)

        self.connector.fills.clear()
        self.connector.asks = [(Decimal("1.05"), Decimal("1000"))]
        self._strategy(side="buy", venue="best")
        self._start()
        self.assertEqual([], self.connector.fills)
        self.assertEqual("buy", self.connector.swaps[-1][0])

    async def test_best_on_a_pool_only_pair_trades_the_pool_and_never_reads_the_book(self):
        self.connector.venues = ("pool",)
        self.connector.bids = [(Decimal("1.5"), Decimal("1000"))]
        self.connector.levels_to_take = Mock(side_effect=AssertionError("a pool-only pair has no book"))
        strategy = self._strategy(venue="best")
        self._start()
        self.assertIsNone(strategy._refused)
        self.assertEqual([Decimal("100")], self._swapped())
        self.assertEqual([], self.connector.fills)
        self.connector.levels_to_take.assert_not_called()
        self.assertEqual("pool", strategy.reference_price()[1])
        self.assertIn("venues: pool only", strategy.summary())

    async def test_best_on_a_book_only_pair_trades_the_book_around_the_book_mid(self):
        self.connector.venues = ("book",)
        self.connector.pool = None
        self.connector.bids = [(Decimal("0.95"), Decimal("1000"))]
        self.connector.asks = [(Decimal("1.05"), Decimal("1000"))]
        strategy = self._strategy(venue="best")
        self._start()
        self.assertEqual([], self.connector.swaps)
        self.assertEqual([("sell", [(Decimal("0.95"), Decimal("100"))])], self.connector.fills)
        self.assertEqual((Decimal("1.00"), "book_mid"), strategy.reference_price())

    async def test_a_pair_without_the_configured_venue_is_refused_naming_it(self):
        cases = [
            (("book",), "pool", "TOKEN-BZE has no liquidity pool (venues: order book only); treasury TWAP with venue: "
                                "pool needs a liquidity pool"),
            (("pool",), "book", "TOKEN-BZE has no order-book market (venues: pool only); treasury TWAP with venue: "
                                "book needs an order-book market"),
        ]
        for venues, venue, reason in cases:
            self.connector = FakeBandsConnector("TOKEN", "BZE")
            self.connector.venues = venues
            self.connector.bids = [(Decimal("1.0"), Decimal("1000"))]
            strategy = self._strategy(venue=venue)
            with patch.object(strategy, "log_with_clock") as log:
                self._start()
                self._next_slice()
            self.assertEqual([], self.connector.swaps)
            self.assertEqual([], self.connector.fills)
            self.assertEqual(1, sum("refuses to start" in str(call) for call in log.call_args_list))
            self.assertIn(f"Refused: {reason}", strategy.format_status())

    async def test_book_walk_stops_at_the_impact_cap(self):
        self.connector.bids = [(Decimal("1.0"), Decimal("30")), (Decimal("0.98"), Decimal("30")),
                               (Decimal("0.9"), Decimal("1000"))]
        strategy = self._strategy(venue="book", max_impact_pct=Decimal("5"))
        self._start()
        self.assertEqual([("sell", [(Decimal("1.0"), Decimal("30")), (Decimal("0.98"), Decimal("30"))])],
                         self.connector.fills)
        self.assertEqual("impact cap", strategy._pending.plan.limit)

    # === Book outcomes ===

    async def test_a_partly_refunded_book_slice_keeps_the_refund_in_what_is_left(self):
        self.connector.bids = [(Decimal("1.0"), Decimal("1000"))]
        strategy = self._strategy(venue="book")
        self._start()
        (order_id,) = self.connector.fill_ids[-1]
        self._fill(order_id, "sell", Decimal("1.0"), Decimal("60"))
        self.connector.trigger_event(MarketEvent.OrderCancelled, OrderCancelledEvent(self.now, order_id))
        self.assertIsNone(strategy._pending)
        self.assertEqual(Decimal("60"), strategy._done_base)
        self.assertEqual(Decimal("1140"), strategy.remaining)
        self.assertEqual(Decimal("0"), strategy._forgone_base)
        self._next_slice()
        # 1140 over the 11 slices left.
        self.assertEqual(Decimal("103.636363"), self.connector.fills[-1][1][0][1])

    async def test_a_book_slice_with_a_failed_entry_books_what_the_others_filled(self):
        self.connector.bids = [(Decimal("1.0"), Decimal("60")), (Decimal("0.99"), Decimal("60"))]
        strategy = self._strategy(venue="book")
        self._start()
        self.assertEqual([("sell", [(Decimal("1.0"), Decimal("60")), (Decimal("0.99"), Decimal("40"))])],
                         self.connector.fills)
        first, second = self.connector.fill_ids[-1]
        self._fill(first, "sell", Decimal("1.0"), Decimal("60"))
        self._complete(first, False)
        self.connector.trigger_event(MarketEvent.OrderFailure, MarketOrderFailureEvent(
            self.now, second, OrderType.LIMIT, "no outcome within 20 blocks"))
        self.assertIsNone(strategy._pending)
        self.assertEqual(Decimal("60"), strategy._done_base)
        self.assertEqual(Decimal("60"), strategy._done_quote)
        self.assertEqual(Decimal("1140"), strategy.remaining)
        self.assertEqual(1, strategy._slices_traded["book"])
        self.assertEqual(TAKER_FEE, strategy._tradebin_fees)
        self.assertIsNone(strategy._finished)

    async def test_a_failed_swap_trades_nothing_and_stays_left(self):
        strategy = self._strategy()
        self._start()
        self.connector.trigger_event(MarketEvent.OrderFailure, MarketOrderFailureEvent(
            self.now, self.connector.swap_ids[-1], OrderType.MARKET, "the resulted amount is too low"))
        self.assertIsNone(strategy._pending)
        self.assertEqual(Decimal("1200"), strategy.remaining)
        self.assertEqual(Decimal("0"), strategy._tradebin_fees)

    # === End ===

    async def test_the_deadline_stops_with_a_leftover_summary(self):
        self._set_pool_price(Decimal("0.8"))
        strategy = self._strategy(duration_hours=Decimal("0.5"), limit_price=Decimal("0.9"))
        self._start()
        for _ in range(6):
            self._next_slice()
        self.assertEqual("deadline reached", strategy._finished)
        self.assertEqual([], self.connector.swaps)
        status = strategy.format_status()
        self.assertIn("STOPPED: deadline reached", status)
        self.assertIn("Done: 0 / 1200 TOKEN (0.00%)", status)
        self.assertIn("left 1200 TOKEN", status)
        self.assertIn("skipped: 6", status)
        # Nothing is forced through once the run stopped.
        self._set_pool_price(Decimal("1"))
        self._next_slice()
        self.assertEqual([], self.connector.swaps)

    async def test_the_deadline_waits_for_the_slice_in_flight(self):
        strategy = self._strategy(duration_hours=Decimal("0.1"))
        self._start()
        self._next_slice()
        self._run(INTERVAL)
        self.assertIsNone(strategy._finished)
        self._settle_last_swap()
        self._run()
        self.assertEqual("deadline reached", strategy._finished)

    async def test_a_halted_pool_stops_the_script(self):
        self.connector.pool.halted = True
        strategy = self._strategy()
        self._start()
        self.connector.pool.halted = False
        self._next_slice()
        self.assertEqual([], self.connector.swaps)
        self.assertIn("halted", strategy._finished)
        self.assertIn("STOPPED", strategy.format_status())

    async def test_a_halted_refusal_stops_the_script(self):
        self.connector.swap_error = BeezeeSwapError("market TOKEN-BZE is halted")
        strategy = self._strategy()
        self._start()
        self.connector.swap_error = None
        self._next_slice()
        self.assertEqual([], self.connector.swaps)
        self.assertEqual("market TOKEN-BZE is halted", strategy._finished)

    async def test_a_refusal_skips_the_slice(self):
        self.connector.swap_error = BeezeeSwapError("balance too low")
        strategy = self._strategy()
        self._start()
        self.assertEqual("refused up front: balance too low", strategy._last_skip)
        self.assertEqual(Decimal("100"), strategy._forgone_base)
        self.connector.swap_error = None
        self._next_slice()
        # The refused slice is spread over the 11 slices left: 1200 / 11.
        self.assertEqual([Decimal("109.090909")], self._swapped())

    # === Reserves ===

    async def test_a_sell_never_touches_the_base_reserve(self):
        self.connector.balances["TOKEN"] = Decimal("1050")
        self._strategy(keep_base_reserve=Decimal("1000"))
        self._start()
        self.assertEqual([Decimal("50")], self._swapped())

    async def test_a_buy_never_touches_the_quote_reserve(self):
        self.connector.balances["BZE"] = Decimal("10000")
        self._strategy(side="buy", keep_quote_reserve=Decimal("9950"))
        self._start()
        amount = self.connector.swaps[0][1]
        # The taker fee and the gas come out of the BZE above the reserve too.
        self.assertLessEqual(amount + TAKER_FEE + GAS, Decimal("50"))
        self.assertGreater(amount, Decimal("49"))

    async def test_nothing_above_the_reserve_skips(self):
        self.connector.balances["TOKEN"] = Decimal("1000")
        strategy = self._strategy(keep_base_reserve=Decimal("1000"))
        self._start()
        self.assertEqual([], self.connector.swaps)
        self.assertEqual("no TOKEN above the reserve", strategy._last_skip)

    async def test_a_reserve_above_the_balance_is_refused(self):
        self.connector.balances["TOKEN"] = Decimal("10")
        strategy = self._strategy(keep_base_reserve=Decimal("11"))
        self._start()
        self.assertEqual([], self.connector.swaps)
        self.assertIn("keep_base_reserve 11 is larger than the TOKEN balance 10", strategy.format_status())

    async def test_a_pair_without_bze_is_refused(self):
        self.connector = FakeBandsConnector("TOKEN", "USDC")
        strategy = self._strategy()
        self._start()
        self.assertEqual([], self.connector.swaps)
        self.assertIn("TOKEN-USDC has no BZE side", strategy.format_status())

    # === Status and example ===

    async def test_status_shows_progress_execution_and_the_next_slice(self):
        strategy = self._strategy(keep_base_reserve=Decimal("5"), keep_quote_reserve=Decimal("7"))
        self._start()
        self._settle_last_swap()
        self._run(INTERVAL // 2)
        status = strategy.format_status()
        self.assertIn("Done: 100 / 1200 TOKEN (8.33%)", status)
        self.assertIn("vs reference TWAP 1", status)
        self.assertIn("Next slice: 100 TOKEN (pool", status)
        self.assertIn("% elapsed", status)
        self.assertIn("Fees: tradebin 0.1 BZE", status)
        self.assertIn("Reserves kept: 5 TOKEN, 7 BZE", status)

    def test_example_config_loads(self):
        config = BeezeeTreasuryTwapConfig(**yaml.safe_load(EXAMPLE.read_text()))
        self.assertEqual("beezee", config.exchange)
        self.assertEqual("TOKEN-BZE", config.trading_pair)
        self.assertEqual("beezee_treasury_twap.py", config.script_file_name)
        self.assertEqual("best", config.venue)
        self.assertEqual(300, config.slice_interval_seconds)

    async def test_an_unknown_venue_is_refused_by_the_config(self):
        with self.assertRaises(ValueError):
            BeezeeTreasuryTwapConfig(total_amount=Decimal("1"), duration_hours=Decimal("1"), venue="cex")
