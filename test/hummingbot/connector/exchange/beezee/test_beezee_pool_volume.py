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
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.clock import Clock
from hummingbot.core.clock_mode import ClockMode
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    MarketEvent,
    MarketOrderFailureEvent,
    SellOrderCompletedEvent,
)
from scripts.beezee_pool_volume import BeezeePoolVolume, BeezeePoolVolumeConfig

STEP = Decimal("0.000001")
TAKER_FEE = Decimal("0.1")
GAS = Decimal("0.005")
# Two taker fees + the gas estimate, in BZE.
FEES_BZE = 2 * TAKER_FEE + GAS
# 08:00 UTC: 57,600 s, i.e. 192 cycles of 300 s, left in the day.
START = 1_800_000_000.0
NEXT_MIDNIGHT = 1_800_057_600.0
EXAMPLE = Path(__file__).resolve().parents[5] / "conf" / "scripts" / "beezee_pool_volume_mainnet.yml.example"


def floor_step(amount: Decimal) -> Decimal:
    return (amount / STEP).to_integral_value(rounding=ROUND_FLOOR) * STEP


@dataclass
class FakePool:
    pool_id: str
    reserve_base: Decimal
    reserve_quote: Decimal
    fee: Decimal = Decimal("0.003")
    halted: bool = False


class FakePoolConnector(ConnectorBase):
    """The parts of the BeeZee connector the script uses: a constant-product pool (fee from the input, like the chain)
    and the round-trip sender. Events are triggered on it like the real one does."""

    def __init__(self, base: str, quote: str):
        super().__init__()
        self.pair = f"{base}-{quote}"
        self.pool: Optional[FakePool] = FakePool(f"{base}_{quote}", Decimal("1000000"), Decimal("1000000"))
        self.balances: Dict[str, Decimal] = {base: Decimal("1000000"), quote: Decimal("1000000")}
        self.taker_fee: Optional[TokenAmount] = TokenAmount("BZE", TAKER_FEE)
        self.gas_paid = Decimal("0")
        self.rules = {self.pair: TradingRule(self.pair, min_base_amount_increment=STEP, min_quote_amount_increment=STEP)}
        self.sent: List[List[BeezeeSwapRequest]] = []
        self.sent_ids: List[List[str]] = []
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

    def get_available_balance(self, currency: str) -> Decimal:
        return self.balances.get(currency, Decimal("0"))

    def gas_fees_paid(self) -> Decimal:
        return self.gas_paid

    def get_fee(self, base, quote, order_type, order_side, amount, price=Decimal("NaN"), is_maker=None):
        return AddedToCostTradeFee(flat_fees=[self.taker_fee] if self.taker_fee is not None else [])

    def amm_swaps(self, swaps: List[BeezeeSwapRequest]) -> List[str]:
        if self.swap_error is not None:
            raise self.swap_error
        ids = []
        for request in swaps:
            self._next_id += 1
            ids.append(f"swap-{request.side}-{self._next_id}")
        self.sent.append(list(swaps))
        self.sent_ids.append(ids)
        return ids

    def get_swap_result(self, client_order_id: str) -> Optional[BeezeeSwapResult]:
        return self.swap_results.get(client_order_id)


class BeezeePoolVolumeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # BZE as the quote: fees count 1:1.
        self.connector = FakePoolConnector("TOKEN", "BZE")

    def _strategy(self, start: float = START, **settings) -> BeezeePoolVolume:
        values = {"trading_pair": self.connector.pair, "estimated_gas_bze": GAS}
        values.update(settings)
        config = BeezeePoolVolumeConfig(**values)
        with patch("asyncio.create_task", return_value=MagicMock()), \
                patch("hummingbot.strategy.strategy_v2_base.StrategyV2Base.listen_to_executor_actions", return_value=AsyncMock()), \
                patch("hummingbot.strategy.strategy_v2_base._get_executor_orchestrator_class"), \
                patch("hummingbot.strategy.strategy_v2_base.MarketDataProvider"):
            strategy = BeezeePoolVolume({"beezee": self.connector}, config)
        # The clock's first tick only finds the connector ready; the first cycle runs on the next one.
        self.clock = Clock(ClockMode.BACKTEST, 1, start - 2, start + 3 * 86400)
        self.clock.add_iterator(strategy)
        self.now = start - 2
        return strategy

    def _run(self, seconds: int = 1):
        self.now += seconds
        self.clock.backtest_til(self.now)

    # --- events, as the connector reports them ---

    def _complete(self, order_id: str, is_buy: bool):
        base, quote = self.connector.pair.split("-")
        if is_buy:
            self.connector.trigger_event(MarketEvent.BuyOrderCompleted, BuyOrderCompletedEvent(
                self.now, order_id, base, quote, Decimal("0"), Decimal("0"), OrderType.MARKET))
        else:
            self.connector.trigger_event(MarketEvent.SellOrderCompleted, SellOrderCompletedEvent(
                self.now, order_id, base, quote, Decimal("0"), Decimal("0"), OrderType.MARKET))

    def _failure(self, order_id: str, message: str):
        self.connector.trigger_event(MarketEvent.OrderFailure, MarketOrderFailureEvent(
            self.now, order_id, OrderType.MARKET, message))

    def _result(self, swap_id: str, side: str, swap_in: Decimal, swap_out: Decimal, pool_fee: Decimal,
                price: Decimal = Decimal("1")) -> BeezeeSwapResult:
        return BeezeeSwapResult(
            client_order_id=swap_id, trading_pair=self.connector.pair, side=side, tx_hash="AB" * 32,
            input_denom="in", output_denom="out", input=swap_in, output=swap_out, pool_fee=pool_fee, price=price,
            input_amount=0, output_amount=0, pool_fee_amount=0,
        )

    def _settle_last(self, results: Optional[List[Tuple[Decimal, Decimal, Decimal]]] = None,
                     price: Decimal = Decimal("1")):
        """Settles the last round trip: each leg's (input, output, pool fee), by default its request's amount, the fake
        pool's quote for it and the fee from the input."""
        requests, ids = self.connector.sent[-1], self.connector.sent_ids[-1]
        for index, (request, swap_id) in enumerate(zip(requests, ids)):
            if results is None:
                quote = self.connector.quote_swap(self.connector.pair, request.side, request.amount)
                leg = (request.amount, quote.output, quote.fee)
            else:
                leg = results[index]
            self.connector.swap_results[swap_id] = self._result(swap_id, request.side, *leg, price=price)
        for request, swap_id in zip(requests, ids):
            self._complete(swap_id, request.side == "buy")

    # === Pacing ===

    async def test_a_leg_is_the_remaining_volume_over_the_cycles_left_halved(self):
        strategy = self._strategy()
        self._run(2)
        self.assertEqual(192, strategy.cycles_left_today())
        [[first, _]] = self.connector.sent
        self.assertEqual("buy", first.side)
        self.assertEqual(floor_step(Decimal("1000") / 192 / 2), first.amount)

    async def test_the_leg_is_clamped_to_the_swap_bounds(self):
        self._strategy(min_swap_amount=Decimal("50"))
        self._run(2)
        self.assertEqual(Decimal("50"), self.connector.sent[0][0].amount)

        self.connector = FakePoolConnector("TOKEN", "BZE")
        self._strategy(daily_volume_quote=Decimal("1000000"), max_swap_amount=Decimal("1"))
        self._run(2)
        self.assertEqual(Decimal("1"), self.connector.sent[0][0].amount)

    async def test_the_leg_never_exceeds_the_reserve_share(self):
        self._strategy(daily_volume_quote=Decimal("1000000000"), max_swap_amount=Decimal("1000000"))
        self._run(2)
        # 0.5% of the 1,000,000 BZE reserve.
        self.assertEqual(Decimal("5000"), self.connector.sent[0][0].amount)

    async def test_the_reserve_share_applies_to_the_base_reserve_on_a_sell(self):
        self.connector.pool.reserve_base = Decimal("500000")
        self.connector.balances["BZE"] = FEES_BZE
        self._strategy(daily_volume_quote=Decimal("1000000000"), max_swap_amount=Decimal("1000000"),
                       max_reserve_share_pct=Decimal("1"))
        self._run(2)
        [[first, second]] = self.connector.sent
        self.assertEqual(("sell", Decimal("5000")), (first.side, first.amount))
        self.assertEqual("buy", second.side)

    async def test_the_leg_keeps_the_configured_reserve_and_the_fees(self):
        self.connector.balances["BZE"] = Decimal("100")
        self._strategy(daily_volume_quote=Decimal("1000000000"), max_swap_amount=Decimal("1000000"),
                       reserve_quote=Decimal("40"))
        self._run(2)
        [[first, _]] = self.connector.sent
        self.assertEqual("buy", first.side)
        self.assertEqual(Decimal("100") - Decimal("40") - FEES_BZE, first.amount)

    async def test_the_base_balance_above_its_reserve_caps_a_sell(self):
        self.connector.balances["TOKEN"] = Decimal("30")
        strategy = self._strategy(daily_volume_quote=Decimal("1000000000"), max_swap_amount=Decimal("1000000"),
                                  reserve_base=Decimal("10"))
        strategy._buy_first = False
        self._run(2)
        self.assertEqual(("sell", Decimal("20")), (self.connector.sent[0][0].side, self.connector.sent[0][0].amount))

    async def test_the_target_reached_idles_until_utc_midnight_then_starts_over(self):
        strategy = self._strategy(start=NEXT_MIDNIGHT - 10, cycle_seconds=5, daily_volume_quote=Decimal("10"))
        self._run(2)
        self._settle_last([(Decimal("5"), Decimal("4.9"), Decimal("0")), (Decimal("4.9"), Decimal("5"), Decimal("0"))])
        self.assertEqual(Decimal("0"), strategy.remaining_quote)
        self._run(5)
        self.assertEqual(1, len(self.connector.sent))
        self.assertEqual("daily volume target reached, idle until UTC midnight", strategy._last_skip_reason)

        self._run(5)
        self.assertEqual(2, len(self.connector.sent))
        self.assertEqual(Decimal("10"), strategy.remaining_quote)

    # === Direction ===

    async def test_the_first_token_alternates_every_cycle(self):
        self._strategy(cycle_seconds=10)
        self._run(2)
        self._settle_last()
        self._run(10)
        self._settle_last()
        self._run(10)
        self.assertEqual([["buy", "sell"], ["sell", "buy"], ["buy", "sell"]],
                         [[request.side for request in trip] for trip in self.connector.sent])

    async def test_it_starts_from_the_other_token_when_one_side_is_short(self):
        # BZE pays the fees but cannot pay for a 1 BZE leg on top of them.
        self.connector.balances["BZE"] = Decimal("0.3")
        strategy = self._strategy(min_swap_amount=Decimal("1"))
        self._run(2)
        [[first, second]] = self.connector.sent
        self.assertEqual(("sell", "buy"), (first.side, second.side))
        self.assertIsNone(strategy._last_skip_reason)

    async def test_neither_side_can_pay_skips_with_both_reasons(self):
        self.connector.balances = {"TOKEN": Decimal("0"), "BZE": Decimal("0.1")}
        strategy = self._strategy()
        self._run(2)
        self.assertEqual([], self.connector.sent)
        self.assertIn("BZE first: the balance above the reserve allows 0 BZE", strategy._last_skip_reason)
        self.assertIn("not enough BZE above the reserve for the fees", strategy._last_skip_reason)

    # === The round trip ===

    async def test_both_legs_go_in_one_tx_and_leg_2_puts_back_the_output_less_the_safety(self):
        self._strategy(slippage_pct=Decimal("0.5"), safety_pct=Decimal("1"))
        self._run(2)
        [[first, second]] = self.connector.sent
        output = self.connector.quote_swap("TOKEN-BZE", "buy", first.amount).output
        self.assertEqual(BeezeeSwapRequest("TOKEN-BZE", "buy", first.amount, Decimal("0.5")), first)
        # Leg 2 has its own min_output: the connector quotes it on the pool as leg 1 leaves it, less slippage_pct.
        self.assertEqual(BeezeeSwapRequest("TOKEN-BZE", "sell", floor_step(output * Decimal("0.99")), Decimal("0.5")),
                         second)

    async def test_leg_2_never_puts_back_more_than_leg_1_minimum_output(self):
        self._strategy(slippage_pct=Decimal("2"), safety_pct=Decimal("0.1"))
        self._run(2)
        [[first, second]] = self.connector.sent
        output = self.connector.quote_swap("TOKEN-BZE", "buy", first.amount).output
        self.assertEqual(floor_step(output * Decimal("0.98")), second.amount)

    async def test_one_round_trip_in_flight_at_a_time(self):
        strategy = self._strategy(cycle_seconds=10)
        self._run(2)
        self._run(10)
        self.assertEqual(1, len(self.connector.sent))
        self.assertEqual("waiting for the last round trip to settle", strategy._last_skip_reason)

    async def test_a_refusal_is_the_skip_reason(self):
        self.connector.swap_error = BeezeeSwapError("balance too low")
        strategy = self._strategy()
        self._run(2)
        self.assertEqual("round trip refused up front: balance too low", strategy._last_skip_reason)
        self.assertFalse(strategy._halted)

    # === Accounting ===

    async def test_settled_legs_count_volume_fees_gas_and_drift(self):
        strategy = self._strategy()
        self._run(2)
        self.connector.gas_paid = Decimal("0.004")
        self._settle_last([
            (Decimal("2.6"), Decimal("2.59"), Decimal("0.0078")),
            (Decimal("2.577"), Decimal("2.56"), Decimal("0.0077")),
        ])
        # The quote side of each leg: what left on the buy, what arrived on the sell.
        self.assertEqual(Decimal("2.6") + Decimal("2.56"), strategy._volume_today)
        self.assertEqual(Decimal("0.0078") + Decimal("0.0077"), strategy._pool_fees_quote_today)
        self.assertEqual(2 * TAKER_FEE, strategy._tradebin_fees_today)
        self.assertEqual(Decimal("0.0155") + 2 * TAKER_FEE + Decimal("0.004"), strategy.spend_today)
        self.assertEqual(Decimal("2.59") - Decimal("2.577"), strategy._base_drift_today)
        self.assertEqual(Decimal("2.56") - Decimal("2.6"), strategy._quote_drift_today)

    async def test_pool_fees_convert_to_bze_at_the_price_when_bze_is_the_base(self):
        self.connector = FakePoolConnector("BZE", "USDC")
        self.connector.pool.reserve_quote = Decimal("2000000")
        strategy = self._strategy()
        self._run(2)
        self._settle_last([
            (Decimal("4"), Decimal("1.99"), Decimal("0.02")),
            (Decimal("1.98"), Decimal("3.9"), Decimal("0.01")),
        ], price=Decimal("2"))
        # Buy leg: 0.02 USDC; sell leg: 0.01 BZE at 2 USDC/BZE (the results' price) = 0.02 USDC. In BZE: 0.04 / 2.
        self.assertEqual(Decimal("0.04"), strategy._pool_fees_quote_today)
        self.assertEqual(Decimal("0.02"), strategy._pool_fees_native_today)

    async def test_a_failed_tx_counts_nothing(self):
        strategy = self._strategy(cycle_seconds=10)
        self._run(2)
        for swap_id in self.connector.sent_ids[-1]:
            self._failure(swap_id, "the resulted amount is too low")
        self.assertEqual(Decimal("0"), strategy._volume_today)
        self.assertEqual(Decimal("0"), strategy._tradebin_fees_today)
        self._run(10)
        self.assertEqual(2, len(self.connector.sent))

    async def test_the_fee_cap_stops_the_day(self):
        strategy = self._strategy(cycle_seconds=10, max_daily_fee_spend=Decimal("0.1"))
        self._run(2)
        self._settle_last()
        self._run(10)
        self.assertEqual(1, len(self.connector.sent))
        self.assertIn("daily fee cap reached", strategy._last_skip_reason)

    # === Guards ===

    async def test_a_large_price_move_skips_the_cycle(self):
        strategy = self._strategy(cycle_seconds=10, max_price_move_pct=Decimal("2"))
        self._run(2)
        self._settle_last()
        self.connector.pool.reserve_quote = Decimal("1050000")
        self._run(10)
        self.assertEqual(1, len(self.connector.sent))
        self.assertIn("pool price moved 5.000% since the last cycle", strategy._last_skip_reason)
        # The next cycle compares with the moved price.
        self._run(10)
        self.assertEqual(2, len(self.connector.sent))

    async def test_a_halted_pool_stops_trading_until_it_is_lifted(self):
        self.connector.pool.halted = True
        strategy = self._strategy(cycle_seconds=10)
        self._run(2)
        self.assertEqual([], self.connector.sent)
        self.assertEqual("pool halted", strategy._last_skip_reason)
        self.assertIn("(HALTED)", strategy.format_status())

        self.connector.pool.halted = False
        self._run(10)
        self.assertEqual(1, len(self.connector.sent))
        self.assertFalse(strategy._halted)

    async def test_a_halted_refusal_stops_trading(self):
        self.connector.swap_error = BeezeeSwapError("pool TOKEN_BZE is halted (halted denom)")
        strategy = self._strategy()
        self._run(2)
        self.assertEqual("pool halted", strategy._last_skip_reason)
        self.assertTrue(strategy._halted)

    async def test_no_pool_skips(self):
        self.connector.pool = None
        strategy = self._strategy()
        self._run(2)
        self.assertEqual("TOKEN-BZE has no liquidity pool", strategy._last_skip_reason)

    async def test_a_pair_without_bze_is_refused(self):
        self.connector = FakePoolConnector("TOKEN", "USDC")
        strategy = self._strategy()
        self._run(2)
        self.assertEqual([], self.connector.sent)
        self.assertIn("Refused: TOKEN-USDC has no BZE side", strategy.format_status())

    # === Status ===

    async def test_status_shows_the_target_costs_and_drift(self):
        strategy = self._strategy()
        self._run(2)
        self.connector.gas_paid = Decimal("0.004")
        self._settle_last([
            (Decimal("2.6"), Decimal("2.59"), Decimal("0.0078")),
            (Decimal("2.577"), Decimal("2.56"), Decimal("0.0077")),
        ])
        status = strategy.format_status()
        self.assertIn("Target / done / remaining: 1000 / 5.16 / 994.84 BZE", status)
        self.assertIn("Next swap:", status)
        self.assertIn("of the reserves", status)
        self.assertIn("Pool price: 1", status)
        self.assertIn("pool fees 0.0155 BZE", status)
        self.assertIn("taker fees 0.2 BZE + gas 0.004 BZE", status)
        self.assertIn("Cost per 1,000 BZE of volume:", status)
        self.assertIn("Inventory drift today (swaps only): 0.013 TOKEN, -0.04 BZE", status)

    # === Example ===

    async def test_example_config_loads(self):
        config = BeezeePoolVolumeConfig(**yaml.safe_load(EXAMPLE.read_text()))
        self.assertEqual("beezee", config.exchange)
        self.assertEqual("BZE-ERC20:0XA00C59FF5A080D2B954D0C75E46E22A0C371235A", config.trading_pair)
        self.assertEqual("beezee_pool_volume.py", config.script_file_name)
        self.assertEqual(Decimal("0.5"), config.max_reserve_share_pct)
        self.assertEqual(900, config.cycle_seconds)
