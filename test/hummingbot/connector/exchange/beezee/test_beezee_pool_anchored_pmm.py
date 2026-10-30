import asyncio
import unittest
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import yaml

from controllers.market_making.beezee_pool_anchored_pmm import (
    BOOK_MID,
    POOL,
    BeezeePoolAnchoredPMM,
    BeezeePoolAnchoredPMMConfig,
)
from hummingbot.connector.exchange.beezee.beezee_amm import BeezeePool, BeezeeSwapError, BeezeeSwapQuote, pool_price
from hummingbot.connector.exchange.beezee.beezee_utils import market_from_data
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.strategy_v2.executors.order_executor.data_types import OrderExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo

USDC = "ibc/81DA528F4C5546208D1D50F84C2D0B388B45D37AA36FCA622A3CB2D8EF014838"  # noqa: mock
PAIR = "BZE-USDC"
START = 1_800_000_000.0
CONTROLLERS_CONF = Path(__file__).resolve().parents[5] / "conf" / "controllers"
EXAMPLES = sorted(CONTROLLERS_CONF.glob("beezee_pool_anchored_pmm*.yml.example"))


def _metadata(symbol: str, denom: str, exponent: int) -> Dict:
    return {"symbol": symbol, "display": symbol.lower(),
            "denom_units": [{"denom": denom, "exponent": 0}, {"denom": symbol.lower(), "exponent": exponent}]}


# The market has ubze as base, its pool (ids are sorted) has USDC as base: the pool price must come out inverted.
MARKET = market_from_data({"base": "ubze", "quote": USDC},
                          {"ubze": _metadata("BZE", "ubze", 6), USDC: _metadata("USDC", USDC, 6)})


def usdc_pool(usdc: int, ubze: int, fee: str = "0.001", halted: bool = False) -> BeezeePool:
    return BeezeePool(pool_id=f"{USDC}_ubze", base=USDC, quote="ubze", lp_denom="ulp", fee=Decimal(fee), fee_dest={},
                      reserve_base=usdc, reserve_quote=ubze, halted=halted)


# 1,000 USDC against 2,000,000 BZE: 0.0005 USDC per BZE (2000 if read the wrong way round).
POOL_0005 = usdc_pool(1_000_000_000, 2_000_000_000_000)


class FakeConnector:
    """The parts of the BeeZee connector the controller uses. The pool price goes through the connector's own
    orientation code, so a pool whose base is the market's quote comes out inverted as on the real connector."""

    def __init__(self):
        self.pool: Optional[BeezeePool] = POOL_0005
        self.ready = True
        self.new_blocks_seen = 0
        self.taker_fee = TokenAmount("BZE", Decimal("0.1"))
        # What a swap gets or costs against the pool price: the 0.1% fee + the price impact.
        self.swap_cost = Decimal("0.002")
        self.swaps: List[Tuple[str, str, Decimal, Decimal]] = []
        self.swap_error: Optional[Exception] = None
        self.swap_results: Dict[str, object] = {}
        self.in_flight_orders: Dict[str, object] = {}

    def get_pool(self, trading_pair: str) -> Optional[BeezeePool]:
        return self.pool

    def get_pool_price(self, trading_pair: str) -> Optional[Decimal]:
        return None if self.pool is None else pool_price(self.pool, MARKET)

    def get_fee(self, *args, **kwargs):
        return AddedToCostTradeFee(flat_fees=[self.taker_fee])

    def quote_swap(self, trading_pair: str, side: str, amount: Decimal) -> BeezeeSwapQuote:
        price = self.get_pool_price(trading_pair)
        return BeezeeSwapQuote(trading_pair, side, input=amount, output=amount * price * (1 - self.swap_cost))

    def quote_input_for_output(self, trading_pair: str, side: str, amount: Decimal) -> BeezeeSwapQuote:
        price = self.get_pool_price(trading_pair)
        return BeezeeSwapQuote(trading_pair, side, input=amount * price * (1 + self.swap_cost), output=amount)

    def amm_swap(self, trading_pair: str, side: str, amount: Decimal, slippage_pct: Decimal) -> str:
        if self.swap_error is not None:
            raise self.swap_error
        swap_id = f"swap-{len(self.swaps)}"
        self.swaps.append((trading_pair, side, amount, slippage_pct))
        self.in_flight_orders[swap_id] = object()
        return swap_id

    def get_swap_result(self, client_order_id: str):
        return self.swap_results.get(client_order_id)


class BeezeePoolAnchoredPMMTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.now = START
        self.connector = FakeConnector()
        self.book_mid = Decimal("NaN")
        self.provider = MagicMock()
        self.provider.time.side_effect = lambda: self.now
        self.provider.get_connector.return_value = self.connector
        self.provider.get_price_by_type.side_effect = lambda *args, **kwargs: self.book_mid

    def make(self, **overrides) -> BeezeePoolAnchoredPMM:
        settings = dict(id="pool_pmm", connector_name="beezee", trading_pair=PAIR, total_amount_quote=Decimal("100"),
                        buy_spreads="0.01,0.02", sell_spreads="0.01,0.02", buy_amounts_pct="50,50",
                        sell_amounts_pct="50,50")
        settings.update(overrides)
        self.controller = BeezeePoolAnchoredPMM(
            config=BeezeePoolAnchoredPMMConfig(**settings), market_data_provider=self.provider,
            actions_queue=asyncio.Queue())
        self.controller.logger = MagicMock()
        return self.controller

    async def tick(self):
        await self.controller.update_processed_data()
        return self.controller.determine_executor_actions()

    def info(self, config, active: bool = True, executed: Decimal = Decimal("0"), price: Decimal = Decimal("0"),
             is_trading: bool = False) -> ExecutorInfo:
        return ExecutorInfo(
            id=config.id, timestamp=config.timestamp, type=config.type, status=RunnableStatus.RUNNING if active else
            RunnableStatus.TERMINATED, config=config, net_pnl_pct=Decimal("0"), net_pnl_quote=Decimal("0"),
            cum_fees_quote=Decimal("0"), filled_amount_quote=executed * price, is_active=active, is_trading=is_trading,
            custom_info={"level_id": config.level_id, "side": config.side, "executed_amount_base": executed,
                         "average_executed_price": price},
            controller_id="pool_pmm")

    def logged(self, level: str) -> List[str]:
        return [call.args[0] for call in getattr(self.controller.logger.return_value, level).call_args_list]


class QuoteTests(BeezeePoolAnchoredPMMTestCase):
    async def test_quotes_are_centred_on_the_inverted_pool_price(self):
        self.make()
        actions = await self.tick()

        self.assertEqual(Decimal("0.0005"), self.controller.processed_data["reference_price"])
        self.assertEqual(POOL, self.controller.processed_data["price_source"])
        prices = {action.executor_config.level_id: action.executor_config.entry_price for action in actions
                  if isinstance(action, CreateExecutorAction)}
        self.assertEqual({
            "buy_0": Decimal("0.0005") * Decimal("0.99"), "buy_1": Decimal("0.0005") * Decimal("0.98"),
            "sell_0": Decimal("0.0005") * Decimal("1.01"), "sell_1": Decimal("0.0005") * Decimal("1.02"),
        }, prices)

    async def test_a_spread_below_the_pool_fee_plus_margin_is_raised_to_the_floor(self):
        # Pool fee 0.1% + margin 0.15% = 0.25% floor; the 0.1% level is raised, the 1% level is not.
        self.make(buy_spreads="0.001,0.01", sell_spreads="0.001,0.01", min_spread_over_pool_fee_pct=Decimal("0.15"))
        await self.tick()

        self.assertEqual(Decimal("0.0025"), self.controller.processed_data["min_spread"])
        self.assertEqual(Decimal("0.0005") * Decimal("0.9975"), self.controller.get_price_and_amount("buy_0")[0])
        self.assertEqual(Decimal("0.0005") * Decimal("1.0025"), self.controller.get_price_and_amount("sell_0")[0])
        self.assertEqual(Decimal("0.0005") * Decimal("1.01"), self.controller.get_price_and_amount("sell_1")[0])

    async def test_order_amount_is_the_level_budget_at_the_order_price(self):
        self.make()
        await self.tick()
        price, amount = self.controller.get_price_and_amount("sell_0")
        self.assertEqual(Decimal("25") / price, amount)

    async def test_without_a_pool_the_book_mid_is_used_with_a_warning(self):
        self.connector.pool = None
        self.book_mid = Decimal("0.0004")
        self.make()
        actions = await self.tick()
        await self.tick()

        self.assertEqual(Decimal("0.0004"), self.controller.processed_data["reference_price"])
        self.assertEqual(BOOK_MID, self.controller.processed_data["price_source"])
        self.assertEqual(4, len([action for action in actions if isinstance(action, CreateExecutorAction)]))
        self.assertEqual(1, len(self.logged("warning")), "warned once, not on every tick")
        self.assertIn("no liquidity pool", self.logged("warning")[0])
        self.assertTrue(any("WARNING" in line and "no liquidity pool" in line for line in self.controller.to_format_status()))

    async def test_an_empty_pool_falls_back_to_the_book_mid_too(self):
        self.connector.pool = usdc_pool(0, 0)
        self.book_mid = Decimal("0.0004")
        self.make()
        await self.tick()
        self.assertEqual(BOOK_MID, self.controller.processed_data["price_source"])
        self.assertIn("is empty", self.logged("warning")[0])

    async def test_no_pool_and_no_book_mid_quotes_nothing(self):
        self.connector.pool = None
        self.make()
        self.assertEqual([], await self.tick())

    async def test_without_hedge_mode_the_quotes_are_pmm_simple_position_executors(self):
        self.make()
        self.assertFalse(self.controller.config.hedge_fills_on_pool)
        actions = await self.tick()
        configs = [action.executor_config for action in actions]
        self.assertTrue(all(isinstance(config, PositionExecutorConfig) for config in configs))
        self.assertIsNone(configs[0].triple_barrier_config.stop_loss, "the book takes no market orders")
        self.assertIsNone(configs[0].triple_barrier_config.time_limit)


class RequoteTests(BeezeePoolAnchoredPMMTestCase):
    async def _placed_quote(self):
        self.make(requote_threshold_pct=Decimal("0.2"))
        await self.tick()
        config = self.controller.get_executor_config("buy_0", *self.controller.get_price_and_amount("buy_0"))
        self.controller.executors_info = [self.info(config)]
        return config

    async def test_a_pool_move_above_the_threshold_requotes(self):
        config = await self._placed_quote()
        # BZE reserve down 0.3%: the price is up about 0.3%.
        self.connector.pool = usdc_pool(1_000_000_000, 1_994_000_000_000)
        actions = await self.tick()
        stops = [action for action in actions if isinstance(action, StopExecutorAction)]
        self.assertEqual([config.id], [action.executor_id for action in stops])
        actions = await self.tick()
        self.assertEqual([], [action for action in actions if isinstance(action, StopExecutorAction)],
                         "stopped once while its cancel is pending")

    async def test_a_pool_move_below_the_threshold_does_not(self):
        await self._placed_quote()
        self.connector.pool = usdc_pool(1_000_000_000, 1_998_000_000_000)  # about 0.1%
        actions = await self.tick()
        self.assertEqual([], [action for action in actions if isinstance(action, StopExecutorAction)])

    async def test_a_trading_executor_is_not_requoted(self):
        config = await self._placed_quote()
        self.controller.executors_info = [self.info(config, is_trading=True)]
        self.connector.pool = usdc_pool(1_000_000_000, 1_900_000_000_000)
        actions = await self.tick()
        self.assertEqual([], [action for action in actions if isinstance(action, StopExecutorAction)])


class HaltTests(BeezeePoolAnchoredPMMTestCase):
    async def test_a_halted_pool_cancels_everything_once_and_stops_quoting(self):
        self.make()
        await self.tick()
        quote = self.controller.get_executor_config("buy_0", *self.controller.get_price_and_amount("buy_0"))
        filled = self.controller.get_executor_config("sell_0", *self.controller.get_price_and_amount("sell_0"))
        self.controller.executors_info = [self.info(quote), self.info(filled, is_trading=True)]
        self.connector.pool = usdc_pool(1_000_000_000, 2_000_000_000_000, halted=True)

        actions = await self.tick()

        self.assertEqual({quote.id, filled.id}, {action.executor_id for action in actions})
        self.assertTrue(all(isinstance(action, StopExecutorAction) and action.keep_position for action in actions))
        self.assertEqual([], await self.tick(), "each executor is stopped once")
        self.connector.pool = POOL_0005
        self.controller.executors_info = []
        self.assertEqual([], await self.tick(), "exit only until the controller is restarted")
        self.assertIn("HALTED", self.controller.to_format_status()[0])


class HedgeTests(BeezeePoolAnchoredPMMTestCase):
    def setUp(self):
        super().setUp()
        self.make(hedge_fills_on_pool=True, hedge_slippage_pct=Decimal("0.5"), min_hedge_profit_pct=Decimal("0.1"))

    def quote(self, level_id: str) -> OrderExecutorConfig:
        self.controller.processed_data.setdefault("reference_price", Decimal("0.0005"))
        config = self.controller.get_executor_config(level_id, Decimal("0.0005"), Decimal("1000"))
        self.assertIsInstance(config, OrderExecutorConfig)
        return config

    async def fill(self, fills: List[Tuple[OrderExecutorConfig, Decimal, Decimal]]):
        """Reports executed amounts, ticks, then lets one block end and ticks again."""
        self.controller.executors_info = [self.info(config, False, amount, price) for config, amount, price in fills]
        await self.tick()
        self.connector.new_blocks_seen += 1
        await self.tick()

    async def test_a_profitable_buy_fill_is_hedged_by_selling_the_same_base_on_the_pool(self):
        buy = self.quote("buy_0")
        self.controller.executors_info = [self.info(buy, False, Decimal("1000"), Decimal("0.000495"))]
        await self.tick()
        self.assertEqual([], self.connector.swaps, "waits for the block to end")

        self.connector.new_blocks_seen += 1
        await self.tick()
        self.assertEqual([(PAIR, "sell", Decimal("1000"), Decimal("0.5"))], self.connector.swaps)

        await self.tick()
        self.assertEqual(1, len(self.connector.swaps), "a fill is hedged once")

    async def test_a_profitable_sell_fill_is_hedged_by_buying_it_back_with_quote(self):
        sell = self.quote("sell_0")
        await self.fill([(sell, Decimal("1000"), Decimal("0.000505"))])
        quote_in = Decimal("1000") * Decimal("0.0005") * Decimal("1.002")
        self.assertEqual([(PAIR, "buy", quote_in, Decimal("0.5"))], self.connector.swaps)

    async def test_an_unprofitable_fill_is_not_hedged(self):
        # 0.1% below the pool: less than pool fee 0.1% + taker fee and gas + 0.1% margin.
        buy = self.quote("buy_0")
        await self.fill([(buy, Decimal("1000"), Decimal("0.0004995"))])
        self.assertEqual([], self.connector.swaps)
        self.assertIn("1000", self.controller.get_custom_info()["unhedged_base"])

    async def test_the_taker_fee_counts_against_a_small_fill(self):
        # 0.5% edge, but 0.105 BZE of fees on a 10 BZE fill is 1.05% of its value.
        buy = self.quote("buy_0")
        await self.fill([(buy, Decimal("10"), Decimal("0.0004975"))])
        self.assertEqual([], self.connector.swaps)

    async def test_a_hedge_the_pool_price_impact_eats_is_not_sent(self):
        # 1% edge per fill passes the pool-fee check, but a 0.9% impact leaves less than the 0.1% margin.
        self.connector.swap_cost = Decimal("0.009")
        buy = self.quote("buy_0")
        await self.fill([(buy, Decimal("1000"), Decimal("0.000495"))])
        self.assertEqual([], self.connector.swaps)
        self.assertEqual("1000", self.controller.get_custom_info()["unhedged_base"])

    async def test_the_fills_of_one_block_become_one_hedge(self):
        buy, other_buy, sell = self.quote("buy_0"), self.quote("buy_1"), self.quote("sell_0")
        await self.fill([
            (buy, Decimal("1000"), Decimal("0.000495")),
            (other_buy, Decimal("500"), Decimal("0.00049")),
            (sell, Decimal("400"), Decimal("0.000505")),
        ])
        self.assertEqual([(PAIR, "sell", Decimal("1100"), Decimal("0.5"))], self.connector.swaps)

    async def test_partial_fills_hedge_only_the_new_part(self):
        buy = self.quote("buy_0")
        self.controller.executors_info = [self.info(buy, True, Decimal("400"), Decimal("0.000495"))]
        await self.tick()
        self.connector.new_blocks_seen += 1
        await self.tick()
        self.connector.swap_results["swap-0"] = MagicMock(input=Decimal("400"), output=Decimal("0.2"))
        # Executed 1000 at an average 0.000494: the new 600 filled at 0.0004933...
        self.controller.executors_info = [self.info(buy, False, Decimal("1000"), Decimal("0.000494"))]
        await self.tick()
        self.connector.new_blocks_seen += 1
        await self.tick()
        self.assertEqual([Decimal("400"), Decimal("600")], [swap[2] for swap in self.connector.swaps])

    async def test_a_refused_hedge_is_kept_and_not_retried(self):
        self.connector.swap_error = BeezeeSwapError("amount is too low to be traded")
        buy = self.quote("buy_0")
        await self.fill([(buy, Decimal("1000"), Decimal("0.000495"))])
        self.connector.swap_error = None
        for _ in range(3):
            self.connector.new_blocks_seen += 1
            await self.tick()
        self.assertEqual([], self.connector.swaps)
        self.assertEqual(1, self.controller.get_custom_info()["hedges_failed"])
        self.assertEqual("1000", self.controller.get_custom_info()["unhedged_base"])
        self.assertTrue(any("stays in the inventory" in message for message in self.logged("warning")))

    async def test_a_hedge_that_fails_on_chain_is_kept_and_not_retried(self):
        buy = self.quote("buy_0")
        await self.fill([(buy, Decimal("1000"), Decimal("0.000495"))])
        self.assertEqual(1, len(self.connector.swaps))
        self.connector.in_flight_orders.clear()  # failed: gone, no swap result
        for _ in range(3):
            self.connector.new_blocks_seen += 1
            await self.tick()
        self.assertEqual(1, len(self.connector.swaps))
        self.assertEqual(1, self.controller.get_custom_info()["hedges_failed"])

    async def test_fills_of_executors_from_before_the_start_are_not_hedged(self):
        old = self.quote("buy_0").model_copy(update={"timestamp": START - 60, "id": "old"})
        await self.fill([(old, Decimal("1000"), Decimal("0.000495"))])
        self.assertEqual([], self.connector.swaps)

    async def test_fees_in_a_token_outside_the_pair_are_not_hedged(self):
        self.connector.taker_fee = TokenAmount("ATOM", Decimal("0.1"))
        buy = self.quote("buy_0")
        await self.fill([(buy, Decimal("1000"), Decimal("0.000495"))])
        self.assertEqual([], self.connector.swaps)
        self.assertTrue(any("no BZE side" in line for line in self.controller.to_format_status()))

    async def test_a_filled_level_waits_the_cooldown(self):
        buy = self.quote("buy_0")
        info = self.info(buy, False, Decimal("1000"), Decimal("0.000495"))
        self.controller.executors_info = [
            info.model_copy(update={"close_type": CloseType.POSITION_HOLD, "close_timestamp": self.now})]
        self.assertNotIn("buy_0", self.controller.get_levels_to_execute())
        self.now += self.controller.config.cooldown_time
        self.assertIn("buy_0", self.controller.get_levels_to_execute())


class ExampleTests(unittest.TestCase):
    def test_every_example_exists(self):
        self.assertEqual(["beezee_pool_anchored_pmm_mainnet.yml.example",
                          "beezee_pool_anchored_pmm_testnet.yml.example",
                          "beezee_pool_anchored_pmm_token_bze.yml.example"],
                         [example.name for example in EXAMPLES])

    def test_every_example_loads_into_the_config(self):
        for example in EXAMPLES:
            with self.subTest(example=example.name):
                data = yaml.safe_load(example.read_text())
                config = BeezeePoolAnchoredPMMConfig(**data)
                self.assertEqual("beezee_pool_anchored_pmm", config.controller_name)
                self.assertEqual("market_making", config.controller_type)
                self.assertEqual("beezee", config.connector_name)
                self.assertFalse(config.hedge_fills_on_pool)
                self.assertIsNone(config.stop_loss)
                self.assertIsNone(config.time_limit)
                self.assertTrue(config.skip_rebalance)
                self.assertGreater(config.total_amount_quote, 0)
                self.assertEqual(0.2, float(config.requote_threshold_pct))
                self.assertEqual(set(), set(data) - set(BeezeePoolAnchoredPMMConfig.model_fields),
                                 "keys the config does not know")
