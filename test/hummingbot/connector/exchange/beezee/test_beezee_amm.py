import asyncio
import json
import logging
import re
import unittest
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Set
from unittest.mock import AsyncMock, Mock

from aioresponses import aioresponses

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS, beezee_web_utils as web_utils
from hummingbot.connector.exchange.beezee.beezee_amm import (
    DEC_PRECISION,
    BeezeePool,
    BeezeePoolTracker,
    BeezeeSwapError,
    dec_mul,
    dec_quo,
    input_for_output,
    pool_id_for,
    swap_output,
)
from hummingbot.connector.exchange.beezee.beezee_api_data_source import BeezeeAPIDataSource
from hummingbot.connector.exchange.beezee.beezee_exchange import BeezeeExchange
from hummingbot.connector.exchange.beezee.beezee_pool_price_delegate import BeezeePoolPriceDelegate
from hummingbot.connector.exchange.beezee.beezee_utils import (
    BeezeeConfigMap,
    BeezeeMainnetNetworkMode,
    BeezeeReadOnlyAccountMode,
    market_from_data,
)
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.data_type.common import PriceType

FIXTURES = Path(__file__).parent / "fixtures"
REST_URL = "https://rest.getbze.com"

USDC = "ibc/81DA528F4C5546208D1D50F84C2D0B388B45D37AA36FCA622A3CB2D8EF014838"
CTL = "factory/bze1972aqfzdg29ugjln74edx0xvcg4ehvysjptk77/CTL"
VDL = "factory/bze13gzq40che93tgfm9kzmkpjamah5nj0j73pyhqk/uvdl"
EMPTY_POOL_BASE = "ibc/6490A7EAB61059BFC1CDDEB05917DD70BDF3A611654162A1A47DB930D40D8AF4"
USDC_POOL = f"{USDC}_ubze"
CTL_POOL = f"{CTL}_ubze"


def _metadata(symbol: str, denom: str, exponent: int) -> Dict:
    units = [{"denom": denom, "exponent": 0}]
    if exponent:
        units.append({"denom": symbol.lower(), "exponent": exponent})
    return {"symbol": symbol, "display": symbol.lower() if exponent else denom, "denom_units": units}


METADATA = {
    "ubze": _metadata("BZE", "ubze", 6),
    USDC: _metadata("USDC", USDC, 6),
    CTL: _metadata("CTL", CTL, 0),
    VDL: _metadata("VDL", VDL, 6),
    EMPTY_POOL_BASE: _metadata("USDCN", EMPTY_POOL_BASE, 6),
}
# Mainnet order-book markets: BZE-USDC has ubze as base, while its pool (ids are sorted) has ubze as quote.
MARKETS = {
    "BZE-USDC": market_from_data({"base": "ubze", "quote": USDC}, METADATA),
    "CTL-BZE": market_from_data({"base": CTL, "quote": "ubze"}, METADATA),
    "VDL-BZE": market_from_data({"base": VDL, "quote": "ubze"}, METADATA),
    "BZE-USDCN": market_from_data({"base": "ubze", "quote": EMPTY_POOL_BASE}, METADATA),
}


def recorded_pools() -> List[Dict]:
    return json.loads((FIXTURES / "amm_all_liquidity_pools_mainnet.json").read_text())["list"]


def recorded_pool(pool_id: str) -> Dict:
    return next(pool for pool in recorded_pools() if pool["id"] == pool_id)


def with_reserves(data: Dict, reserve_base: int, reserve_quote: int) -> Dict:
    return {**data, "reserve_base": str(reserve_base), "reserve_quote": str(reserve_quote)}


class FakePoolSource:
    """The data source as the tracker sees it, answering from the recorded pools."""

    def __init__(self):
        self.pools = {pool["id"]: pool for pool in recorded_pools()}
        self.all_pools_calls = 0
        self.pool_calls: List[str] = []

    async def get_all_liquidity_pools(self) -> List[Dict]:
        self.all_pools_calls += 1
        return list(self.pools.values())

    async def get_liquidity_pool(self, pool_id: str) -> Optional[Dict]:
        self.pool_calls.append(pool_id)
        return self.pools.get(pool_id)

    async def get_market_by_trading_pair(self, trading_pair: str):
        return MARKETS[trading_pair]


class BeezeeAmmMathTests(unittest.TestCase):
    """The pool math against the chain v8.2.0 keeper (see the golden fixture's comment)."""

    @classmethod
    def setUpClass(cls):
        cls.pools = {pool["id"]: BeezeePool.from_data(pool) for pool in recorded_pools()}
        cls.golden = json.loads((FIXTURES / "amm_golden_quotes_v820.json").read_text())["cases"]

    def test_golden_fixture_covers_three_pools_both_directions(self):
        self.assertEqual({USDC_POOL, CTL_POOL, f"{VDL}_ubze"}, {case["pool_id"] for case in self.golden})
        directions = {(case["pool_id"], case["kind"], case["input_denom"]) for case in self.golden}
        self.assertEqual(12, len(directions))

    def test_swap_output_matches_the_chain(self):
        for case in (case for case in self.golden if case["kind"] == "swap"):
            with self.subTest(**case):
                pool = self.pools[case["pool_id"]]
                if "error" in case:
                    with self.assertRaises(BeezeeSwapError) as raised:
                        swap_output(pool, case["input_denom"], int(case["amount"]))
                    self.assertIn(str(raised.exception), case["error"])
                else:
                    swap = swap_output(pool, case["input_denom"], int(case["amount"]))
                    self.assertEqual(int(case["result"]), swap.output_amount)
                    self.assertEqual(case["output_denom"], swap.output_denom)

    def test_input_for_output_matches_the_chain(self):
        for case in (case for case in self.golden if case["kind"] == "input_for_output"):
            with self.subTest(**case):
                pool = self.pools[case["pool_id"]]
                if "error" in case:
                    with self.assertRaises(BeezeeSwapError) as raised:
                        input_for_output(pool, case["output_denom"], int(case["amount"]))
                    self.assertIn(str(raised.exception), case["error"])
                else:
                    self.assertEqual(int(case["result"]), input_for_output(pool, case["output_denom"], int(case["amount"])))

    def test_fee_is_taken_from_the_input(self):
        swap = swap_output(self.pools[USDC_POOL], "ubze", 1_000_000)
        # 0.1% pool fee, truncated
        self.assertEqual(1000, swap.fee_amount)
        self.assertEqual(1_000_000, swap.input_amount)
        self.assertEqual(USDC, swap.output_denom)

    def test_legacy_dec_rounds_half_to_even(self):
        half = DEC_PRECISION // 2
        self.assertEqual(0, dec_mul(half, 1))
        self.assertEqual(2, dec_mul(3 * half, 1))
        self.assertEqual(1, dec_mul(half + 1, 1))
        # 1e-18 / 2 sits exactly on half a unit of the 18th decimal
        self.assertEqual(0, dec_quo(1, 2 * DEC_PRECISION))
        self.assertEqual(2, dec_quo(3, 2 * DEC_PRECISION))

    def test_pool_id_sorts_the_denoms(self):
        self.assertEqual(USDC_POOL, pool_id_for("ubze", USDC))
        self.assertEqual(USDC_POOL, pool_id_for(USDC, "ubze"))

    def test_stable_flag_does_not_change_the_math(self):
        pool = self.pools[USDC_POOL]
        stable = BeezeePool.from_data({**recorded_pool(USDC_POOL), "stable": True})
        self.assertEqual(swap_output(pool, "ubze", 123_456_789), swap_output(stable, "ubze", 123_456_789))


class BeezeePoolTrackerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.source = FakePoolSource()
        self.halted: Set[str] = set()
        self.trading_pairs = ["BZE-USDC", "CTL-BZE"]
        self.tracker = BeezeePoolTracker(self.source, lambda: self.trading_pairs, lambda: self.halted)

    async def test_first_refresh_reads_all_pools_then_only_the_traded_ones(self):
        await self.tracker.refresh()
        self.assertEqual(1, self.source.all_pools_calls)
        self.assertEqual([], self.source.pool_calls)

        await self.tracker.refresh()
        self.assertEqual(1, self.source.all_pools_calls)
        self.assertEqual([USDC_POOL, CTL_POOL], self.source.pool_calls)

    async def test_pool_price_is_inverted_when_the_pool_base_is_the_market_quote(self):
        await self.tracker.refresh()
        data = recorded_pool(USDC_POOL)
        # pool base = USDC, pool quote = ubze; the market is BZE-USDC: USDC per BZE = reserve_base / reserve_quote
        expected = Decimal(data["reserve_base"]) / Decimal(data["reserve_quote"])
        self.assertAlmostEqual(expected, self.tracker.get_pool_price("BZE-USDC"), places=20)
        self.assertEqual(USDC, self.tracker.get_pool("BZE-USDC").base)

    async def test_pool_price_keeps_orientation_when_the_pool_base_is_the_market_base(self):
        await self.tracker.refresh()
        data = recorded_pool(CTL_POOL)
        # CTL has no decimals, BZE six: the order book shows BZE per CTL
        expected = Decimal(data["reserve_quote"]) / Decimal(data["reserve_base"]) / Decimal(10 ** 6)
        self.assertAlmostEqual(expected, self.tracker.get_pool_price("CTL-BZE"), places=20)

    async def test_quotes_map_the_side_to_the_pool_denoms(self):
        await self.tracker.refresh()
        pool = BeezeePool.from_data(recorded_pool(USDC_POOL))

        sell = self.tracker.quote_swap("BZE-USDC", "sell", Decimal("1000"))
        self.assertTrue(sell.ok)
        self.assertEqual(("ubze", USDC), (sell.input_denom, sell.output_denom))
        self.assertEqual(swap_output(pool, "ubze", 1_000_000_000).output_amount, sell.output_amount)
        self.assertEqual(Decimal(sell.output_amount) / Decimal(10 ** 6), sell.output)
        self.assertEqual(Decimal("1"), sell.fee)

        buy = self.tracker.quote_swap("BZE-USDC", "buy", Decimal("2.5"))
        self.assertEqual((USDC, "ubze"), (buy.input_denom, buy.output_denom))
        self.assertEqual(swap_output(pool, USDC, 2_500_000).output_amount, buy.output_amount)

    async def test_effective_price_and_impact_are_worse_than_the_pool_price_on_both_sides(self):
        await self.tracker.refresh()
        price = self.tracker.get_pool_price("BZE-USDC")
        sell = self.tracker.quote_swap("BZE-USDC", "sell", Decimal("100000"))
        buy = self.tracker.quote_swap("BZE-USDC", "buy", Decimal("100"))
        self.assertLess(sell.effective_price, price)
        self.assertGreater(buy.effective_price, price)
        self.assertAlmostEqual(sell.output / sell.input, sell.effective_price, places=20)
        self.assertAlmostEqual(buy.input / buy.output, buy.effective_price, places=20)
        for quote in (sell, buy):
            # at least the 0.1% pool fee
            self.assertGreater(quote.price_impact_pct, Decimal("0.1"))

    async def test_input_for_output_quote_buys_at_least_the_target(self):
        await self.tracker.refresh()
        pool = BeezeePool.from_data(recorded_pool(USDC_POOL))
        quote = self.tracker.quote_input_for_output("BZE-USDC", "buy", Decimal("5000"))
        self.assertTrue(quote.ok)
        self.assertEqual(input_for_output(pool, "ubze", 5_000_000_000), quote.input_amount)
        self.assertGreaterEqual(quote.output_amount, 5_000_000_000)

        sell = self.tracker.quote_input_for_output("BZE-USDC", "sell", Decimal("1"))
        self.assertEqual(input_for_output(pool, USDC, 1_000_000), sell.input_amount)
        self.assertGreaterEqual(sell.output_amount, 1_000_000)

    async def test_halted_pool_is_flagged_and_answers_halted(self):
        self.halted = {USDC}
        await self.tracker.refresh()

        self.assertTrue(self.tracker.get_pool("BZE-USDC").halted)
        self.assertFalse(self.tracker.get_pool("CTL-BZE").halted)
        for quote in (
            self.tracker.quote_swap("BZE-USDC", "sell", Decimal("10")),
            self.tracker.quote_input_for_output("BZE-USDC", "buy", Decimal("10")),
        ):
            self.assertFalse(quote.ok)
            self.assertTrue(quote.halted)
            self.assertIn("is halted", quote.error)
            self.assertIsNone(quote.output)

        self.halted = set()
        self.assertTrue(self.tracker.quote_swap("BZE-USDC", "sell", Decimal("10")).ok)

    async def test_missing_pool_and_unknown_pair_give_none(self):
        self.trading_pairs = ["BZE-USDC", "UNKNOWN-BZE"]
        del self.source.pools[USDC_POOL]
        await self.tracker.refresh()

        self.assertIsNone(self.tracker.get_pool("BZE-USDC"))
        self.assertIsNone(self.tracker.get_pool_price("BZE-USDC"))
        self.assertIsNone(self.tracker.get_pool("UNKNOWN-BZE"))
        quote = self.tracker.quote_swap("BZE-USDC", "sell", Decimal("1"))
        self.assertFalse(quote.ok)
        self.assertFalse(quote.halted)
        self.assertIn("no liquidity pool", quote.error)

    async def test_pool_gone_on_refresh_is_dropped(self):
        await self.tracker.refresh()
        self.assertIsNotNone(self.tracker.get_pool("BZE-USDC"))
        del self.source.pools[USDC_POOL]
        await self.tracker.refresh()
        self.assertIsNone(self.tracker.get_pool("BZE-USDC"))

    async def test_empty_pool_has_no_price_and_refuses_quotes(self):
        self.trading_pairs = ["BZE-USDCN"]
        await self.tracker.refresh()
        self.assertIsNotNone(self.tracker.get_pool("BZE-USDCN"))
        self.assertIsNone(self.tracker.get_pool_price("BZE-USDCN"))
        self.assertFalse(self.tracker.quote_swap("BZE-USDCN", "sell", Decimal("1")).ok)
        self.assertIn("insufficient liquidity", self.tracker.quote_input_for_output("BZE-USDCN", "sell", Decimal("1")).error)

    async def test_refresh_reloads_reserves(self):
        await self.tracker.refresh()
        before = self.tracker.get_pool_price("BZE-USDC")
        self.source.pools[USDC_POOL] = with_reserves(recorded_pool(USDC_POOL), 3_000_000_000, 6_000_000_000_000)
        await self.tracker.refresh()
        self.assertNotEqual(before, self.tracker.get_pool_price("BZE-USDC"))
        self.assertEqual(Decimal("0.0005"), self.tracker.get_pool_price("BZE-USDC"))

    async def test_tiny_amount_is_refused_like_the_chain(self):
        await self.tracker.refresh()
        quote = self.tracker.quote_swap("BZE-USDC", "sell", Decimal("0.000999"))
        self.assertFalse(quote.ok)
        self.assertEqual("amount is too low to be traded", quote.error)


class BeezeePoolDataSourceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.data_source = BeezeeAPIDataSource(
            api_factory=web_utils.build_api_factory(throttler=AsyncThrottler(CONSTANTS.RATE_LIMITS)),
            rest_endpoint=REST_URL,
            chain_id="beezee-1",
            address_prefix="bze",
            native_denom="ubze",
            gas_price=Decimal("0.01"),
        )
        self.all_pools_url = re.compile(rf"^{re.escape(REST_URL + CONSTANTS.ALL_LIQUIDITY_POOLS_PATH_URL)}.*")
        self.pool_url = re.compile(rf"^{re.escape(REST_URL + CONSTANTS.LIQUIDITY_POOL_PATH_URL)}.*")

    @aioresponses()
    async def test_all_pools_follow_next_key(self, mock_api):
        pools = recorded_pools()
        mock_api.get(self.all_pools_url, payload={"list": pools[:4], "pagination": {"next_key": "NEXT=="}})
        mock_api.get(self.all_pools_url, payload={"list": pools[4:], "pagination": {"next_key": None}})

        result = await self.data_source.get_all_liquidity_pools()

        self.assertEqual([pool["id"] for pool in pools], [pool["id"] for pool in result])
        urls = [url for (method, url) in mock_api.requests if method == "GET"]
        self.assertEqual(2, len(urls))
        self.assertEqual(["NEXT=="], [url.query["pagination.key"] for url in urls if "pagination.key" in url.query])

    @aioresponses()
    async def test_one_pool_by_id(self, mock_api):
        mock_api.get(self.pool_url, payload={"pool": recorded_pool(USDC_POOL)})
        pool = await self.data_source.get_liquidity_pool(USDC_POOL)
        self.assertEqual(USDC_POOL, pool["id"])
        url = next(url for (method, url) in mock_api.requests if method == "GET")
        self.assertEqual(USDC_POOL, url.query["pool_id"])

    @aioresponses()
    async def test_missing_pool_is_none(self, mock_api):
        mock_api.get(self.pool_url, status=404, payload={"code": 5, "message": "not found", "details": []})
        self.assertIsNone(await self.data_source.get_liquidity_pool("nope_ubze"))

    @aioresponses()
    async def test_other_errors_raise(self, mock_api):
        mock_api.get(self.pool_url, status=500, payload={"code": 13, "message": "internal"})
        with self.assertRaises(IOError):
            await self.data_source.get_liquidity_pool(USDC_POOL)


class BeezeeExchangePoolTests(unittest.IsolatedAsyncioTestCase):
    level = 0

    def handle(self, record):
        self.log_records.append(record)

    def setUp(self):
        self.log_records = []
        config = BeezeeConfigMap(connector="beezee", network=BeezeeMainnetNetworkMode(), account_type=BeezeeReadOnlyAccountMode())
        # No trading: a new block refreshes the pools only.
        self.exchange = BeezeeExchange(connector_configuration=config, trading_pairs=["BZE-USDC"], trading_required=False)
        self.source = FakePoolSource()
        data_source = Mock()
        data_source.get_all_liquidity_pools = AsyncMock(side_effect=self.source.get_all_liquidity_pools)
        data_source.get_liquidity_pool = AsyncMock(side_effect=self.source.get_liquidity_pool)
        data_source.get_market_by_trading_pair = AsyncMock(side_effect=self.source.get_market_by_trading_pair)
        data_source.get_halted_denoms = AsyncMock(return_value=set())
        self.data_source = data_source
        self.exchange._data_source = data_source
        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)

    async def asyncTearDown(self):
        await self.exchange.stop_network()
        for _ in range(5):
            await asyncio.sleep(0)
        leftover = [task for task in asyncio.all_tasks() if task is not asyncio.current_task() and not task.done()]
        for task in leftover:
            task.cancel()
        self.assertEqual([], [task.get_coro() for task in leftover])

    def tearDown(self):
        self.exchange.logger().removeHandler(self)

    async def _new_block(self):
        self.exchange._on_new_block()
        await self.exchange._pool_refresh_task

    async def test_new_block_loads_pools_and_reads_halted_denoms(self):
        self.assertIsNone(self.exchange.get_pool_price("BZE-USDC"))
        await self._new_block()
        self.assertIsNotNone(self.exchange.get_pool_price("BZE-USDC"))
        self.assertTrue(self.exchange.quote_swap("BZE-USDC", "sell", Decimal("10")).ok)
        self.data_source.get_halted_denoms.assert_awaited()

    async def test_halted_denom_from_the_connector_cache_halts_the_pool(self):
        self.data_source.get_halted_denoms = AsyncMock(return_value={"ubze"})
        await self._new_block()
        self.assertTrue(self.exchange.get_pool("BZE-USDC").halted)
        self.assertTrue(self.exchange.quote_input_for_output("BZE-USDC", "buy", Decimal("1")).halted)

    async def test_failed_refresh_keeps_the_last_reserves(self):
        await self._new_block()
        price = self.exchange.get_pool_price("BZE-USDC")
        self.data_source.get_liquidity_pool = AsyncMock(side_effect=IOError("node down"))
        await self._new_block()
        self.assertEqual(price, self.exchange.get_pool_price("BZE-USDC"))
        self.assertEqual(1, len([r for r in self.log_records if r.levelno == logging.WARNING and "liquidity pools" in r.getMessage()]))

    async def test_delegate_returns_the_pool_price_and_follows_new_blocks(self):
        delegate = BeezeePoolPriceDelegate(self.exchange, "BZE-USDC")
        self.assertTrue(delegate.get_mid_price().is_nan())

        await self._new_block()
        first = self.exchange.get_pool_price("BZE-USDC")
        self.assertEqual(first, delegate.get_mid_price())
        self.assertEqual(first, delegate.get_price_by_type(PriceType.MidPrice))
        self.assertEqual(first, delegate.get_price_by_type(PriceType.BestAsk))

        self.source.pools[USDC_POOL] = with_reserves(recorded_pool(USDC_POOL), 3_000_000_000, 6_000_000_000_000)
        await self._new_block()
        self.assertEqual(Decimal("0.0005"), delegate.get_mid_price())
        self.assertIs(self.exchange, delegate.market)
        self.assertEqual("BZE-USDC", delegate.trading_pair)

    async def test_delegate_is_ready_with_a_ready_market_and_a_pool_price(self):
        market = Mock()
        market.ready = True
        market.get_pool_price = Mock(return_value=None)
        delegate = BeezeePoolPriceDelegate(market, "BZE-USDC")
        self.assertFalse(delegate.ready)
        market.get_pool_price.return_value = Decimal("0.0005")
        self.assertTrue(delegate.ready)
        market.ready = False
        self.assertFalse(delegate.ready)
