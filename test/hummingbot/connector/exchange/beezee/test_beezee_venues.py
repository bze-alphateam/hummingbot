import asyncio
import math
import unittest
from decimal import Decimal
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, Mock

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_amm import BeezeeSwapError
from hummingbot.connector.exchange.beezee.beezee_api_data_source import BeezeeAPIDataSource
from hummingbot.connector.exchange.beezee.beezee_api_order_book_data_source import BeezeeAPIOrderBookDataSource
from hummingbot.connector.exchange.beezee.beezee_exchange import BeezeeExchange
from hummingbot.connector.exchange.beezee.beezee_fills import BeezeeFillError
from hummingbot.connector.exchange.beezee.beezee_utils import (
    BeezeeConfigMap,
    BeezeeMainnetNetworkMode,
    BeezeeReadOnlyAccountMode,
    BeezeeVenueError,
    describe_venues,
)
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker

# A pool and an order-book market are unrelated on chain. The pairs below cover every combination:
#   BZE-USDC  market ubze/USDC and pool USDC_ubze (the pool's base is the pair's quote),
#   OTHER-BZE market OTHER/ubze, no pool,
#   TKN-BZE   market TKN/ubze, no pool,
#   TUSDC-BZE pool TUSDC_ubze, no market,
#   a pool of a second TKN token (same symbol as the TKN market), no market.
USDC = "ibc/81DA528F4C5546208D1D50F84C2D0B388B45D37AA36FCA622A3CB2D8EF014838"  # noqa: mock
OTHER = "factory/bze1other/uother"
TKN = "factory/bze1first/utkn"
TKN_TWIN = "factory/bze1second/utkn"
TUSDC = "factory/bze1z3mkcr2jz424w6m49frgjmy9uhlrx69phqwg3l/testusd"


def _metadata(symbol: str, denom: str, exponent: int = 6) -> Dict[str, Any]:
    return {"symbol": symbol, "display": symbol.lower(),
            "denom_units": [{"denom": denom, "exponent": 0}, {"denom": symbol.lower(), "exponent": exponent}]}


METADATA = {
    "ubze": _metadata("BZE", "ubze"),
    USDC: _metadata("USDC", USDC),
    OTHER: _metadata("OTHER", OTHER),
    TKN: _metadata("TKN", TKN),
    TKN_TWIN: _metadata("TKN", TKN_TWIN),
    TUSDC: _metadata("TUSDC", TUSDC),
}
MARKETS = [{"base": "ubze", "quote": USDC}, {"base": OTHER, "quote": "ubze"}, {"base": TKN, "quote": "ubze"}]


def pool(base: str, quote: str, reserve_base: int, reserve_quote: int, fee: str = "0.002") -> Dict[str, Any]:
    return {"id": f"{base}_{quote}", "base": base, "quote": quote, "lp_denom": f"ulp_{base}_{quote}", "fee": fee,
            "fee_dest": {"treasury": "0", "burner": "0", "providers": "1"},
            "reserve_base": str(reserve_base), "reserve_quote": str(reserve_quote)}


# 1,000 USDC / 2,000,000 BZE (0.0005 USDC per BZE); 1,000 TUSDC / 1,310 BZE (1.31 BZE per TUSDC).
POOLS = [
    pool(USDC, "ubze", 1_000_000_000, 2_000_000_000_000, fee="0.001"),
    pool(TUSDC, "ubze", 1_000_000_000, 1_310_000_000),
    pool(TKN_TWIN, "ubze", 5_000_000_000, 5_000_000_000),
]
POOLS_BY_ID = {data["id"]: data for data in POOLS}


def make_data_source(markets: List[Dict[str, Any]] = MARKETS, pools: List[Dict[str, Any]] = POOLS) -> BeezeeAPIDataSource:
    data_source = BeezeeAPIDataSource(
        api_factory=AsyncMock(), rest_endpoint="https://rest.getbze.com", chain_id="beezee-1", address_prefix="bze",
        native_denom="ubze", gas_price=Decimal("0.01"), account_address="bze1test",
    )

    async def paged_request(path_url: str, item_key: str, *args, **kwargs) -> List[Dict[str, Any]]:
        return {CONSTANTS.TRADING_PAIRS_PATH_URL: markets, CONSTANTS.ALL_LIQUIDITY_POOLS_PATH_URL: pools}[path_url]

    async def liquidity_pool(pool_id: str) -> Optional[Dict[str, Any]]:
        return {data["id"]: data for data in pools}.get(pool_id)

    data_source._paged_request = AsyncMock(side_effect=paged_request)
    data_source.get_denom_metadata = AsyncMock(side_effect=lambda denom: METADATA[denom])
    data_source.get_liquidity_pool = AsyncMock(side_effect=liquidity_pool)
    return data_source


class BeezeePairDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_pairs_come_from_the_markets_and_the_pools_each_with_its_venues(self):
        pairs = await make_data_source().get_all_pairs()

        twin = next(name for name, pair in pairs.items() if pair.base.denom == TKN_TWIN)
        self.assertEqual({"BZE-USDC", "OTHER-BZE", "TKN-BZE", "TUSDC-BZE", twin}, set(pairs))
        self.assertEqual(("book", "pool"), pairs["BZE-USDC"].venues)
        self.assertEqual(f"{USDC}_ubze", pairs["BZE-USDC"].pool_id)
        self.assertEqual(("book",), pairs["OTHER-BZE"].venues)
        self.assertEqual(("book",), pairs["TKN-BZE"].venues)
        self.assertEqual(("pool",), pairs["TUSDC-BZE"].venues)
        self.assertIsNone(pairs["TUSDC-BZE"].market)
        self.assertEqual(f"{TUSDC}_ubze", pairs["TUSDC-BZE"].pool_id)
        self.assertEqual(("pool",), pairs[twin].venues)

    async def test_a_pair_with_both_venues_keeps_the_market_orientation(self):
        pair = (await make_data_source().get_pair("BZE-USDC"))
        # The pool's base is USDC (ids are sorted), the pair's base stays the market's.
        self.assertEqual(("ubze", USDC), (pair.base.denom, pair.quote.denom))
        self.assertEqual("ubze", pair.market.base.denom)

    async def test_a_pool_only_pair_takes_the_pool_orientation_and_the_denom_metadata(self):
        pair = await make_data_source().get_pair("TUSDC-BZE")
        self.assertEqual((TUSDC, "ubze"), (pair.base.denom, pair.quote.denom))
        self.assertEqual(("TUSDC", "BZE"), (pair.base.symbol, pair.quote.symbol))
        self.assertEqual((6, 6), (pair.base.exponent, pair.quote.exponent))

    async def test_market_pair_names_do_not_change_because_of_the_pools(self):
        without_pools = await make_data_source(pools=[]).get_all_pairs()
        with_pools = await make_data_source().get_all_pairs()

        self.assertEqual({"BZE-USDC", "OTHER-BZE", "TKN-BZE"}, set(without_pools))
        for name in without_pools:
            self.assertEqual(without_pools[name].market, with_pools[name].market)
        # The pool of the second TKN token gets the collision name, never the market's.
        self.assertEqual(TKN, with_pools["TKN-BZE"].base.denom)

    async def test_the_symbol_map_and_the_markets(self):
        data_source = make_data_source()
        symbol_map = await data_source.get_symbol_map()
        markets = await data_source.get_all_markets()

        self.assertEqual("TUSDC-BZE", symbol_map[f"{TUSDC}/ubze"])
        self.assertEqual("BZE-USDC", symbol_map[f"ubze/{USDC}"])
        self.assertEqual({f"ubze/{USDC}", f"{OTHER}/ubze", f"{TKN}/ubze"}, set(markets))

    async def test_the_market_of_a_pool_only_pair_is_a_clear_venue_error(self):
        data_source = make_data_source()
        with self.assertRaises(BeezeeVenueError) as context:
            await data_source.get_market_by_trading_pair("TUSDC-BZE")
        self.assertIsInstance(context.exception, KeyError)
        self.assertIn("TUSDC-BZE has no order-book market, only a liquidity pool", str(context.exception))
        with self.assertRaises(KeyError):
            await data_source.get_market_by_trading_pair("NOPE-BZE")
        self.assertEqual("ubze", (await data_source.get_market_by_trading_pair("BZE-USDC")).base.denom)

    async def test_markets_of_both_orientations_share_the_pool_of_their_denoms(self):
        markets = MARKETS + [{"base": "ubze", "quote": OTHER}]
        pairs = await make_data_source(markets=markets, pools=[pool(OTHER, "ubze", 1, 1)]).get_all_pairs()
        self.assertEqual(("book", "pool"), pairs["OTHER-BZE"].venues)
        self.assertEqual(("book", "pool"), pairs["BZE-OTHER"].venues)

    def test_venue_descriptions(self):
        self.assertEqual("order book + pool", describe_venues(("book", "pool")))
        self.assertEqual("order book only", describe_venues(("book",)))
        self.assertEqual("pool only", describe_venues(("pool",)))
        self.assertEqual("no venue", describe_venues(()))


class BeezeeExchangeVenueTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        config = BeezeeConfigMap(connector="beezee", network=BeezeeMainnetNetworkMode(), account_type=BeezeeReadOnlyAccountMode())
        self.exchange = BeezeeExchange(
            connector_configuration=config, trading_pairs=["BZE-USDC", "TUSDC-BZE", "OTHER-BZE"], trading_required=False,
        )
        self.data_source = make_data_source()
        self.data_source.get_trade_fees = AsyncMock(return_value={"maker": None, "taker": None})
        self.data_source.get_halted_denoms = AsyncMock(return_value=set())
        self.data_source.simulate_sign_and_broadcast = AsyncMock()
        self.exchange._data_source = self.data_source
        self.rules = {rule.trading_pair: rule for rule in await self.exchange._format_trading_rules({})}
        await self.exchange._build_pool_tracker().refresh()

    async def asyncTearDown(self) -> None:
        await self.exchange.stop_network()
        for _ in range(5):
            await asyncio.sleep(0)

    def test_every_pair_has_a_rule_and_only_markets_support_limit_orders(self):
        self.assertTrue(self.rules["BZE-USDC"].supports_limit_orders)
        self.assertTrue(self.rules["OTHER-BZE"].supports_limit_orders)
        pool_rule = self.rules["TUSDC-BZE"]
        self.assertFalse(pool_rule.supports_limit_orders)
        self.assertEqual(Decimal("0.000001"), pool_rule.min_base_amount_increment)
        self.assertEqual(Decimal("0.000001"), pool_rule.min_quote_amount_increment)
        self.assertEqual(Decimal("0"), pool_rule.min_notional_size)

    def test_trading_pair_venues(self):
        self.assertEqual(("book", "pool"), self.exchange.trading_pair_venues("BZE-USDC"))
        self.assertEqual(("pool",), self.exchange.trading_pair_venues("TUSDC-BZE"))
        self.assertEqual(("book",), self.exchange.trading_pair_venues("OTHER-BZE"))
        self.assertEqual((), self.exchange.trading_pair_venues("NOPE-BZE"))

    async def test_the_symbol_map_lists_the_pool_only_pairs(self):
        self.assertIn("TUSDC-BZE", await self.exchange.all_trading_pairs())

    # === A pool-only pair ===

    def test_pool_only_pair_reads_its_pool_in_its_own_orientation(self):
        self.assertEqual(Decimal("1.31"), self.exchange.get_pool_price("TUSDC-BZE"))
        self.assertEqual((Decimal("1000"), Decimal("1310")), self.exchange.get_pool_reserves("TUSDC-BZE"))
        sell = self.exchange.quote_swap("TUSDC-BZE", "sell", Decimal("10"))
        self.assertTrue(sell.ok)
        self.assertEqual((TUSDC, "ubze"), (sell.input_denom, sell.output_denom))
        self.assertTrue(self.exchange.quote_input_for_output("TUSDC-BZE", "buy", Decimal("1")).ok)
        # The pair with both venues still reads its pool inverted (the pool's base is the pair's quote).
        self.assertEqual(Decimal("0.0005"), self.exchange.get_pool_price("BZE-USDC"))

    def test_pool_only_pair_refuses_a_swap_too_small_for_the_pool_fee(self):
        # 0.2% of 0.000499 TUSDC truncates to no fee at all.
        quote = self.exchange.quote_swap("TUSDC-BZE", "sell", Decimal("0.000499"))
        self.assertFalse(quote.ok)
        self.assertEqual("amount is too low to be traded", quote.error)

    def test_pool_only_pair_refuses_orders_before_tracking_them(self):
        for place in (self.exchange.buy, self.exchange.sell):
            with self.assertRaises(BeezeeVenueError) as context:
                place("TUSDC-BZE", Decimal("1"), OrderType.LIMIT, Decimal("1.3"))
            self.assertIn("TUSDC-BZE has no order-book market", str(context.exception))
        with self.assertRaises(BeezeeVenueError):
            self.exchange.batch_order_create([LimitOrder("x", "TUSDC-BZE", True, "TUSDC", "BZE", Decimal("1.3"), Decimal("1"))])
        self.assertEqual({}, self.exchange.in_flight_orders)

    async def test_pool_only_pair_never_sends_an_order(self):
        self.exchange._signer = Mock()
        with self.assertRaises(BeezeeVenueError):
            await self.exchange._place_order("OID-1", "TUSDC-BZE", Decimal("1"), TradeType.BUY, OrderType.LIMIT, Decimal("1.3"))
        self.data_source.simulate_sign_and_broadcast.assert_not_called()

    def test_pool_only_pair_refuses_book_fills(self):
        self.exchange._signer = Mock()
        with self.assertRaisesRegex(BeezeeFillError, "TUSDC-BZE has no order-book market"):
            self.exchange.levels_to_take("TUSDC-BZE", "buy", Decimal("1"), Decimal("2"))
        with self.assertRaisesRegex(BeezeeFillError, "TUSDC-BZE has no order-book market"):
            self.exchange.fill_orders("TUSDC-BZE", "buy", [(Decimal("1.3"), Decimal("1"))])
        self.assertEqual({}, self.exchange.in_flight_orders)
        with self.assertRaisesRegex(BeezeeFillError, "unknown Beezee trading pair"):
            self.exchange.levels_to_take("NOPE-BZE", "buy", Decimal("1"), Decimal("2"))

    async def test_a_halted_denom_halts_a_pool_only_pair(self):
        self.exchange._halted_denoms = {TUSDC}
        await self.exchange._apply_halt_transitions()
        self.assertEqual({"TUSDC-BZE": f"denom {TUSDC}"}, self.exchange._halted_trading_pairs)
        self.assertTrue(self.exchange.get_pool("TUSDC-BZE").halted)
        self.exchange._signer = Mock()
        with self.assertRaisesRegex(BeezeeSwapError, "is halted"):
            self.exchange.amm_swap("TUSDC-BZE", "sell", Decimal("10"), Decimal("1"))

    async def test_a_halted_refusal_on_a_pool_only_pair_marks_its_pool(self):
        pair = await self.data_source.get_pair("TUSDC-BZE")
        await self.exchange._mark_market_halted_by_chain(pair)
        self.assertIn(f"{TUSDC}_ubze", self.exchange._refused_halted_market_ids)
        self.assertEqual("denom unknown, refused by the chain", self.exchange._halt_reason(pair))
        self.assertIn("TUSDC-BZE", self.exchange._halted_trading_pairs)

    # === A book-only pair ===

    def test_book_only_pair_has_no_pool(self):
        self.assertIsNone(self.exchange.get_pool("OTHER-BZE"))
        self.assertIsNone(self.exchange.get_pool_price("OTHER-BZE"))
        self.assertIsNone(self.exchange.get_pool_reserves("OTHER-BZE"))
        self.assertEqual("no liquidity pool for OTHER-BZE", self.exchange.quote_swap("OTHER-BZE", "buy", Decimal("1")).error)
        self.assertFalse(self.exchange.quote_input_for_output("OTHER-BZE", "sell", Decimal("1")).ok)
        self.exchange._signer = Mock()
        with self.assertRaisesRegex(BeezeeSwapError, "no liquidity pool for OTHER-BZE"):
            self.exchange.amm_swap("OTHER-BZE", "buy", Decimal("1"), Decimal("1"))
        self.assertEqual({}, self.exchange.in_flight_orders)

    def test_book_only_pair_quotes_on_the_book(self):
        self.assertEqual(Decimal("0"), self.exchange.minimum_order_amount("TUSDC-BZE", Decimal("1")))
        self.assertGreater(self.exchange.minimum_order_amount("OTHER-BZE", Decimal("1")), Decimal("0"))


class BeezeeOrderBookVenueTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.data_source = make_data_source()
        self.data_source.get_order_book = AsyncMock(return_value={
            "bids": [{"price": "0.0005", "amount": "1000000"}], "asks": [{"price": "0.0006", "amount": "1000000"}],
        })
        self.data_source.get_market_history = AsyncMock(return_value=[])
        self.data_source.get_last_traded_price = AsyncMock(return_value=Decimal("0.00055"))
        self.order_books = BeezeeAPIOrderBookDataSource(
            trading_pairs=["BZE-USDC", "TUSDC-BZE"], data_source=self.data_source, api_factory=Mock(),
            websocket_endpoint="wss://rpc.getbze.com/websocket",
        )

    async def test_a_pool_only_pair_gets_an_empty_book_without_a_read(self):
        book = await self.order_books.get_new_order_book("TUSDC-BZE")
        bids, asks = book.snapshot
        self.assertTrue(bids.empty and asks.empty)
        self.data_source.get_order_book.assert_not_called()
        self.assertIsNone(self.order_books.chain_levels("TUSDC-BZE"))

    async def test_polling_skips_the_pool_only_pair(self):
        await self.order_books._poll_and_publish()
        self.data_source.get_order_book.assert_awaited_once_with(f"ubze/{USDC}")
        snapshots = self.order_books._message_queue[self.order_books._snapshot_messages_queue_key]
        self.assertEqual(["BZE-USDC"], [snapshots.get_nowait().trading_pair for _ in range(snapshots.qsize())])

    async def test_last_traded_prices_answer_the_pool_only_pair_with_nan_without_a_pool_price(self):
        prices = await self.order_books.get_last_traded_prices(["TUSDC-BZE"])
        self.assertEqual(["TUSDC-BZE"], list(prices))
        self.assertTrue(math.isnan(prices["TUSDC-BZE"]))
        self.data_source.get_last_traded_price.assert_not_called()
        prices = await self.order_books.get_last_traded_prices(["BZE-USDC", "TUSDC-BZE"])
        self.assertEqual(0.00055, prices["BZE-USDC"])
        self.assertTrue(math.isnan(prices["TUSDC-BZE"]))

    async def test_last_traded_prices_answer_the_pool_only_pair_with_its_pool_price(self):
        pool_prices = {"TUSDC-BZE": Decimal("12.5")}
        self.order_books._pool_price_provider = pool_prices.get
        self.assertEqual({"BZE-USDC": 0.00055, "TUSDC-BZE": 12.5},
                         await self.order_books.get_last_traded_prices(["BZE-USDC", "TUSDC-BZE"]))
        self.data_source.get_last_traded_price.assert_awaited_once_with(f"ubze/{USDC}")

    async def test_last_traded_prices_answer_nan_when_the_pool_price_lookup_fails(self):
        self.order_books._pool_price_provider = Mock(side_effect=KeyError("TUSDC-BZE"))
        prices = await self.order_books.get_last_traded_prices(["TUSDC-BZE"])
        self.assertTrue(math.isnan(prices["TUSDC-BZE"]))

    async def test_the_last_trade_price_loop_throttles_a_pool_only_pair(self):
        # The upstream loop re-asks at once for every pair that does not come back, sleeping only when none is
        # outdated. A pool-only pair answers without any I/O, so leaving it out spun the loop without ever yielding.
        tracker = OrderBookTracker(data_source=self.order_books, trading_pairs=["TUSDC-BZE"])
        await tracker._init_order_books()
        calls: List[List[str]] = []
        get_last_traded_prices = self.order_books.get_last_traded_prices
        never = asyncio.Event()

        async def counting_get_last_traded_prices(trading_pairs: List[str], domain: Optional[str] = None):
            calls.append(list(trading_pairs))
            if len(calls) > 5:
                await never.wait()  # a spinning loop: yield so the timeout below can end the test
            return await get_last_traded_prices(trading_pairs)

        self.order_books.get_last_traded_prices = counting_get_last_traded_prices
        try:
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(tracker._update_last_trade_prices_loop(), timeout=0.3)
            self.assertEqual([["TUSDC-BZE"]], calls)
            self.assertGreater(tracker.order_books["TUSDC-BZE"].last_trade_price_rest_updated, 0)
        finally:
            for task in tracker._tracking_tasks.values():
                task.cancel()

    async def test_the_order_book_tracker_gets_ready_with_a_pool_only_pair(self):
        tracker = OrderBookTracker(data_source=self.order_books, trading_pairs=["BZE-USDC", "TUSDC-BZE"])
        tracker._sleep = AsyncMock()
        await tracker._init_order_books()
        try:
            self.assertTrue(tracker.ready)
            self.assertEqual({"BZE-USDC", "TUSDC-BZE"}, set(tracker.order_books))
        finally:
            for task in tracker._tracking_tasks.values():
                task.cancel()
