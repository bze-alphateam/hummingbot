import asyncio
import math
import time
import unittest
from decimal import Decimal
from test.hummingbot.connector.exchange.beezee.test_beezee_venues import make_data_source
from typing import List
from unittest.mock import AsyncMock, Mock, patch

from hummingbot.client.performance import PerformanceMetrics
from hummingbot.connector.exchange.beezee.beezee_exchange import BeezeeExchange
from hummingbot.connector.exchange.beezee.beezee_utils import (
    BeezeeConfigMap,
    BeezeeMainnetNetworkMode,
    BeezeeReadOnlyAccountMode,
)
from hummingbot.core.data_type.common import PositionAction, PriceType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.model.order import Order  # noqa — Order needs to be defined for TradeFill
from hummingbot.model.order_status import OrderStatus  # noqa — Order needs to be defined for TradeFill
from hummingbot.model.trade_fill import TradeFill

PRICE_TYPES = (PriceType.BestBid, PriceType.BestAsk, PriceType.MidPrice, PriceType.LastTrade)


class BeezeePoolOnlyPricingTests(unittest.IsolatedAsyncioTestCase):
    """TUSDC-BZE has a pool and no order-book market, so its book is always empty. The rate oracle prices pairs
    through the connector (for `history`, the trade monitor and fee conversions) and raised on the empty book's NaN."""

    async def asyncSetUp(self) -> None:
        self.exchange = await make_exchange(["TUSDC-BZE", "BZE-USDC"])
        await self.exchange._build_pool_tracker().refresh()
        # The order books the tracker holds: always empty for the pool-only pair; left empty here for the market too.
        for trading_pair in ("TUSDC-BZE", "BZE-USDC"):
            self.exchange.order_book_tracker._order_books[trading_pair] = OrderBook()
        self.pool_price = self.exchange.get_pool_price("TUSDC-BZE")
        self.oracle = RateOracle(source=Mock())
        # A filled price cache, as in a running client: a pair it lacks is looked up through the connectors.
        self.oracle._prices = {"BTC-USDT": Decimal("100000")}
        self.oracle.register_connector(self.exchange)

    async def asyncTearDown(self) -> None:
        await stop(self.exchange)

    def test_every_price_of_a_pool_only_pair_is_its_pool_price(self):
        self.assertEqual(Decimal("1.31"), self.pool_price)
        for price_type in PRICE_TYPES:
            self.assertEqual(self.pool_price, self.exchange.get_price_by_type("TUSDC-BZE", price_type), price_type)
        self.assertEqual(self.pool_price, self.exchange.get_price("TUSDC-BZE", True))
        self.assertEqual(self.pool_price, self.exchange.get_price("TUSDC-BZE", False))
        self.assertEqual(self.pool_price, self.exchange.get_mid_price("TUSDC-BZE"))

    def test_a_pair_with_an_order_book_market_keeps_the_book_prices(self):
        # BZE-USDC also has a pool, but its prices stay the book's: NaN on an empty book, as before.
        self.assertIsNotNone(self.exchange.get_pool_price("BZE-USDC"))
        for price_type in PRICE_TYPES:
            self.assertTrue(self.exchange.get_price_by_type("BZE-USDC", price_type).is_nan(), price_type)

    def test_a_pool_only_pair_without_a_pool_price_answers_none(self):
        self.exchange.get_pool_price = Mock(return_value=None)
        for price_type in PRICE_TYPES:
            self.assertIsNone(self.exchange.get_price_by_type("TUSDC-BZE", price_type), price_type)
        self.assertIsNone(self.oracle.get_pair_rate("TUSDC-BZE"))
        self.assertIsNone(self.oracle.get_pair_rate("BZE-TUSDC"))

    def test_the_rate_oracle_prices_a_pool_only_pair_through_the_connector(self):
        self.assertEqual(self.pool_price, self.oracle.get_pair_rate("TUSDC-BZE"))
        # The reverse pair, as when a fee paid in BZE is converted on a TUSDC-quoted pair.
        self.assertEqual(Decimal("1") / self.pool_price, self.oracle.get_pair_rate("BZE-TUSDC"))

    async def test_performance_metrics_with_one_pool_swap_trade(self):
        # A 2 TUSDC sell on the pool, recorded net of the pool fee (0.004 TUSDC), plus the 0.1 BZE taker fee.
        trade_fee = AddedToCostTradeFee(flat_fees=[TokenAmount("TUSDC", Decimal("0.004")), TokenAmount("BZE", Decimal("0.1"))])
        trade = TradeFill(
            config_file_path="beezee_treasury_twap.yml", strategy="beezee_treasury_twap", market="beezee",
            symbol="TUSDC-BZE", base_asset="TUSDC", quote_asset="BZE", timestamp=int(time.time() * 1e3),
            order_id="swap-1", trade_type="SELL", order_type="MARKET", price=1.310141, amount=1.996,
            trade_fee=trade_fee.to_json(), exchange_trade_id="AB" * 32, position=PositionAction.NIL.value,
        )
        with patch.object(RateOracle, "get_instance", return_value=self.oracle):
            metrics = await PerformanceMetrics.create(
                "TUSDC-BZE", [trade], {"TUSDC": Decimal("4998"), "BZE": Decimal("15002.5")})
        self.assertEqual(1, metrics.num_trades)
        self.assertEqual(self.pool_price, metrics.cur_price)
        self.assertEqual(Decimal("0.1") + Decimal("0.004") * self.pool_price, metrics.fee_in_quote)

    async def test_the_last_traded_price_is_the_pool_price_or_the_market_last_trade(self):
        self.exchange._orderbook_ds._data_source.get_last_traded_price = AsyncMock(return_value=Decimal("0.00055"))
        self.assertEqual(1.31, await self.exchange._get_last_traded_price("TUSDC-BZE"))
        self.assertEqual(0.00055, await self.exchange._get_last_traded_price("BZE-USDC"))
        self.exchange.get_pool_price = Mock(return_value=None)
        self.exchange._orderbook_ds._pool_price_provider = self.exchange.get_pool_price
        self.assertTrue(math.isnan(await self.exchange._get_last_traded_price("TUSDC-BZE")))


class BeezeePoolReadinessTests(unittest.IsolatedAsyncioTestCase):
    """The pools are read in the background on each new block. The connector reported ready before the first read,
    so a script's first tick found no pool and took the pair for one without a pool."""

    async def asyncTearDown(self) -> None:
        for exchange in getattr(self, "exchanges", []):
            await stop(exchange)

    async def _exchange(self, trading_pairs: List[str]) -> BeezeeExchange:
        exchange = await make_exchange(trading_pairs)
        self.exchanges = getattr(self, "exchanges", []) + [exchange]
        return exchange

    async def test_not_ready_until_the_first_pool_refresh(self):
        for trading_pairs in (["TUSDC-BZE"], ["BZE-USDC"], ["OTHER-BZE", "TUSDC-BZE"]):
            exchange = await self._exchange(trading_pairs)
            self.assertFalse(exchange.status_dict["pools_initialized"], trading_pairs)
            self.assertFalse(exchange.ready, trading_pairs)
            await exchange._refresh_pools()
            self.assertTrue(exchange.status_dict["pools_initialized"], trading_pairs)

    async def test_pairs_without_a_pool_do_not_wait_for_the_pools(self):
        exchange = await self._exchange(["OTHER-BZE", "TKN-BZE"])
        self.assertTrue(exchange.status_dict["pools_initialized"])


async def make_exchange(trading_pairs: List[str]) -> BeezeeExchange:
    """A read-only connector on the pairs of test_beezee_venues, with its pairs (and venues) loaded and no pool read."""
    config = BeezeeConfigMap(connector="beezee", network=BeezeeMainnetNetworkMode(), account_type=BeezeeReadOnlyAccountMode())
    exchange = BeezeeExchange(connector_configuration=config, trading_pairs=trading_pairs, trading_required=False)
    data_source = make_data_source()
    data_source.get_trade_fees = AsyncMock(return_value={"maker": None, "taker": None})
    data_source.get_halted_denoms = AsyncMock(return_value=set())
    exchange._data_source = data_source
    exchange._orderbook_ds._data_source = data_source
    await exchange._format_trading_rules({})
    return exchange


async def stop(exchange: BeezeeExchange):
    await exchange.stop_network()
    for _ in range(5):
        await asyncio.sleep(0)
