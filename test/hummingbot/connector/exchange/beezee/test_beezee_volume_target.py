import asyncio
import math
import re
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_FLOOR, Decimal
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
from aioresponses import aioresponses

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.clock import Clock
from hummingbot.core.clock_mode import ClockMode
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount
from hummingbot.core.event.events import (
    BuyOrderCreatedEvent,
    MarketEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCreatedEvent,
)
from hummingbot.data_feed.coin_gecko_data_feed.coin_gecko_constants import CoinGeckoAPITier
from scripts.beezee_volume_target import BeezeeVolumeTarget, BeezeeVolumeTargetConfig, CoinGeckoTickersFeed

PAIR = "BZE-USDC"
TICK = Decimal("0.0001")
BASE_STEP = Decimal("0.000001")
MAKER_FEE = Decimal("0.001")
TAKER_FEE = Decimal("0.1")
# Gas of one self-fill cycle's tx on testnet.
CYCLE_GAS = Decimal("0.00315")
# 2026-10-28 12:00:00 UTC: 43,200 s left in the day, i.e. 144 cycles of 300 s.
NOON = datetime(2026, 10, 28, 12, tzinfo=timezone.utc).timestamp()
EXAMPLE = Path(__file__).resolve().parents[5] / "conf" / "scripts" / "beezee_volume_target.yml.example"


@dataclass
class FakeOrder:
    client_order_id: str
    trading_pair: str
    is_buy: bool
    price: Decimal
    amount: Decimal
    is_done: bool = False


class FakeOrderBook:
    def __init__(self):
        self.bid: Optional[float] = None
        self.ask: Optional[float] = None

    def get_price(self, is_buy: bool) -> float:
        price = self.ask if is_buy else self.bid
        if price is None:
            raise EnvironmentError("Order book is empty - no price quote is possible.")
        return price


class FakeBeezeeConnector(ConnectorBase):
    """The parts of the BeeZee connector the script uses. Events are triggered on it like the real one does."""

    def __init__(self):
        super().__init__()
        self.book = FakeOrderBook()
        self.orders: Dict[str, FakeOrder] = {}
        self.batches: List[List[FakeOrder]] = []
        self.cancels: List[str] = []
        self.gas_paid = Decimal("0")
        self.chain_minimum = Decimal("0")
        self.pool_price: Optional[Decimal] = None
        self.balances = {"BZE": Decimal("1000000000"), "USDC": Decimal("1000000000")}
        self.rules = {PAIR: TradingRule(PAIR, min_price_increment=TICK, min_base_amount_increment=BASE_STEP)}
        # A market with no pool by default: the script needs only the book.
        self.venues: Tuple[str, ...] = ("book",)
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
    def in_flight_orders(self):
        return dict(self.orders)

    @property
    def trading_rules(self):
        return self.rules

    def get_order_book(self, trading_pair: str):
        return self.book

    def get_pool_price(self, trading_pair: str) -> Optional[Decimal]:
        return self.pool_price

    def gas_fees_paid(self) -> Decimal:
        return self.gas_paid

    def minimum_order_amount(self, trading_pair: str, price: Decimal) -> Decimal:
        return self.chain_minimum

    def quantize_order_amount(self, trading_pair: str, amount: Decimal) -> Decimal:
        return (amount / BASE_STEP).to_integral_value(rounding=ROUND_FLOOR) * BASE_STEP

    def get_available_balance(self, currency: str) -> Decimal:
        return self.balances.get(currency, Decimal("0"))

    def get_fee(self, base, quote, order_type, order_side, amount, price=Decimal("NaN"), is_maker=None):
        return AddedToCostTradeFee(flat_fees=[TokenAmount("BZE", MAKER_FEE if is_maker else TAKER_FEE)])

    def batch_order_create(self, orders_to_create: List[LimitOrder]) -> List[LimitOrder]:
        created, batch = [], []
        for order in orders_to_create:
            self._next_id += 1
            client_order_id = f"{'buy' if order.is_buy else 'sell'}-{self._next_id}"
            fake = FakeOrder(client_order_id, order.trading_pair, order.is_buy, order.price, order.quantity)
            self.orders[client_order_id] = fake
            batch.append(fake)
            created.append(LimitOrder(client_order_id, order.trading_pair, order.is_buy, order.base_currency,
                                      order.quote_currency, order.price, order.quantity))
        self.batches.append(batch)
        return created

    def cancel(self, trading_pair: str, client_order_id: str):
        self.cancels.append(client_order_id)
        return client_order_id


class BeezeeVolumeTargetTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.connector = FakeBeezeeConnector()
        self.connector.book.bid = 330.0
        self.connector.book.ask = 340.0
        self.feed = MagicMock()
        self.feed.get_coin_tickers = AsyncMock(return_value=[])
        self.feed.get_prices_by_token_id = AsyncMock(return_value=[{"id": "usd-coin", "current_price": 1}])

    def _strategy(self, start: float = NOON, **settings) -> BeezeeVolumeTarget:
        values = {"trading_pair": PAIR, "daily_volume_quote": Decimal("1440000"), "max_daily_fee_spend": Decimal("1000")}
        values.update(settings)
        config = BeezeeVolumeTargetConfig(**values)
        with patch("asyncio.create_task", return_value=MagicMock()), \
                patch("hummingbot.strategy.strategy_v2_base.StrategyV2Base.listen_to_executor_actions", return_value=AsyncMock()), \
                patch("hummingbot.strategy.strategy_v2_base._get_executor_orchestrator_class"), \
                patch("hummingbot.strategy.strategy_v2_base.MarketDataProvider"):
            strategy = BeezeeVolumeTarget({"beezee": self.connector}, config, coingecko_feed=self.feed)
        # The clock's first tick (start - 1) only finds the connector ready; the first cycle runs at `start`.
        self.clock = Clock(ClockMode.BACKTEST, 1, start - 2, start + 10 * 86400)
        self.clock.add_iterator(strategy)
        self.now = start
        self._started = False
        return strategy

    async def _run_until(self, timestamp: float):
        """Moves the clock, then lets an anchor refresh started on the way finish. The first call starts the
        strategy (which reads the anchor) and lets that read finish before the first cycle."""
        if not self._started:
            self._started = True
            self.clock.backtest_til(timestamp - 1)
            for _ in range(5):
                await asyncio.sleep(0)
        self.clock.backtest_til(timestamp)
        self.now = timestamp
        for _ in range(5):
            await asyncio.sleep(0)

    def _set_anchor_tickers(self, *tickers):
        self.feed.get_coin_tickers.return_value = [
            {"market": {"name": name}, "converted_last": {"usd": price}, "converted_volume": {"usd": volume}}
            for name, price, volume in tickers
        ]

    def _last_batch(self) -> List[FakeOrder]:
        return self.connector.batches[-1]

    def _fill(self, order: FakeOrder, amount: Decimal, trade_id: str, fee: Decimal = Decimal("0")):
        self.connector.trigger_event(MarketEvent.OrderFilled, OrderFilledEvent(
            timestamp=self.now, order_id=order.client_order_id, trading_pair=PAIR,
            trade_type=TradeType.BUY if order.is_buy else TradeType.SELL, order_type=OrderType.LIMIT,
            price=order.price, amount=amount,
            trade_fee=AddedToCostTradeFee(flat_fees=[TokenAmount("BZE", fee)] if fee else []),
            exchange_trade_id=trade_id,
        ))

    def _self_fill(self, batch: List[FakeOrder], height: int):
        """Both legs of a batch match each other: one trade, the second leg is the taker. Both were signed while the
        other was not on the book yet, so the connector reports the maker fee on each."""
        maker, taker = batch
        self._fill(maker, maker.amount, f"{height}-0-maker", MAKER_FEE)
        self._fill(taker, taker.amount, f"{height}-0-taker", MAKER_FEE)
        maker.is_done = taker.is_done = True

    def _status_value(self, strategy: BeezeeVolumeTarget, label: str) -> str:
        line = next(line for line in strategy.format_status().splitlines() if label in line)
        return line.split(":", 1)[1].strip()

    # === Pacing ===

    async def test_size_is_remaining_volume_over_cycles_left_today(self):
        strategy = self._strategy()
        await self._run_until(NOON)

        batch = self._last_batch()
        # 1,440,000 / 144 cycles / 335 = 29.850746... base, rounded down to the base step.
        expected = Decimal("29.850746")
        self.assertEqual([expected, expected], [order.amount for order in batch])
        self.assertIsNone(strategy._last_skip_reason)

    async def test_size_follows_the_remaining_volume_and_the_cycles_left(self):
        self._strategy(cycle_seconds=600)
        await self._run_until(NOON)
        self._self_fill(self._last_batch(), 1)
        await self._run_until(NOON + 600)

        # 72 cycles at noon; after one fill of 59.701492 x 335 the rest is spread over the remaining 71.
        first = Decimal("59.701492")
        remaining = Decimal("1440000") - first * Decimal("335")
        expected = (remaining / 71 / Decimal("335") / BASE_STEP).to_integral_value(rounding=ROUND_FLOOR) * BASE_STEP
        self.assertEqual(first, self.connector.batches[0][0].amount)
        self.assertEqual(expected, self._last_batch()[0].amount)

    async def test_size_is_clamped_to_the_maximum(self):
        self._strategy(max_order_amount=Decimal("10"))
        await self._run_until(NOON)
        self.assertEqual([Decimal("10"), Decimal("10")], [order.amount for order in self._last_batch()])

    async def test_size_is_raised_to_the_minimum(self):
        self._strategy(min_order_amount=Decimal("100"))
        await self._run_until(NOON)
        self.assertEqual([Decimal("100"), Decimal("100")], [order.amount for order in self._last_batch()])

    async def test_size_is_raised_to_the_chain_minimum(self):
        self.connector.chain_minimum = Decimal("500")
        self._strategy(min_order_amount=Decimal("100"))
        await self._run_until(NOON)
        self.assertEqual([Decimal("500"), Decimal("500")], [order.amount for order in self._last_batch()])

    async def test_cycles_run_every_cycle_seconds(self):
        self._strategy(cycle_seconds=300)
        await self._run_until(NOON)
        self._self_fill(self._last_batch(), 1)
        await self._run_until(NOON + 299)
        self.assertEqual(1, len(self.connector.batches))
        await self._run_until(NOON + 300)
        self.assertEqual(2, len(self.connector.batches))

    async def test_target_reached_idles_until_utc_midnight_and_the_next_day_starts_from_zero(self):
        late = datetime(2026, 10, 28, 23, 55, tzinfo=timezone.utc).timestamp()
        strategy = self._strategy(start=late, daily_volume_quote=Decimal("3350"), cycle_seconds=120,
                                  min_order_amount=Decimal("10"))
        await self._run_until(late)
        # 3 cycles left: 3.33 base, raised to the 10 minimum, which is the whole target at 335.
        self.assertEqual(Decimal("10"), self._last_batch()[0].amount)
        self._self_fill(self._last_batch(), 1)
        await self._run_until(late + 119)
        self.assertEqual(Decimal("0"), strategy.remaining_quote)

        midnight = datetime(2026, 10, 29, tzinfo=timezone.utc).timestamp()
        await self._run_until(midnight - 1)
        self.assertEqual(1, len(self.connector.batches))
        self.assertIn("target reached", strategy._last_skip_reason)

        await self._run_until(midnight + 60)
        self.assertEqual(2, len(self.connector.batches))
        self.assertEqual(Decimal("3350"), strategy.remaining_quote)
        self.assertIn("2026-10-29", strategy.format_status())

    # === Price ===

    async def test_anchor_inside_the_spread_is_the_trade_price_rounded_down_to_the_tick(self):
        self._set_anchor_tickers(("Nonkyc.io", "334.56789", "100"))
        self._strategy(coingecko_id_base="beezee")
        await self._run_until(NOON)
        self.assertEqual({Decimal("334.5678")}, {order.price for order in self._last_batch()})

    async def test_anchor_below_the_bid_is_clamped_to_one_tick_above_it(self):
        self._set_anchor_tickers(("Nonkyc.io", "300", "100"))
        self._strategy(coingecko_id_base="beezee", max_anchor_deviation_pct=Decimal("50"))
        await self._run_until(NOON)
        self.assertEqual({Decimal("330.0001")}, {order.price for order in self._last_batch()})

    async def test_anchor_above_the_ask_is_clamped_to_one_tick_below_it(self):
        self._set_anchor_tickers(("Nonkyc.io", "400", "100"))
        self._strategy(coingecko_id_base="beezee", max_anchor_deviation_pct=Decimal("50"))
        await self._run_until(NOON)
        self.assertEqual({Decimal("339.9999")}, {order.price for order in self._last_batch()})

    async def test_book_prices_are_read_on_the_tick_grid(self):
        # Floats from the book: 330.1 is 330.09999999999997 in binary.
        self.connector.book.bid = 330.1
        self.connector.book.ask = 330.1003
        self._set_anchor_tickers(("Nonkyc.io", "300", "100"))
        self._strategy(coingecko_id_base="beezee", max_anchor_deviation_pct=Decimal("50"))
        await self._run_until(NOON)
        self.assertEqual({Decimal("330.1001")}, {order.price for order in self._last_batch()})

    async def test_spread_of_one_tick_skips_the_cycle(self):
        self.connector.book.bid = 330.0
        self.connector.book.ask = 330.0001
        strategy = self._strategy()
        await self._run_until(NOON)
        self.assertEqual([], self.connector.batches)
        self.assertIn("one tick", strategy._last_skip_reason)

    async def test_spread_of_two_ticks_trades_on_the_tick_between(self):
        self.connector.book.bid = 330.0
        self.connector.book.ask = 330.0002
        self._strategy()
        await self._run_until(NOON)
        self.assertEqual({Decimal("330.0001")}, {order.price for order in self._last_batch()})

    async def test_empty_bid_side_skips_the_cycle(self):
        self.connector.book.bid = None
        strategy = self._strategy()
        await self._run_until(NOON)
        self.assertEqual([], self.connector.batches)
        self.assertIn("empty", strategy._last_skip_reason)

    async def test_empty_ask_side_skips_the_cycle(self):
        self.connector.book.ask = None
        strategy = self._strategy()
        await self._run_until(NOON)
        self.assertEqual([], self.connector.batches)
        self.assertIn("empty", strategy._last_skip_reason)

    # === Deviation guard ===

    async def test_book_far_from_the_anchor_pauses(self):
        # Book mid 335 vs anchor 318: 5.35% off, over the default 5%.
        self._set_anchor_tickers(("Nonkyc.io", "318", "100"))
        strategy = self._strategy(coingecko_id_base="beezee")
        await self._run_until(NOON)
        self.assertEqual([], self.connector.batches)
        self.assertIn("off the anchor", strategy._last_skip_reason)

    async def test_book_within_the_deviation_limit_trades(self):
        # 335 vs 320: 4.69% off.
        self._set_anchor_tickers(("Nonkyc.io", "320", "100"))
        self._strategy(coingecko_id_base="beezee")
        await self._run_until(NOON)
        self.assertEqual({Decimal("330.0001")}, {order.price for order in self._last_batch()})

    async def test_deviation_guard_does_not_apply_on_the_book_mid_fallback(self):
        # A stale anchor far from the book: the fallback trades at the book mid instead of pausing.
        self._set_anchor_tickers(("Nonkyc.io", "200", "100"))
        strategy = self._strategy(coingecko_id_base="beezee", anchor_refresh_seconds=86400, anchor_max_age_seconds=600)
        await self._run_until(NOON)
        self.assertEqual([], self.connector.batches)
        await self._run_until(NOON + 900)
        self.assertEqual({Decimal("335")}, {order.price for order in self._last_batch()})
        self.assertIn("book_mid", strategy._last_anchor_source)

    # === Fallbacks ===

    async def test_without_a_coingecko_id_the_book_mid_is_the_anchor(self):
        strategy = self._strategy()
        await self._run_until(NOON)
        self.assertEqual({Decimal("335")}, {order.price for order in self._last_batch()})
        self.feed.get_coin_tickers.assert_not_awaited()
        self.assertIn("no coingecko_id_base", strategy._last_anchor_source)

    async def test_stale_anchor_falls_back_to_the_book_mid(self):
        self._set_anchor_tickers(("Nonkyc.io", "334", "100"))
        self._strategy(coingecko_id_base="beezee", anchor_refresh_seconds=86400, anchor_max_age_seconds=1800)
        await self._run_until(NOON)
        self.assertEqual(Decimal("334"), self._last_batch()[0].price)
        for batch in self.connector.batches:
            self._self_fill(batch, 1)
        await self._run_until(NOON + 1800)
        self.assertEqual(Decimal("334"), self._last_batch()[0].price)
        self._self_fill(self._last_batch(), 2)
        await self._run_until(NOON + 2100)
        self.assertEqual(Decimal("335"), self._last_batch()[0].price)

    async def test_pause_fallback_without_a_coingecko_id_does_not_trade(self):
        strategy = self._strategy(anchor_fallback="pause")
        await self._run_until(NOON)
        self.assertEqual([], self.connector.batches)
        self.assertIn("pause", strategy._last_skip_reason)

    async def test_pause_fallback_with_a_stale_anchor_does_not_trade(self):
        self._set_anchor_tickers(("Nonkyc.io", "334", "100"))
        strategy = self._strategy(coingecko_id_base="beezee", anchor_fallback="pause", cycle_seconds=900,
                                  anchor_refresh_seconds=86400, anchor_max_age_seconds=600)
        await self._run_until(NOON)
        self._self_fill(self._last_batch(), 1)
        await self._run_until(NOON + 900)
        self.assertEqual(1, len(self.connector.batches))
        self.assertIn("old", strategy._last_skip_reason)

    async def test_failed_anchor_read_keeps_the_previous_anchor(self):
        self._set_anchor_tickers(("Nonkyc.io", "334", "100"))
        self._strategy(coingecko_id_base="beezee", anchor_refresh_seconds=300)
        await self._run_until(NOON)
        self._self_fill(self._last_batch(), 1)
        self.feed.get_coin_tickers.side_effect = IOError("rate limited")
        await self._run_until(NOON + 300)
        self.assertEqual(Decimal("334"), self._last_batch()[0].price)

    # === CoinGecko anchor ===

    async def test_anchor_ignores_excluded_markets_weights_by_volume_and_divides_by_the_quote(self):
        # BeeZee DEX would dominate; the external venues weigh 48 : 42.
        self._set_anchor_tickers(("BeeZee DEX", "0.0009", "73"), ("Nonkyc.io", "0.0004", "48"), ("Osmosis", "0.0003", "42"))
        self.feed.get_prices_by_token_id.return_value = [{"id": "usd-coin", "current_price": 0.5}]
        self.connector.book.bid = 1.0
        self.connector.book.ask = 2000.0
        self._strategy(coingecko_id_base="beezee", coingecko_id_quote="usd-coin",
                       anchor_price_scale=Decimal("1000000"), max_anchor_deviation_pct=Decimal("100000"))
        await self._run_until(NOON)

        usd = (Decimal("0.0004") * 48 + Decimal("0.0003") * 42) / 90
        expected = (usd / Decimal("0.5") * 1000000 / TICK).to_integral_value(rounding=ROUND_FLOOR) * TICK
        self.assertEqual(expected, self._last_batch()[0].price)
        self.feed.get_coin_tickers.assert_awaited_once_with("beezee")
        self.feed.get_prices_by_token_id.assert_awaited_once_with("usd", ["usd-coin"])

    async def test_excluded_market_names_are_matched_ignoring_case(self):
        self._set_anchor_tickers(("beezee dex", "0.0009", "73"), ("Nonkyc.io", "0.0004", "48"))
        self.connector.book.bid = 1.0
        self.connector.book.ask = 2000.0
        self._strategy(coingecko_id_base="beezee", anchor_price_scale=Decimal("1000000"),
                       max_anchor_deviation_pct=Decimal("100000"))
        await self._run_until(NOON)
        self.assertEqual(Decimal("400"), self._last_batch()[0].price)

    async def test_without_a_quote_id_the_quote_is_one_usd(self):
        self._set_anchor_tickers(("Nonkyc.io", "0.000334", "48"))
        self._strategy(coingecko_id_base="beezee", anchor_price_scale=Decimal("1000000"))
        await self._run_until(NOON)
        self.assertEqual(Decimal("334"), self._last_batch()[0].price)
        self.feed.get_prices_by_token_id.assert_not_awaited()

    async def test_only_excluded_tickers_leave_no_anchor(self):
        self._set_anchor_tickers(("BeeZee DEX", "0.0009", "73"))
        strategy = self._strategy(coingecko_id_base="beezee", anchor_fallback="pause")
        await self._run_until(NOON)
        self.assertEqual([], self.connector.batches)
        self.assertIn("no CoinGecko anchor", strategy._last_skip_reason)

    async def test_first_cycle_waits_for_the_first_anchor_read(self):
        read = asyncio.get_event_loop().create_future()

        async def wait_for_read(coin_id):
            return await read

        self.feed.get_coin_tickers.side_effect = wait_for_read
        self._strategy(coingecko_id_base="beezee")
        await self._run_until(NOON + 10)
        self.assertEqual([], self.connector.batches)

        read.set_result([{"market": {"name": "Nonkyc.io"}, "converted_last": {"usd": "334"}, "converted_volume": {"usd": "9"}}])
        for _ in range(5):
            await asyncio.sleep(0)
        await self._run_until(NOON + 11)
        self.assertEqual(Decimal("334"), self._last_batch()[0].price)

    async def test_anchor_is_refreshed_every_anchor_refresh_seconds(self):
        self._set_anchor_tickers(("Nonkyc.io", "334", "100"))
        self._strategy(coingecko_id_base="beezee", anchor_refresh_seconds=300)
        # First read when the strategy starts, at NOON - 2.
        await self._run_until(NOON + 297)
        self.assertEqual(1, self.feed.get_coin_tickers.await_count)
        await self._run_until(NOON + 298)
        self.assertEqual(2, self.feed.get_coin_tickers.await_count)

    async def test_tickers_feed_reads_the_coin_tickers_endpoint(self):
        feed = CoinGeckoTickersFeed(api_key="", api_tier=CoinGeckoAPITier.PUBLIC)
        with aioresponses() as mock_api:
            url = re.compile(r"^https://api\.coingecko\.com/api/v3/coins/beezee/tickers")
            mock_api.get(url, payload={"name": "BeeZee", "tickers": [{"market": {"name": "Osmosis"}}]})
            tickers = await feed.get_coin_tickers("beezee")
        self.assertEqual([{"market": {"name": "Osmosis"}}], tickers)

    def test_api_key_is_not_a_config_field(self):
        self.assertNotIn("coingecko_api_key", BeezeeVolumeTargetConfig.model_fields)

    # === Costs ===

    async def test_fee_cap_stops_the_day_and_resets_at_utc_midnight(self):
        strategy = self._strategy(max_daily_fee_spend=Decimal("0.202"))
        await self._run_until(NOON)
        self._self_fill(self._last_batch(), 1)
        self.connector.gas_paid = Decimal("0.2")
        # 0.002 tradebin + 0.2 gas >= 0.202.
        await self._run_until(NOON + 300)
        self.assertEqual(1, len(self.connector.batches))
        self.assertIn("fee cap", strategy._last_skip_reason)
        await self._run_until(NOON + 6 * 3600)
        self.assertEqual(1, len(self.connector.batches))

        midnight = datetime(2026, 10, 29, tzinfo=timezone.utc).timestamp()
        await self._run_until(midnight + 300)
        self.assertEqual(2, len(self.connector.batches))
        self.assertEqual(Decimal("0"), strategy.spend_today)

    async def test_spend_below_the_cap_keeps_trading(self):
        self._strategy(max_daily_fee_spend=Decimal("0.2021"))
        await self._run_until(NOON)
        self._self_fill(self._last_batch(), 1)
        self.connector.gas_paid = Decimal("0.2")
        await self._run_until(NOON + 300)
        self.assertEqual(2, len(self.connector.batches))

    async def test_tradebin_fees_alone_reach_the_cap(self):
        strategy = self._strategy(max_daily_fee_spend=Decimal("0.004"))
        await self._run_until(NOON)
        self._self_fill(self._last_batch(), 1)
        await self._run_until(NOON + 300)
        self._self_fill(self._last_batch(), 2)
        await self._run_until(NOON + 600)
        self.assertEqual(2, len(self.connector.batches))
        self.assertEqual(Decimal("0.004"), strategy.spend_today)

    async def test_self_fill_cycle_costs_two_maker_fees_plus_gas_and_the_cap_stops_on_that_spend(self):
        cycle_cost = 2 * MAKER_FEE + CYCLE_GAS
        strategy = self._strategy(max_daily_fee_spend=3 * cycle_cost)
        await self._run_until(NOON)
        self._self_fill(self._last_batch(), 1)
        self.connector.gas_paid += CYCLE_GAS
        self.assertEqual(cycle_cost, strategy.spend_today)
        for cycle in (1, 2):
            await self._run_until(NOON + cycle * 300)
            self._self_fill(self._last_batch(), cycle + 1)
            self.connector.gas_paid += CYCLE_GAS
        self.assertEqual(3, len(self.connector.batches))
        self.assertEqual(3 * cycle_cost, strategy.spend_today)

        await self._run_until(NOON + 900)
        self.assertEqual(3, len(self.connector.batches))
        self.assertIn("fee cap", strategy._last_skip_reason)

    async def test_fill_reported_after_the_cancel_of_its_order_does_not_add_a_second_fee(self):
        strategy = self._strategy()
        await self._run_until(NOON)
        sell, buy = self._last_batch()
        self.connector.trigger_event(MarketEvent.BuyOrderCreated, BuyOrderCreatedEvent(
            self.now, OrderType.LIMIT, PAIR, buy.amount, buy.price, buy.client_order_id, self.now, "AB" * 32
        ))
        # The cancel lands first; the fill the order had before it is read back afterwards with the order's fee.
        self.connector.trigger_event(MarketEvent.OrderCancelled, OrderCancelledEvent(self.now, buy.client_order_id))
        self._fill(buy, buy.amount / 2, "7-0-maker", MAKER_FEE)
        self.assertEqual(MAKER_FEE, strategy.spend_today)

    async def test_cancelled_leg_that_never_filled_costs_its_maker_fee(self):
        strategy = self._strategy()
        await self._run_until(NOON)
        sell, buy = self._last_batch()
        for order, event_tag, event_class in ((sell, MarketEvent.SellOrderCreated, SellOrderCreatedEvent),
                                              (buy, MarketEvent.BuyOrderCreated, BuyOrderCreatedEvent)):
            self.connector.trigger_event(event_tag, event_class(
                self.now, OrderType.LIMIT, PAIR, order.amount, order.price, order.client_order_id, self.now, "AB" * 32
            ))
        self._fill(sell, sell.amount, "7-0-taker", TAKER_FEE)
        sell.is_done = True
        self.connector.trigger_event(MarketEvent.OrderCancelled, OrderCancelledEvent(self.now, buy.client_order_id))
        self.assertEqual(TAKER_FEE + MAKER_FEE, strategy.spend_today)

    async def test_cost_per_1000_quote_is_shown(self):
        strategy = self._strategy()
        await self._run_until(NOON)
        self._self_fill(self._last_batch(), 1)
        self.connector.gas_paid = Decimal("0.05")
        filled = self._last_batch()[0].amount * Decimal("335")
        expected = (Decimal("0.052") / filled * 1000).quantize(Decimal("0.000001"))
        self.assertEqual(f"{expected} BZE", self._status_value(strategy, "Cost per 1,000"))

    # === Leftover legs and filled volume ===

    async def test_leftover_leg_is_cancelled_and_the_cycle_waits(self):
        strategy = self._strategy()
        await self._run_until(NOON)
        sell, buy = self._last_batch()
        # Someone else took the sell; the buy still rests.
        self._fill(sell, sell.amount, "9-3-maker", MAKER_FEE)
        sell.is_done = True
        foreign = FakeOrder("other-1", PAIR, True, Decimal("331"), Decimal("1"))
        self.connector.orders[foreign.client_order_id] = foreign
        await self._run_until(NOON + 300)

        self.assertEqual([buy.client_order_id], self.connector.cancels)
        self.assertEqual(1, len(self.connector.batches))
        self.assertIn("leftover", strategy._last_skip_reason)

        buy.is_done = True
        await self._run_until(NOON + 600)
        self.assertEqual(2, len(self.connector.batches))

    async def test_restored_leftover_leg_is_adopted_and_cancelled_before_trading(self):
        # A crash left one leg of a cycle resting: the restart restored it into the connector before the clock started.
        leftover = FakeOrder("buy-old", PAIR, True, Decimal("334"), Decimal("5"))
        other_pair = FakeOrder("sell-other", "TKN-BZE", False, Decimal("2"), Decimal("1"))
        done = FakeOrder("sell-old", PAIR, False, Decimal("334"), Decimal("5"), is_done=True)
        for order in (leftover, other_pair, done):
            self.connector.orders[order.client_order_id] = order
        strategy = self._strategy()
        await self._run_until(NOON)

        self.assertEqual(["buy-old"], self.connector.cancels)
        self.assertEqual([], self.connector.batches)
        self.assertIn("leftover", strategy._last_skip_reason)

        leftover.is_done = True
        await self._run_until(NOON + 300)
        self.assertEqual(1, len(self.connector.batches))
        self.assertEqual(["buy-old"], self.connector.cancels)

    async def test_fill_of_an_adopted_order_counts_toward_the_volume(self):
        leftover = FakeOrder("sell-old", PAIR, False, Decimal("335"), Decimal("4"))
        self.connector.orders[leftover.client_order_id] = leftover
        strategy = self._strategy()
        await self._run_until(NOON)

        self._fill(leftover, Decimal("4"), "20-1-maker", MAKER_FEE)

        self.assertEqual(Decimal("1440000") - Decimal("4") * Decimal("335"), strategy.remaining_quote)

    async def test_only_filled_volume_counts_and_a_self_fill_counts_once(self):
        strategy = self._strategy()
        await self._run_until(NOON)
        sell, buy = self._last_batch()
        half = Decimal("10")
        self._fill(sell, half, "12-4-maker")
        self._fill(buy, half, "12-4-taker")
        self.assertEqual(Decimal("1440000") - half * Decimal("335"), strategy.remaining_quote)
        # A separate trade with an outside taker counts too.
        self._fill(sell, Decimal("1"), "13-0-maker")
        self.assertEqual(Decimal("1440000") - Decimal("11") * Decimal("335"), strategy.remaining_quote)

    async def test_fills_of_other_orders_do_not_count(self):
        strategy = self._strategy()
        await self._run_until(NOON)
        self._fill(FakeOrder("other-1", PAIR, True, Decimal("335"), Decimal("5")), Decimal("5"), "14-0-taker", TAKER_FEE)
        self.assertEqual(Decimal("1440000"), strategy.remaining_quote)
        self.assertEqual(Decimal("0"), strategy.spend_today)

    # === One batched tx ===

    async def test_both_legs_go_out_in_one_batch_and_the_first_side_alternates(self):
        self._strategy()
        await self._run_until(NOON)
        for height in range(1, 4):
            self._self_fill(self._last_batch(), height)
            await self._run_until(NOON + 300 * height)

        self.assertEqual(4, len(self.connector.batches))
        for batch in self.connector.batches:
            self.assertEqual(2, len(batch))
            self.assertEqual({True, False}, {order.is_buy for order in batch})
            self.assertEqual(1, len({order.price for order in batch}))
            self.assertEqual(1, len({order.amount for order in batch}))
        self.assertEqual([False, True, False, True], [batch[0].is_buy for batch in self.connector.batches])

    async def test_it_runs_on_a_market_with_or_without_a_pool(self):
        for venues in (("book",), ("book", "pool")):
            self.connector = FakeBeezeeConnector()
            self.connector.book.bid = 330.0
            self.connector.book.ask = 340.0
            self.connector.venues = venues
            strategy = self._strategy()
            await self._run_until(NOON)
            self.assertIsNone(strategy._refused)
            self.assertEqual(1, len(self.connector.batches))

    async def test_a_pool_only_pair_is_refused_once_naming_the_market(self):
        self.connector.venues = ("pool",)
        strategy = self._strategy()
        with patch.object(strategy, "log_with_clock") as log:
            await self._run_until(NOON)
            await self._run_until(NOON + 600)
        self.assertEqual([], self.connector.batches)
        self.assertEqual(1, sum("refuses to start" in str(call) for call in log.call_args_list))
        self.assertEqual(
            f"  Refused: {PAIR} has no order-book market (venues: pool only); volume target needs an order-book market",
            strategy.format_status(),
        )

    async def test_missing_balance_skips_the_cycle(self):
        self.connector.balances["USDC"] = Decimal("100")
        strategy = self._strategy()
        await self._run_until(NOON)
        self.assertEqual([], self.connector.batches)
        self.assertIn("not enough USDC", strategy._last_skip_reason)

    # === Status ===

    async def test_status_shows_target_anchor_book_and_spend(self):
        self._set_anchor_tickers(("Nonkyc.io", "334", "100"))
        strategy = self._strategy(coingecko_id_base="beezee")
        await self._run_until(NOON)
        await self._run_until(NOON + 60)
        self.assertEqual("1440000 / 0 / 1440000 USDC", self._status_value(strategy, "Target / filled / remaining"))
        self.assertEqual("334 (source: coingecko, age: 61s)", self._status_value(strategy, "Anchor"))
        self.assertEqual("335.0", self._status_value(strategy, "Book mid"))
        self.assertIn("/ 1000 BZE", self._status_value(strategy, "Spend today"))
        self.assertEqual("-", self._status_value(strategy, "Pool price"))
        self.connector.pool_price = Decimal("336.5")
        self.assertEqual("336.5", self._status_value(strategy, "Pool price"))

    # === Example config ===

    def test_example_config_loads_with_the_mainnet_market(self):
        data = yaml.safe_load(EXAMPLE.read_text())
        config = BeezeeVolumeTargetConfig(**data)
        self.assertEqual("beezee", config.exchange)
        self.assertEqual("BZE-ERC20:0XA00C59FF5A080D2B954D0C75E46E22A0C371235A", config.trading_pair)
        self.assertEqual("bzedge", config.coingecko_id_base)
        self.assertEqual("usd-coin", config.coingecko_id_quote)
        self.assertEqual(["BeeZee DEX"], config.exclude_markets)
        self.assertEqual(Decimal("1000000"), config.anchor_price_scale)
        self.assertEqual("beezee_volume_target.py", config.script_file_name)
        self.assertTrue(math.isfinite(float(config.daily_volume_quote)))
