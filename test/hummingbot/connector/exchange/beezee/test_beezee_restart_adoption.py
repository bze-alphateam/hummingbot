import logging
from decimal import Decimal
from test.hummingbot.connector.exchange.beezee.test_beezee_order_batching import BeezeeBatchingTestCase, SentMessage
from test.hummingbot.connector.exchange.beezee.test_beezee_order_tracking import (
    ADDRESS,
    BZE_USDC,
    REST_URL,
    TKN_BZE,
    USDC,
    BeezeeChainTestCase,
    H,
    load_fixture,
)
from typing import List, Optional
from unittest.mock import PropertyMock, patch

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_exchange import BeezeeExchange
from hummingbot.core.clock import Clock
from hummingbot.core.clock_mode import ClockMode
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.event.events import MarketEvent
from hummingbot.strategy.asset_price_delegate import AssetPriceDelegate
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.pure_market_making import PureMarketMakingStrategy

START = 1640780000
MID = Decimal("0.0003")
BID_ID = "000000000000000006810001"
ASK_ID = "000000000000000006810002"
TKN_ID = "000000000000000006810003"


class FixedPrice(AssetPriceDelegate):
    """PMM's reference price, set by the test."""

    def __init__(self, price: Decimal):
        super().__init__()
        self.price = price

    def get_price_by_type(self, price_type: PriceType) -> Decimal:
        return self.price

    @property
    def ready(self) -> bool:
        return True

    @property
    def market(self):
        return None

    @property
    def trading_pair(self) -> str:
        return "BZE-USDC"


class RestartedPmmMixin:
    """A restart: the connector restores the previous run's orders, then PMM starts on the same connector."""

    def _start_harness(self):
        # The connector's own readiness needs live order books and a user stream: the strategy only needs it ready.
        ready = patch.object(BeezeeExchange, "ready", new_callable=PropertyMock, return_value=True)
        ready.start()
        self.addCleanup(ready.stop)
        self.price = FixedPrice(MID)
        self.pmm_logs: List[logging.LogRecord] = []
        handler = logging.Handler()
        handler.emit = self.pmm_logs.append
        PureMarketMakingStrategy.logger().addHandler(handler)
        PureMarketMakingStrategy.logger().setLevel(logging.INFO)
        self.addCleanup(PureMarketMakingStrategy.logger().removeHandler, handler)

    def _mock_balances(self):
        self.mock_api.get(f"{REST_URL}{CONSTANTS.BALANCES_PATH_URL}/{ADDRESS}", repeat=True, payload={"balances": [
            {"denom": "ubze", "amount": "1000000000"}, {"denom": USDC, "amount": "1000000000"},
        ]})

    def _restored(self, client_order_id: str, order_id: str, is_buy: bool, price: Decimal,
                  trading_pair: str = "BZE-USDC", amount: Decimal = Decimal("10")) -> InFlightOrder:
        return InFlightOrder(
            client_order_id=client_order_id, trading_pair=trading_pair, order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY if is_buy else TradeType.SELL, amount=amount, price=price,
            creation_timestamp=START - 600, exchange_order_id=order_id, initial_state=OrderState.OPEN,
        )

    def _pmm(self, **params) -> PureMarketMakingStrategy:
        settings = dict(
            market_info=MarketTradingPairTuple(self.exchange, "BZE-USDC", "BZE", "USDC"),
            bid_spread=Decimal("0.01"), ask_spread=Decimal("0.01"), order_amount=Decimal("10"),
            order_refresh_time=30, order_refresh_tolerance_pct=Decimal("0.01"), filled_order_delay=60,
            asset_price_delegate=self.price,
            # The taker filter reads the connector's live order book, which these tests do not run.
            take_if_crossed=True,
        )
        settings.update(params)
        strategy = PureMarketMakingStrategy()
        strategy.init_params(**settings)
        self.clock = Clock(ClockMode.BACKTEST, 1, START, START + 3600)
        self.clock.add_iterator(strategy)
        return strategy

    def _quote(self, price: Decimal) -> Decimal:
        return self.exchange.quantize_order_price("BZE-USDC", price)

    def _active_ids(self, strategy: PureMarketMakingStrategy) -> List[str]:
        return sorted(order.client_order_id for order in strategy.active_orders)

    def _pmm_logged(self, level: int, fragment: str) -> bool:
        return any(record.levelno == level and fragment in record.getMessage() for record in self.pmm_logs)


class RestartedPmmTestCase(RestartedPmmMixin, BeezeeBatchingTestCase):
    """The restart against a FakeNode, whose book holds the restored orders."""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self._start_harness()
        await self.exchange._update_balances()
        self.now = START - 1

    def _mock_chain_state(self):
        self._mock_balances()
        super()._mock_chain_state()

    def _rest(self, order: InFlightOrder, market_id: str = BZE_USDC):
        self.node.resting[order.exchange_order_id] = SentMessage(
            "create", market_id, "buy" if order.trade_type is TradeType.BUY else "sell",
            amount=str(int(order.amount * 1000000)), price=str(order.price),
        )

    async def _restart_with(self, orders: List[InFlightOrder], market_ids: Optional[List[str]] = None):
        for order, market_id in zip(orders, market_ids or [BZE_USDC] * len(orders)):
            self._rest(order, market_id)
        await self._restore_resting(orders)

    async def _tick(self, seconds: int):
        """The clock runs `seconds` more ticks; the txs they cause go out, land and are read back."""
        for _ in range(seconds):
            self.now += 1
            self.clock.backtest_til(self.now)
            await self._settle()
            while self.windows:
                self.windows.pop(0).set_result(None)
                await self._settle()
            await self._status_update()
            await self._settle()


class BeezeeRestartAdoptionTests(RestartedPmmTestCase):
    """PMM after a restart with the previous run's orders resting: it manages them as its own."""

    def _restored_pair(self) -> List[InFlightOrder]:
        return [
            self._restored("OID-BID", BID_ID, True, self._quote(MID * Decimal("0.99"))),
            self._restored("OID-ASK", ASK_ID, False, self._quote(MID * Decimal("1.01"))),
        ]

    async def test_restored_orders_are_adopted_and_no_second_set_is_created(self):
        await self._restart_with(self._restored_pair())
        strategy = self._pmm()

        await self._tick(10)

        self.assertEqual([], self.node.broadcast)
        self.assertEqual(["OID-ASK", "OID-BID"], sorted(self.exchange.in_flight_orders))
        self.assertEqual(["OID-ASK", "OID-BID"], self._active_ids(strategy))
        self.assertEqual({BID_ID, ASK_ID}, set(self.node.resting))
        self.assertTrue(self._pmm_logged(logging.INFO, "Adopted restored order OID-BID"))

    async def test_orders_still_waiting_for_their_id_are_adopted_before_the_first_tick(self):
        # Killed right after the create tx: the orders carry the tx hash and the connector has not resumed them yet.
        tx_hash = "AB" * 32
        orders = self._restored_pair()
        for order in orders:
            order.exchange_order_id = tx_hash
            order.current_state = OrderState.PENDING_CREATE
        self._restore(orders, {"last_processed_height": H, "creates": {}, "cancels": {}})
        strategy = self._pmm()

        self.clock.backtest_til(START + 3)
        await self._settle()

        self.assertEqual([], self.windows)
        self.assertEqual([], self.node.simulated)
        self.assertEqual(["OID-ASK", "OID-BID"], sorted(self.exchange.in_flight_orders))
        self.assertEqual(["OID-ASK", "OID-BID"], self._active_ids(strategy))

    async def test_adopted_orders_beyond_the_tolerance_are_replaced_once(self):
        await self._restart_with(self._restored_pair())
        self.price.price = MID * Decimal("1.1")
        strategy = self._pmm()

        await self._tick(10)

        self.assertEqual(2, len(self.node.landed))
        cancel_tx, create_tx = (sent for _, sent in self.node.landed)
        self.assertEqual(["cancel", "cancel"], cancel_tx.kinds)
        self.assertEqual({BID_ID, ASK_ID}, {message.order_id for message in cancel_tx.messages})
        self.assertEqual(["create", "create"], create_tx.kinds)
        self.assertEqual(
            {self._quote(MID * Decimal("1.1") * Decimal("0.99")), self._quote(MID * Decimal("1.1") * Decimal("1.01"))},
            {Decimal(message.price) for message in create_tx.messages},
        )
        self.assertEqual(["OID-ASK", "OID-BID"], sorted(self._event_ids(MarketEvent.OrderCancelled)))
        new_ids = sorted(self.exchange.in_flight_orders)
        self.assertEqual(2, len(new_ids))
        self.assertNotIn("OID-BID", new_ids)
        self.assertEqual(new_ids, self._active_ids(strategy))
        self.assertEqual(2, len(self.node.resting))


class BeezeeRestartUntradedPairTests(RestartedPmmTestCase):
    """The config changed between runs: PMM now trades BZE-USDC only."""

    trading_pairs = ["BZE-USDC"]

    async def test_restored_order_on_a_pair_the_strategy_does_not_trade_is_cancelled(self):
        await self._restart_with([
            self._restored("OID-BID", BID_ID, True, self._quote(MID * Decimal("0.99"))),
            self._restored("OID-ASK", ASK_ID, False, self._quote(MID * Decimal("1.01"))),
            self._restored("OID-TKN", TKN_ID, False, Decimal("2"), trading_pair="TKN-BZE"),
        ], [BZE_USDC, BZE_USDC, TKN_BZE])
        strategy = self._pmm()

        await self._tick(5)

        self.assertEqual(1, len(self.node.landed))
        cancel_tx = self.node.landed[0][1]
        self.assertEqual([("cancel", TKN_BZE, TKN_ID)], [(m.kind, m.market_id, m.order_id) for m in cancel_tx.messages])
        self.assertEqual(["OID-TKN"], self._event_ids(MarketEvent.OrderCancelled))
        self.assertEqual(["OID-ASK", "OID-BID"], sorted(self.exchange.in_flight_orders))
        self.assertEqual(["OID-ASK", "OID-BID"], self._active_ids(strategy))
        self.assertTrue(self._pmm_logged(logging.INFO, "the strategy does not trade TKN-BZE"))
        self.assertFalse(any(record.levelno >= logging.WARNING for record in self.pmm_logs))


class BeezeeRestartFillWhileDownTests(RestartedPmmMixin, BeezeeChainTestCase):
    """An adopted order that was filled while the bot was down."""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self._start_harness()

    async def test_fill_while_down_is_reconciled_and_closes_the_adopted_order(self):
        # Order 6810887 (buy 10 BZE @ 0.0003) is filled in block H + 3, which the bot missed while down.
        self._restore([self._restored("OID-REST", "000000000000000006810887", True, MID)],
                      {"last_processed_height": H + 2, "creates": {}, "cancels": {}})
        self._mock_latest_height(H + 3)
        self._mock_blocks({H + 3: load_fixture("block_results_25278999.json")})
        self._mock_user_orders(BZE_USDC, [])
        # One bid at the mid: the adopted order is exactly what PMM proposes, so it keeps it.
        strategy = self._pmm(order_override={"bid": ["buy", "0", "10"]})
        self.clock.backtest_til(START + 1)
        self.assertEqual(["OID-REST"], self._active_ids(strategy))

        await self._status_update()
        await self._run_until(lambda: "OID-REST" in self._event_ids(MarketEvent.BuyOrderCompleted), "no fill")
        self.clock.backtest_til(START + 2)

        fill = self.events[MarketEvent.OrderFilled].event_log[0]
        self.assertEqual((Decimal("10"), MID), (fill.amount, fill.price))
        self.assertEqual([], self._active_ids(strategy))
        self.assertEqual({}, self.exchange.in_flight_orders)
        self.assertTrue(self._pmm_logged(logging.INFO, "(BZE-USDC) Maker buy order of 10"))
        self.assertFalse(any("Creating" in record.getMessage() for record in self.pmm_logs))
