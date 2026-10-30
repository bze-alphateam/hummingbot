import asyncio
import logging
import re
import time
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

from aioresponses import aioresponses

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_api_data_source import BeezeeTxRefusedError
from hummingbot.connector.exchange.beezee.beezee_exchange import BeezeeExchange
from hummingbot.connector.exchange.beezee.beezee_utils import (
    BeezeeConfigMap,
    BeezeeMainnetNetworkMode,
    BeezeeMnemonicWalletAccountMode,
    BeezeePair,
    BeezeeReadOnlyAccountMode,
    market_from_data,
)
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent


async def no_wait(_delay: float):
    await asyncio.sleep(0)


class BeezeeExchangeTests(unittest.IsolatedAsyncioTestCase):
    level = 0

    def handle(self, record):
        self.log_records.append(record)

    def _logged(self, level: int, fragment: str) -> int:
        return len([record for record in self.log_records if record.levelno == level and fragment in record.getMessage()])

    def setUp(self) -> None:
        self.log_records = []
        config = BeezeeConfigMap(
            connector="beezee",
            network=BeezeeMainnetNetworkMode(),
            account_type=BeezeeReadOnlyAccountMode(),
        )
        self.exchange = BeezeeExchange(
            connector_configuration=config,
            trading_pairs=["BZE-USDC"],
            trading_required=True,
        )
        self.market = market_from_data(
            {"base": "ubze", "quote": "uusdc"},
            {
                "ubze": {
                    "symbol": "BZE",
                    "display": "bze",
                    "denom_units": [{"denom": "ubze", "exponent": 0}, {"denom": "bze", "exponent": 6}],
                },
                "uusdc": {
                    "symbol": "USDC",
                    "display": "usdc",
                    "denom_units": [{"denom": "uusdc", "exponent": 0}, {"denom": "usdc", "exponent": 6}],
                },
            },
        )
        # The batch window and the tx lookup delays pass at once.
        self.exchange._sleep = no_wait
        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)

    async def asyncTearDown(self) -> None:
        # Nothing the connector started may outlive the test.
        await self.exchange.stop_network()
        for _ in range(5):
            await asyncio.sleep(0)
        leftover = [task for task in asyncio.all_tasks() if task is not asyncio.current_task() and not task.done()]
        for task in leftover:
            task.cancel()
        self.assertEqual([], [task.get_coro() for task in leftover])

    def tearDown(self) -> None:
        self.exchange.logger().removeHandler(self)

    def _halt_data_source(self, halted_denoms) -> Mock:
        data_source = Mock()
        data_source.get_market_by_trading_pair = AsyncMock(return_value=self.market)
        data_source.get_halted_denoms = AsyncMock(side_effect=halted_denoms)
        data_source.get_user_market_orders = AsyncMock(return_value=[])
        data_source.create_order_message = Mock(
            side_effect=lambda market, order_type, amount, price: f"create:{order_type}:{amount}@{price}".encode()
        )
        data_source.cancel_order_message = Mock(side_effect=lambda market_id, order_type, order_id: f"cancel:{order_id}".encode())
        data_source.simulate_sign_and_broadcast = AsyncMock(return_value={"tx_response": {"code": 0, "txhash": "AB" * 32}})
        self.exchange._data_source = data_source
        self.exchange._signer = Mock()
        return data_source

    async def _status_poll(self, force_halted_refresh: bool = False):
        # Drives the connector's status poll; only upstream's balance and order-status steps are stubbed.
        self.exchange._update_balances = AsyncMock()
        self.exchange._update_order_status = AsyncMock()
        if force_halted_refresh:
            self.exchange._last_halted_denoms_refresh = time.time() - CONSTANTS.HALTED_DENOMS_REFRESH_INTERVAL
        await self.exchange._status_polling_loop_fetch_updates()
        for _ in range(20):
            await asyncio.sleep(0)

    async def _force_halted_denoms_refresh(self):
        self.exchange._last_halted_denoms_refresh = time.time() - CONSTANTS.HALTED_DENOMS_REFRESH_INTERVAL
        await self.exchange._update_halted_denoms_if_due()

    async def _place_buy(self, order_id: str = "OID-H"):
        return await self.exchange._place_order(
            order_id=order_id,
            trading_pair="BZE-USDC",
            amount=Decimal("1"),
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("2.5"),
        )

    async def test_format_trading_rules_derives_chain_based_rules(self):
        data_source = Mock()
        data_source.get_trade_fees = AsyncMock(return_value={"maker": TokenAmount("BZE", Decimal("0.001")), "taker": None})
        data_source.get_all_pairs = AsyncMock(return_value={"BZE-USDC": BeezeePair(self.market.base, self.market.quote, market=self.market)})
        self.exchange._data_source = data_source

        rules = await self.exchange._format_trading_rules({})

        self.assertEqual(1, len(rules))
        rule = rules[0]
        self.assertEqual("BZE-USDC", rule.trading_pair)
        self.assertEqual(Decimal("0.000001"), rule.min_order_size)
        self.assertEqual(Decimal("0.0000000001"), rule.min_price_increment)
        self.assertEqual(Decimal("0.000002"), rule.min_notional_size)

    async def test_minimum_order_amount_is_the_chain_minimum_once_the_market_is_loaded(self):
        data_source = Mock()
        data_source.get_trade_fees = AsyncMock(return_value={"maker": None, "taker": None})
        data_source.get_all_pairs = AsyncMock(return_value={"BZE-USDC": BeezeePair(self.market.base, self.market.quote, market=self.market)})
        data_source.gas_fees_paid = Decimal("0.0042")
        self.exchange._data_source = data_source
        self.assertEqual(Decimal("0"), self.exchange.minimum_order_amount("BZE-USDC", Decimal("0.0003")))

        await self.exchange._format_trading_rules({})

        # ceil(1 / 0.0003) x 2 = 6668 ubze.
        self.assertEqual(Decimal("0.006668"), self.exchange.minimum_order_amount("BZE-USDC", Decimal("0.0003")))
        self.assertEqual(Decimal("0.000008"), self.exchange.minimum_order_amount("BZE-USDC", Decimal("0.25")))
        self.assertEqual(Decimal("0.0042"), self.exchange.gas_fees_paid())

    async def test_update_balances_combines_bank_and_dust(self):
        data_source = Mock()
        data_source._metadata_by_denom = {}
        data_source.get_balances = AsyncMock(
            return_value=[
                {"denom": "ubze", "amount": "1000000"},
                {"denom": "uusdc", "amount": "5000000"},
            ]
        )
        data_source.get_user_dust = AsyncMock(return_value=[{"denom": "ubze", "amount": "500000"}])
        data_source.get_denom_metadata = AsyncMock(
            side_effect=[
                {
                    "symbol": "BZE",
                    "display": "bze",
                    "denom_units": [{"denom": "ubze", "exponent": 0}, {"denom": "bze", "exponent": 6}],
                },
                {
                    "symbol": "USDC",
                    "display": "usdc",
                    "denom_units": [{"denom": "uusdc", "exponent": 0}, {"denom": "usdc", "exponent": 6}],
                },
                {
                    "symbol": "BZE",
                    "display": "bze",
                    "denom_units": [{"denom": "ubze", "exponent": 0}, {"denom": "bze", "exponent": 6}],
                },
            ]
        )
        self.exchange._data_source = data_source

        await self.exchange._update_balances()

        self.assertEqual(Decimal("1.5"), self.exchange._account_balances["BZE"])
        self.assertEqual(Decimal("1"), self.exchange._account_available_balances["BZE"])
        self.assertEqual(Decimal("5"), self.exchange._account_balances["USDC"])

    async def test_request_order_status_returns_open_when_exchange_order_is_visible(self):
        order = InFlightOrder(
            client_order_id="OID1",
            trading_pair="BZE-USDC",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            creation_timestamp=1.0,
            price=Decimal("2.5"),
            exchange_order_id="000000000000000000000001",
            initial_state=OrderState.OPEN,
        )
        data_source = Mock()
        data_source.get_market_by_trading_pair = AsyncMock(return_value=self.market)
        data_source.get_user_market_orders = AsyncMock(
            return_value=[{"id": "000000000000000000000001", "market_id": self.market.market_id, "order_type": "buy"}]
        )
        self.exchange._data_source = data_source

        update = await self.exchange._request_order_status(order)

        self.assertEqual(OrderState.OPEN, update.new_state)
        self.assertEqual(order.exchange_order_id, update.exchange_order_id)

    async def test_place_cancel_defers_when_exchange_order_id_is_unresolved(self):
        order = InFlightOrder(
            client_order_id="OID7",
            trading_pair="BZE-USDC",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            creation_timestamp=1.0,
            price=Decimal("2.5"),
        )
        self.exchange._signer = Mock()
        data_source = Mock()
        data_source.get_market_by_trading_pair = AsyncMock(return_value=self.market)
        data_source.simulate_sign_and_broadcast = AsyncMock()
        self.exchange._data_source = data_source

        canceled = await self.exchange._place_cancel(order.client_order_id, order)

        self.assertFalse(canceled)
        data_source.simulate_sign_and_broadcast.assert_not_awaited()

    @aioresponses()
    async def test_get_fee_uses_v820_tradebin_coin_fees(self, mock_api):
        rest_url = self.exchange._rest_endpoint
        mock_api.get(
            f"{rest_url}{CONSTANTS.TRADEBIN_PARAMS_PATH_URL}",
            payload={"params": {
                "marketMakerFee": {"denom": "ubze", "amount": "1000"},
                "marketTakerFee": {"denom": "ubze", "amount": "100000"},
            }},
        )
        mock_api.get(
            f"{rest_url}{CONSTANTS.DENOM_METADATA_PATH_URL}/ubze",
            payload={"metadata": {
                "symbol": "BZE",
                "display": "bze",
                "denom_units": [{"denom": "ubze", "exponent": 0}, {"denom": "bze", "exponent": 6}],
            }},
        )

        await self.exchange._update_trading_fees()
        fee_args = dict(base_currency="BZE", quote_currency="USDC", order_type=OrderType.LIMIT, order_side=TradeType.BUY, amount=Decimal("1"), price=Decimal("2.5"))
        taker_fee = self.exchange._get_fee(is_maker=False, **fee_args)
        maker_fee = self.exchange._get_fee(is_maker=True, **fee_args)

        self.assertEqual([TokenAmount("BZE", Decimal("0.1"))], taker_fee.flat_fees)
        self.assertEqual([TokenAmount("BZE", Decimal("0.001"))], maker_fee.flat_fees)

    async def test_place_order_refuses_halted_pair_before_signing(self):
        data_source = self._halt_data_source([{"uusdc"}])

        with self.assertRaises(ValueError) as context:
            await self._place_buy()

        self.assertEqual("Beezee market ubze/uusdc is halted (denom uusdc)", str(context.exception))
        data_source.simulate_sign_and_broadcast.assert_not_awaited()
        data_source.get_user_market_orders.assert_not_awaited()

    async def _drain_tasks(self):
        for _ in range(20):
            await asyncio.sleep(0)

    def _track_order(self, order_id: str, exchange_order_id=None):
        self.exchange.start_tracking_order(
            order_id=order_id,
            exchange_order_id=exchange_order_id,
            trading_pair="BZE-USDC",
            trade_type=TradeType.SELL,
            price=Decimal("2.5"),
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
        )

    async def test_halt_transition_cancels_open_orders_once(self):
        data_source = self._halt_data_source([{"ubze"}, {"ubze"}])
        data_source.simulate_sign_and_broadcast = AsyncMock(return_value={"tx_response": {"code": 0, "txhash": "CA" * 32}})
        self._track_order("OID-OPEN", "000000000000000000000042")

        await self._force_halted_denoms_refresh()
        await self._drain_tasks()
        await self._force_halted_denoms_refresh()
        await self._drain_tasks()

        data_source.simulate_sign_and_broadcast.assert_awaited_once()
        self.assertEqual([b"cancel:000000000000000000000042"], data_source.simulate_sign_and_broadcast.call_args.args[0])
        self.assertTrue(self.exchange.in_flight_orders["OID-OPEN"].is_pending_cancel_confirmation)
        self.assertEqual(1, self._logged(logging.WARNING, "Beezee market BZE-USDC is halted (denom ubze)"))

    async def test_halt_cancel_is_retried_once_the_exchange_order_id_resolves(self):
        data_source = self._halt_data_source([{"ubze"}, {"ubze"}])
        data_source.simulate_sign_and_broadcast = AsyncMock(return_value={"tx_response": {"code": 0, "txhash": "CA" * 32}})
        self._track_order("OID-QUEUED")

        await self._force_halted_denoms_refresh()
        await self._drain_tasks()
        data_source.simulate_sign_and_broadcast.assert_not_awaited()

        # The create queued before the halt executed after all: its order id is now known.
        self.exchange.in_flight_orders["OID-QUEUED"].update_exchange_order_id("000000000000000000000043")
        await self._force_halted_denoms_refresh()
        await self._drain_tasks()

        data_source.simulate_sign_and_broadcast.assert_awaited_once()
        self.assertEqual([b"cancel:000000000000000000000043"], data_source.simulate_sign_and_broadcast.call_args.args[0])

    async def test_failed_halt_cancel_is_retried_on_the_next_poll(self):
        data_source = self._halt_data_source([{"ubze"}])
        data_source.simulate_sign_and_broadcast = AsyncMock(side_effect=[
            IOError("Beezee tx broadcast failed: timed out"),
            {"tx_response": {"code": 0, "txhash": "CA" * 32}},
        ])
        self._track_order("OID-OPEN", "000000000000000000000044")

        await self._status_poll()
        self.assertEqual(1, data_source.simulate_sign_and_broadcast.await_count)
        # The halted list is not due again: only the status poll's re-cancel runs.
        await self._status_poll()

        self.assertEqual(1, data_source.get_halted_denoms.await_count)
        self.assertEqual(2, data_source.simulate_sign_and_broadcast.await_count)
        self.assertTrue(self.exchange.in_flight_orders["OID-OPEN"].is_pending_cancel_confirmation)

    async def test_unhalt_stops_cancelling_and_logs_once(self):
        data_source = self._halt_data_source([{"ubze"}, set(), set(), set()])
        data_source.simulate_sign_and_broadcast = AsyncMock(side_effect=IOError("Beezee tx broadcast failed: timed out"))
        self._track_order("OID-OPEN", "000000000000000000000045")

        await self._status_poll()
        self.assertEqual(1, data_source.simulate_sign_and_broadcast.await_count)
        for _ in range(3):
            await self._status_poll(force_halted_refresh=True)

        self.assertEqual(1, data_source.simulate_sign_and_broadcast.await_count)
        self.assertEqual(1, self._logged(logging.INFO, "Beezee market BZE-USDC is no longer halted"))
        self.assertFalse(self.exchange.in_flight_orders["OID-OPEN"].is_done)

    async def test_buy_on_halted_pair_fails_the_order_without_signing(self):
        data_source = self._halt_data_source([{"uusdc"}])
        self.exchange._set_current_timestamp(1640000000)
        self.exchange._trading_rules["BZE-USDC"] = TradingRule(
            trading_pair="BZE-USDC",
            min_order_size=Decimal("0.000001"),
            min_price_increment=Decimal("0.0000000001"),
            min_base_amount_increment=Decimal("0.000001"),
            min_notional_size=Decimal("0.000002"),
        )
        failure_logger = EventLogger()
        self.exchange.add_listener(MarketEvent.OrderFailure, failure_logger)

        order_id = self.exchange.buy("BZE-USDC", Decimal("1"), OrderType.LIMIT, Decimal("2.5"))
        for _ in range(20):
            await asyncio.sleep(0)

        self.assertEqual(OrderState.FAILED, self.exchange._order_tracker.all_orders[order_id].current_state)
        self.assertEqual(
            [(order_id, "Beezee market ubze/uusdc is halted (denom uusdc)", "ValueError")],
            [(event.order_id, event.error_message, event.error_type) for event in failure_logger.event_log],
        )
        data_source.simulate_sign_and_broadcast.assert_not_awaited()

    async def test_halted_denoms_refresh_waits_for_the_interval(self):
        data_source = self._halt_data_source([set(), set(), set()])

        await self.exchange._update_halted_denoms_if_due()
        await self.exchange._update_halted_denoms_if_due()
        self.assertEqual(1, data_source.get_halted_denoms.await_count)

        self.exchange._last_halted_denoms_refresh -= CONSTANTS.HALTED_DENOMS_REFRESH_INTERVAL
        await self.exchange._update_halted_denoms_if_due()
        self.assertEqual(2, data_source.get_halted_denoms.await_count)

    async def test_denom_halted_refusal_marks_pair_halted(self):
        # The node's halted list lags behind the refusal, so the market itself is marked halted.
        data_source = self._halt_data_source([set(), set()])
        data_source.simulate_sign_and_broadcast = AsyncMock(side_effect=BeezeeTxRefusedError(
            "simulation", "failed to execute message; message index: 0: market ubze/uusdc is halted: denom is halted",
        ))

        with self.assertRaises(IOError):
            await self._place_buy("OID-1")
        with self.assertRaises(ValueError) as context:
            await self._place_buy("OID-2")

        self.assertIn("Beezee market ubze/uusdc is halted", str(context.exception))
        self.assertEqual(1, data_source.simulate_sign_and_broadcast.await_count)
        self.assertEqual(1, self._logged(logging.WARNING, "Beezee market BZE-USDC is halted"))

    async def test_delivered_create_tx_with_code_4017_marks_pair_halted(self):
        # The node does not list the halted denom yet: the delivered 4017 is the only halt signal.
        data_source = self._halt_data_source([set(), set()])
        data_source.get_tx = AsyncMock(return_value={"tx_response": {
            "code": 4017, "codespace": "tradebin", "raw_log": "denom is halted", "height": "100",
        }})

        self.exchange.start_tracking_order(
            order_id="OID-D",
            exchange_order_id=None,
            trading_pair="BZE-USDC",
            trade_type=TradeType.BUY,
            price=Decimal("2.5"),
            amount=Decimal("1"),
            order_type=OrderType.LIMIT,
        )

        await self._place_buy("OID-D")
        await self._drain_tasks()

        self.assertEqual(OrderState.FAILED, self.exchange._order_tracker.fetch_order("OID-D").current_state)
        with self.assertRaises(ValueError) as context:
            await self._place_buy()
        self.assertEqual("Beezee market ubze/uusdc is halted (denom unknown, refused by the chain)", str(context.exception))
        data_source.simulate_sign_and_broadcast.assert_awaited_once()

    async def test_unhalt_allows_trading_again(self):
        data_source = self._halt_data_source([{"ubze"}, set()])

        await self._force_halted_denoms_refresh()
        with self.assertRaises(ValueError):
            await self._place_buy("OID-1")
        await self._force_halted_denoms_refresh()
        tx_hash, _ = await self._place_buy("OID-2")

        self.assertEqual("AB" * 32, tx_hash)
        data_source.simulate_sign_and_broadcast.assert_awaited_once()
        self.assertEqual(1, self._logged(logging.INFO, "Beezee market BZE-USDC is no longer halted"))

    @aioresponses()
    async def test_mnemonic_wallet_mode_trades_as_the_bzed_address(self, mock_api):
        # bzed v8.2.0 recovers this address from the public BIP39 test mnemonic on m/44'/118'/0'/0/0.
        expected_address = "bze19rl4cm2hmr8afy4kldpxz3fka4jguq0apypkdz"
        config = BeezeeConfigMap(
            connector="beezee",
            network=BeezeeMainnetNetworkMode(),
            account_type=BeezeeMnemonicWalletAccountMode(
                mnemonic="abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
            ),
        )
        exchange = BeezeeExchange(connector_configuration=config, trading_pairs=["BZE-USDC"], trading_required=True)
        rest_url = exchange._rest_endpoint
        mock_api.get(f"{rest_url}{CONSTANTS.BALANCES_PATH_URL}/{expected_address}", payload={"balances": [{"denom": "ubze", "amount": "1000000"}]})
        mock_api.get(re.compile(rf"^{re.escape(rest_url + CONSTANTS.ALL_USER_DUST_PATH_URL)}\?address={expected_address}$"), payload={"list": []})
        mock_api.get(f"{rest_url}{CONSTANTS.DENOM_METADATA_PATH_URL}/ubze", payload={"metadata": {
            "symbol": "BZE",
            "display": "bze",
            "denom_units": [{"denom": "ubze", "exponent": 0}, {"denom": "bze", "exponent": 6}],
        }})

        await exchange._update_balances()

        self.assertEqual(Decimal("1"), exchange.get_balance("BZE"))
