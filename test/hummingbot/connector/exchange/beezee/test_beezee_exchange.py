import asyncio
import logging
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
    BeezeeReadOnlyAccountMode,
    market_from_data,
    private_key_from_account_mode,
)
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import TokenAmount


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
        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)

    def tearDown(self) -> None:
        self.exchange.logger().removeHandler(self)

    def _halt_data_source(self, halted_denoms) -> Mock:
        data_source = Mock()
        data_source.get_market_by_trading_pair = AsyncMock(return_value=self.market)
        data_source.get_halted_denoms = AsyncMock(side_effect=halted_denoms)
        data_source.get_user_market_orders = AsyncMock(return_value=[])
        data_source.broadcast_create_order = AsyncMock(return_value={"tx_response": {"code": 0, "txhash": "AB" * 32}})
        self.exchange._data_source = data_source
        self.exchange._signer = Mock()
        return data_source

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
        data_source.get_all_markets = AsyncMock(return_value={self.market.market_id: self.market})
        self.exchange._data_source = data_source

        rules = await self.exchange._format_trading_rules({})

        self.assertEqual(1, len(rules))
        rule = rules[0]
        self.assertEqual("BZE-USDC", rule.trading_pair)
        self.assertEqual(Decimal("0.000001"), rule.min_order_size)
        self.assertEqual(Decimal("0.0000000001"), rule.min_price_increment)
        self.assertEqual(Decimal("0.000002"), rule.min_notional_size)

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
        data_source.broadcast_cancel_order = AsyncMock()
        self.exchange._data_source = data_source

        canceled = await self.exchange._place_cancel(order.client_order_id, order)

        self.assertFalse(canceled)
        data_source.broadcast_cancel_order.assert_not_awaited()

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
        data_source.broadcast_create_order.assert_not_awaited()
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
        data_source.broadcast_cancel_order = AsyncMock(return_value={"tx_response": {"code": 0, "txhash": "CA" * 32}})
        self._track_order("OID-OPEN", "000000000000000000000042")

        await self._force_halted_denoms_refresh()
        await self._drain_tasks()
        await self._force_halted_denoms_refresh()
        await self._drain_tasks()

        data_source.broadcast_cancel_order.assert_awaited_once()
        self.assertEqual("000000000000000000000042", data_source.broadcast_cancel_order.call_args.kwargs["order_id"])
        self.assertTrue(self.exchange.in_flight_orders["OID-OPEN"].is_pending_cancel_confirmation)
        self.assertEqual(1, self._logged(logging.WARNING, "Beezee market BZE-USDC is halted (denom ubze)"))

    async def test_halt_cancel_is_retried_once_the_exchange_order_id_resolves(self):
        data_source = self._halt_data_source([{"ubze"}, {"ubze"}])
        data_source.broadcast_cancel_order = AsyncMock(return_value={"tx_response": {"code": 0, "txhash": "CA" * 32}})
        self._track_order("OID-QUEUED")

        await self._force_halted_denoms_refresh()
        await self._drain_tasks()
        data_source.broadcast_cancel_order.assert_not_awaited()

        # The create queued before the halt executed after all: its order id is now known.
        self.exchange.in_flight_orders["OID-QUEUED"].update_exchange_order_id("000000000000000000000043")
        await self._force_halted_denoms_refresh()
        await self._drain_tasks()

        data_source.broadcast_cancel_order.assert_awaited_once()
        self.assertEqual("000000000000000000000043", data_source.broadcast_cancel_order.call_args.kwargs["order_id"])

    async def test_failed_halt_cancel_is_retried_on_the_next_poll(self):
        data_source = self._halt_data_source([{"ubze"}, {"ubze"}])
        data_source.broadcast_cancel_order = AsyncMock(side_effect=[
            IOError("Beezee tx broadcast failed: timed out"),
            {"tx_response": {"code": 0, "txhash": "CA" * 32}},
        ])
        self._track_order("OID-OPEN", "000000000000000000000044")

        await self._force_halted_denoms_refresh()
        await self._drain_tasks()
        await self._force_halted_denoms_refresh()
        await self._drain_tasks()

        self.assertEqual(2, data_source.broadcast_cancel_order.await_count)
        self.assertTrue(self.exchange.in_flight_orders["OID-OPEN"].is_pending_cancel_confirmation)

    async def test_halted_denoms_refresh_waits_for_the_interval(self):
        data_source = self._halt_data_source([set(), set()])

        await self.exchange._update_halted_denoms_if_due()
        await self.exchange._update_halted_denoms_if_due()

        self.assertEqual(1, data_source.get_halted_denoms.await_count)

    async def test_denom_halted_refusal_marks_pair_halted(self):
        # The node's halted list lags behind the refusal, so the market itself is marked halted.
        data_source = self._halt_data_source([set(), set()])
        data_source.broadcast_create_order = AsyncMock(side_effect=BeezeeTxRefusedError(
            "simulation", "failed to execute message; message index: 0: market ubze/uusdc is halted: denom is halted",
        ))

        with self.assertRaises(IOError):
            await self._place_buy("OID-1")
        with self.assertRaises(ValueError) as context:
            await self._place_buy("OID-2")

        self.assertIn("Beezee market ubze/uusdc is halted", str(context.exception))
        self.assertEqual(1, data_source.broadcast_create_order.await_count)
        self.assertEqual(1, self._logged(logging.WARNING, "Beezee market BZE-USDC is halted"))

    async def test_unhalt_allows_trading_again(self):
        data_source = self._halt_data_source([{"ubze"}, set()])

        await self._force_halted_denoms_refresh()
        with self.assertRaises(ValueError):
            await self._place_buy("OID-1")
        await self._force_halted_denoms_refresh()
        tx_hash, _ = await self._place_buy("OID-2")

        self.assertEqual("AB" * 32, tx_hash)
        data_source.broadcast_create_order.assert_awaited_once()
        self.assertEqual(1, self._logged(logging.INFO, "Beezee market BZE-USDC is no longer halted"))

    async def test_exchange_initializes_signer_from_mnemonic_wallet_mode(self):
        mnemonic_account = BeezeeMnemonicWalletAccountMode(
            mnemonic="abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
        )
        config = BeezeeConfigMap(
            connector="beezee",
            network=BeezeeMainnetNetworkMode(),
            account_type=mnemonic_account,
        )

        exchange = BeezeeExchange(
            connector_configuration=config,
            trading_pairs=["BZE-USDC"],
            trading_required=True,
        )

        self.assertIsNotNone(exchange._signer)
        self.assertEqual(
            exchange._signer.address,
            exchange._account_address,
        )
        self.assertEqual(
            private_key_from_account_mode(mnemonic_account),
            exchange._signer._private_key_bytes.hex(),
        )
