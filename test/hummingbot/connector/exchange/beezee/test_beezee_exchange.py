import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

from hummingbot.connector.exchange.beezee.beezee_exchange import BeezeeExchange
from hummingbot.connector.exchange.beezee.beezee_utils import (
    BeezeeConfigMap,
    BeezeeMainnetNetworkMode,
    BeezeeMnemonicWalletAccountMode,
    BeezeeReadOnlyAccountMode,
    private_key_from_account_mode,
    market_from_data,
)
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState


class BeezeeExchangeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
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

    async def test_format_trading_rules_derives_chain_based_rules(self):
        data_source = Mock()
        data_source.get_tradebin_params = AsyncMock(return_value={"market_maker_fee": "1000ubze"})
        data_source.get_all_markets = AsyncMock(return_value={self.market.market_id: self.market})
        self.exchange._data_source = data_source

        rules = await self.exchange._format_trading_rules({})

        self.assertEqual(1, len(rules))
        rule = rules[0]
        self.assertEqual("BZE-USDC", rule.trading_pair)
        self.assertEqual(Decimal("0.000001"), rule.min_order_size)
        self.assertEqual(Decimal("0.000000001"), rule.min_price_increment)
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

    async def test_request_order_status_claims_new_exchange_order_id_from_open_orders(self):
        order = InFlightOrder(
            client_order_id="OID2",
            trading_pair="BZE-USDC",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            creation_timestamp=1.0,
            price=Decimal("2.5"),
        )
        self.exchange._pre_create_order_ids[order.client_order_id] = set()
        self.exchange._create_order_specs[order.client_order_id] = (self.market.market_id, "buy", "1000000", "2.5")
        data_source = Mock()
        data_source.get_market_by_trading_pair = AsyncMock(return_value=self.market)
        data_source.get_user_market_orders = AsyncMock(
            return_value=[{"id": "000000000000000000000009", "market_id": self.market.market_id, "order_type": "buy"}]
        )
        data_source.candidate_order_ids = Mock(return_value=["000000000000000000000009"])
        data_source.get_market_order = AsyncMock(return_value={"price": "2.5", "amount": "1000000.0"})
        self.exchange._data_source = data_source

        update = await self.exchange._request_order_status(order)

        self.assertEqual(OrderState.OPEN, update.new_state)
        self.assertEqual("000000000000000000000009", update.exchange_order_id)

    async def test_request_order_status_marks_failed_when_create_tx_fails(self):
        order = InFlightOrder(
            client_order_id="OID3",
            trading_pair="BZE-USDC",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            creation_timestamp=1.0,
            price=Decimal("2.5"),
        )
        self.exchange._creation_tx_hashes[order.client_order_id] = "A" * 64
        data_source = Mock()
        data_source.get_market_by_trading_pair = AsyncMock(return_value=self.market)
        data_source.get_user_market_orders = AsyncMock(return_value=[])
        data_source.candidate_order_ids = Mock(return_value=[])
        data_source.get_tx = AsyncMock(return_value={"tx_response": {"code": 11}})
        self.exchange._data_source = data_source

        update = await self.exchange._request_order_status(order)

        self.assertEqual(OrderState.FAILED, update.new_state)

    async def test_request_order_status_preserves_open_state_after_confirmed_create_tx(self):
        order = InFlightOrder(
            client_order_id="OID3A",
            trading_pair="BZE-USDC",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            creation_timestamp=1.0,
            price=Decimal("2.5"),
            exchange_order_id="A" * 64,
            initial_state=OrderState.OPEN,
        )
        data_source = Mock()
        data_source.get_market_by_trading_pair = AsyncMock(return_value=self.market)
        data_source.get_user_market_orders = AsyncMock(return_value=[])
        data_source.get_tx = AsyncMock(return_value={"tx_response": {"code": 0}})
        self.exchange._data_source = data_source

        update = await self.exchange._request_order_status(order)

        self.assertEqual(OrderState.OPEN, update.new_state)

    async def test_request_order_status_marks_canceled_after_cancel_tx(self):
        order = InFlightOrder(
            client_order_id="OID4",
            trading_pair="BZE-USDC",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            creation_timestamp=1.0,
            price=Decimal("2.5"),
            exchange_order_id="000000000000000000000004",
            initial_state=OrderState.PENDING_CANCEL,
        )
        self.exchange._cancel_tx_hashes[order.client_order_id] = "B" * 64
        data_source = Mock()
        data_source.get_market_by_trading_pair = AsyncMock(return_value=self.market)
        data_source.get_user_market_orders = AsyncMock(return_value=[])
        data_source.candidate_order_ids = Mock(return_value=[])
        data_source.get_tx = AsyncMock(return_value={"tx_response": {"code": 0}})
        self.exchange._data_source = data_source

        update = await self.exchange._request_order_status(order)

        self.assertEqual(OrderState.CANCELED, update.new_state)

    async def test_request_order_status_keeps_cancel_pending_until_tx_is_confirmed(self):
        order = InFlightOrder(
            client_order_id="OID5",
            trading_pair="BZE-USDC",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            creation_timestamp=1.0,
            price=Decimal("2.5"),
            exchange_order_id="000000000000000000000005",
            initial_state=OrderState.PENDING_CANCEL,
        )
        self.exchange._cancel_tx_hashes[order.client_order_id] = "C" * 64
        data_source = Mock()
        data_source.get_market_by_trading_pair = AsyncMock(return_value=self.market)
        data_source.get_user_market_orders = AsyncMock(return_value=[])
        data_source.candidate_order_ids = Mock(return_value=[])
        data_source.get_tx = AsyncMock(return_value=None)
        self.exchange._data_source = data_source

        update = await self.exchange._request_order_status(order)

        self.assertEqual(OrderState.PENDING_CANCEL, update.new_state)

    async def test_request_order_status_does_not_claim_ambiguous_order_candidates(self):
        order = InFlightOrder(
            client_order_id="OID6",
            trading_pair="BZE-USDC",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            creation_timestamp=1.0,
            price=Decimal("2.5"),
        )
        self.exchange._creation_tx_hashes[order.client_order_id] = "D" * 64
        self.exchange._pre_create_order_ids[order.client_order_id] = set()
        self.exchange._create_order_specs[order.client_order_id] = (self.market.market_id, "buy", "1000000", "2.5")
        data_source = Mock()
        data_source.get_market_by_trading_pair = AsyncMock(return_value=self.market)
        data_source.get_user_market_orders = AsyncMock(
            return_value=[
                {"id": "000000000000000000000010", "market_id": self.market.market_id, "order_type": "buy"},
                {"id": "000000000000000000000011", "market_id": self.market.market_id, "order_type": "buy"},
            ]
        )
        data_source.candidate_order_ids = Mock(return_value=["000000000000000000000010", "000000000000000000000011"])
        data_source.get_market_order = AsyncMock(return_value={"price": "2.5", "amount": "1000000"})
        data_source.get_tx = AsyncMock(return_value=None)
        self.exchange._data_source = data_source

        update = await self.exchange._request_order_status(order)

        self.assertEqual(OrderState.PENDING_CREATE, update.new_state)
        self.assertEqual("D" * 64, update.exchange_order_id)

    async def test_request_order_status_recovers_unique_order_id_after_restart(self):
        order = InFlightOrder(
            client_order_id="OID6A",
            trading_pair="BZE-USDC",
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1"),
            creation_timestamp=1.0,
            price=Decimal("2.5"),
            exchange_order_id="E" * 64,
        )
        data_source = Mock()
        data_source.get_market_by_trading_pair = AsyncMock(return_value=self.market)
        data_source.get_user_market_orders = AsyncMock(
            return_value=[{"id": "000000000000000000000012", "market_id": self.market.market_id, "order_type": "buy"}]
        )
        data_source.candidate_order_ids = Mock(return_value=["000000000000000000000012"])
        data_source.get_market_order = AsyncMock(
            return_value={"price": "2.5", "amount": "1000000", "created_at": "1"}
        )
        data_source.get_tx = AsyncMock(return_value={"tx_response": {"code": 0}})
        self.exchange._data_source = data_source

        update = await self.exchange._request_order_status(order)

        self.assertEqual(OrderState.OPEN, update.new_state)
        self.assertEqual("000000000000000000000012", update.exchange_order_id)

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
        self.exchange._request_order_status = AsyncMock(return_value=Mock(exchange_order_id=None))

        canceled = await self.exchange._place_cancel(order.client_order_id, order)

        self.assertFalse(canceled)
        data_source.broadcast_cancel_order.assert_not_awaited()

    async def test_get_fee_uses_flat_native_fee_from_params(self):
        self.exchange._tradebin_params = {"market_taker_fee": "2500ubze"}

        fee = self.exchange._get_fee(
            base_currency="BZE",
            quote_currency="USDC",
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            amount=Decimal("1"),
            price=Decimal("2.5"),
            is_maker=False,
        )

        self.assertEqual("UBZE", fee.flat_fees[0].token)
        self.assertEqual(Decimal("0.0025"), fee.flat_fees[0].amount)

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
