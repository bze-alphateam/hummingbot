import base64
import unittest
from decimal import Decimal

from hummingbot.connector.exchange.beezee.beezee_tx import (
    build_broadcast_request,
    build_simulate_request,
    build_simulation_transaction,
    fee_amount_from_gas,
    gas_limit_from_simulation,
    msg_cancel_order,
    msg_create_order,
)


class BeezeeTxTests(unittest.TestCase):
    def test_msg_create_order_contains_expected_type_url(self):
        payload = msg_create_order(
            creator="bze1test",
            order_type="buy",
            amount="1000000",
            price="2.5",
            market_id="ubze/uusdc",
        )

        self.assertIn(b"/bze.tradebin.MsgCreateOrder", payload)
        self.assertIn(b"bze1test", payload)
        self.assertIn(b"ubze/uusdc", payload)

    def test_msg_cancel_order_contains_expected_type_url(self):
        payload = msg_cancel_order(
            creator="bze1test",
            market_id="ubze/uusdc",
            order_id="000000000000000000000123",
            order_type="sell",
        )

        self.assertIn(b"/bze.tradebin.MsgCancelOrder", payload)
        self.assertIn(b"000000000000000000000123", payload)

    def test_build_broadcast_request_base64_encodes_payload(self):
        request = build_broadcast_request(b"abc")

        self.assertEqual("BROADCAST_MODE_SYNC", request["mode"])
        self.assertEqual(base64.b64encode(b"abc").decode(), request["tx_bytes"])

    def test_fee_amount_from_gas_rounds_up(self):
        fee_amount = fee_amount_from_gas(gas_limit=250000, gas_price=Decimal("0.025"))

        self.assertEqual("6250", fee_amount)

    def test_gas_limit_from_simulation_rounds_up(self):
        self.assertEqual(130002, gas_limit_from_simulation(gas_used=100001, gas_adjustment=Decimal("1.3")))
        self.assertEqual(130000, gas_limit_from_simulation(gas_used=100000, gas_adjustment=Decimal("1.3")))

    def test_simulation_transaction_has_empty_fee_and_signature(self):
        tx_bytes = build_simulation_transaction(b"\x02" * 33, sequence=5, messages=[b"msg"])

        # TxRaw ends with field 3 (signatures) holding one empty signature.
        self.assertTrue(tx_bytes.endswith(b"\x1a\x00"))
        # AuthInfo field 2 (fee) is present and empty.
        self.assertIn(b"\x12\x00", tx_bytes)
        self.assertEqual({"tx_bytes": base64.b64encode(tx_bytes).decode()}, build_simulate_request(tx_bytes))
