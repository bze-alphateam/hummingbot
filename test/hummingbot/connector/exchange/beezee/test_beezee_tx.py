import base64
import unittest
from decimal import Decimal

from hummingbot.connector.exchange.beezee.beezee_tx import (
    build_broadcast_request,
    fee_amount_from_gas,
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

