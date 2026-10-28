import base64
import json
import unittest
from decimal import Decimal
from pathlib import Path

from hummingbot.connector.exchange.beezee.beezee_signer import BeezeeSigner
from hummingbot.connector.exchange.beezee.beezee_tx import (
    build_broadcast_request,
    build_signed_transaction,
    build_simulate_request,
    build_simulation_transaction,
    fee_amount_from_gas,
    gas_limit_from_simulation,
    msg_cancel_order,
    msg_create_order,
)

FIXTURES = Path(__file__).parent / "fixtures"
# Private key bzed v8.2.0 derives for the public BIP39 test mnemonic ("abandon" x11 + "about") on m/44'/118'/0'/0/0.
TEST_MNEMONIC_PRIVATE_KEY = "c4a48e2fce1481cd3294b4490f6678090ea98d3d0e5cd984558ab0968741b104"  # noqa: mock
# Golden vectors. Independently confirmed: `bzed tx decode` (v8.2.0) gives the JSON fixtures next to this file, and the
# signature verifies over a SignDoc built by cosmos-sdk v0.50.15 itself (chain beezee-1, account 7).
GOLDEN_SIGNED_TX = (
    "CvQBCmsKHC9iemUudHJhZGViaW4uTXNnQ3JlYXRlT3JkZXISSwoqYnplMTlybDRjbTJobXI4YWZ5NGtsZHB4ejNma2E0amd1cTBh"
    "cHlwa2R6EgNidXkaBzIwMDAwMDAiAzAuNSoKdWJ6ZS91dXNkYwp4ChwvYnplLnRyYWRlYmluLk1zZ0NhbmNlbE9yZGVyElgKKmJ6"
    "ZTE5cmw0Y20yaG1yOGFmeTRrbGRweHozZmthNGpndXEwYXB5cGtkehIKdWJ6ZS91dXNkYxoYMDAwMDAwMDAwMDAwMDAwMDAwMDAw"
    "MDQyIgRzZWxsEgtoYm90OmdvbGRlbhJmClAKRgofL2Nvc21vcy5jcnlwdG8uc2VjcDI1NmsxLlB1YktleRIjCiECT04q2Zw01gub"
    "pig8lDGoQYr4ZzISlh+Xp3tjd/zQW2ISBAoCCAEYAxISCgwKBHViemUSBDEzMDEQ0vcHGkBOJy/I6gvuV/Zl8xj3GHVOMK4osY9+"
    "rqiy6Sc1VdfvVjyKEJCCDm0QiRm+HrNWC/iZW2Q7rvLQRihibPuTIHK/"
)
GOLDEN_SIMULATION_TX = (
    "CvQBCmsKHC9iemUudHJhZGViaW4uTXNnQ3JlYXRlT3JkZXISSwoqYnplMTlybDRjbTJobXI4YWZ5NGtsZHB4ejNma2E0amd1cTBh"
    "cHlwa2R6EgNidXkaBzIwMDAwMDAiAzAuNSoKdWJ6ZS91dXNkYwp4ChwvYnplLnRyYWRlYmluLk1zZ0NhbmNlbE9yZGVyElgKKmJ6"
    "ZTE5cmw0Y20yaG1yOGFmeTRrbGRweHozZmthNGpndXEwYXB5cGtkehIKdWJ6ZS91dXNkYxoYMDAwMDAwMDAwMDAwMDAwMDAwMDAw"
    "MDQyIgRzZWxsEgtoYm90OmdvbGRlbhJUClAKRgofL2Nvc21vcy5jcnlwdG8uc2VjcDI1NmsxLlB1YktleRIjCiECT04q2Zw01gub"
    "pig8lDGoQYr4ZzISlh+Xp3tjd/zQW2ISBAoCCAEYAxIAGgA="
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

    def test_build_simulate_request_base64_encodes_payload(self):
        self.assertEqual({"tx_bytes": base64.b64encode(b"abc").decode()}, build_simulate_request(b"abc"))


class BeezeeTxGoldenVectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.signer = BeezeeSigner(TEST_MNEMONIC_PRIVATE_KEY, "bze")
        self.messages = [
            msg_create_order(self.signer.address, "buy", "2000000", "0.5", "ubze/uusdc"),
            msg_cancel_order(self.signer.address, "ubze/uusdc", "000000000000000000000042", "sell"),
        ]

    def test_signed_transaction_matches_golden_vector(self):
        tx_bytes = build_signed_transaction(
            signer=self.signer,
            chain_id="beezee-1",
            account_number=7,
            sequence=3,
            messages=self.messages,
            fee_denom="ubze",
            fee_amount="1301",
            gas_limit=130002,
            memo="hbot:golden",
        )

        self.assertEqual(GOLDEN_SIGNED_TX, base64.b64encode(tx_bytes).decode())

    def test_simulation_transaction_matches_golden_vector(self):
        tx_bytes = build_simulation_transaction(self.signer.public_key_bytes, sequence=3, messages=self.messages, memo="hbot:golden")

        self.assertEqual(GOLDEN_SIMULATION_TX, base64.b64encode(tx_bytes).decode())

    def test_golden_vectors_decode_as_expected_with_bzed(self):
        signed = json.loads((FIXTURES / "golden_signed_tx.bzed_decode.json").read_text())
        simulation = json.loads((FIXTURES / "golden_simulation_tx.bzed_decode.json").read_text())

        self.assertEqual(
            [
                {"@type": "/bze.tradebin.MsgCreateOrder", "creator": self.signer.address, "order_type": "buy",
                 "amount": "2000000", "price": "0.5", "market_id": "ubze/uusdc"},
                {"@type": "/bze.tradebin.MsgCancelOrder", "creator": self.signer.address, "market_id": "ubze/uusdc",
                 "order_id": "000000000000000000000042", "order_type": "sell"},
            ],
            signed["body"]["messages"],
        )
        self.assertEqual(signed["body"], simulation["body"])
        self.assertEqual("hbot:golden", signed["body"]["memo"])
        signer_info = signed["auth_info"]["signer_infos"][0]
        self.assertEqual(base64.b64encode(self.signer.public_key_bytes).decode(), signer_info["public_key"]["key"])
        self.assertEqual({"single": {"mode": "SIGN_MODE_DIRECT"}}, signer_info["mode_info"])
        self.assertEqual("3", signer_info["sequence"])
        self.assertEqual(signer_info, simulation["auth_info"]["signer_infos"][0])
        self.assertEqual({"amount": [{"denom": "ubze", "amount": "1301"}], "gas_limit": "130002", "payer": "", "granter": ""}, signed["auth_info"]["fee"])
        self.assertEqual({"amount": [], "gas_limit": "0", "payer": "", "granter": ""}, simulation["auth_info"]["fee"])
        self.assertEqual(64, len(base64.b64decode(signed["signatures"][0])))
        self.assertEqual([""], simulation["signatures"])
