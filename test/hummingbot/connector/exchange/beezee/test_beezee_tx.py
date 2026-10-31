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
    msg_exec,
    msg_fill_orders,
    msg_multi_swap,
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

# Two MsgMultiSwap on the BZE/USDC pool in one tx (a round trip). `bzed tx decode` (v8.2.0) gives the JSON fixtures
# next to this file, and `bzed tx encode` of that JSON gives these bytes back.
USDC = "ibc/81DA528F4C5546208D1D50F84C2D0B388B45D37AA36FCA622A3CB2D8EF014838"  # noqa: mock
USDC_BZE_POOL = f"{USDC}_ubze"
GOLDEN_SWAP_TX = (
    "CocECvsBChovYnplLnRyYWRlYmluLk1zZ011bHRpU3dhcBLcAQoqYnplMTlybDRjbTJobXI4YWZ5NGtsZHB4ejNma2E0amd1cTBh"
    "cHlwa2R6EklpYmMvODFEQTUyOEY0QzU1NDYyMDhEMUQ1MEY4NEMyRDBCMzg4QjQ1RDM3QUEzNkZDQTYyMkEzQ0IyRDhFRjAxNDgz"
    "OF91YnplGk8KRGliYy84MURBNTI4RjRDNTU0NjIwOEQxRDUwRjg0QzJEMEIzODhCNDVEMzdBQTM2RkNBNjIyQTNDQjJEOEVGMDE0"
    "ODM4EgcxMDAwMDAwIhIKBHViemUSCjMzMDAwMDAwMDAK+gEKGi9iemUudHJhZGViaW4uTXNnTXVsdGlTd2FwEtsBCipiemUxOXJs"
    "NGNtMmhtcjhhZnk0a2xkcHh6M2ZrYTRqZ3VxMGFweXBrZHoSSWliYy84MURBNTI4RjRDNTU0NjIwOEQxRDUwRjg0QzJEMEIzODhC"
    "NDVEMzdBQTM2RkNBNjIyQTNDQjJEOEVGMDE0ODM4X3ViemUaEgoEdWJ6ZRIKMzMwMDAwMDAwMCJOCkRpYmMvODFEQTUyOEY0QzU1"
    "NDYyMDhEMUQ1MEY4NEMyRDBCMzg4QjQ1RDM3QUEzNkZDQTYyMkEzQ0IyRDhFRjAxNDgzOBIGOTkwMDAwEgpoYm90LXN3YXBzEmYK"
    "UApGCh8vY29zbW9zLmNyeXB0by5zZWNwMjU2azEuUHViS2V5EiMKIQJPTirZnDTWC5umKDyUMahBivhnMhKWH5ene2N3/NBbYhIE"
    "CgIIARgDEhIKDAoEdWJ6ZRIEMzAwMBDgpxIaQM6gIdRVyulv9lXMe1PDtllm93SuMArjgWWG7sl1h6x3GhSI01omq5deFvjLQT+i"
    "PAbDRKaPF6xhxobcS6yy+hA="
)
GOLDEN_SWAP_SIMULATION_TX = (
    "CocECvsBChovYnplLnRyYWRlYmluLk1zZ011bHRpU3dhcBLcAQoqYnplMTlybDRjbTJobXI4YWZ5NGtsZHB4ejNma2E0amd1cTBh"
    "cHlwa2R6EklpYmMvODFEQTUyOEY0QzU1NDYyMDhEMUQ1MEY4NEMyRDBCMzg4QjQ1RDM3QUEzNkZDQTYyMkEzQ0IyRDhFRjAxNDgz"
    "OF91YnplGk8KRGliYy84MURBNTI4RjRDNTU0NjIwOEQxRDUwRjg0QzJEMEIzODhCNDVEMzdBQTM2RkNBNjIyQTNDQjJEOEVGMDE0"
    "ODM4EgcxMDAwMDAwIhIKBHViemUSCjMzMDAwMDAwMDAK+gEKGi9iemUudHJhZGViaW4uTXNnTXVsdGlTd2FwEtsBCipiemUxOXJs"
    "NGNtMmhtcjhhZnk0a2xkcHh6M2ZrYTRqZ3VxMGFweXBrZHoSSWliYy84MURBNTI4RjRDNTU0NjIwOEQxRDUwRjg0QzJEMEIzODhC"
    "NDVEMzdBQTM2RkNBNjIyQTNDQjJEOEVGMDE0ODM4X3ViemUaEgoEdWJ6ZRIKMzMwMDAwMDAwMCJOCkRpYmMvODFEQTUyOEY0QzU1"
    "NDYyMDhEMUQ1MEY4NEMyRDBCMzg4QjQ1RDM3QUEzNkZDQTYyMkEzQ0IyRDhFRjAxNDgzOBIGOTkwMDAwEgpoYm90LXN3YXBzElQK"
    "UApGCh8vY29zbW9zLmNyeXB0by5zZWNwMjU2azEuUHViS2V5EiMKIQJPTirZnDTWC5umKDyUMahBivhnMhKWH5ene2N3/NBbYhIE"
    "CgIIARgDEgAaAA=="
)

# A MsgFillOrders taking two BZE/USDC asks, then a MsgMultiSwap selling the BZE on the pool, in one tx (the arbitrage
# shape of fill_orders_and_swap). `bzed tx decode` (v8.2.0) gives the JSON fixtures next to this file, and `bzed tx
# encode` of that JSON gives these bytes back.
GOLDEN_FILL_SWAP_TX = (
    "CtkDCsYBChsvYnplLnRyYWRlYmluLk1zZ0ZpbGxPcmRlcnMSpgEKKmJ6ZTE5cmw0Y20yaG1yOGFmeTRrbGRweHozZmthNGpndXEw"
    "YXB5cGtkehJJdWJ6ZS9pYmMvODFEQTUyOEY0QzU1NDYyMDhEMUQ1MEY4NEMyRDBCMzg4QjQ1RDM3QUEzNkZDQTYyMkEzQ0IyRDhF"
    "RjAxNDgzOBoEc2VsbCISCgYwLjAwMDMSCDIwMDAwMDAwIhMKBzAuMDAwMzESCDEwMDAwMDAwCvYBChovYnplLnRyYWRlYmluLk1z"
    "Z011bHRpU3dhcBLXAQoqYnplMTlybDRjbTJobXI4YWZ5NGtsZHB4ejNma2E0amd1cTBhcHlwa2R6EklpYmMvODFEQTUyOEY0QzU1"
    "NDYyMDhEMUQ1MEY4NEMyRDBCMzg4QjQ1RDM3QUEzNkZDQTYyMkEzQ0IyRDhFRjAxNDgzOF91YnplGhAKBHViemUSCDMwMDAwMDAw"
    "IkwKRGliYy84MURBNTI4RjRDNTU0NjIwOEQxRDUwRjg0QzJEMEIzODhCNDVEMzdBQTM2RkNBNjIyQTNDQjJEOEVGMDE0ODM4EgQ5"
    "MDAwEhVoYm90LWZpbGwtc3dhcDpnb2xkZW4SZgpQCkYKHy9jb3Ntb3MuY3J5cHRvLnNlY3AyNTZrMS5QdWJLZXkSIwohAk9OKtmc"
    "NNYLm6YoPJQxqEGK+GcyEpYfl6d7Y3f80FtiEgQKAggBGAMSEgoMCgR1YnplEgQzMDAwEOCnEhpALZsWBEX+b+xDOo3UEbDQI+v1"
    "2zej57QqJM6zIh+BV/IY7Ri3w61GmMKHdmxDBOdDGz9sHct+dl+leYgrHfdfpA=="
)
GOLDEN_FILL_SWAP_SIMULATION_TX = (
    "CtkDCsYBChsvYnplLnRyYWRlYmluLk1zZ0ZpbGxPcmRlcnMSpgEKKmJ6ZTE5cmw0Y20yaG1yOGFmeTRrbGRweHozZmthNGpndXEw"
    "YXB5cGtkehJJdWJ6ZS9pYmMvODFEQTUyOEY0QzU1NDYyMDhEMUQ1MEY4NEMyRDBCMzg4QjQ1RDM3QUEzNkZDQTYyMkEzQ0IyRDhF"
    "RjAxNDgzOBoEc2VsbCISCgYwLjAwMDMSCDIwMDAwMDAwIhMKBzAuMDAwMzESCDEwMDAwMDAwCvYBChovYnplLnRyYWRlYmluLk1z"
    "Z011bHRpU3dhcBLXAQoqYnplMTlybDRjbTJobXI4YWZ5NGtsZHB4ejNma2E0amd1cTBhcHlwa2R6EklpYmMvODFEQTUyOEY0QzU1"
    "NDYyMDhEMUQ1MEY4NEMyRDBCMzg4QjQ1RDM3QUEzNkZDQTYyMkEzQ0IyRDhFRjAxNDgzOF91YnplGhAKBHViemUSCDMwMDAwMDAw"
    "IkwKRGliYy84MURBNTI4RjRDNTU0NjIwOEQxRDUwRjg0QzJEMEIzODhCNDVEMzdBQTM2RkNBNjIyQTNDQjJEOEVGMDE0ODM4EgQ5"
    "MDAwEhVoYm90LWZpbGwtc3dhcDpnb2xkZW4SVApQCkYKHy9jb3Ntb3MuY3J5cHRvLnNlY3AyNTZrMS5QdWJLZXkSIwohAk9OKtmc"
    "NNYLm6YoPJQxqEGK+GcyEpYfl6d7Y3f80FtiEgQKAggBGAMSABoA"
)

# A delegated create order: the bot key (the test mnemonic's key) sends the granter's MsgCreateOrder in an authz MsgExec,
# with the granter as fee granter. Produced by `bzed tx authz exec inner.json --from <bot> --fee-granter <granter>
# --fees 1301ubze --gas 130002 --note hbot:golden --generate-only`, signed offline (`bzed tx sign --offline -a 7 -s 3
# --chain-id beezee-1`) and re-encoded with `bzed tx encode` (bzed v8.2.0). The tx body and the auth info, without the
# signature.
DELEGATED_GRANTER = "bze1k8py7ynzrrnmzqwk2mkz3a6znatmqshdnnsehy"
GOLDEN_DELEGATED_BODY = (
    "CrsBCh0vY29zbW9zLmF1dGh6LnYxYmV0YTEuTXNnRXhlYxKZAQoqYnplMTlybDRjbTJobXI4YWZ5NGtsZHB4ejNma2E0amd1cTBh"
    "cHlwa2R6EmsKHC9iemUudHJhZGViaW4uTXNnQ3JlYXRlT3JkZXISSwoqYnplMWs4cHk3eW56cnJubXpxd2sybWt6M2E2em5hdG1x"
    "c2hkbm5zZWh5EgNidXkaBzIwMDAwMDAiAzAuNSoKdWJ6ZS91dXNkYxILaGJvdDpnb2xkZW4="
)
GOLDEN_DELEGATED_AUTH_INFO = (
    "ClAKRgofL2Nvc21vcy5jcnlwdG8uc2VjcDI1NmsxLlB1YktleRIjCiECT04q2Zw01gubpig8lDGoQYr4ZzISlh+Xp3tjd/zQW2IS"
    "BAoCCAEYAxI+CgwKBHViemUSBDEzMDEQ0vcHIipiemUxazhweTd5bnpycm5tenF3azJta3ozYTZ6bmF0bXFzaGRubnNlaHk="
)


def _fields(data: bytes):
    """(field number, value) of a protobuf message: an int for varints, bytes for length-delimited fields."""
    fields, position = [], 0

    def varint() -> int:
        nonlocal position
        value, shift = 0, 0
        while True:
            byte = data[position]
            position += 1
            value |= (byte & 0x7F) << shift
            shift += 7
            if not byte & 0x80:
                return value

    while position < len(data):
        key = varint()
        if key & 7 == 0:
            fields.append((key >> 3, varint()))
        else:
            length = varint()
            fields.append((key >> 3, data[position:position + length]))
            position += length
    return fields


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

    def test_msg_multi_swap_contains_expected_type_url_and_coins(self):
        payload = msg_multi_swap("bze1test", ["ubze_uusdc"], "uusdc", "1000", "ubze", "990")

        self.assertIn(b"/bze.tradebin.MsgMultiSwap", payload)
        self.assertIn(b"ubze_uusdc", payload)
        self.assertIn(b"990", payload)

    def test_msg_fill_orders_contains_expected_type_url_and_entries(self):
        payload = msg_fill_orders("bze1test", "ubze/uusdc", "buy", [("0.5", "1000"), ("0.4", "2000")])

        self.assertIn(b"/bze.tradebin.MsgFillOrders", payload)
        self.assertIn(b"ubze/uusdc", payload)
        self.assertLess(payload.index(b"0.5"), payload.index(b"0.4"))

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


class BeezeeSwapTxGoldenVectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.signer = BeezeeSigner(TEST_MNEMONIC_PRIVATE_KEY, "bze")
        self.messages = [
            msg_multi_swap(self.signer.address, [USDC_BZE_POOL], USDC, "1000000", "ubze", "3300000000"),
            msg_multi_swap(self.signer.address, [USDC_BZE_POOL], "ubze", "3300000000", USDC, "990000"),
        ]

    def test_signed_swap_transaction_matches_golden_vector(self):
        tx_bytes = build_signed_transaction(
            signer=self.signer,
            chain_id="beezee-1",
            account_number=7,
            sequence=3,
            messages=self.messages,
            fee_denom="ubze",
            fee_amount="3000",
            gas_limit=300000,
            memo="hbot-swaps",
        )

        self.assertEqual(GOLDEN_SWAP_TX, base64.b64encode(tx_bytes).decode())

    def test_swap_simulation_transaction_matches_golden_vector(self):
        tx_bytes = build_simulation_transaction(self.signer.public_key_bytes, sequence=3, messages=self.messages, memo="hbot-swaps")

        self.assertEqual(GOLDEN_SWAP_SIMULATION_TX, base64.b64encode(tx_bytes).decode())

    def test_golden_swap_vectors_decode_as_expected_with_bzed(self):
        signed = json.loads((FIXTURES / "golden_swap_tx.bzed_decode.json").read_text())
        simulation = json.loads((FIXTURES / "golden_swap_simulation_tx.bzed_decode.json").read_text())

        self.assertEqual(
            [
                {"@type": "/bze.tradebin.MsgMultiSwap", "creator": self.signer.address, "routes": [USDC_BZE_POOL],
                 "input": {"denom": USDC, "amount": "1000000"}, "min_output": {"denom": "ubze", "amount": "3300000000"}},
                {"@type": "/bze.tradebin.MsgMultiSwap", "creator": self.signer.address, "routes": [USDC_BZE_POOL],
                 "input": {"denom": "ubze", "amount": "3300000000"}, "min_output": {"denom": USDC, "amount": "990000"}},
            ],
            signed["body"]["messages"],
        )
        self.assertEqual(signed["body"], simulation["body"])
        self.assertEqual("hbot-swaps", signed["body"]["memo"])
        self.assertEqual("3", signed["auth_info"]["signer_infos"][0]["sequence"])
        self.assertEqual({"amount": [{"denom": "ubze", "amount": "3000"}], "gas_limit": "300000", "payer": "", "granter": ""}, signed["auth_info"]["fee"])
        self.assertEqual({"amount": [], "gas_limit": "0", "payer": "", "granter": ""}, simulation["auth_info"]["fee"])
        self.assertEqual(64, len(base64.b64decode(signed["signatures"][0])))
        self.assertEqual([""], simulation["signatures"])


class BeezeeFillTxGoldenVectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.signer = BeezeeSigner(TEST_MNEMONIC_PRIVATE_KEY, "bze")
        self.messages = [
            msg_fill_orders(self.signer.address, f"ubze/{USDC}", "sell", [("0.0003", "20000000"), ("0.00031", "10000000")]),
            msg_multi_swap(self.signer.address, [USDC_BZE_POOL], "ubze", "30000000", USDC, "9000"),
        ]

    def test_signed_fill_swap_transaction_matches_golden_vector(self):
        tx_bytes = build_signed_transaction(
            signer=self.signer,
            chain_id="beezee-1",
            account_number=7,
            sequence=3,
            messages=self.messages,
            fee_denom="ubze",
            fee_amount="3000",
            gas_limit=300000,
            memo="hbot-fill-swap:golden",
        )

        self.assertEqual(GOLDEN_FILL_SWAP_TX, base64.b64encode(tx_bytes).decode())

    def test_fill_swap_simulation_transaction_matches_golden_vector(self):
        tx_bytes = build_simulation_transaction(
            self.signer.public_key_bytes, sequence=3, messages=self.messages, memo="hbot-fill-swap:golden"
        )

        self.assertEqual(GOLDEN_FILL_SWAP_SIMULATION_TX, base64.b64encode(tx_bytes).decode())

    def test_golden_fill_swap_vectors_decode_as_expected_with_bzed(self):
        signed = json.loads((FIXTURES / "golden_fill_swap_tx.bzed_decode.json").read_text())
        simulation = json.loads((FIXTURES / "golden_fill_swap_simulation_tx.bzed_decode.json").read_text())

        self.assertEqual(
            [
                {"@type": "/bze.tradebin.MsgFillOrders", "creator": self.signer.address, "market_id": f"ubze/{USDC}",
                 "order_type": "sell", "orders": [{"price": "0.0003", "amount": "20000000"},
                                                  {"price": "0.00031", "amount": "10000000"}]},
                {"@type": "/bze.tradebin.MsgMultiSwap", "creator": self.signer.address, "routes": [USDC_BZE_POOL],
                 "input": {"denom": "ubze", "amount": "30000000"}, "min_output": {"denom": USDC, "amount": "9000"}},
            ],
            signed["body"]["messages"],
        )
        self.assertEqual(signed["body"], simulation["body"])
        self.assertEqual("hbot-fill-swap:golden", signed["body"]["memo"])
        self.assertEqual("3", signed["auth_info"]["signer_infos"][0]["sequence"])
        self.assertEqual({"amount": [{"denom": "ubze", "amount": "3000"}], "gas_limit": "300000", "payer": "", "granter": ""}, signed["auth_info"]["fee"])
        self.assertEqual({"amount": [], "gas_limit": "0", "payer": "", "granter": ""}, simulation["auth_info"]["fee"])
        self.assertEqual(64, len(base64.b64decode(signed["signatures"][0])))
        self.assertEqual([""], simulation["signatures"])


class BeezeeDelegatedTxTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bot = BeezeeSigner(TEST_MNEMONIC_PRIVATE_KEY, "bze")
        self.inner = [msg_create_order(DELEGATED_GRANTER, "buy", "2000000", "0.5", "ubze/uusdc")]

    def _tx_parts(self, tx_bytes: bytes):
        tx_raw = _fields(tx_bytes)
        return dict(tx_raw)[1], dict(tx_raw)[2], dict(tx_raw)[3]

    def test_delegated_create_order_matches_bzed_authz_exec_with_fee_granter(self):
        tx_bytes = build_signed_transaction(
            signer=self.bot,
            chain_id="beezee-1",
            account_number=7,
            sequence=3,
            messages=[msg_exec(self.bot.address, self.inner)],
            fee_denom="ubze",
            fee_amount="1301",
            gas_limit=130002,
            memo="hbot:golden",
            fee_granter=DELEGATED_GRANTER,
        )

        body, auth_info, signature = self._tx_parts(tx_bytes)
        self.assertEqual(GOLDEN_DELEGATED_BODY, base64.b64encode(body).decode())
        self.assertEqual(GOLDEN_DELEGATED_AUTH_INFO, base64.b64encode(auth_info).decode())
        self.assertEqual(64, len(signature))

    def test_msg_exec_holds_the_inner_messages_in_order(self):
        cancel = msg_cancel_order(DELEGATED_GRANTER, "ubze/uusdc", "000000000000000000000042", "sell")

        any_fields = _fields(msg_exec(self.bot.address, self.inner + [cancel]))

        self.assertEqual([(1, b"/cosmos.authz.v1beta1.MsgExec")], any_fields[:1])
        payload = _fields(dict(any_fields)[2])
        self.assertEqual((1, self.bot.address.encode()), payload[0])
        self.assertEqual([(2, self.inner[0]), (2, cancel)], payload[1:])

    def test_fee_granter_is_fee_field_4_and_payer_stays_empty(self):
        tx_bytes = build_signed_transaction(
            signer=self.bot, chain_id="beezee-1", account_number=7, sequence=3,
            messages=[msg_exec(self.bot.address, self.inner)], fee_denom="ubze", fee_amount="1301", gas_limit=130002,
            fee_granter=DELEGATED_GRANTER,
        )

        fee = _fields(dict(_fields(self._tx_parts(tx_bytes)[1]))[2])
        self.assertEqual([1, 2, 4], [number for number, _ in fee])
        self.assertEqual(DELEGATED_GRANTER.encode(), dict(fee)[4])

    def test_simulation_has_an_empty_fee_with_the_granter(self):
        tx_bytes = build_simulation_transaction(
            self.bot.public_key_bytes, sequence=3, messages=[msg_exec(self.bot.address, self.inner)],
            memo="hbot:golden", fee_granter=DELEGATED_GRANTER,
        )

        body, auth_info, signature = self._tx_parts(tx_bytes)
        self.assertEqual(GOLDEN_DELEGATED_BODY, base64.b64encode(body).decode())
        auth_fields = _fields(auth_info)
        self.assertEqual(_fields(base64.b64decode(GOLDEN_DELEGATED_AUTH_INFO))[0], auth_fields[0])  # the signer info
        self.assertEqual([(4, DELEGATED_GRANTER.encode())], _fields(dict(auth_fields)[2]))
        self.assertEqual(b"", signature)

    def test_no_granter_keeps_the_wallet_mode_bytes(self):
        self.assertEqual(
            build_simulation_transaction(self.bot.public_key_bytes, 3, self.inner),
            build_simulation_transaction(self.bot.public_key_bytes, 3, self.inner, fee_granter=None),
        )
