import hashlib
import unittest
from unittest.mock import patch

import ecdsa

from hummingbot.connector.exchange.beezee.beezee_signer import BeezeeSigner, bech32_encode, ripemd160


class BeezeeSignerTests(unittest.TestCase):
    # Public test vectors (never real keys): addresses produced by `bzed keys import-hex` (bze v8.2.0).
    BZED_ADDRESS_VECTORS = {
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f": "bze185p2ew8xztcuyame8g3j7g9kj058wq8rwmn23u",  # noqa: mock
        "0000000000000000000000000000000000000000000000000000000000000001": "bze1w508d6qejxtdg4y5r3zarvary0c5xw7k8em3a0",  # noqa: mock
    }

    def test_address_uses_legacy_cosmos_sdk_ripemd160_sha256(self):
        private_key_hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"  # noqa: mock

        signer = BeezeeSigner(private_key_hex, "bze")

        verifying_key = ecdsa.SigningKey.from_string(bytes.fromhex(private_key_hex), curve=ecdsa.SECP256k1).get_verifying_key()
        public_key_bytes = verifying_key.to_string("compressed")
        expected_address = bech32_encode("bze", ripemd160(hashlib.sha256(public_key_bytes).digest()))

        self.assertEqual(expected_address, signer.address)

    def test_address_matches_bzed(self):
        for private_key_hex, expected_address in self.BZED_ADDRESS_VECTORS.items():
            with self.subTest(private_key_hex=private_key_hex):
                self.assertEqual(expected_address, BeezeeSigner(private_key_hex, "bze").address)

    def test_address_matches_bzed_without_hashlib_ripemd160(self):
        real_new = hashlib.new

        def new_without_ripemd160(name, *args, **kwargs):
            if name.lower() == "ripemd160":
                raise ValueError("unsupported hash type ripemd160")
            return real_new(name, *args, **kwargs)

        with patch("hashlib.new", side_effect=new_without_ripemd160):
            for private_key_hex, expected_address in self.BZED_ADDRESS_VECTORS.items():
                with self.subTest(private_key_hex=private_key_hex):
                    self.assertEqual(expected_address, BeezeeSigner(private_key_hex, "bze").address)

    def test_ripemd160_known_answer(self):
        self.assertEqual("9c1185a5c5e9fc54612808977ee8f548b2258d31", ripemd160(b"").hex())
