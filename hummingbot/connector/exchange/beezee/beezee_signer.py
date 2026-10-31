import hashlib

import ecdsa
from Crypto.Hash import RIPEMD160

CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"


def _bech32_polymod(values):
    generator = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3]
    checksum = 1
    for value in values:
        top = checksum >> 25
        checksum = ((checksum & 0x1FFFFFF) << 5) ^ value
        for i in range(5):
            if ((top >> i) & 1) != 0:
                checksum ^= generator[i]
    return checksum


def _bech32_hrp_expand(hrp: str):
    return [ord(char) >> 5 for char in hrp] + [0] + [ord(char) & 31 for char in hrp]


def _bech32_create_checksum(hrp: str, data):
    values = _bech32_hrp_expand(hrp) + data
    polymod = _bech32_polymod(values + [0, 0, 0, 0, 0, 0]) ^ 1
    return [(polymod >> 5 * (5 - i)) & 31 for i in range(6)]


def _convertbits(data, from_bits: int, to_bits: int, pad: bool = True):
    accumulator = 0
    bits = 0
    result = []
    max_value = (1 << to_bits) - 1
    max_accumulator = (1 << (from_bits + to_bits - 1)) - 1
    for value in data:
        accumulator = ((accumulator << from_bits) | value) & max_accumulator
        bits += from_bits
        while bits >= to_bits:
            bits -= to_bits
            result.append((accumulator >> bits) & max_value)
    if pad and bits:
        result.append((accumulator << (to_bits - bits)) & max_value)
    return result


def ripemd160(data: bytes) -> bytes:
    # OpenSSL 3 builds may disable the legacy ripemd160 digest in hashlib.
    try:
        return hashlib.new("ripemd160", data).digest()
    except ValueError:
        return RIPEMD160.new(data).digest()


def bech32_encode(hrp: str, payload: bytes) -> str:
    data = _convertbits(payload, 8, 5)
    combined = data + _bech32_create_checksum(hrp, data)
    return hrp + "1" + "".join(CHARSET[d] for d in combined)


def bech32_decode(address: str):
    """(hrp, payload bytes) of a bech32 address. Raises ValueError when it is not a valid bech32 string."""
    if not isinstance(address, str) or address.lower() != address and address.upper() != address:
        raise ValueError("mixed-case bech32 string")
    address = address.lower()
    separator = address.rfind("1")
    if separator < 1 or separator + 7 > len(address) or len(address) > 90:
        raise ValueError("bech32 string has no prefix or a too short data part")
    hrp, data_part = address[:separator], address[separator + 1:]
    if any(char not in CHARSET for char in data_part):
        raise ValueError("bech32 string has a character outside the bech32 charset")
    data = [CHARSET.find(char) for char in data_part]
    if _bech32_polymod(_bech32_hrp_expand(hrp) + data) != 1:
        raise ValueError("bech32 checksum mismatch")
    payload = _convertbits(data[:-6], 5, 8, pad=False)
    return hrp, bytes(payload)


class BeezeeSigner:
    def __init__(self, private_key_hex: str, address_prefix: str):
        self._private_key_bytes = bytes.fromhex(private_key_hex)
        self._signing_key = ecdsa.SigningKey.from_string(self._private_key_bytes, curve=ecdsa.SECP256k1)
        self._verifying_key = self._signing_key.get_verifying_key()
        self._address_prefix = address_prefix

    @property
    def public_key_bytes(self) -> bytes:
        return self._verifying_key.to_string("compressed")

    @property
    def address(self) -> str:
        # Beezee is built on a Cosmos SDK 0.47.x fork, where secp256k1 account
        # addresses are still Bitcoin-style RIPEMD160(SHA256(pubkey)).
        sha_hash = hashlib.sha256(self.public_key_bytes).digest()
        ripe_hash = ripemd160(sha_hash)
        return bech32_encode(self._address_prefix, ripe_hash)

    def sign(self, payload: bytes) -> bytes:
        return self._signing_key.sign_deterministic(
            payload,
            hashfunc=hashlib.sha256,
            sigencode=ecdsa.util.sigencode_string_canonize,
        )
