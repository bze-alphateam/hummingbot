import base64
from decimal import Decimal
from typing import Dict, List

from hummingbot.connector.exchange.beezee.beezee_signer import BeezeeSigner


def _encode_varint(value: int) -> bytes:
    encoded = bytearray()
    while True:
        to_write = value & 0x7F
        value >>= 7
        encoded.append(to_write | 0x80 if value else to_write)
        if not value:
            break
    return bytes(encoded)


def _encode_key(field_number: int, wire_type: int) -> bytes:
    return _encode_varint((field_number << 3) | wire_type)


def _encode_bytes(field_number: int, value: bytes) -> bytes:
    return _encode_key(field_number, 2) + _encode_varint(len(value)) + value


def _encode_string(field_number: int, value: str) -> bytes:
    return _encode_bytes(field_number, value.encode())


def _encode_uint64(field_number: int, value: int) -> bytes:
    return _encode_key(field_number, 0) + _encode_varint(value)


def _coin_message(denom: str, amount: str) -> bytes:
    return _encode_string(1, denom) + _encode_string(2, amount)


def _any_message(type_url: str, payload: bytes) -> bytes:
    return _encode_string(1, type_url) + _encode_bytes(2, payload)


def _pub_key_any(public_key_bytes: bytes) -> bytes:
    return _any_message("/cosmos.crypto.secp256k1.PubKey", _encode_bytes(1, public_key_bytes))


def _mode_info_message() -> bytes:
    return _encode_bytes(1, _encode_uint64(1, 1))


def _signer_info_message(public_key_bytes: bytes, sequence: int) -> bytes:
    return _encode_bytes(1, _pub_key_any(public_key_bytes)) + _encode_bytes(2, _mode_info_message()) + _encode_uint64(3, sequence)


def _fee_message(denom: str, amount: str, gas_limit: int) -> bytes:
    return _encode_bytes(1, _coin_message(denom, amount)) + _encode_uint64(2, gas_limit)


def _auth_info_message(public_key_bytes: bytes, sequence: int, fee_denom: str, fee_amount: str, gas_limit: int) -> bytes:
    return _encode_bytes(1, _signer_info_message(public_key_bytes, sequence)) + _encode_bytes(2, _fee_message(fee_denom, fee_amount, gas_limit))


def _tx_body_message(messages: List[bytes], memo: str = "") -> bytes:
    payload = b"".join(_encode_bytes(1, message) for message in messages)
    if memo:
        payload += _encode_string(2, memo)
    return payload


def _sign_doc_message(body_bytes: bytes, auth_info_bytes: bytes, chain_id: str, account_number: int) -> bytes:
    return _encode_bytes(1, body_bytes) + _encode_bytes(2, auth_info_bytes) + _encode_string(3, chain_id) + _encode_uint64(4, account_number)


def _tx_raw_message(body_bytes: bytes, auth_info_bytes: bytes, signature: bytes) -> bytes:
    return _encode_bytes(1, body_bytes) + _encode_bytes(2, auth_info_bytes) + _encode_bytes(3, signature)


def msg_create_order(creator: str, order_type: str, amount: str, price: str, market_id: str) -> bytes:
    payload = _encode_string(1, creator) + _encode_string(2, order_type) + _encode_string(3, amount) + _encode_string(4, price) + _encode_string(5, market_id)
    return _any_message("/bze.tradebin.MsgCreateOrder", payload)


def msg_cancel_order(creator: str, market_id: str, order_id: str, order_type: str) -> bytes:
    payload = _encode_string(1, creator) + _encode_string(2, market_id) + _encode_string(3, order_id) + _encode_string(4, order_type)
    return _any_message("/bze.tradebin.MsgCancelOrder", payload)


def build_signed_transaction(
    signer: BeezeeSigner,
    chain_id: str,
    account_number: int,
    sequence: int,
    messages: List[bytes],
    fee_denom: str,
    fee_amount: str,
    gas_limit: int,
    memo: str = "",
) -> bytes:
    body_bytes = _tx_body_message(messages=messages, memo=memo)
    auth_info_bytes = _auth_info_message(signer.public_key_bytes, sequence, fee_denom, fee_amount, gas_limit)
    sign_doc = _sign_doc_message(body_bytes, auth_info_bytes, chain_id, account_number)
    signature = signer.sign(sign_doc)
    return _tx_raw_message(body_bytes, auth_info_bytes, signature)


def build_broadcast_request(tx_bytes: bytes, mode: str = "BROADCAST_MODE_SYNC") -> Dict[str, str]:
    return {"tx_bytes": base64.b64encode(tx_bytes).decode(), "mode": mode}


def fee_amount_from_gas(gas_limit: int, gas_price: Decimal) -> str:
    return str(int((Decimal(gas_limit) * gas_price).to_integral_value(rounding="ROUND_CEILING")))
