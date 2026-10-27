import hashlib
import hmac
import re
import unicodedata
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from bidict import bidict
import ecdsa
from pydantic import ConfigDict, Field, SecretStr, field_validator

from hummingbot.client.config.config_data_types import BaseClientModel, BaseConnectorConfigMap
from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeSchema

CENTRALIZED = False
EXAMPLE_PAIR = "BZE-USDC"
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0"),
    maker_fixed_fees=[TokenAmount(CONSTANTS.MAINNET_NATIVE_DENOM, Decimal("0.001"))],
    taker_fixed_fees=[TokenAmount(CONSTANTS.MAINNET_NATIVE_DENOM, Decimal("0.1"))],
)

RE_HEX_PRIVATE_KEY = re.compile(r"^(?:0x)?[0-9a-fA-F]{64}$")
RE_BIP32_PATH = re.compile(r"^m(?:/[0-9]+'?)+$")
BZE_ENCRYPTED_SECRET_HEX = re.compile(r"^[0-9a-fA-F]+$")
BIP39_WORD_COUNTS = {12, 15, 18, 21, 24}
COSMOS_HUB_DERIVATION_PATH = "m/44'/118'/0'/0/0"


@dataclass(frozen=True)
class BeezeeToken:
    denom: str
    symbol: str
    display_name: str
    display_denom: str
    exponent: int

    @property
    def quantum(self) -> Decimal:
        return Decimal("1").scaleb(-self.exponent)


@dataclass(frozen=True)
class BeezeeMarket:
    market_id: str
    base: BeezeeToken
    quote: BeezeeToken

    @property
    def trading_pair(self) -> str:
        return f"{self.base.symbol}-{self.quote.symbol}"

    @property
    def display_price_scaler(self) -> Decimal:
        return Decimal("1").scaleb(self.quote.exponent - self.base.exponent)

    @property
    def chain_price_scaler(self) -> Decimal:
        return Decimal("1") / self.display_price_scaler

    @property
    def min_price_increment(self) -> Decimal:
        return Decimal("1").scaleb(self.base.exponent - self.quote.exponent - CONSTANTS.DEFAULT_PRICE_SIG_DIGITS)


def safe_symbol(symbol: str, denom: str) -> str:
    cleaned = (symbol or "").strip().upper()
    if cleaned:
        return cleaned
    return denom.rsplit("/", 1)[-1].replace("-", "_").upper()


def token_from_metadata(denom: str, metadata: Optional[Dict[str, Any]]) -> BeezeeToken:
    metadata = metadata or {}
    display = metadata.get("display", denom)
    exponent = 0
    for unit in metadata.get("denom_units", []):
        if unit.get("denom") == display:
            exponent = int(unit.get("exponent", 0))
            break
    return BeezeeToken(
        denom=denom,
        symbol=safe_symbol(metadata.get("symbol", display), denom),
        display_name=metadata.get("name", display),
        display_denom=display,
        exponent=exponent,
    )


def unique_market_symbols(markets: List[Tuple[str, str]], metadatas: Dict[str, Dict[str, Any]]) -> bidict:
    mapping = bidict()
    used_pairs = set()
    for base_denom, quote_denom in markets:
        base_token = token_from_metadata(base_denom, metadatas.get(base_denom))
        quote_token = token_from_metadata(quote_denom, metadatas.get(quote_denom))
        pair = f"{base_token.symbol}-{quote_token.symbol}"
        if pair in used_pairs:
            pair = f"{base_token.symbol}_{hashlib.sha1(base_denom.encode()).hexdigest()[:4]}-{quote_token.symbol}_{hashlib.sha1(quote_denom.encode()).hexdigest()[:4]}"
        used_pairs.add(pair)
        mapping[f"{base_denom}/{quote_denom}"] = pair
    return mapping


def market_from_data(market_data: Dict[str, Any], metadata_by_denom: Dict[str, Dict[str, Any]], symbol_map: Optional[bidict] = None) -> BeezeeMarket:
    market_id = f"{market_data['base']}/{market_data['quote']}"
    base = token_from_metadata(market_data["base"], metadata_by_denom.get(market_data["base"]))
    quote = token_from_metadata(market_data["quote"], metadata_by_denom.get(market_data["quote"]))
    if symbol_map is not None and market_id in symbol_map:
        base_symbol, quote_symbol = symbol_map[market_id].split("-")
        base = BeezeeToken(**{**base.__dict__, "symbol": base_symbol})
        quote = BeezeeToken(**{**quote.__dict__, "symbol": quote_symbol})
    return BeezeeMarket(market_id=market_id, base=base, quote=quote)


def normalize_private_key(value: str) -> str:
    stripped = value.strip()
    if not RE_HEX_PRIVATE_KEY.match(stripped):
        raise ValueError("Beezee wallet mode expects a 32-byte secp256k1 private key in hex format.")
    return stripped[2:] if stripped.startswith("0x") else stripped


def looks_like_encrypted_secret(value: str) -> bool:
    stripped = value.strip()
    return len(stripped) > 64 and len(stripped) % 2 == 0 and BZE_ENCRYPTED_SECRET_HEX.match(stripped) is not None


def normalize_mnemonic(value: str) -> str:
    normalized = " ".join(value.strip().lower().split())
    words = normalized.split()
    if len(words) not in BIP39_WORD_COUNTS:
        raise ValueError("Beezee mnemonic mode expects a 12, 15, 18, 21, or 24 word BIP39 mnemonic.")
    return normalized


def normalize_hd_path(value: str) -> str:
    normalized = value.strip()
    if not RE_BIP32_PATH.match(normalized):
        raise ValueError("Beezee mnemonic mode expects a valid BIP32 path like m/44'/118'/0'/0/0.")
    return normalized


def _compressed_public_key_from_private_key(private_key_bytes: bytes) -> bytes:
    signing_key = ecdsa.SigningKey.from_string(private_key_bytes, curve=ecdsa.SECP256k1)
    return signing_key.get_verifying_key().to_string("compressed")


def _derive_child_private_key(parent_private_key: bytes, parent_chain_code: bytes, index: int) -> Tuple[bytes, bytes]:
    if index >= 0x80000000:
        data = b"\x00" + parent_private_key + index.to_bytes(4, "big")
    else:
        data = _compressed_public_key_from_private_key(parent_private_key) + index.to_bytes(4, "big")
    child_material = hmac.new(parent_chain_code, data, hashlib.sha512).digest()
    left, right = child_material[:32], child_material[32:]
    curve_order = ecdsa.SECP256k1.order
    derived_value = (int.from_bytes(left, "big") + int.from_bytes(parent_private_key, "big")) % curve_order
    if derived_value == 0:
        raise ValueError("Beezee mnemonic derivation produced an invalid private key.")
    return derived_value.to_bytes(32, "big"), right


def derive_private_key_from_mnemonic(
    mnemonic: str,
    passphrase: str = "",
    hd_path: str = COSMOS_HUB_DERIVATION_PATH,
) -> str:
    mnemonic = normalize_mnemonic(mnemonic)
    hd_path = normalize_hd_path(hd_path)
    normalized_mnemonic = unicodedata.normalize("NFKD", mnemonic)
    normalized_passphrase = unicodedata.normalize("NFKD", passphrase)
    seed = hashlib.pbkdf2_hmac(
        "sha512",
        normalized_mnemonic.encode("utf-8"),
        f"mnemonic{normalized_passphrase}".encode("utf-8"),
        2048,
        dklen=64,
    )
    master_material = hmac.new(b"Bitcoin seed", seed, hashlib.sha512).digest()
    private_key_bytes = master_material[:32]
    chain_code = master_material[32:]
    if int.from_bytes(private_key_bytes, "big") == 0 or int.from_bytes(private_key_bytes, "big") >= ecdsa.SECP256k1.order:
        raise ValueError("Beezee mnemonic derivation produced an invalid master private key.")
    for path_element in hd_path.split("/")[1:]:
        hardened = path_element.endswith("'")
        index = int(path_element[:-1] if hardened else path_element)
        if hardened:
            index += 0x80000000
        private_key_bytes, chain_code = _derive_child_private_key(private_key_bytes, chain_code, index)
    return private_key_bytes.hex()


def private_key_from_account_mode(account_mode: Any) -> Optional[str]:
    if isinstance(account_mode, BeezeeWalletAccountMode):
        return account_mode.private_key.get_secret_value()
    if isinstance(account_mode, BeezeeMnemonicWalletAccountMode):
        return derive_private_key_from_mnemonic(
            mnemonic=account_mode.mnemonic.get_secret_value(),
            passphrase=account_mode.passphrase.get_secret_value(),
            hd_path=account_mode.hd_path,
        )
    return None


def display_amount_to_chain(amount: Decimal, token: BeezeeToken) -> str:
    return str(int((amount / token.quantum).to_integral_value()))


def chain_amount_to_display(amount: Union[str, Decimal], token: BeezeeToken) -> Decimal:
    return Decimal(str(amount)) * token.quantum


def display_price_to_chain(price: Decimal, market: BeezeeMarket) -> str:
    return format(price * market.display_price_scaler, "f")


def chain_price_to_display(price: Union[str, Decimal], market: BeezeeMarket) -> Decimal:
    return Decimal(str(price)) * market.chain_price_scaler


def minimum_order_size_for_price(chain_price: Union[str, Decimal], market: BeezeeMarket) -> Decimal:
    price = Decimal(str(chain_price))
    if price <= Decimal("0"):
        return market.base.quantum
    minimum_chain_amount = (Decimal("1") / price).to_integral_value(rounding="ROUND_CEILING") * Decimal("2")
    return chain_amount_to_display(minimum_chain_amount, market.base)


class BeezeeMainnetNetworkMode(BaseClientModel):
    rest_endpoint: str = CONSTANTS.MAINNET_REST_URL
    rpc_endpoint: str = CONSTANTS.MAINNET_RPC_URL
    websocket_endpoint: str = CONSTANTS.MAINNET_WS_URL
    chain_id: str = CONSTANTS.MAINNET_CHAIN_ID
    address_prefix: str = CONSTANTS.MAINNET_ADDRESS_PREFIX
    native_denom: str = CONSTANTS.MAINNET_NATIVE_DENOM
    gas_price: Decimal = CONSTANTS.DEFAULT_GAS_PRICE
    model_config = ConfigDict(title="mainnet")


class BeezeeTestnetNetworkMode(BaseClientModel):
    rest_endpoint: str = Field(
        default=...,
        json_schema_extra={"prompt": "Enter the Beezee testnet REST endpoint", "prompt_on_new": True},
    )
    rpc_endpoint: str = Field(
        default=...,
        json_schema_extra={"prompt": "Enter the Beezee testnet RPC endpoint", "prompt_on_new": True},
    )
    websocket_endpoint: Optional[str] = Field(
        default=None,
        json_schema_extra={"prompt": "Enter the Beezee testnet websocket endpoint (optional)", "prompt_on_new": True},
    )
    chain_id: str = Field(
        default=CONSTANTS.TESTNET_CHAIN_ID,
        json_schema_extra={"prompt": "Enter the Beezee testnet chain id", "prompt_on_new": True},
    )
    address_prefix: str = Field(
        default=CONSTANTS.TESTNET_ADDRESS_PREFIX,
        json_schema_extra={"prompt": "Enter the Beezee testnet bech32 prefix", "prompt_on_new": True},
    )
    native_denom: str = Field(
        default=CONSTANTS.TESTNET_NATIVE_DENOM,
        json_schema_extra={"prompt": "Enter the Beezee testnet native fee denom", "prompt_on_new": True},
    )
    gas_price: Decimal = Field(
        default=CONSTANTS.DEFAULT_GAS_PRICE,
        json_schema_extra={"prompt": "Enter the Beezee testnet gas price", "prompt_on_new": True},
    )
    model_config = ConfigDict(title="testnet")


class BeezeeCustomNetworkMode(BaseClientModel):
    rest_endpoint: str = Field(default=..., json_schema_extra={"prompt": "Enter the Beezee REST endpoint", "prompt_on_new": True})
    rpc_endpoint: str = Field(default=..., json_schema_extra={"prompt": "Enter the Beezee RPC endpoint", "prompt_on_new": True})
    websocket_endpoint: Optional[str] = Field(default=None, json_schema_extra={"prompt": "Enter the Beezee websocket endpoint (optional)", "prompt_on_new": True})
    chain_id: str = Field(default=..., json_schema_extra={"prompt": "Enter the Beezee chain id", "prompt_on_new": True})
    address_prefix: str = Field(default="bze", json_schema_extra={"prompt": "Enter the Beezee bech32 prefix", "prompt_on_new": True})
    native_denom: str = Field(default="ubze", json_schema_extra={"prompt": "Enter the Beezee native fee denom", "prompt_on_new": True})
    gas_price: Decimal = Field(default=CONSTANTS.DEFAULT_GAS_PRICE, json_schema_extra={"prompt": "Enter the Beezee gas price", "prompt_on_new": True})
    model_config = ConfigDict(title="custom")


NETWORK_MODES = {
    BeezeeMainnetNetworkMode.model_config["title"]: BeezeeMainnetNetworkMode,
    BeezeeTestnetNetworkMode.model_config["title"]: BeezeeTestnetNetworkMode,
    BeezeeCustomNetworkMode.model_config["title"]: BeezeeCustomNetworkMode,
}


class BeezeeWalletAccountMode(BaseClientModel):
    name: Literal["wallet"] = "wallet"
    private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Beezee private key (hex)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    address: Optional[str] = Field(default=None, json_schema_extra={"prompt": "Enter your Beezee address (optional)", "prompt_on_new": True})
    create_order_gas_limit: int = CONSTANTS.DEFAULT_CREATE_ORDER_GAS_LIMIT
    cancel_order_gas_limit: int = CONSTANTS.DEFAULT_CANCEL_ORDER_GAS_LIMIT
    model_config = ConfigDict(title="wallet")

    @field_validator("private_key", mode="before")
    @classmethod
    def validate_private_key(cls, value: Any):
        if hasattr(value, "get_secret_value"):
            value = value.get_secret_value()
        value = str(value)
        if looks_like_encrypted_secret(value):
            return value
        return normalize_private_key(value)


class BeezeeMnemonicWalletAccountMode(BaseClientModel):
    name: Literal["wallet_mnemonic"] = "wallet_mnemonic"
    mnemonic: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Beezee mnemonic",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    passphrase: SecretStr = Field(
        default=SecretStr(""),
        json_schema_extra={
            "prompt": "Enter your Beezee mnemonic passphrase (optional)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    hd_path: str = Field(
        default=COSMOS_HUB_DERIVATION_PATH,
        json_schema_extra={
            "prompt": "Enter the Beezee HD derivation path",
            "prompt_on_new": True,
        },
    )
    address: Optional[str] = Field(default=None, json_schema_extra={"prompt": "Enter your Beezee address (optional)", "prompt_on_new": True})
    create_order_gas_limit: int = CONSTANTS.DEFAULT_CREATE_ORDER_GAS_LIMIT
    cancel_order_gas_limit: int = CONSTANTS.DEFAULT_CANCEL_ORDER_GAS_LIMIT
    model_config = ConfigDict(title="wallet_mnemonic")

    @field_validator("mnemonic", mode="before")
    @classmethod
    def validate_mnemonic(cls, value: Any):
        if hasattr(value, "get_secret_value"):
            value = value.get_secret_value()
        value = str(value)
        if looks_like_encrypted_secret(value):
            return value
        return normalize_mnemonic(value)

    @field_validator("hd_path", mode="before")
    @classmethod
    def validate_hd_path(cls, value: Any):
        return normalize_hd_path(str(value))


class BeezeeReadOnlyAccountMode(BaseClientModel):
    name: Literal["read_only"] = "read_only"
    address: Optional[str] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter your Beezee address (optional)",
            "prompt_on_new": True,
            "is_connect_key": True,
        },
    )
    model_config = ConfigDict(title="read_only")


ACCOUNT_MODES = {
    BeezeeWalletAccountMode.model_config["title"]: BeezeeWalletAccountMode,
    BeezeeMnemonicWalletAccountMode.model_config["title"]: BeezeeMnemonicWalletAccountMode,
    BeezeeReadOnlyAccountMode.model_config["title"]: BeezeeReadOnlyAccountMode,
}


class BeezeeConfigMap(BaseConnectorConfigMap):
    connector: str = "beezee"
    receive_connector_configuration: bool = Field(default=True)
    network: Union[tuple(NETWORK_MODES.values())] = Field(
        default=BeezeeMainnetNetworkMode(),
        json_schema_extra={"prompt": f"Select the Beezee network ({'/'.join(NETWORK_MODES)})", "prompt_on_new": True},
    )
    account_type: Union[tuple(ACCOUNT_MODES.values())] = Field(
        default=BeezeeReadOnlyAccountMode(),
        discriminator="name",
        json_schema_extra={"prompt": f"Select the Beezee account type ({'/'.join(ACCOUNT_MODES)})", "prompt_on_new": True},
    )
    model_config = ConfigDict(title="beezee")

    @field_validator("network", mode="before")
    @classmethod
    def validate_network(cls, value: Any):
        if isinstance(value, tuple(NETWORK_MODES.values())):
            return value
        if isinstance(value, dict):
            if value.get("chain_id") == CONSTANTS.TESTNET_CHAIN_ID:
                mode = BeezeeTestnetNetworkMode
            elif "rest_endpoint" in value and "rpc_endpoint" in value:
                mode = BeezeeCustomNetworkMode
            else:
                mode = BeezeeMainnetNetworkMode
            return mode(**value)
        if value not in NETWORK_MODES:
            raise ValueError(f"Invalid Beezee network. Choose one of {list(NETWORK_MODES)}.")
        return NETWORK_MODES[value].model_construct()

    @field_validator("account_type", mode="before")
    @classmethod
    def validate_account_type(cls, value: Any):
        if isinstance(value, tuple(ACCOUNT_MODES.values())):
            return value
        if isinstance(value, dict):
            return ACCOUNT_MODES[value.get("name", "read_only")](**value)
        if value not in ACCOUNT_MODES:
            raise ValueError(f"Invalid Beezee account type. Choose one of {list(ACCOUNT_MODES)}.")
        return ACCOUNT_MODES[value].model_construct()


KEYS = BeezeeConfigMap.model_construct()
