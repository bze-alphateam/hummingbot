import hashlib
import hmac
import re
import unicodedata
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Union

import ecdsa
from bidict import bidict
from pydantic import ConfigDict, Field, SecretStr, ValidationInfo, field_validator, model_validator

from hummingbot.client.config.config_data_types import BaseClientModel, BaseConnectorConfigMap
from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_signer import BeezeeSigner, bech32_decode
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeSchema

CENTRALIZED = False
EXAMPLE_PAIR = "BZE-USDC"
# Static fallback for fee estimates (e.g. paper trading). Mainnet tradebin fees on v8.2.0: 1000ubze maker, 100000ubze
# taker. The token is the BZE symbol, as balances are keyed by symbol. The live connector reads the fees from the chain.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0"),
    maker_fixed_fees=[TokenAmount("BZE", Decimal("0.001"))],
    taker_fixed_fees=[TokenAmount("BZE", Decimal("0.1"))],
)

RE_HEX_PRIVATE_KEY = re.compile(r"^(?:0x)?[0-9a-fA-F]{64}$")
RE_BIP32_PATH = re.compile(r"^m(?:/[0-9]+'?)+$")
BZE_ENCRYPTED_SECRET_HEX = re.compile(r"^[0-9a-fA-F]+$")
RE_LEGACY_COIN = re.compile(r"^([0-9]+)([a-zA-Z][a-zA-Z0-9/:._-]*)$")
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


# A trading pair's venues. On chain an order-book market and a liquidity pool are unrelated: a pool can exist with no
# market and a market with no pool, and a pair that has both has them by coincidence.
VENUE_BOOK = "book"
VENUE_POOL = "pool"
VENUE_LABELS = {VENUE_BOOK: "order-book market", VENUE_POOL: "liquidity pool"}
_VENUE_ARTICLES = {VENUE_BOOK: "an", VENUE_POOL: "a"}


class BeezeeVenueError(KeyError):
    """The pair lacks the venue (order-book market or liquidity pool) the call needs. A KeyError, like an unknown
    pair, with a readable message."""

    def __str__(self) -> str:
        return str(self.args[0]) if self.args else ""


@dataclass(frozen=True)
class BeezeePair:
    """A trading pair and its venues: `market` when it has an order-book market, `pool_id` when it has a liquidity
    pool (as discovered; a pool created later is still found by the pool tracker). `base` and `quote` give the pair's
    orientation: the market's when it has one, else the pool's (denoms sorted, as the chain stores pools)."""
    base: BeezeeToken
    quote: BeezeeToken
    market: Optional[BeezeeMarket] = None
    pool_id: Optional[str] = None

    @property
    def trading_pair(self) -> str:
        return f"{self.base.symbol}-{self.quote.symbol}"

    @property
    def market_id(self) -> Optional[str]:
        return self.market.market_id if self.market is not None else None

    @property
    def has_book(self) -> bool:
        return self.market is not None

    @property
    def has_pool(self) -> bool:
        return self.pool_id is not None

    @property
    def venues(self) -> Tuple[str, ...]:
        return tuple(venue for venue, present in ((VENUE_BOOK, self.has_book), (VENUE_POOL, self.has_pool)) if present)

    @property
    def display_price_scaler(self) -> Decimal:
        return Decimal("1").scaleb(self.quote.exponent - self.base.exponent)

    @property
    def chain_price_scaler(self) -> Decimal:
        return Decimal("1") / self.display_price_scaler

    @property
    def min_price_increment(self) -> Decimal:
        return Decimal("1").scaleb(self.base.exponent - self.quote.exponent - CONSTANTS.DEFAULT_PRICE_SIG_DIGITS)


def describe_venues(venues: Sequence[str]) -> str:
    """The venues for status lines: order book + pool, order book only, pool only, or no venue."""
    names = {VENUE_BOOK: "order book", VENUE_POOL: "pool"}
    present = [names[venue] for venue in (VENUE_BOOK, VENUE_POOL) if venue in venues]
    if not present:
        return "no venue"
    return " + ".join(present) if len(present) > 1 else f"{present[0]} only"


def venue_refusal(connector: Any, trading_pair: str, needed: Sequence[str], user: str) -> Optional[str]:
    """Why `user` (a script or controller) cannot run on the pair, naming the venue it lacks, or None when the pair has
    every venue in `needed`. Reads the connector's `trading_pair_venues`."""
    venues = tuple(connector.trading_pair_venues(trading_pair))
    missing = [venue for venue in needed if venue not in venues]
    if not missing:
        return None
    lacks = " and no ".join(VENUE_LABELS[venue] for venue in missing)
    wants = " and ".join(f"{_VENUE_ARTICLES[venue]} {VENUE_LABELS[venue]}" for venue in needed)
    return f"{trading_pair} has no {lacks} (venues: {describe_venues(venues)}); {user} needs {wants}"


def pool_not_loaded(connector: Any, trading_pair: str) -> bool:
    """True while the pair has a liquidity pool but the connector has not read it yet (right after a start, before the
    first pool refresh). A script then waits: it neither uses up a slice or a cycle nor says the pair has no pool."""
    return VENUE_POOL in tuple(connector.trading_pair_venues(trading_pair)) and connector.get_pool(trading_pair) is None


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


def normalize_bech32_address(value: Any, prefix: Optional[str] = None) -> str:
    """The address, stripped, when it is a valid bech32 account address (with `prefix`, when given)."""
    address = str(value or "").strip()
    try:
        hrp, payload = bech32_decode(address)
    except ValueError as error:
        raise ValueError(f"Beezee granter_address '{address}' is not a valid bech32 address ({error}).") from error
    if len(payload) not in (20, 32):
        raise ValueError(f"Beezee granter_address '{address}' is not an account address.")
    if prefix is not None and hrp != prefix:
        raise ValueError(f"Beezee granter_address '{address}' must start with the network's prefix '{prefix}1'.")
    return address.lower()


def _secret_value(value: Any) -> str:
    if hasattr(value, "get_secret_value"):
        value = value.get_secret_value()
    return str(value)


def validate_private_key_field(value: Any) -> str:
    value = _secret_value(value)
    if looks_like_encrypted_secret(value):
        return value
    return normalize_private_key(value)


def validate_mnemonic_field(value: Any) -> str:
    value = _secret_value(value)
    if looks_like_encrypted_secret(value):
        return value
    return normalize_mnemonic(value)


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
    """The signing key: the wallet's own key, or the bot key in the delegated modes."""
    if isinstance(account_mode, (BeezeeWalletAccountMode, BeezeeDelegatedAccountMode)):
        return account_mode.private_key.get_secret_value()
    if isinstance(account_mode, (BeezeeMnemonicWalletAccountMode, BeezeeDelegatedMnemonicAccountMode)):
        return derive_private_key_from_mnemonic(
            mnemonic=account_mode.mnemonic.get_secret_value(),
            passphrase=account_mode.passphrase.get_secret_value(),
            hd_path=account_mode.hd_path,
        )
    return None


def granter_address_from_account_mode(account_mode: Any) -> Optional[str]:
    """The wallet the bot key trades for (through authz, gas paid by its feegrant) in the delegated modes, else None."""
    if isinstance(account_mode, (BeezeeDelegatedAccountMode, BeezeeDelegatedMnemonicAccountMode)):
        return account_mode.granter_address
    return None


def display_amount_to_chain(amount: Decimal, token: BeezeeToken) -> str:
    return str(int((amount / token.quantum).to_integral_value()))


def chain_amount_to_display(amount: Union[str, Decimal], token: BeezeeToken) -> Decimal:
    return Decimal(str(amount)) * token.quantum


def display_price_to_chain(price: Decimal, market: BeezeeMarket) -> str:
    return format(price * market.display_price_scaler, "f")


def chain_price_to_display(price: Union[str, Decimal], market: BeezeeMarket) -> Decimal:
    return Decimal(str(price)) * market.chain_price_scaler


def parse_coin(raw: Any) -> Optional[Tuple[str, int]]:
    """Parses a tradebin fee param into (denom, amount).

    Chain v8.2.0 returns a Coin object ({"denom": "ubze", "amount": "1000"}); older chains a "1000ubze" string.
    """
    if isinstance(raw, dict):
        denom, amount = raw.get("denom"), raw.get("amount")
        if not denom or amount is None:
            return None
        return denom, int(str(amount))
    if isinstance(raw, str):
        match = RE_LEGACY_COIN.match(raw.strip())
        if match is None:
            return None
        return match.group(2), int(match.group(1))
    return None


def drop_legacy_gas_limits(values: Any) -> Any:
    if isinstance(values, dict):
        return {key: value for key, value in values.items() if key not in CONSTANTS.LEGACY_GAS_LIMIT_CONFIG_KEYS}
    return values


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
    gas_adjustment: Decimal = CONSTANTS.DEFAULT_GAS_ADJUSTMENT
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
    gas_adjustment: Decimal = Field(
        default=CONSTANTS.DEFAULT_GAS_ADJUSTMENT,
        json_schema_extra={"prompt": "Enter the Beezee testnet gas adjustment (multiplier on simulated gas)", "prompt_on_new": True},
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
    gas_adjustment: Decimal = Field(default=CONSTANTS.DEFAULT_GAS_ADJUSTMENT, json_schema_extra={"prompt": "Enter the Beezee gas adjustment (multiplier on simulated gas)", "prompt_on_new": True})
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
    model_config = ConfigDict(title="wallet")

    @model_validator(mode="before")
    @classmethod
    def ignore_legacy_gas_limits(cls, values: Any):
        return drop_legacy_gas_limits(values)

    @field_validator("private_key", mode="before")
    @classmethod
    def validate_private_key(cls, value: Any):
        return validate_private_key_field(value)


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
    model_config = ConfigDict(title="wallet_mnemonic")

    @model_validator(mode="before")
    @classmethod
    def ignore_legacy_gas_limits(cls, values: Any):
        return drop_legacy_gas_limits(values)

    @field_validator("mnemonic", mode="before")
    @classmethod
    def validate_mnemonic(cls, value: Any):
        return validate_mnemonic_field(value)

    @field_validator("hd_path", mode="before")
    @classmethod
    def validate_hd_path(cls, value: Any):
        return normalize_hd_path(str(value))


def _granter_address_field():
    return Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter the Beezee wallet the bot trades for (the authz and feegrant granter)",
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )


class BeezeeDelegatedAccountMode(BaseClientModel):
    """The bot key (`private_key`) signs; `granter_address` owns the funds. The bot key sends the tradebin messages
    of `granter_address` in an authz MsgExec, and the gas is paid by the granter's feegrant allowance."""
    name: Literal["delegated"] = "delegated"
    private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter the bot key's Beezee private key (hex)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    granter_address: str = _granter_address_field()
    model_config = ConfigDict(title="delegated")

    @field_validator("private_key", mode="before")
    @classmethod
    def validate_private_key(cls, value: Any):
        return validate_private_key_field(value)

    @field_validator("granter_address", mode="before")
    @classmethod
    def validate_granter_address(cls, value: Any):
        return normalize_bech32_address(value)


class BeezeeDelegatedMnemonicAccountMode(BaseClientModel):
    """`delegated`, with the bot key derived from a mnemonic."""
    name: Literal["delegated_mnemonic"] = "delegated_mnemonic"
    mnemonic: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter the bot key's Beezee mnemonic",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    passphrase: SecretStr = Field(
        default=SecretStr(""),
        json_schema_extra={
            "prompt": "Enter the bot key's mnemonic passphrase (optional)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    hd_path: str = Field(
        default=COSMOS_HUB_DERIVATION_PATH,
        json_schema_extra={
            "prompt": "Enter the bot key's HD derivation path",
            "prompt_on_new": True,
        },
    )
    granter_address: str = _granter_address_field()
    model_config = ConfigDict(title="delegated_mnemonic")

    @field_validator("mnemonic", mode="before")
    @classmethod
    def validate_mnemonic(cls, value: Any):
        return validate_mnemonic_field(value)

    @field_validator("hd_path", mode="before")
    @classmethod
    def validate_hd_path(cls, value: Any):
        return normalize_hd_path(str(value))

    @field_validator("granter_address", mode="before")
    @classmethod
    def validate_granter_address(cls, value: Any):
        return normalize_bech32_address(value)


def check_delegated_account(account_mode: Any, address_prefix: str):
    """Refuses a delegated account whose granter has another network's prefix or is the bot key itself (authz refuses
    a grant to oneself). The bot key is only checked when it is readable, i.e. not still encrypted."""
    granter_address = getattr(account_mode, "granter_address", None)
    if not isinstance(account_mode, (BeezeeDelegatedAccountMode, BeezeeDelegatedMnemonicAccountMode)) or not granter_address:
        return
    normalize_bech32_address(granter_address, prefix=address_prefix)
    secret = getattr(account_mode, "private_key", None) or getattr(account_mode, "mnemonic", None)
    if secret is None or looks_like_encrypted_secret(secret.get_secret_value()):
        return
    bot_address = BeezeeSigner(private_key_from_account_mode(account_mode), address_prefix).address
    if bot_address == granter_address:
        raise ValueError(
            f"Beezee granter_address '{granter_address}' is the bot key's own address. In the delegated modes the bot "
            f"key must be a separate key that the granter wallet authorizes."
        )


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
    BeezeeDelegatedAccountMode.model_config["title"]: BeezeeDelegatedAccountMode,
    BeezeeDelegatedMnemonicAccountMode.model_config["title"]: BeezeeDelegatedMnemonicAccountMode,
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
    batch_window_ms: int = Field(
        default=CONSTANTS.DEFAULT_BATCH_WINDOW_MS,
        ge=0,
        json_schema_extra={"prompt": "Enter the window in ms in which creates and cancels are batched into one tx"},
    )
    max_messages_per_tx: int = Field(
        default=CONSTANTS.DEFAULT_MAX_MESSAGES_PER_TX,
        ge=1,
        json_schema_extra={"prompt": "Enter the maximum number of order messages in one tx"},
    )
    # Label per denom, over the labels from the asset list and the metadata (see beezee_denom_labels). Set it in the
    # connector config file; it is not prompted.
    denom_labels: Dict[str, str] = Field(default_factory=dict)
    model_config = ConfigDict(title="beezee")

    @field_validator("denom_labels", mode="before")
    @classmethod
    def validate_denom_labels(cls, value: Any, info: ValidationInfo):
        from hummingbot.connector.exchange.beezee.beezee_denom_labels import BeezeeDenomLabels

        labels = {str(denom): str(label) for denom, label in (value or {}).items()}
        network = info.data.get("network")
        BeezeeDenomLabels.for_chain(getattr(network, "chain_id", None) or "", labels)
        return labels

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

    @model_validator(mode="after")
    def validate_delegated_account(self):
        network = self.network
        check_delegated_account(self.account_type, getattr(network, "address_prefix", None) or CONSTANTS.MAINNET_ADDRESS_PREFIX)
        return self


KEYS = BeezeeConfigMap.model_construct()
