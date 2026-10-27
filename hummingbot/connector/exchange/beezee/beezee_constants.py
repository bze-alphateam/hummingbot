from decimal import Decimal

from hummingbot.connector.constants import MINUTE
from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "beezee"
DEFAULT_DOMAIN = "mainnet"

HBOT_ORDER_ID_PREFIX = "BZE"
MAX_ORDER_ID_LEN = 32

MAINNET_REST_URL = "https://rest.getbze.com"
MAINNET_RPC_URL = "https://rpc.getbze.com"
MAINNET_WS_URL = "wss://rpc.getbze.com/websocket"
MAINNET_CHAIN_ID = "beezee-1"
MAINNET_ADDRESS_PREFIX = "bze"
MAINNET_NATIVE_DENOM = "ubze"

TESTNET_CHAIN_ID = "bzetestnet-3"
TESTNET_ADDRESS_PREFIX = "bze"
TESTNET_NATIVE_DENOM = "ubze"

PING_PATH_URL = "/cosmos/base/tendermint/v1beta1/blocks/latest"
TRADING_PAIRS_PATH_URL = "/bze/tradebin/all_markets"
TRADING_RULES_PATH_URL = "/bze/tradebin/all_markets"
TRADEBIN_PARAMS_PATH_URL = "/bze/tradebin/params"
MARKET_AGGREGATED_ORDERS_PATH_URL = "/bze/tradebin/market_aggregated_orders"
MARKET_HISTORY_PATH_URL = "/bze/tradebin/market_history"
MARKET_ORDER_PATH_URL = "/bze/tradebin/market_order"
ALL_USER_DUST_PATH_URL = "/bze/tradebin/all_user_dust"
BALANCES_PATH_URL = "/cosmos/bank/v1beta1/balances"
DENOM_METADATA_PATH_URL = "/cosmos/bank/v1beta1/denoms_metadata"
DENOM_METADATA_BY_QUERY_PATH_URL = "/cosmos/bank/v1beta1/denoms_metadata_by_query_string"
ACCOUNT_INFO_PATH_URL = "/cosmos/auth/v1beta1/accounts"
GET_TX_PATH_URL = "/cosmos/tx/v1beta1/txs"
BROADCAST_TX_PATH_URL = "/cosmos/tx/v1beta1/txs"

SIDE_BUY = "buy"
SIDE_SELL = "sell"

ORDER_ID_LENGTH = 24
TX_HASH_LENGTH = 64
ORDER_ID_RECOVERY_TIME_TOLERANCE = 120

DEFAULT_HISTORY_LIMIT = 200
DEFAULT_MARKET_PAGE_SIZE = 200
DEFAULT_ORDER_BOOK_DEPTH = 200
DEFAULT_PRICE_SIG_DIGITS = 10
DEFAULT_ORDER_BOOK_POLL_INTERVAL = 5.0
DEFAULT_BLOCK_STREAM_TIMEOUT = 20.0

DEFAULT_GAS_PRICE = Decimal("0.025")
DEFAULT_CREATE_ORDER_GAS_LIMIT = 250000
DEFAULT_CANCEL_ORDER_GAS_LIMIT = 200000
DEFAULT_GAS_ADJUSTMENT = Decimal("1.20")

GENERAL_LIMIT_ID = "beezee_general"
QUERY_LIMIT_ID = "beezee_query"
TX_LIMIT_ID = "beezee_tx"
BLOCK_STREAM_LIMIT_ID = "beezee_block_stream"

ORDER_STATE = {
    "PENDING_CREATE": OrderState.PENDING_CREATE,
    "OPEN": OrderState.OPEN,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "PENDING_CANCEL": OrderState.PENDING_CANCEL,
    "CANCELED": OrderState.CANCELED,
    "FILLED": OrderState.FILLED,
    "FAILED": OrderState.FAILED,
}

RATE_LIMITS = [
    RateLimit(limit_id=GENERAL_LIMIT_ID, limit=120, time_interval=MINUTE),
    RateLimit(limit_id=QUERY_LIMIT_ID, limit=120, time_interval=MINUTE),
    RateLimit(limit_id=TX_LIMIT_ID, limit=30, time_interval=MINUTE),
    RateLimit(limit_id=BLOCK_STREAM_LIMIT_ID, limit=5, time_interval=MINUTE),
    RateLimit(limit_id=PING_PATH_URL, limit=120, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(GENERAL_LIMIT_ID, 1)]),
    RateLimit(limit_id=TRADING_PAIRS_PATH_URL, limit=120, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(QUERY_LIMIT_ID, 1)]),
    RateLimit(limit_id=TRADEBIN_PARAMS_PATH_URL, limit=120, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(QUERY_LIMIT_ID, 1)]),
    RateLimit(limit_id=MARKET_AGGREGATED_ORDERS_PATH_URL, limit=120, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(QUERY_LIMIT_ID, 1)]),
    RateLimit(limit_id=MARKET_HISTORY_PATH_URL, limit=120, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(QUERY_LIMIT_ID, 1)]),
    RateLimit(limit_id=MARKET_ORDER_PATH_URL, limit=120, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(QUERY_LIMIT_ID, 1)]),
    RateLimit(limit_id=ALL_USER_DUST_PATH_URL, limit=120, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(QUERY_LIMIT_ID, 1)]),
    RateLimit(limit_id=BALANCES_PATH_URL, limit=120, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(QUERY_LIMIT_ID, 1)]),
    RateLimit(limit_id=DENOM_METADATA_PATH_URL, limit=120, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(QUERY_LIMIT_ID, 1)]),
    RateLimit(limit_id=DENOM_METADATA_BY_QUERY_PATH_URL, limit=120, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(QUERY_LIMIT_ID, 1)]),
    RateLimit(limit_id=ACCOUNT_INFO_PATH_URL, limit=120, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(QUERY_LIMIT_ID, 1)]),
    RateLimit(limit_id=GET_TX_PATH_URL, limit=120, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(QUERY_LIMIT_ID, 1)]),
    RateLimit(limit_id=BROADCAST_TX_PATH_URL, limit=30, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(TX_LIMIT_ID, 1)]),
]
