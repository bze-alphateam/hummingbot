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
SIMULATE_TX_PATH_URL = "/cosmos/tx/v1beta1/simulate"
HALTED_DENOMS_PATH_URL = "/bze/tradebin/halted_denoms"
# CometBFT RPC
RPC_STATUS_PATH_URL = "/status"
BLOCK_RESULTS_PATH_URL = "/block_results"

SIDE_BUY = "buy"
SIDE_SELL = "sell"

ORDER_ID_LENGTH = 24
TX_HASH_LENGTH = 64

# Order tracking from the chain v8.2.0 queue `message_id` events.
TRADEBIN_EVENT_PREFIX = "bze.tradebin."
ORDER_CREATE_MESSAGE_EVENT = "bze.tradebin.OrderCreateMessageEvent"
ORDER_CANCEL_MESSAGE_EVENT = "bze.tradebin.OrderCancelMessageEvent"
ORDER_EXECUTED_EVENT = "bze.tradebin.OrderExecutedEvent"
ORDER_SAVED_EVENT = "bze.tradebin.OrderSavedEvent"
ORDER_CANCELED_EVENT = "bze.tradebin.OrderCanceledEvent"
# A queued message's outcome is looked for in its tx block and the 19 blocks after it.
ORDER_OUTCOME_BLOCK_WINDOW = 20
# A cancel of an order whose create tx is on its way waits this long (s) for the order's chain id before it is
# deferred; below cancel_all's 20 s at stop, so a stop right after a refresh still cancels its orders.
CANCEL_WAIT_FOR_ORDER_ID = 15.0
# Tx lookup after a broadcast: wait 1 s, 2 s, 3 s, then every 3 s, at most 60 s in total.
TX_LOOKUP_DELAYS = (1.0, 2.0, 3.0) + (3.0,) * 18
# Fills missed while the bot was down are read back from at most this many blocks.
MAX_BLOCK_CATCH_UP = 1000
# A block whose results cannot be read this many times in a row is skipped (pruned or refused by the node).
MAX_BLOCK_READ_ATTEMPTS = 3
BLOCK_EVENTS_CACHE_SIZE = 64
# Reserved tracking-states key holding the block cursor and the in-flight tx messages.
TRACKING_STATE_KEY = "__beezee_block_tracking__"

DEFAULT_HISTORY_LIMIT = 200
DEFAULT_MARKET_PAGE_SIZE = 200
DEFAULT_ORDER_BOOK_DEPTH = 200
DEFAULT_PRICE_SIG_DIGITS = 10
DEFAULT_ORDER_BOOK_POLL_INTERVAL = 5.0
DEFAULT_BLOCK_STREAM_TIMEOUT = 20.0

# Mainnet public-node minimum gas price (ubze per gas unit) on chain v8.2.0.
DEFAULT_GAS_PRICE = Decimal("0.01")
DEFAULT_GAS_ADJUSTMENT = Decimal("1.3")
# Simulation runs against the last committed state, where the tradebin queue is empty. In a block, every create pays
# extra gas for each message already queued on its market: the tradebin `orderBookQueueMessageScanExtraGas` param
# plus the store reads of that message (SDK KV gas: a flat cost per iterator step and a cost per key and value byte).
DEFAULT_QUEUE_MESSAGE_SCAN_GAS = 5000
KV_ITER_NEXT_COST_FLAT = 30
KV_READ_COST_PER_BYTE = 3
# A queued message's store key and value hold its market id twice; this covers the rest (owner, price, amount, ids).
QUEUE_MESSAGE_FIXED_BYTES = 200
# Creates and cancels arriving within the window share one tx, at most this many messages per tx.
DEFAULT_BATCH_WINDOW_MS = 300
DEFAULT_MAX_MESSAGES_PER_TX = 25
BATCH_MEMO = "hbot-batch"
# Legacy fixed gas limits, replaced by tx simulation. Still accepted (and ignored) in saved configs.
LEGACY_GAS_LIMIT_CONFIG_KEYS = ("create_order_gas_limit", "cancel_order_gas_limit")

HALTED_DENOMS_REFRESH_INTERVAL = 60.0
TRADEBIN_CODESPACE = "tradebin"
DENOM_HALTED_ERROR_CODE = 4017
DENOM_HALTED_ERROR_MESSAGE = "denom is halted"
ACCOUNT_SEQUENCE_MISMATCH_MESSAGE = "account sequence mismatch"

GENERAL_LIMIT_ID = "beezee_general"
QUERY_LIMIT_ID = "beezee_query"
TX_LIMIT_ID = "beezee_tx"
BLOCK_STREAM_LIMIT_ID = "beezee_block_stream"
RPC_LIMIT_ID = "beezee_rpc"

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
    RateLimit(limit_id=RPC_LIMIT_ID, limit=300, time_interval=MINUTE),
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
    RateLimit(limit_id=SIMULATE_TX_PATH_URL, limit=120, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(QUERY_LIMIT_ID, 1)]),
    RateLimit(limit_id=HALTED_DENOMS_PATH_URL, limit=120, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(QUERY_LIMIT_ID, 1)]),
    RateLimit(limit_id=BROADCAST_TX_PATH_URL, limit=30, time_interval=MINUTE, linked_limits=[LinkedLimitWeightPair(TX_LIMIT_ID, 1)]),
]
