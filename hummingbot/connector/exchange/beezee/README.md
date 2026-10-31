# Beezee Connector

This connector integrates Beezee DEX spot markets through the Beezee chain `tradebin` module.

## v1 support

- Pair discovery from `/bze/tradebin/all_markets` and `/bze/tradebin/all_liquidity_pools`: a pair has an order-book
  market, a liquidity pool, or both (the two are unrelated on chain)
- Readable denom labels as on the BeeZee dapps: the `denom_labels` config override, then the chain-registry asset
  list (`beezee_denom_labels.py`), then the bank metadata; labels are unique per chain (see `README_BZE.md`, "Denom
  labels")
- Trading rule synthesis from those labels' decimals and `tradebin` params
- Order book snapshots
- Block-driven polling diffs and public trade updates
- Bank balances and Beezee dust balances
- Limit buy and sell order creation with Cosmos tx signing
- Order cancellation with Cosmos tx signing
- Exact order tracking from the chain's order events (queue `message_id` and order id, chain v8.2.0+)

## Validated state

Validated on Beezee testnet:

- connector startup in standalone Hummingbot
- market discovery and trading pair mapping
- bank balance retrieval
- order book initialization
- signed order submission
- signed order cancellation

Validated wallet modes:

- `account_type=wallet`
- `account_type=wallet_mnemonic` (BIP39 to BIP44 derivation checked against `bzed` with the public BIP39 test mnemonic)

## Configuration model

The Beezee connector is configured through Hummingbot connector settings under `conf/connectors/beezee.yml`.

Important:

- secure fields such as `private_key`, `mnemonic`, and `passphrase` should be entered through `connect beezee`
- let Hummingbot write and encrypt those values
- do not hand-edit encrypted secrets into the file unless you know Hummingbot's secure config format

The connector supports these network modes:

- `network=mainnet` to use Beezee mainnet defaults
- `network=testnet` to use Beezee testnet chain defaults with user-supplied testnet endpoints
- `network=custom` to point at a local/dev Beezee REST, RPC, and websocket endpoint

The connector supports these account modes:

- `account_type=read_only` for market data and balances only
- `account_type=wallet` for signed order placement and cancellation from a raw private key
- `account_type=wallet_mnemonic` for signed order placement and cancellation from a BIP39 mnemonic

## Recommended setup

For the currently validated path, use:

- `network=testnet`
- `account_type=wallet`

Wallet mode requires:

- a 32-byte secp256k1 private key in hex
- an explicit Beezee address

Gas is not configured per tx type: every create and cancel is simulated first (see "Gas and fees" below).

Recommended Beezee testnet values:

- `chain_id=bzetestnet-3`
- `address_prefix=bze`
- `native_denom=ubze`
- `gas_price=0.01`
- `gas_adjustment=1.3`

The current Beezee testnet endpoints used during validation were:

- REST: `https://testnet-api.bze.chaintools.tech:443`
- RPC: `https://testnet-rpc.bze.chaintools.tech:443`
- websocket: `wss://testnet-rpc.bze.chaintools.tech:443/websocket`

## Example config shape

The effective config shape Hummingbot expects is flattened by selected mode.

Example `wallet` mode:

```yaml
connector: beezee
receive_connector_configuration: true

network:
  rest_endpoint: "https://testnet-api.bze.chaintools.tech:443"
  rpc_endpoint: "https://testnet-rpc.bze.chaintools.tech:443"
  websocket_endpoint: "wss://testnet-rpc.bze.chaintools.tech:443/websocket"
  chain_id: "bzetestnet-3"
  address_prefix: "bze"
  native_denom: "ubze"
  gas_price: "0.01"
  gas_adjustment: "1.3"

account_type:
  name: "wallet"
  private_key: "replace-with-32-byte-hex-private-key"
  address: "bze1..."
```

Example `read_only` mode:

```yaml
connector: beezee
receive_connector_configuration: true

network:
  rest_endpoint: "https://testnet-api.bze.chaintools.tech:443"
  rpc_endpoint: "https://testnet-rpc.bze.chaintools.tech:443"
  websocket_endpoint: "wss://testnet-rpc.bze.chaintools.tech:443/websocket"
  chain_id: "bzetestnet-3"
  address_prefix: "bze"
  native_denom: "ubze"
  gas_price: "0.01"
  gas_adjustment: "1.3"

account_type:
  name: "read_only"
  address: "bze1..."
```

Example `wallet_mnemonic` mode:

```yaml
connector: beezee
receive_connector_configuration: true

network:
  rest_endpoint: "https://testnet-api.bze.chaintools.tech:443"
  rpc_endpoint: "https://testnet-rpc.bze.chaintools.tech:443"
  websocket_endpoint: "wss://testnet-rpc.bze.chaintools.tech:443/websocket"
  chain_id: "bzetestnet-3"
  address_prefix: "bze"
  native_denom: "ubze"
  gas_price: "0.01"
  gas_adjustment: "1.3"

account_type:
  name: "wallet_mnemonic"
  mnemonic: "replace with your 12 or 24 word mnemonic"
  passphrase: ""
  hd_path: "m/44'/118'/0'/0/0"
  address: "bze1..."
```

Notes:

- The mnemonic mode derives the wallet key using the Cosmos-standard default path `m/44'/118'/0'/0/0`.
- The built-in testnet preset currently only hardcodes the Beezee testnet chain settings confirmed from the upstream Beezee repo, especially `chain_id=bzetestnet-3`. REST/RPC/WS endpoints still need to be provided explicitly because no stable public testnet endpoints were confirmed in the local Beezee sources.

## Limitations

- Beezee does not expose a dedicated private stream, so v1 is polling-based for balances and order state.
- `HistoryOrder` objects do not include order ids, so the connector does not use them for its own fills (see "Order tracking").
- v8.2.0 emits no event for a queued create that the chain refunds without a fill (for example, no match for a remainder below the minimum amount). Such an order is marked failed once a later tx reuses its message id, or after 20 blocks.
- Tick size and minimum order size are not explicit per-market fields on-chain. v1 derives them from denom exponents and the chain's `CalculateMinAmount(price)` logic.
- Maker/taker fees are fixed amounts on-chain, not percentages. The connector models them as flat fees (see "Gas and fees"). Gas remains an external tx cost.
- Bank balances and `all_user_dust` are available, but no dedicated reserved-balance endpoint was found for open orders.
- Some token metadata may be unavailable from public testnet endpoints. The connector falls back to raw or synthesized metadata when required, especially for the native `ubze` denom.

## Gas and fees

- Before every broadcast the connector simulates the tx (`/cosmos/tx/v1beta1/simulate`). The gas limit is
  `ceil(gas_used * gas_adjustment)` plus a queue extra, and the fee is `ceil(gas_limit * gas_price)` in the native
  denom.
- Queue extra: in a block, each create pays tradebin's `orderBookQueueMessageScanExtraGas` (5000) plus store reads for
  every message queued before it on its market. The simulation runs on committed state and cannot see them, so each
  create gets that gas added once per message of this account on the same market whose tx is not in a block yet.
- A tx whose simulation fails is never broadcast; the order fails with the chain's message. An account sequence
  mismatch resets the cached sequence and retries once. Txs are sent one at a time from sequence reservation to
  broadcast, so orders placed together (a bid and an ask) get consecutive sequences.
- `gas_price` defaults to `0.01` (ubze per gas unit), `gas_adjustment` to `1.3`. Raise `gas_price` if your node
  requires a higher minimum.
- The old `create_order_gas_limit` / `cancel_order_gas_limit` settings are gone; saved configs that still have them load
  and ignore them.
- Tradebin maker/taker fees come from `/bze/tradebin/params` (Coin objects on chain v8.2.0, `"1000ubze"` strings on older
  chains) and are converted with the fee denom's metadata exponent.
- The chain charges the tradebin fee once per order, when the create tx runs (taker fee if an opposite order rests at
  that exact price, else maker fee; cancels are free). The connector books it on the order's first fill only: taker fee
  when that fill comes while the create is processed, else maker fee. An order that rests and never fills still paid
  the maker fee, which Hummingbot cannot record without a fill.

## Halted markets

Governance can halt a denom on the DEX. A market is halted when its base or quote denom is halted: cancels still work,
new orders do not.

- The connector reads `/bze/tradebin/halted_denoms` at start and every 60 s.
- An order on a halted market is refused before signing.
- When a tracked market becomes halted, one warning is logged and its open orders are cancelled. While the market
  stays halted, every status poll re-issues the cancel for orders that are still open and have no cancel in flight
  (an order whose id was not known yet, a cancel that failed, or a create queued before the halt that executed).
- A tx refused with tradebin code 4017 (`denom is halted`) marks the market halted at once.
- When the denom is no longer halted, the connector logs it once and trades again.

## Order tracking

Tradebin queues every create and cancel and executes them in `EndBlock`, in the tx block or a later one. Chain v8.2.0
tags each queued message with a `message_id` (24 digits; reused once the queue is empty, which happens almost every
block) and puts it on every outcome event.

- After a broadcast the connector polls `GET /cosmos/tx/v1beta1/txs/{hash}` (1 s, 2 s, 3 s, then every 3 s, at most
  60 s). The i-th `OrderCreateMessageEvent` of the account maps to the i-th create message of the tx. Only the node's
  "not found" answer counts toward the 60 s; a failed request is retried on the next status poll without failing
  anything. A cancel tx not found in time leaves the order open. A create tx not found in time takes an order of the
  account on the book with the same market, side, amount and price that no tracked order claims, else it fails.
- It then reads RPC `/block_results` from the tx height on, at most 20 blocks, for that `message_id` with the order's
  market and side: `OrderExecutedEvent` with the account as taker is a fill, `OrderSavedEvent` gives the order id and
  rests the order, `OrderCanceledEvent` confirms a cancel. A tx event that reuses the same id at a later height proves
  the message was already processed.
- Outcomes: saved → `OPEN` (`PARTIALLY_FILLED` with taker fills first); taker fills covering the amount and no save →
  `FILLED`. Taker fills short of the amount and no save means the chain refunded the rest: the order ends `CANCELED`
  with those fills, not `FILLED`.
- Every block (websocket `NewBlock`, with the status poll as fallback) is read once in order, missed heights included.
  An `OrderExecutedEvent` whose `id` is one of the account's resting orders is a maker fill of that order, and an
  `OrderCanceledEvent` of the account with that `id` cancels it (order ids are never reused): a cancel made outside the
  bot, or one sent just before a stop whose state was saved earlier, is seen in its block.
- A block that fails to load 3 times in a row (pruned or refused by the node) is skipped with a warning. Creates and
  cancels whose outcome block it was are settled from the order book, and resting orders take their fills through
  that block from the chain state at that height.
- A cancel with no event within 20 blocks: the order is checked against `user_market_orders`. Gone means it was filled
  first (`FILLED` when the fills cover it, else `CANCELED`); still there means the cancel did not take effect.
- The block cursor and the queued messages are saved with the order states. After a restart the connector resumes
  queued creates from their tx, checks resting orders against `user_market_orders` and reads back missed fills, up to
  1000 blocks. It never reads further back. Beyond that, queued creates and cancels are settled from the order book.
  A resting order still on the book gets the amount it lost as one fill at the order price. An order gone from the
  book has an unknown history, so it is closed as `CANCELED` without a fill. Each case logs a warning.

## Safe/dev workflow

Use `network=custom` to point the connector at a local or dev Beezee node. Wallet mode signs standard Cosmos protobuf transactions directly against the configured REST endpoint.
