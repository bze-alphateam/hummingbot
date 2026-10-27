# Beezee Connector

This connector integrates Beezee DEX spot markets through the Beezee chain `tradebin` module.

## v1 support

- Market discovery from `/bze/tradebin/all_markets`
- Trading rule synthesis from denom metadata and `tradebin` params
- Order book snapshots
- Block-driven polling diffs and public trade updates
- Bank balances and Beezee dust balances
- Limit buy and sell order creation with Cosmos tx signing
- Order cancellation with Cosmos tx signing
- Open order polling and order status tracking

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
- `HistoryOrder` objects do not include order ids. When the same address has multiple resting orders at the same market, side, and price, fills are allocated FIFO client-side.
- Tick size and minimum order size are not explicit per-market fields on-chain. v1 derives them from denom exponents and the chain's `CalculateMinAmount(price)` logic.
- Maker/taker fees are fixed amounts on-chain, not percentages. The connector models them as flat fees (see "Gas and fees"). Gas remains an external tx cost.
- Bank balances and `all_user_dust` are available, but no dedicated reserved-balance endpoint was found for open orders.
- Some token metadata may be unavailable from public testnet endpoints. The connector falls back to raw or synthesized metadata when required, especially for the native `ubze` denom.

## Gas and fees

- Before every broadcast the connector simulates the tx (`/cosmos/tx/v1beta1/simulate`). The gas limit is
  `ceil(gas_used * gas_adjustment)` and the fee is `ceil(gas_limit * gas_price)` in the native denom.
- A tx whose simulation fails is never broadcast; the order fails with the chain's message. An account sequence
  mismatch resets the cached sequence and retries once. Txs are sent one at a time from sequence reservation to
  broadcast, so orders placed together (a bid and an ask) get consecutive sequences.
- `gas_price` defaults to `0.01` (ubze per gas unit), `gas_adjustment` to `1.3`. Raise `gas_price` if your node
  requires a higher minimum.
- The old `create_order_gas_limit` / `cancel_order_gas_limit` settings are gone; saved configs that still have them load
  and ignore them.
- Tradebin maker/taker fees come from `/bze/tradebin/params` (Coin objects on chain v8.2.0, `"1000ubze"` strings on older
  chains) and are converted with the fee denom's metadata exponent.

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

## Safe/dev workflow

Use `network=custom` to point the connector at a local or dev Beezee node. Wallet mode signs standard Cosmos protobuf transactions directly against the configured REST endpoint.
