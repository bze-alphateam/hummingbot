# Beezee Discovery Notes

## Summary

Beezee DEX is an on-chain spot CLOB implemented by the Beezee chain's custom `tradebin` Cosmos SDK module. It is not a Binance-style API exchange.

## Interfaces found

From Beezee chain source `proto/bze/tradebin/query.proto` and generated OpenAPI:

- Markets: `GET /bze/tradebin/all_markets`
- Single market: `GET /bze/tradebin/market`
- Aggregated order book: `GET /bze/tradebin/market_aggregated_orders`
- Market history / public trades: `GET /bze/tradebin/market_history`
- User open orders: `GET /bze/tradebin/user_market_orders/{address}`
- Single order lookup: `GET /bze/tradebin/market_order`
- User dust balances: `GET /bze/tradebin/all_user_dust`

Generic Cosmos endpoints used:

- Balances: `GET /cosmos/bank/v1beta1/balances/{address}`
- Denom metadata: `GET /cosmos/bank/v1beta1/denoms_metadata/{denom}`
- Account number and sequence: `GET /cosmos/auth/v1beta1/accounts/{address}`
- Broadcast tx: `POST /cosmos/tx/v1beta1/txs`
- Query tx: `GET /cosmos/tx/v1beta1/txs/{hash}`
- Latest block: `GET /cosmos/base/tendermint/v1beta1/blocks/latest`

## Trading/auth model

Beezee trading is wallet-signed and on-chain.

From `proto/bze/tradebin/tx.proto`:

- `MsgCreateOrder`
- `MsgCancelOrder`

No Beezee API-key trading flow was found in the chain source.

## Realtime findings

- Beezee has generic CometBFT RPC websocket support through RPC nodes.
- No dedicated Beezee public or private exchange websocket feed was found.
- v1 uses new-block websocket triggers when available and polling fallback otherwise.

## Trading rule findings

Beezee `Market` objects only contain:

- `base`
- `quote`
- `creator`

No explicit per-market tick size, lot size, or min order size is stored on-chain.

Derived from chain code:

- Base increment: 1 minimal base unit
- Quote increment: 1 minimal quote unit
- Price precision: practical 10 decimal places in chain price units because storage keys normalize prices via `%024.10f`
- Minimum amount: dynamic, enforced by `CalculateMinAmount(price) = ceil(1 / price) * 2` in base minimal units
- Fees: module params `market_maker_fee` and `market_taker_fee` are fixed native-denom coin amounts

## Order lifecycle findings

- `MsgCreateOrder` does not return an order id.
- Orders are first queued and then processed in `tradebin` `EndBlock`.
- Order ids are generated only when an order is actually saved to the book.
- Because of that, client order id to Beezee order id mapping requires post-broadcast polling of user open orders.
- `HistoryOrder` records fills without order ids, only market, side, price, amount, maker, taker, and timestamp.

## Current chain metadata

From current Cosmos chain registry:

- chain id: `beezee-1`
- bech32 prefix: `bze`
- native denom: `ubze`
- REST: `https://rest.getbze.com`
- RPC: `https://rpc.getbze.com`
- Websocket: `wss://rpc.getbze.com/websocket`

Additional testnet finding from the Beezee source repository:

- upstream Beezee repo includes testnet genesis files up to `bzetestnet-3`
- no stable public testnet REST/RPC/WebSocket URLs were confirmed from the local Beezee sources used for this connector draft
