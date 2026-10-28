# Hummingbot for BeeZee (BZE)

This is the BeeZee team's fork of [Hummingbot](https://github.com/hummingbot/hummingbot). It is upstream Hummingbot plus the
**BeeZee** spot connector for the BZE DEX order book (`tradebin` module), and it ships as one Docker image:
`ghcr.io/bze-alphateam/hummingbot`.

The connector was written by [qf3l3k](https://github.com/qf3l3k) in
[qf3l3k/hummingbot_beezee](https://github.com/qf3l3k/hummingbot_beezee) and imported here from v0.4.0 (`f9b169f`).
Its own documentation lives in [`hummingbot/connector/exchange/beezee/README.md`](hummingbot/connector/exchange/beezee/README.md),
with an example of the config shape in `hummingbot/connector/exchange/beezee/beezee.example.yml`.

## Branch model

- **`bze`**: the default branch. It is an upstream release tag plus our changes. Images are built from it.
- **`master`**: a pure mirror of upstream `master`. Never commit to it.
- Work goes on a feature branch off `bze`, with a PR into `bze`.

Our changes stay in a few places, so syncing upstream stays a clean merge:

- `hummingbot/connector/exchange/beezee/` and `test/hummingbot/connector/exchange/beezee/`
- `scripts/beezee_*` and `controllers/**/beezee_*`
- `README_BZE.md` and `.github/workflows/bze-*.yml`
- small edits to upstream files: dependencies (`setup.py`, `setup/environment.yml`) and a repository guard on
  upstream's Docker Hub workflow so it never runs in this fork

## Syncing upstream

When upstream publishes a new release tag (e.g. `v2.18.0`):

```bash
git fetch upstream --tags
git checkout -b chore/sync-v2.18.0 origin/bze
git merge v2.18.0
# resolve conflicts (normally only setup.py / setup/environment.yml), then
git push -u origin chore/sync-v2.18.0
```

Open a PR into `bze`. CI runs the lint, the full upstream test suite and the image build. After the merge, the new image
is published.

## Image

Every push to `bze` publishes:

- `ghcr.io/bze-alphateam/hummingbot:<first 8 chars of the commit>`
- `ghcr.io/bze-alphateam/hummingbot:bze-latest`

Pull requests to `bze` build the image without pushing it.

## Running

Use upstream's `docker-compose.yml` with our image:

```yaml
services:
  hummingbot:
    image: ghcr.io/bze-alphateam/hummingbot:bze-latest
    # the rest as in upstream docker-compose.yml (conf, logs, data, certs volumes; tty + stdin_open)
```

```bash
docker compose up -d
docker attach hummingbot
```

Inside the Hummingbot client:

1. `connect beezee`, then answer the prompts: network (`mainnet`, `testnet` or `custom`), account type (`wallet`,
   `wallet_mnemonic` or `read_only`), the key or mnemonic, and the address. Hummingbot encrypts the secrets into
   `conf/connectors/beezee.yml`. Never edit them in by hand.
2. `balance` to check the account.
3. Create a strategy or a script with `beezee` as the exchange, e.g. the pair `BZE-USDC`.

Use a dedicated trading wallet that holds only the funds the bot should trade with.

## Connector behaviour on chain v8.2.0

- **Wallet modes.** `wallet` takes a hex private key. `wallet_mnemonic` takes a BIP39 mnemonic (optional passphrase)
  and derives the key on `hd_path`, default `m/44'/118'/0'/0/0`, the same as `bzed keys add --recover`. The
  derivation is pinned by a test against the address `bzed` v8.2.0 recovers from the public BIP39 test mnemonic.
- **Gas.** Every create and cancel is simulated first. Gas limit = `ceil(gas_used × gas_adjustment)`, fee =
  `ceil(gas limit × gas_price)` in ubze. Defaults: `gas_price` `0.01` (the mainnet public-node minimum),
  `gas_adjustment` `1.3`. Both are network settings. A failed simulation is never broadcast. The fixed
  `create_order_gas_limit` / `cancel_order_gas_limit` settings are gone, and old configs that still have them load
  and ignore them.
- **Trading fees.** The tradebin maker/taker fees (flat amounts, in ubze on mainnet) are read from the chain params
  and added to every order's fee estimate, so PnL and profitability checks include them.
- **Halted markets.** A market whose base or quote denom is halted by governance is exit-only: the connector refuses
  new orders on it, cancels its open orders (and keeps cancelling any still open while the halt lasts), and trades
  again once the halt is lifted. The halted list is read at start and every 60 s, and a tx refused with
  `denom is halted` takes effect at once.
- **Order tracking.** The order book is asynchronous: a tx only queues the order, and the chain executes it in that
  block or a later one. The connector looks up each create/cancel tx for its queue `message_id`, then reads the blocks
  (RPC `/block_results`) for the events carrying that id: fills, the saved order and its id, the cancel. A resting
  order's later fills are matched by its order id. An order with no outcome within 20 blocks of its tx is marked
  failed. A partial fill whose rest the chain refunds ends `CANCELED` with its fills. After a restart, missed fills
  are read back from the last processed block (up to 1000 blocks); beyond that the connector reconciles resting
  orders from the chain and logs a warning. The RPC endpoint must serve `/block_results` for recent heights.
- **Batched txs.** Creates and cancels go out in batch txs: all messages submitted within `batch_window_ms`
  (default `300`) of the first one share one tx, simulated, signed and broadcast once, with at most
  `max_messages_per_tx` messages (default `25`; a larger batch is split into consecutive txs). Cancels come first, so
  stale levels leave the book in the same tx that places the new ones. `batch_order_create` / `batch_order_cancel`
  send their tx as soon as every order of the call is submitted, without waiting for the window. If the chain refuses
  a batch in simulation, the creates on a market it names as halted are failed and the rest is simulated once more;
  any other refusal sends each message in its own tx, so only the bad order fails with the chain's error. A sequence
  mismatch re-signs the whole batch once. The 30 tx/min limit counts batch txs, and the messages of one tx share one
  tx lookup. Both settings are top-level keys of the connector config (`batch_window_ms: 0` still batches whatever
  is submitted together, but waits for nothing).
- **Fee bookkeeping.** The chain charges the tradebin fee once per order when the create runs, so the connector books
  it on the order's first fill only. An order that rests and never fills still paid the maker fee, which Hummingbot
  cannot record without a fill.
