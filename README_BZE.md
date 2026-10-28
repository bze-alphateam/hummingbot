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
- `conf/strategies/beezee_*.yml.example`
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

## Pure market making examples

Two ready strategy configs for upstream's `pure_market_making` live in `conf/strategies/`:

| File | Network | Pair |
|------|---------|------|
| `beezee_pmm_testnet.yml.example` | testnet `bzetestnet-3` | `ULMN-TUSDC` |
| `beezee_pmm_bze_usdc.yml.example` | mainnet `beezee-1` | `BZE-ERC20:0XA00C59FF5A080D2B954D0C75E46E22A0C371235A` (BZE/USDC.inj) |

Both quote 5 levels per side, 1% from the mid price and 0.5% apart, refresh every 30 s (orders within 0.2% of
their target stay), keep the inventory near 50/50, wait 30 s after a fill and take the mid price from the BeeZee book
itself. `order_amount` is the size of each order in the base token; set your own. The chain refuses an order smaller
than `ceil(1 / price) × 2` base units, with the price in quote base units per base unit, so check that minimum
against the lowest bid level. A test loads every `*.yml.example` through the client's `import` path, so a misspelled
key or an invalid value fails CI.

A refresh cancels the old levels in one batch tx and, once the chain has confirmed those cancels, places the new ones
in the next (`should_wait_order_cancel_confirmation: true`, upstream's default). With `false`, the cancels and the
creates share one tx, but the wallet then needs enough free balance for both sets of levels, since the chain only
releases a cancelled order's funds at the end of the block.

The pair names are what the connector lists: it names a token after its on-chain denom metadata. On mainnet that
metadata gives USDC.inj the symbol `ERC20:0XA00C…` and no display unit, so the bot counts USDC in base units
(1 = 0.000001 USDC) and shows the BZE price as about 333 instead of 0.000333. Orders are still priced right on chain.
USDC.n lists as `BZE-UUSDC`. To see every pair the connector knows, run `create`, pick `pure_market_making` and
`beezee`, and look at the trading pair autocomplete.

### Seeding the book

Hummingbot reads the mid price from the book and does not start quoting on an empty or one-sided book. Before the
first `start`, make sure the market has at least one bid and one ask: place them from a second wallet in the dex app
(testnet: testnet-dex.getbze.com, mainnet: dex.getbze.com), or let the
[tradebin-mm](https://github.com/faneaatiku/tradebin-mm) stopgap bot quote the market. Prices anchored to the
liquidity pool come later.

## Running the image

Pull the published image, or build it from this repo:

```bash
docker pull ghcr.io/bze-alphateam/hummingbot:bze-latest
# or, from a checkout of this repo:
docker build -t ghcr.io/bze-alphateam/hummingbot:bze-latest .
```

Run it with the config, logs and data on the host, so the encrypted wallet, the strategy and the order tracking state
survive restarts:

```bash
mkdir -p hb/conf/connectors hb/conf/strategies hb/logs hb/data
docker run -it --name hummingbot-bze \
  -v "$PWD/hb/conf:/home/hummingbot/conf" \
  -v "$PWD/hb/logs:/home/hummingbot/logs" \
  -v "$PWD/hb/data:/home/hummingbot/data" \
  ghcr.io/bze-alphateam/hummingbot:bze-latest
```

The client asks for a password the first time; it encrypts the wallet secrets with it. Detach with `Ctrl+P Ctrl+Q`,
come back with `docker attach hummingbot-bze`, start it again after a stop with `docker start -ai hummingbot-bze`.

The kill switch stays off: it is a client setting (`kill_switch_mode` in `conf/conf_client.yml`), disabled by default.

## Testnet run

Network facts (October 2026, chain v8.2.0):

| | |
|---|---|
| Chain id | `bzetestnet-3` |
| REST | `https://testnet.getbze.com` |
| RPC | `https://testnet-rpc.getbze.com` (serves `/block_results` for recent heights, as order tracking needs) |
| Minimum gas price | `0.001ubze` |
| Market | `ibc/9DA252F9…80CC7/factory/bze1z3mkcr2jz424w6m49frgjmy9uhlrx69phqwg3l/testusd`, the only one, listed as `ULMN-TUSDC` |
| Explorer / dex | testnet-explorer.getbze.com / testnet-dex.getbze.com |

1. **Throwaway wallet.** Create a fresh key that never holds real funds, e.g. `bzed keys add hb-testnet
   --keyring-backend test`, and keep the printed mnemonic. Send it some testnet BZE from a test wallet you already
   have (it pays the gas and the tradebin fees). In testnet-dex.getbze.com, swap part of it for ULMN and for TUSDC in
   their pools: a few thousand ULMN and a few TUSDC are plenty for the example config.
2. **Seed the book** if it lacks a bid or an ask (see "Seeding the book"). A second testnet wallet is also what you
   use to fill the bot's orders by hand.
3. **Connect.** In the client run `connect beezee` and answer:
   - network: `testnet`; REST `https://testnet.getbze.com`; RPC `https://testnet-rpc.getbze.com`; websocket: leave
     empty (derived from the RPC); chain id `bzetestnet-3`; prefix `bze`; native denom `ubze`;
   - gas price `0.001` (the node minimum; a higher one works too), gas adjustment `1.3`;
   - account type `wallet_mnemonic`, the throwaway mnemonic, empty passphrase, the default HD path, and the address.

   Then `balance`: it shows BZE, ULMN and TUSDC as in the dex app.
4. **Batching.** `batch_window_ms` (default `300`) and `max_messages_per_tx` (default `25`) are not prompted. To change
   them, edit those two top-level keys in `conf/connectors/beezee.yml` while the client is stopped (leave the
   encrypted fields alone), then start it again.
5. **Strategy.** Copy the example into the mounted config and import it:

   ```bash
   cp conf/strategies/beezee_pmm_testnet.yml.example hb/conf/strategies/beezee_pmm_testnet.yml
   ```

   In the client: `import beezee_pmm_testnet.yml`, then `start`.
6. **Watch.** `status` shows the levels, balances and open orders; `history` the fills with their fees. The log
   (`hb/logs/`) shows each `Beezee batch tx … submitted with N messages` and each order's
   `processed at height … exchange_order_id=…`.
7. **Stop.** `stop` cancels every open order of the strategy, batched into as few txs as `max_messages_per_tx`
   allows. Afterwards open testnet-dex.getbze.com with the bot's address and check that no order of it is left on
   the book; cancel any leftover there.

## Mainnet run

Same steps as the testnet run, with:

- `connect beezee` → network `mainnet`. Its endpoints are built in: REST `https://rest.getbze.com`, RPC
  `https://rpc.getbze.com`, chain id `beezee-1`, gas price `0.01` ubze (the public-node minimum), gas adjustment
  `1.3`. To change the gas price or adjustment, edit `network.gas_price` / `network.gas_adjustment` in
  `conf/connectors/beezee.yml` while the client is stopped.
- A dedicated wallet holding only the BZE and USDC.inj the bot may trade, plus BZE for gas: every tx pays gas, and every
  order pays the tradebin fee when it is created (mainnet: 0.1 BZE taker, 0.001 BZE maker).
- `cp conf/strategies/beezee_pmm_bze_usdc.yml.example hb/conf/strategies/beezee_pmm_bze_usdc.yml`, set `order_amount`,
  then `import beezee_pmm_bze_usdc.yml` and `start`.
- The book must have a bid and an ask (see "Seeding the book"). Cancel-all check after `stop` in dex.getbze.com.

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
- **Cancelling an order that is not on chain yet.** If its create still waits for its batch window, the create is
  taken out of the batch: nothing is sent and the order ends `CANCELED` (so a `stop` during the window places
  nothing). If its create tx is already on its way, the cancel is sent as soon as the chain gives the order its id,
  and a refused batch's single-send fallback skips that create.
- **Fee bookkeeping.** The chain charges the tradebin fee once per order when the create runs, so the connector books
  it on the order's first fill only. An order that rests and never fills still paid the maker fee, which Hummingbot
  cannot record without a fill.
