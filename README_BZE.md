# Hummingbot for BeeZee (BZE)

This is the BeeZee team's fork of [Hummingbot](https://github.com/hummingbot/hummingbot). It is upstream Hummingbot plus the
**BeeZee** spot connector for the BZE DEX order book (`tradebin` module), and it ships as one Docker image:
`ghcr.io/bze-alphateam/hummingbot`.

The connector was written by [qf3l3k](https://github.com/qf3l3k) in
[qf3l3k/hummingbot_beezee](https://github.com/qf3l3k/hummingbot_beezee) and imported here from v0.4.0 (`f9b169f`).
Its own documentation lives in [`hummingbot/connector/exchange/beezee/README.md`](hummingbot/connector/exchange/beezee/README.md),
with an example of the config shape in `hummingbot/connector/exchange/beezee/beezee.example.yml` (delegated trading:
`beezee.delegated.example.yml` beside it).

## Branch model

- **`bze`**: the default branch. It is an upstream release tag plus our changes. Images are built from it.
- **`master`**: a pure mirror of upstream `master`. Never commit to it.
- Work goes on a feature branch off `bze`, with a PR into `bze`.

Our changes stay in a few places, so syncing upstream stays a clean merge:

- `hummingbot/connector/exchange/beezee/` and `test/hummingbot/connector/exchange/beezee/`
- `scripts/beezee_*` and `controllers/**/beezee_*`
- `conf/strategies/beezee_*.yml.example`, `conf/scripts/beezee_*.yml.example`, `conf/controllers/beezee_*.yml.example`
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
   `wallet_mnemonic`, `delegated`, `delegated_mnemonic` or `read_only`), the key or mnemonic, and the address. Hummingbot encrypts the secrets into
   `conf/connectors/beezee.yml`. Never edit them in by hand.
2. `balance` to check the account.
3. Create a strategy or a script with `beezee` as the exchange, e.g. the pair `BZE-USDC.INJ` (see "Denom labels").

Use a dedicated trading wallet that holds only the funds the bot should trade with.

### Delegated account

In the two delegated modes the funds stay in the user's wallet (the **granter**) and a separate **bot key** (the
grantee) signs. The bot key sends the wallet's tradebin messages, each created by the wallet, in one authz `MsgExec`
per tx, and the tx's fee granter is the wallet, so the wallet's feegrant allowance pays the gas. The bot key needs no
funds, and its own balance is never read.

- `delegated`: `private_key` (the bot key, hex) and `granter_address`.
- `delegated_mnemonic`: `mnemonic`, `passphrase` and `hd_path` (the bot key, derived as in `wallet_mnemonic`) and
  `granter_address`.

`granter_address` is required, must carry the network's prefix (`bze1...`) and must not be the bot key's own address.
Balances, open orders, fills and events are the granter's; the account number and sequence are the bot key's. The
startup log names both: `account <granter> (delegated: bot key <grantee>, gas paid by the account's feegrant)`. Both
modes can be set in `conf/connectors/beezee.yml` without prompts.

The rule of "One wallet per bot" (see the AMM run guide) holds here too: one granter wallet = one bot, and the user
does not trade the bot's pair by hand from that wallet while it runs (a restarted bot adopts every open order of the
account on its pair). Every bot gets its own bot key, never shared between bots. Without the grants (authz for the
tradebin messages, a feegrant allowance that allows `/cosmos.authz.v1beta1.MsgExec`) the chain refuses the
simulation and nothing is sent. The grant, check and revoke commands follow in "Delegated trading" below.

#### When grants are revoked, expire or run out

The bot needs, from the account (granter) to its bot key (grantee):

- an authz `GenericAuthorization` for each of `/bze.tradebin.MsgCreateOrder`, `/bze.tradebin.MsgCancelOrder`,
  `/bze.tradebin.MsgMultiSwap` and `/bze.tradebin.MsgFillOrders`, all four whatever the strategy;
- one feegrant allowance: a `BasicAllowance`, a `PeriodicAllowance`, or an `AllowedMsgAllowance` wrapping either that
  lists `/cosmos.authz.v1beta1.MsgExec` (the chain checks the tx's outer message, so a list of tradebin messages alone
  refuses every tx). It must not be expired and must have ubze left to spend (an empty spend limit is unlimited).

The connector reads them before it reports ready, every 5 minutes after that, and once right after the chain refuses a
tx because of them (authz: authorization not found or expired; feegrant: fee limit exceeded, allowance expired, no
allowance, message not allowed). A grant past its expiration counts as missing even while the chain still lists it.

While anything is missing, expired or spent:

- the connector is not ready: its `status_dict` shows `delegation_grants_valid: False`, so strategies, scripts and
  controllers stop placing orders, and the log says once what is wrong (the missing or expired message types, or the
  allowance problem);
- it sends no orders, cancels, swaps or fills: cancels need the grants and the allowance too. The tx that discovered
  the problem fails like any refused tx; nothing is retried;
- **resting orders stay on the book.** They belong to the account: cancel them from the account's own wallet if
  needed (e.g. in the dapp at dex.getbze.com).

Renew the grants (only the account owner can: the bot never asks for, renews or widens them) and the next 5-minute
check makes the connector ready again, with one log line that delegated trading resumed. No restart is needed. A spent
`spend_limit` passes simulation (the simulated fee is zero) and is caught at broadcast, so it costs no fee.

A read that fails (node down) keeps the last known state. When a grant or the allowance expires within 72 hours the log
warns once per item. `BeezeeExchange.delegation_status` gives the state, the earliest expiry and, for a spend-limited
allowance, what it can still spend in BZE.

### Delegated trading

How a user lets a bot key trade their wallet, with the `bzed` CLI (v8.2.0 or later). The user runs every command
below from their own wallet (`<user>`, a key in their `bzed` keyring); the bot operator only hands over the bot key's
address (`<bot>`). The rules the connector applies to the grants are in "When grants are revoked, expire or run out"
above.

#### What the user grants

- Four authz `generic` grants, one for each of `/bze.tradebin.MsgCreateOrder`, `/bze.tradebin.MsgCancelOrder`,
  `/bze.tradebin.MsgMultiSwap` and `/bze.tradebin.MsgFillOrders`, each with an expiry. All four are needed whatever the
  strategy runs.
- One feegrant allowance limited to `/cosmos.authz.v1beta1.MsgExec`, with a total spend limit, a daily period limit
  and an expiry. The gas is paid from the user's own BZE balance, up to those limits, so the wallet keeps some BZE
  for it.

That is all the bot key can do with the wallet: place and cancel orders, swap in pools and fill orders, with the
wallet's funds and in the wallet's name. It can **never** send coins, IBC-transfer, withdraw, stake, vote, or add or
remove liquidity: no grant covers those messages, so the chain refuses them. The feegrant pays only for `MsgExec`
txs, never for anything else the bot key signs.

#### The bot key

A fresh key that never holds funds, made by whoever runs the bot:

```bash
bzed keys add <bot-key-name>
```

Keep the printed mnemonic for `connect beezee` (mode `delegated_mnemonic`), or export the hex key for mode
`delegated` with `bzed keys export <bot-key-name> --unarmored-hex --unsafe`. Hand only the address (`<bot>`) to the
user. The key needs no balance: the feegrant grant below creates its account on chain. One bot key per bot, never
shared between bots, and the user's wallet runs one bot and is not traded by hand on that pair while the bot runs
(see "Delegated account").

#### Grant

Set the addresses, the expiry (30 days is a good default; renew before it runs out) and the limits. The examples use
testnet (`bzetestnet-3`); on mainnet use chain id `beezee-1` and a mainnet RPC. `date -d` is GNU date (on macOS use
`gdate`).

```bash
GRANTER=<user>    # the user's wallet: the bze1... address of a key in the bzed keyring
BOT=<bot>         # the bot key's bze1... address
EXP=$(date -u -d '+30 days' +%s)                    # authz expiry, Unix seconds
EXP_RFC=$(date -u -d "@$EXP" +%Y-%m-%dT%H:%M:%SZ)  # the same instant in RFC 3339, for the feegrant
SPEND=60000000ubze  # total gas the allowance may pay (60 BZE)
DAILY=2000000ubze   # gas it may pay per day (2 BZE)
NET="--chain-id bzetestnet-3 --node https://testnet-rpc.getbze.com:443"
```

**In one tx (recommended).** Build the five messages unsigned, merge them with `jq`, then sign and broadcast once:

```bash
i=0
for type in MsgCreateOrder MsgCancelOrder MsgMultiSwap MsgFillOrders; do
  i=$((i + 1))
  bzed tx authz grant "$BOT" generic --msg-type=/bze.tradebin.$type --expiration="$EXP" \
    --from "$GRANTER" $NET --generate-only > grant-$i.json
done
bzed tx feegrant grant "$GRANTER" "$BOT" --spend-limit="$SPEND" --period=86400 --period-limit="$DAILY" \
  --expiration="$EXP_RFC" --allowed-messages=/cosmos.authz.v1beta1.MsgExec \
  --gas=600000 --gas-prices=0.01ubze $NET --generate-only > grant-5.json
# the feegrant file carries the fee (600000 gas x 0.01 = 6000 ubze); its body gets all five messages
jq -s '.[4].body.messages = [.[].body.messages[]] | .[4]' \
  grant-1.json grant-2.json grant-3.json grant-4.json grant-5.json > grants.json
bzed tx sign grants.json --from "$GRANTER" $NET --output-document grants-signed.json
bzed tx broadcast grants-signed.json $NET
```

`bzed tx sign` reads the wallet's account number and sequence from the node. Check that `grants.json` lists four `/cosmos.authz.v1beta1.MsgGrant` and one
`/cosmos.feegrant.v1beta1.MsgGrantAllowance` before signing:
`jq -r '.body.messages[]."@type"' grants.json`.

**One tx per grant.** The same five commands without `--generate-only`, each signed and broadcast on its own:

```bash
for type in MsgCreateOrder MsgCancelOrder MsgMultiSwap MsgFillOrders; do
  bzed tx authz grant "$BOT" generic --msg-type=/bze.tradebin.$type --expiration="$EXP" \
    --from "$GRANTER" --gas-prices=0.01ubze $NET
done
bzed tx feegrant grant "$GRANTER" "$BOT" --spend-limit="$SPEND" --period=86400 --period-limit="$DAILY" \
  --expiration="$EXP_RFC" --allowed-messages=/cosmos.authz.v1beta1.MsgExec --gas-prices=0.01ubze $NET
```

`bzed tx feegrant grant` takes the granter as its first argument and ignores `--from`.

**Sizing the limits.** Each bot tx pays its gas limit (the simulated gas times `gas_adjustment`, 1.3 by default, plus
30,000 for the fee deduction the simulation skips) times the gas price, 0.01 ubze on testnet. A tx with one order is
about 160,000 gas, i.e. about 0.0016 BZE; a batch with more orders costs more, but much less than one tx per order.
Run the bot for an hour, then read the gas it paid: the beezee scripts print it in `status` (the gas on their spend or
fees line); for a plain strategy, add up the fees of the wallet's recent txs in the explorer (the wallet is their fee
payer). Scale that to a day and set `DAILY` to about twice it, and `SPEND` to `DAILY` times the days until the expiry. Both are in ubze (1 BZE = 1,000,000 ubze). A spent limit
only stops the bot (see "When grants are revoked, expire or run out"); it never costs more than the limit.

**Renewing.** Running the authz grant again for a type replaces that grant, with the new expiry. A feegrant allowance
cannot be replaced while it exists: revoke it, then grant it again (both can go in one tx with the same `jq` merge).

#### Check

```bash
bzed q authz grants "$GRANTER" "$BOT" $NET
bzed q feegrant grant "$GRANTER" "$BOT" $NET
```

The first lists the four grants, each with its `msg` type and `expiration`. The second shows the allowance:
`allowed_messages: [/cosmos.authz.v1beta1.MsgExec]` around a `PeriodicAllowance` with the `spend_limit` left, its
`expiration`, `period_can_spend` (left today) and `period_reset`. With no allowance it fails with
`fee-grant not found`. Both queries take addresses, not key names.

In the bot:

- the startup log names the account and the bot key, and logs once whether the grants are valid;
- while the grants are missing, expired or spent, `start` and `status` list the connector as not ready with the row
  `delegation_grants_valid False`, and the log says once what is missing;
- 72 hours before a grant or the allowance expires, the log warns once per item;
- the scripts' `status` shows the gas paid since they started; what is left of the allowance is in
  `bzed q feegrant grant` above.

#### Revoke

```bash
for type in MsgCreateOrder MsgCancelOrder MsgMultiSwap MsgFillOrders; do
  bzed tx authz revoke "$BOT" /bze.tradebin.$type --from "$GRANTER" --gas-prices=0.01ubze $NET
done
bzed tx feegrant revoke "$GRANTER" "$BOT" --gas-prices=0.01ubze $NET
```

Revoking any one of them is enough to stop the bot; revoking all five removes every right it had. Like the grants,
the five revokes can be built with `--generate-only` and merged into one tx with `jq`.

What follows: the connector notices within one tx (the chain refuses it) or at its next 5-minute check, turns not
ready and sends nothing more. **Resting orders stay on the book, in the user's wallet.** The user cancels them from
the wallet, in the dex app (dex.getbze.com; testnet: testnet-dex.getbze.com) or with
`bzed tx tradebin cancel-order <market-id> <order-id> <buy|sell> --from "$GRANTER" --gas-prices=0.01ubze $NET`.
Granting again later resumes the bot within 5 minutes, without a restart.

#### Connecting the bot

`connect beezee` asks for the network as in "Testnet run", then the account type. Answer `delegated_mnemonic` and the
client asks, in this order:

1. the bot key's mnemonic,
2. the bot key's mnemonic passphrase (optional, Enter for none),
3. the bot key's HD derivation path (Enter for the default `m/44'/118'/0'/0/0`),
4. "the Beezee wallet the bot trades for (the authz and feegrant granter)": `<user>`.

`delegated` asks for the bot key's hex private key, then the same granter address. The client encrypts the key into
`conf/connectors/beezee.yml`. `hummingbot/connector/exchange/beezee/beezee.delegated.example.yml` shows the resulting
config shape with placeholders. Then `balance` shows the user wallet's balances, and strategies and scripts run as in
wallet mode.

## Denom labels

The connector names every denom the way the BeeZee dapps do, so pairs, balances, `status` and the logs show the
tokens users see on dex.getbze.com (testnet: testnet-dex.getbze.com). A denom's label, and its decimals, come from
the first of:

1. **`denom_labels` in the connector config**, an override per denom (see below);
2. **the chain-registry asset list** the dapps are built with (cosmos/chain-registry `beezee/assetlist.json` on
   mainnet, the dapps' own list on testnet): an IBC token takes the entry's `symbol`, a native or factory token its
   `display` unit, upper-cased, with the decimals of that display unit. The connector carries a copy in
   `beezee_denom_labels.py`; a token added to the list later is named by its metadata until that copy is updated, or by
   an override;
3. **the denom's bank metadata**: its symbol, else its display unit, else the last part of the denom (the names the
   connector used before labels).

A label is upper-case, without spaces, and with `_` for `-` (Hummingbot splits a pair name on `-`). Some labels:

| Denom | Mainnet `beezee-1` | Testnet `bzetestnet-3` |
|---|---|---|
| `ubze` | `BZE` | `TBZE` |
| USDC.inj `ibc/81DA…` | `USDC.INJ` (6 decimals) | |
| USDC.n `ibc/6490…` | `USDC.N` | |
| Lumen | `LMN` (`ibc/693D…`) | `LMN` (`ibc/9DA2…`, 6 decimals) |
| Vidulum | `VDL` | `VDL` |
| TUSDC `factory/…/testusd` | | `TUSDC` (metadata) |

**Unique labels.** Labels from 1 and 2 must be unique on the chain: when two denoms get the same one, the config is
refused and the connector does not start, with an error naming the label and both denoms. A metadata label is chosen by
whoever created the token, so it never takes another denom's label: a factory token whose metadata says `BZE` is listed
as `BZE_<4 hex>` instead (the first 4 hex digits of its denom's sha1), with a warning in the log. Give it a label of
your own with an override.

**Overrides.** `denom_labels` is not prompted. Add it to `conf/connectors/beezee.yml` while the client is stopped
(leave the encrypted fields alone), then start the client again:

```yaml
denom_labels:
  "ibc/81DA528F4C5546208D1D50F84C2D0B388B45D37AA36FCA622A3CB2D8EF014838": "USDC"
```

**Old pair names.** Before labels the connector named every token after its metadata, so mainnet BZE/USDC.inj was
`BZE-ERC20:0XA00C59FF5A080D2B954D0C75E46E22A0C371235A` (with USDC counted in base units) and testnet BZE/TUSDC was
`BZE-TUSDC`. A strategy, script or controller that still uses an old name does not start: the connector stays not
ready (status flag `trading_pair_names_current`) and logs `Beezee pair '<old>' is now named '<new>'`. Put the new
name in the config. A token whose decimals now come from the asset list (USDC.inj: 6 instead of 0) also counts its
amounts and prices in its display unit, so a mainnet config written for `BZE-ERC20:0XA00C…` needs its USDC amounts
and prices divided by 1,000,000 (e.g. `daily_volume_quote: 20000000` becomes `20`, a price of `333` becomes
`0.000333`, and the volume target's `anchor_price_scale` goes back to `1`).

## Pure market making examples

Two ready strategy configs for upstream's `pure_market_making` live in `conf/strategies/`:

| File | Network | Pair |
|------|---------|------|
| `beezee_pmm_testnet.yml.example` | testnet `bzetestnet-3` | `LMN-TUSDC` |
| `beezee_pmm_bze_usdc.yml.example` | mainnet `beezee-1` | `BZE-USDC.INJ` (BZE/USDC.inj) |

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

The pair names are what the connector lists, from the denom labels (see "Denom labels"): BZE/USDC.inj is
`BZE-USDC.INJ`, with USDC in whole USDC (BZE at about 0.000333), and USDC.n lists as `BZE-USDC.N`. To see every pair
the connector knows, run `create`, pick `pure_market_making` and `beezee`, and look at the trading pair autocomplete.

### Seeding the book

Hummingbot reads the mid price from the book and does not start quoting on an empty or one-sided book. Before the
first `start`, make sure the market has at least one bid and one ask: place them from a second wallet in the dex app
(testnet: testnet-dex.getbze.com, mainnet: dex.getbze.com), or let the
[tradebin-mm](https://github.com/faneaatiku/tradebin-mm) stopgap bot quote the market. To quote around the
liquidity pool price instead, see "Pool-anchored market making".

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
| Minimum gas price | `0.01ubze` (chain param `ValidatorMinGasFee`; 0.001 is rejected as insufficient fees) |
| Markets | `ibc/9DA252F9…80CC7/factory/bze1z3mkcr2jz424w6m49frgjmy9uhlrx69phqwg3l/testusd`, listed as `LMN-TUSDC`, and `ubze/factory/bze1z3mkcr2jz424w6m49frgjmy9uhlrx69phqwg3l/testusd`, listed as `TBZE-TUSDC` (see "Testnet pair") |
| Explorer / dex | testnet-explorer.getbze.com / testnet-dex.getbze.com |

1. **Throwaway wallet.** Create a fresh key that never holds real funds, e.g. `bzed keys add hb-testnet
   --keyring-backend test`, and keep the printed mnemonic. Send it some testnet BZE from a test wallet you already
   have (it pays the gas and the tradebin fees). In testnet-dex.getbze.com, swap part of it for LMN and for TUSDC in
   their pools: a few LMN and a few TUSDC are plenty for the example config.
2. **Seed the book** if it lacks a bid or an ask (see "Seeding the book"). A second testnet wallet is also what you
   use to fill the bot's orders by hand.
3. **Connect.** In the client run `connect beezee` and answer:
   - network: `testnet`; REST `https://testnet.getbze.com`; RPC `https://testnet-rpc.getbze.com`; websocket: leave
     empty (derived from the RPC); chain id `bzetestnet-3`; prefix `bze`; native denom `ubze`;
   - gas price `0.01` (the chain minimum; a higher one works too), gas adjustment `1.3`;
   - account type `wallet_mnemonic`, the throwaway mnemonic, empty passphrase, the default HD path, and the address.

   Then `balance`: it shows TBZE, LMN and TUSDC as in the dex app (testnet BZE is labelled TBZE there).
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
  order pays the tradebin fee when it is created (mainnet: 0.1 BZE taker, 0.001 BZE maker). The taker fee applies only
  when the order crosses an order that already rested on the book from an earlier block; crossing an order placed in
  the same block or tx costs the maker fee.
- `cp conf/strategies/beezee_pmm_bze_usdc.yml.example hb/conf/strategies/beezee_pmm_bze_usdc.yml`, set `order_amount`,
  then `import beezee_pmm_bze_usdc.yml` and `start`.
- The book must have a bid and an ask (see "Seeding the book"). Cancel-all check after `stop` in dex.getbze.com.

## Volume target

`scripts/beezee_volume_target.py` prints a set daily volume on one order-book market by self-filling. Every
`cycle_seconds` it sells and buys the same amount at the same price, in one batch tx. The price sits strictly inside
the current spread, so the first leg rests as the new best order and the second meets it at exactly its price: the
two fill each other, no other trader's order is taken, and the inventory stays where it was apart from fees. Tradebin
accepts a self-fill from one wallet. The side that goes first alternates every cycle.

Each cycle:

1. **Leftover legs.** An order of an earlier cycle that is still open (one leg filled, or someone took one side) is
   cancelled, and the cycle waits. Only volume that filled counts, and a self-filled trade counts once.
2. **Size.** `(daily_volume_quote - filled today) / cycles left in the UTC day`, converted to base units at the trade
   price, clamped to `[min_order_amount, max_order_amount]` and raised to the chain minimum at that price. Once the
   target is reached it idles until UTC midnight, when the day's counters start from zero.
3. **Price.** With the best bid `b`, the best ask `a` and the price tick `t`: if a side is empty or `a - b <= t`, the
   cycle is skipped. Otherwise `p = clamp(anchor, b + t, a - t)`, rounded down to the tick.
4. **Costs.** Fees and gas are counted in BZE. Reaching `max_daily_fee_spend` stops it until UTC midnight.

`status` shows the target, filled and remaining volume, the anchor with its source and age, the book mid, the pool
price (see "Pools"), the day's
fees + gas, the cost per 1,000 quote units of volume and why the last cycle skipped.

**Anchor.** Where in the spread the trades print, which becomes the chart price and reaches CoinGecko through the
aggregator. It is the volume-weighted `converted_last.usd` of CoinGecko's tickers for `coingecko_id_base`, leaving out
the venues in `exclude_markets` (default `BeeZee DEX`: CoinGecko's own BZE price includes this DEX, which the script
would come to dominate) and tickers CoinGecko flags as stale or anomalous. It is divided by the USD price of
`coingecko_id_quote` (1 when unset) and multiplied by `anchor_price_scale` to get the connector's price units: 1
(the default) when both tokens count in display units, as BZE/USDC.inj does, 1000000 for a quote counted in base
units. It refreshes every `anchor_refresh_seconds` (two requests;
300 fits the public rate limit). An optional API key comes from the `COINGECKO_API_KEY` env var (demo tier;
`COINGECKO_API_TIER=PRO` for a pro key), never from the yml.

- No `coingecko_id_base` (most community tokens), no external ticker, or an anchor older than
  `anchor_max_age_seconds`: `anchor_fallback` applies, either `book_mid` (trade at the mid of best bid and ask) or
  `pause`. A failed refresh keeps the previous anchor until it is too old.
- When the book mid is more than `max_anchor_deviation_pct` away from the anchor, the cycle pauses: volume there would
  print an off-market price, and fixing the book is arbitrage's job. Not checked on the `book_mid` fallback.

**Cost.** Tradebin charges each order its fee when the tx runs: the taker fee only when an opposite order already
rests at that price from an earlier block, else the maker fee. Both legs of a cycle land in the same tx, so neither is
on the book when the other is signed, and both pay the maker fee (mainnet: 2 x 0.001 BZE). The tx pays gas once for both
legs, about 0.003 BZE. A cycle costs about 0.005 BZE, so about 1.5 BZE a day at `cycle_seconds: 300` (288 cycles),
whatever the volume. The wallet needs the BZE for that, the base for the
sell leg and the quote for the buy leg; a cycle without both balances is skipped. The counters live in memory: a
restart starts the day's volume and spend from zero.

**Run.** `cp conf/scripts/beezee_volume_target.yml.example hb/conf/scripts/beezee_volume_target.yml` (mount
`hb/conf` as in "Running the image"), set `daily_volume_quote` and `max_daily_fee_spend`, then in the client:
`start --v2 beezee_volume_target.yml`. The example is for BZE/USDC.inj on mainnet
and documents every key; a test loads it.

**Testnet.** Testnet has no CoinGecko price, so leave `coingecko_id_base` empty with `anchor_fallback: book_mid`, set
`trading_pair: LMN-TUSDC`, `anchor_price_scale: 1` and a small `daily_volume_quote`. The book needs a bid and an ask
several ticks apart (see "Seeding the book"). Checks:

- [ ] With `cycle_seconds: 60`, run for about 30 minutes: the filled volume BZE Scan shows for the bot's address
      matches what `status` reports.
- [ ] Each cycle is one tx holding a sell and a buy at the same price, strictly between the best bid and ask of that
      moment; the side that goes first alternates.
- [ ] No other address's order was filled by the bot: every trade of the bot's address has the bot on both sides.
- [ ] Base and quote balances are unchanged apart from fees + gas.
- [ ] With `max_daily_fee_spend` set low (e.g. 0.02), it stops after about 4 cycles with "daily fee cap reached".
- [ ] A spread of one tick (or an empty side) skips the cycle with that reason in `status`.
- [ ] Take one leg from a second wallet before it fills: the next cycle cancels the other leg, and only the filled
      amount counts.

## Pools

The connector reads the tradebin liquidity pools (AMM) of the pairs it trades. Nothing is sent to the chain for this.

- **Pools and markets are separate venues.** The connector lists pairs from both `all_markets` and
  `all_liquidity_pools`: a pair has an order-book market, a pool, or both (see "Pools and markets are separate venues"
  in the AMM run guide).
- **Which pool.** A pair has at most one pool, whose id is its two denoms sorted and joined by `_`. A pair with a market
  keeps the market's orientation (`base/quote`), so the pool's base can be the pair's quote: BZE-USDC.INJ (market
  `ubze/ibc/81DA…`) trades against pool `ibc/81DA…_ubze`, and testnet TBZE-TUSDC (market `ubze/factory/…/testusd`)
  against pool `factory/…/testusd_ubze`. Such a pool quotes the inverse of its market. A pool-only pair takes the
  pool's orientation (testnet LMN-TBZE for pool `ibc/9DA2…_ubze`). The connector reads reserves by denom, so every
  price and quote comes out in the pair's own orientation.
- **Refresh.** All pools are read once, then the pools of the traded pairs on every new block. A failed read keeps
  the last reserves and logs a warning. A pool is halted while either of its denoms is in the halted list (the same
  list the order book uses). When a traded pair has a pool, the connector is not `ready` (status flag
  `pools_initialized`) until that first read is done, so a script's first tick finds the pool. A script that still
  sees a pair with the pool venue but no pool yet waits: it uses up no slice or cycle and does not report "no
  liquidity pool".
- **Prices of a pool-only pair.** Its order book is always empty, so the connector answers every price type (best
  bid and ask, mid, last trade) with the pool price, or `None` while the pool has none. The rate oracle behind
  `history`, the trade monitor and the fee conversions then prices the pair from its pool. A pair with an order-book
  market keeps its book prices.
- **Pool price.** `connector.get_pool_price(trading_pair)` is the pool's quote reserve over its base reserve, in the
  units of the pair's order prices (display units, quote per base), so it compares directly with the book. `None`
  when there is no pool or it is empty. `connector.get_pool(trading_pair)` gives the pool itself (reserves in base
  units, fee, `halted`), and `connector.get_pool_reserves(trading_pair)` its `(base, quote)` reserves in the pair's
  orientation and display units.
- **Quotes.** `connector.quote_swap(trading_pair, side, amount)` tells what a swap of `amount` input pays out: on
  `"buy"` the input is quote and the output base, on `"sell"` the reverse. `connector.quote_input_for_output(...)`
  starts from the output wanted (base on a buy, quote on a sell) and returns the input that gets at least that much.
  Both return `input`, `output`, `fee` (the pool fee, taken from the input), `effective_price` (quote per base, fee
  included) and `price_impact_pct` (how much worse than the pool price, fee included), plus the same amounts in base
  units. The math is the chain's own (v8.2.0 `service_amm.go`, including its 18-decimal rounding), checked to the
  unit against the chain code on recorded mainnet pools. A quote the chain would refuse comes back with `ok` false
  and the reason in `error`: a halted pool (`halted` true), an amount too small to pay any pool fee, an output the
  pool cannot pay. The flat tradebin taker fee a swap tx also pays is not in the quote.
- **Pool price as a price source.** `BeezeePoolPriceDelegate(connector, trading_pair)`
  (`hummingbot.connector.exchange.beezee.beezee_pool_price_delegate`) is an asset price delegate that returns the
  pool price for every price type (a pool has no spread), NaN while there is none. A strategy or script that takes an
  `AssetPriceDelegate`, such as pure market making's `asset_price_delegate`, uses it in place of the order-book mid.
- **Swaps.** `connector.amm_swap(trading_pair, side, amount, slippage_pct)` swaps `amount` of the input token (quote on
  `"buy"`, base on `"sell"`, display units) on the pair's pool with one `MsgMultiSwap` (always that one pool, never a
  route through other pools). `min_output` is the quoted output less `slippage_pct` percent, rounded down in base
  units, so the chain refuses the swap ("the resulted amount is too low") rather than pay out less.
  `connector.amm_swaps([BeezeeSwapRequest(trading_pair, side, amount, slippage_pct), ...])` puts several swaps in one
  tx, executed in list order: a round trip (A→B then B→A) is two swaps, since one message cannot use the same pool
  twice. Each swap is quoted on the pool as the swaps before it leave it, and a later swap may spend what an earlier
  one is guaranteed to pay out. Gas is simulated and the tx signed, like the order batches; swap txs and order
  batches from the same wallet take their account sequences one tx at a time.
  - *Refused up front* (a `BeezeeSwapError`, nothing tracked, nothing sent): no pool or a halted one, an amount the
    pool cannot swap (too small to pay any pool fee, or no output at all), an output that the slippage rounds to 0,
    or an available balance too low for the input plus the taker fee (in BZE).
  - *Tracked as taker trades.* Each swap is a market order with its own client order id: created once its tx is
    accepted, then one fill and completed as soon as the tx is found (swaps settle inside their tx, no order queue).
    The fill is read from the swap's `SwapEvent`: price in the pair's order-price units (quote per base), the input
    amount net of the pool fee (what the pool swapped: a 2 TUSDC sell is recorded as 1.996 TUSDC), and fees = the
    pool fee (in the input token, shown separately as a flat fee) + the tradebin taker fee (0.1 BZE on mainnet, read
    from the chain params, once per swap). So `OrderFilledEvent`, the trade history and PnL treat a swap like an
    order fill. The balances change by the event's amounts at once, and the pool is read again from the chain (part
    of the fee stays in the pool, so its new reserves are never guessed). `connector.get_swap_result(order_id)` gives
    what a settled swap put in and got out.
  - *Failures.* A tx refused in simulation (e.g. the slippage limit, a pool halted meanwhile) or failing in its block
    fails every swap in it with the chain's error (`OrderFailure`); a halted refusal also re-reads the halted denoms.
    A tx not found within 60 s fails too, and the balances are read again. Nothing is retried, and a swap cannot be
    cancelled. A swap still waiting for its tx when the bot stops is settled from that tx after the restart.
  It is `ready` once the connector is ready and the pool has a price.

## Taker fills

A taker fill takes resting orders off the order book at exactly their prices with one `MsgFillOrders`. Unlike a
crossing limit order, it never leaves an order resting: what does not match at that exact price is refunded.

- **Which levels.** `connector.levels_to_take(trading_pair, side, max_amount, worst_price)` returns `(price, amount)`
  levels, best first, from the last order book read: on `"buy"` the asks up to `worst_price`, on `"sell"` the bids
  down to it, at most `max_amount` base in total (display units). The prices are the chain's own strings, so they
  match exactly, even with 18 decimals (the Hummingbot order book keeps floats, which cannot). The list stops before
  a part of a level below the chain minimum amount there, and at 50 levels. Empty until the book was read once.
- **Sending.** `connector.fill_orders(trading_pair, side, levels)` takes those levels in one message. `side` is ours:
  `"buy"` takes resting sells, `"sell"` takes resting buys (the message's `order_type` is the resting side). Amounts
  are rounded down to whole base units and levels at the same price merged. A level below the chain minimum at its
  price (`ceil(1 / price) × 2` base units) is dropped and logged, since the chain would skip it silently. Returns one
  client order id per entry sent. The message goes through the order batcher like creates and cancels, so it can
  share a tx with them; gas is simulated (a fill pays `FillOrdersExtraGas` growing with its entries, which the
  simulation counts). It is not cancellable.
  - *Refused up front* (a `BeezeeFillError`, nothing tracked, nothing sent): no wallet, an unknown pair, a halted
    market, a level with a non-positive price or amount, more than 50 entries, or every level below the minimum.
- **Fill and swap in one tx.** `connector.fill_orders_and_swap(trading_pair, side, levels, [BeezeeSwapRequest(...)])`
  sends the `MsgFillOrders` and then one `MsgMultiSwap` per swap in a single tx (not batched), e.g. buying on the book
  and selling the same amount on the pool. The tx passes or fails as a whole. The swaps settle in the tx; the fill
  settles at the end of the block and may be refunded in part, which the swaps cannot follow. Returns the fill's and
  the swaps' client order ids.
- **Tracked as taker orders.** Each entry is a limit order at its price and amount (exchange id = the tx hash),
  followed by its own queue `message_id` like a create. Every match is an `OrderFilledEvent`. The taker fee is paid
  once per message, so it goes on the first fill of any of its entries. Once the chain has processed the message, an
  entry that got its full amount is completed; otherwise it ends `CANCELED` with the fills it got (the rest was
  refunded), and the balances are read again. Chain v8.3.0 announces the refund with `OrderMessageRefundedEvent`.
  v8.2.0 emits nothing for it, so a fill that matched nothing leaves no event at all: the connector then reads the
  tradebin queue store at the block height (RPC `abci_query`) and takes the message as processed once it is gone.
  An entry with no outcome within 20 blocks fails with a warning. A fill in flight at a stop is resumed from its tx
  after the restart; one stopped before its tx was known fails, since whether it was sent is unknown.
- **Matching your own orders.** The chain fills the resting orders at a price in their order, whoever placed them,
  so a fill at a price where this wallet also rests an order may take that order too.

## Pool / order-book arbitrage

`scripts/beezee_pool_book_arb.py` keeps one pair's liquidity pool and its own order book in line and takes the gap
itself. It trades that one pair only, its pool against its book: no other exchange, no route through other pools.

On every new block (and at least every `check_interval_seconds`):

1. **Read** the pool (`get_pool`, `get_pool_price`) and the book levels around the pool price.
2. **Opportunity A** (book ask below the pool price): take those asks with a `MsgFillOrders` and sell the same base
   on the pool. **Opportunity B** (book bid above the pool price): buy the base on the pool and sell it into those
   bids. When both exist, the one with the larger net profit goes.
3. **Size.** Book levels are taken best first (`levels_to_take` up to the pool price), each only as far as the part
   taken from it still beats the pool by `min_profit_pct`: on A the extra quote the pool pays for that part must be at
   least its book cost × (1 + `min_profit_pct`/100), on B the book's proceeds at least the extra quote the pool
   charges × (1 + `min_profit_pct`/100). The pool side is the connector's quote, so the pool fee and the price impact are in it.
   A level is cut at the largest base step that still holds, and dropped when that is below the chain minimum. The
   total is capped at `max_trade_base` and at the balances above `reserve_base` / `reserve_quote`.
4. **Profit check.** Net = pool output − book cost (A), or book proceeds − pool input (B), − 2 × the tradebin taker
   fee (the fill and the swap each pay one, even a fill that matches nothing) − `estimated_gas_bze`. Fees and gas are
   in BZE: they count 1:1 when BZE is the pair's quote, and at the pool price when BZE is its base. A pair without
   BZE on either side is refused at start (logged, shown in `status`). The arb is sent only when net ≥
   `min_profit_pct` % of what it spends.
5. **One tx.** `fill_orders_and_swap`: the `MsgFillOrders` first, then the `MsgMultiSwap` with `min_output` = quote −
   `slippage_pct` %. Both legs spend what the wallet already holds, because the fill only executes at the end of the
   block: A escrows quote for the asks and sells base it has, B escrows base for the bids and spends quote on the
   pool. Afterwards the base is back where it was and the profit is in quote.
6. **Partial fills.** The swap settles in the tx, the fill at the end of the block, where an order it aimed at may
   already be gone: that part is refunded, so the inventory is off by it (A: short of base, B: long). The script
   records the imbalance. On the next check, with `rebalance_on_partial: true`, it swaps the amount back on the pool
   if that costs at most `max_rebalance_cost_pct` % of its value (pool fee, impact, a taker fee and gas); otherwise it
   keeps it. Each imbalance gets at most one attempt, whatever the outcome; kept ones stay in `status`.
7. **Halted** pool or market (either denom halted by governance, or a refusal saying so): nothing is sent while it
   lasts, logged once. The script holds no open orders, so there is nothing to exit.
8. **Cooldown.** After an arb or a rebalance settles, it waits `cooldown_blocks` new blocks (connector
   `new_blocks_seen`; `cooldown_blocks` × 10 s when no block notification arrives) so the book and the pool show the
   trade. Only one arb or rebalance is in flight at a time.

`status` shows the pool price against the best bid and ask, the last gap (before the pool fee) and the fixed fees per
arb, the day's trades, profit in quote (the quote made plus the base made at the pool price, less tradebin fees and
gas), tradebin fees, gas and pool fees, the open imbalance with each one's state, an arb in flight and the last skip
reason. The counters live in memory: a restart starts them from zero and forgets open imbalances.

**Run.** `cp conf/scripts/beezee_pool_book_arb_mainnet.yml.example hb/conf/scripts/beezee_pool_book_arb.yml`, set
`max_trade_base` and the reserves, fund the wallet with both tokens (plus BZE for fees), then
`start --v2 beezee_pool_book_arb.yml`. The `_mainnet` example is for BZE/USDC.inj, `_token_bze` is a TOKEN/BZE
template and `_testnet` runs on testnet TBZE/TUSDC (see "AMM run guide"). Each documents every key; a test loads them.

**Testnet.** Use a pair with BZE on one side that has both a market and a pool, and a second wallet.

- [ ] Place a resting ask well below the pool price from the second wallet and start the script: one arb tx
      (MsgFillOrders + MsgMultiSwap) goes out, its profit shows in `status`, and BZE Scan shows the tx with both
      messages.
- [ ] The mirror: a resting bid well above the pool price gives one tx that sells into it and buys on the pool.
- [ ] A gap smaller than the pool fee + taker fees + `min_profit_pct` sends nothing; `status` says why.
- [ ] Cancel the second wallet's ask in the same block as the arb (or let another wallet take it first): the fill is
      refunded, `status` shows the imbalance, and one rebalance swap follows (or it is kept), never more.

## Pool volume

`scripts/beezee_pool_volume.py` prints a set daily volume on one pair's liquidity pool with round trips. Every
`cycle_seconds` it swaps one token of the pair for the other and back again. One message cannot use the same pool
twice, so the round trip is two `MsgMultiSwap` in one tx (`amm_swaps`): A→B, then B→A. Swaps settle inside the tx,
so leg 2 spends what leg 1 delivered. The pair is traded in its own terms on its own pool: a TOKEN/BZE bot never
routes through BZE → USDC. A pair without BZE on either side is refused at start (logged, shown in `status`), since
the fees and the gas are paid in BZE.

Each cycle:

1. **Target and pace.** `remaining = daily_volume_quote − volume today` (UTC day, in the pair's quote, both legs
   counted). One leg is `remaining / cycles left today / 2` in quote, clamped to `[min_swap_amount,
   max_swap_amount]`. Leg 1's input is then capped at `max_reserve_share_pct` (default 0.5) % of the pool's reserve
   of that token, and at the balance above `reserve_base` / `reserve_quote` (less the fees on the BZE side). A leg
   that ends up below `min_swap_amount` is not sent. Once the target is reached it idles until UTC midnight, when the
   day's counters start from zero.
2. **Direction.** The token that goes first alternates every cycle. When the balance of one side cannot pay for leg
   1, it starts from the other side; when neither can, the cycle is skipped with both reasons.
3. **Send.** Leg 1 with `min_output` = its quote − `slippage_pct` %. Leg 2 puts back leg 1's quoted output less
   `safety_pct` % (never more than leg 1's minimum output, so it never spends what the wallet held before the tx),
   with its own `min_output` on the pool as leg 1 leaves it. The small leftover of leg 1 stays on its output side;
   alternating the first token evens it out.
4. **Account.** From the swap results: volume (the quote side of each leg: what left on a buy, what arrived on a
   sell), pool fees, 2 tradebin taker fees (one per message), gas, and the inventory change. Reaching
   `max_daily_fee_spend` (BZE: pool fees + taker fees + gas) stops it until UTC midnight.
5. **Price guard.** When the pool price moved more than `max_price_move_pct` since the last cycle, someone else is
   trading heavily: the cycle is skipped so the script does not amplify the move.
6. **Halted** pool (either denom halted by governance, or a refusal saying so): nothing is sent while it lasts,
   logged once. Only one round trip is in flight at a time.

`status` shows the target, done and remaining volume, the next leg's size and its share of the reserves, the pool
price, the day's cost split (pool fees, taker fees, gas) against the cap, the cost per 1,000 quote of volume, the
inventory drift from the swaps and the last skip reason. The counters live in memory: a restart starts the day's
volume and spend from zero.

`Inventory drift` already includes the pool fees (they come out of the swapped amounts), so do not add it to
`Cost today`, which lists them too: the wallet's change over the day is the drift + the taker fees + the gas.

**Cost.** A round trip costs 2 × the pool fee on its size (to the LPs) + 2 × the tradebin taker fee + gas. The pool
fee scales with the volume, the rest is fixed per round trip, so fewer, larger cycles are cheaper. A bot owner who is
also an LP in that pool gets their share of the pool fee back. Example on BZE/USDC.inj (pool fee 0.1%, taker fee
0.1 BZE, gas about 0.005 BZE, BZE at 0.000333 USDC), 20 USDC a day at `cycle_seconds: 900` (96 round trips of about
0.104 USDC per leg):

| | per day |
|---|---|
| pool fees: 0.1% × 20 USDC = 0.02 USDC | about 60 BZE |
| taker fees: 96 × 2 × 0.1 BZE | 19.2 BZE |
| gas: 96 × 0.005 BZE | 0.48 BZE |
| total | about 80 BZE, i.e. about 4 BZE per USDC of volume |

**Run.** `cp conf/scripts/beezee_pool_volume_mainnet.yml.example hb/conf/scripts/beezee_pool_volume.yml`, set
`daily_volume_quote` and `max_daily_fee_spend`, fund the wallet with both tokens (plus BZE for fees), then
`start --v2 beezee_pool_volume.yml`. The `_mainnet` example is for BZE/USDC.inj, `_token_bze` is a TOKEN/BZE template
and `_testnet` runs on testnet TBZE/TUSDC (see "AMM run guide"). Each documents every key; a test loads them.

**Testnet.** Use a pair with BZE on one side that has a pool.

- [ ] With a small `daily_volume_quote` and `cycle_seconds: 60`, run for about 30 minutes: the pool volume BZE Scan
      shows for the bot's address matches what `status` reports, and the inventory changes only by the fees (pool
      fees, taker fees, gas) plus the small leftover `status` shows as drift.
- [ ] Each cycle is one tx with two `MsgMultiSwap` on the same pool, A→B then B→A; the token that goes first
      alternates.
- [ ] No leg is larger than `max_reserve_share_pct` % of the pool's reserve, and the balances never go below
      `reserve_base` / `reserve_quote`.
- [ ] With `max_daily_fee_spend` set low, it stops with "daily fee cap reached".
- [ ] A large swap from a second wallet between two cycles (more than `max_price_move_pct`) skips the next cycle with
      that reason in `status`.

## Pool-anchored market making

`controllers/market_making/beezee_pool_anchored_pmm.py` is upstream's `pmm_simple` controller quoting around the
pair's liquidity pool price. On a thin BeeZee book the book's own mid is easy to push, and a bot quoting around it
gets picked off; on most BZE pairs the pool holds more liquidity and is the better reference. The pair is traded in
its own terms: a TOKEN/BZE controller prices and budgets in BZE and never goes through USDC.

- **Reference price.** The pool price (`BeezeePoolPriceDelegate`, refreshed on every new block), in the pair's
  orientation even when the pool lists the denoms the other way round. A pair without a pool, or with an empty one,
  quotes around the book mid instead: logged once and shown as a `WARNING` line in `status`.
- **Spreads, levels, amounts, refresh** as in `pmm_simple`: `buy_spreads` / `sell_spreads` (fractions, 0.01 = 1%),
  `buy_amounts_pct` / `sell_amounts_pct` of `total_amount_quote`, `executor_refresh_time`, `cooldown_time`.
- **Spread floor.** Every quote is at least pool fee + `min_spread_over_pool_fee_pct` (in %) away from the pool price;
  a closer level is placed at the floor. Inside the pool fee, a pool arbitrageur could buy from us and sell to the pool
  (or the reverse) at a profit from our quote alone.
- **Requote.** Quotes not filled yet are replaced as soon as the reference moved more than `requote_threshold_pct`
  (in %, default 0.2) from where they were placed, without waiting for `executor_refresh_time`. The cancels and the
  new orders go out in the connector's batch txs.
- **Halted pair** (either denom halted by governance): every quote is cancelled once, filled amounts are kept (no
  market close), and the controller places nothing more. Restart it once the halt is lifted.
- **Market orders.** The BeeZee book takes limit orders only, so the stop loss, the time limit and the position
  rebalance (all market orders in upstream's executors) are off by default (`stop_loss`, `time_limit` empty,
  `skip_rebalance: true`). Without hedge mode a fill is closed by the take-profit limit order, as in `pmm_simple`.

**Hedge mode** (`hedge_fills_on_pool: true`, off by default) makes the controller a cross-venue maker: each fill of
one of our quotes is swapped back on the pool in the opposite direction (`amm_swap`), so the inventory stays flat and
we keep the spread minus the pool fee. The quotes are then plain limit orders (an order executor per level), since
nothing else may close a hedged fill on the book, and a level waits `cooldown_time` after a fill.

1. A fill is hedged only when its distance from the pool price (bought below it, sold above it) is at least the pool
   fee + the hedge's tradebin taker fee and `estimated_gas_bze` (in % of the fill's value) + `min_hedge_profit_pct`.
   Otherwise it stays in the inventory (counted as unhedged in `status`).
2. The fills of one block are netted into one swap, sent once the next block is seen (or after 10 s without block
   notifications): bought − sold base is sold on the pool, or bought back with quote. The whole swap must still net
   `min_hedge_profit_pct` of the fills' value after its price impact on the pool, or it is not sent.
3. The swap's `min_output` is the quote − `hedge_slippage_pct` %. A refused or failed hedge (slippage, halt, an
   amount too small for the pool) is logged and the inventory kept: it is never retried. One hedge is in flight at a
   time; the fills meanwhile wait for the next one.
4. The taker fee and the gas are paid in BZE, so hedge mode needs a pair with BZE on one side; on any other pair the
   fills are kept and `status` says why. Fills of executors from before the controller started are never hedged.

**Cost example** (BZE/USDC.inj, pool price 0.000333 USDC per BZE, pool fee 0.1%, taker fee 0.1 BZE):

- With `min_spread_over_pool_fee_pct: 0.1` the floor is 0.2%: a 0.1% level is placed at 0.000333666 (sell) and
  0.000332334 (buy) instead.
- A sell quote at 0.5% (0.00033467) fills 30,000 BZE for 10.040 USDC. Hedge check: 0.5% ≥ 0.1% pool fee + 0.105 BZE
  of fees (0.00035% of 10 USDC) + 0.1% margin, so it is hedged. Buying the 30,000 BZE back costs 10.000 USDC at the
  pool price including the fee, plus the price impact: about 0.45% more on a pool holding 6.7M BZE, which leaves
  less than the 0.1% margin, so this swap is not sent and the 30,000 BZE sale stays in the inventory. On that pool
  hedge 5,000 BZE fills (impact about 0.07%): net ≈ 0.5 − 0.1 − 0.07 ≈ 0.33% of the fill, about 0.0055 USDC each.

**Run.** Copy `conf/controllers/beezee_pool_anchored_pmm_mainnet.yml.example` (BZE/USDC.inj),
`beezee_pool_anchored_pmm_token_bze.yml.example` (a TOKEN/BZE template, replace the placeholders) or
`beezee_pool_anchored_pmm_testnet.yml.example` (testnet TBZE/TUSDC) to `conf/controllers/<name>.yml` and set
`total_amount_quote`. A controller runs inside upstream's `v2_with_controllers` script:
`conf/scripts/beezee_pool_anchored_pmm_runner_{mainnet,testnet}.yml.example` are ready script configs that list the
mainnet / testnet controller (for a TOKEN/BZE copy, change the name under `controllers_config`). Copy the runner to
`conf/scripts/` without `.example` and `start --v2 beezee_pool_anchored_pmm_runner_mainnet.yml`. Every example
documents every key and a test loads them.

**Testnet.** Use a pair that has both a market and a pool, and a second wallet.

- [ ] Run the controller on the pair: the quotes sit around the pool price, none closer than pool fee +
      `min_spread_over_pool_fee_pct`. Move the pool price with a manual swap from the second wallet: the quotes follow
      within a block or two (a move above `requote_threshold_pct`), not at the next refresh.
- [ ] With `hedge_fills_on_pool: true`, take one of our quotes from the second wallet: exactly one hedge swap follows
      (BZE Scan shows the MsgMultiSwap), `status` counts it, and the inventory is back where it was.

## Price support bands

`scripts/beezee_price_bands.py` defends a price band on one pair: it buys base when the price falls below
`floor_price`, sells base when it rises above `ceiling_price`, and does nothing inside the band. The pair is traded
in its own terms: a TOKEN/BZE band is set in BZE, the buy budget is in BZE, the sell budget in TOKEN, and nothing is
routed through other pools (never BZE → USDC). A pair without BZE on either side is refused at start, since the fees
and the gas are paid in BZE.

On every new block (and at least every `check_interval_seconds`):

1. **Reference price.** `price_source: pool` reads the pair's pool through `BeezeePoolPriceDelegate`; `book_mid` uses
   the order-book mid. Left empty, it is the pool when the pair has one, the book mid otherwise.
2. **Below the floor: buy.** The input (BZE on a TOKEN/BZE pair) is the smallest of:
   - what brings the venue's price back to the floor and no further: on the pool, the base taken out until
     reserve_quote / reserve_base = floor, priced with `quote_input_for_output`; on the book, the asks up to the
     floor;
   - `max_trade_quote`;
   - the budget left: `buy_budget_quote` less what the buys already spent in the period;
   - the balance above `keep_quote_reserve` (less the taker fee and `estimated_gas_bze` when the quote is BZE).
   The trade is then cut to the largest size whose own price impact is at most `max_impact_pct`: its average price
   against the venue's price before it (the pool price, or the best ask), pool fee included. An impact cap below the
   pool fee trades nothing.
3. **Above the ceiling: sell.** The mirror, in base: `max_trade_base`, `sell_budget_base`, `keep_base_reserve`, bids
   down to the ceiling. Selling TOKEN still needs BZE above `keep_quote_reserve` for the fees.
4. **Venue.** `venue: pool` (default) sends one `amm_swap` with `slippage_pct`. `venue: best` also sizes a taker fill
   on the book (`levels_to_take` + `fill_orders`) and sends whichever is cheaper per base, its taker fee and gas
   included. A trade is never split across both.
5. **Cooldown.** After a trade settles (or fails) it waits `cooldown_blocks` new blocks (`cooldown_blocks` × 10 s
   without block notifications). If the price is still outside the band, the next trade follows, capped again. One
   trade is in flight at a time.
6. **Budgets.** `budget_period: daily` (default) starts both budgets again at UTC midnight; `total` covers the whole
   run. A side whose budget is spent stops for the period and `status` says so. A failed trade spends nothing.
7. **Sanity.** The script refuses to start when `floor_price >= ceiling_price` or a kept reserve is larger than its
   balance. A halted pair (the pool's halted flag, or a refusal saying so) stops it, logged once; once the halt is
   lifted, `stop` and `start` it again.

`status` shows the reference price against the band, the budget used and left per side and period, the reserves
kept, today's buys and sells with their average price, the tradebin fees, gas and pool fees, a trade in flight and the
last skip reason (e.g. "impact cap", "buy budget spent today"). The counters live in memory: a restart starts the
budgets from zero, so with `budget_period: total` lower the budgets by what was already spent before restarting.

**Example** for a TOKEN/BZE pair whose pool sits around 1 BZE per TOKEN (pool fee 0.2%): `floor_price: 0.9`,
`ceiling_price: 1.2`, `max_trade_quote: 500`, `max_trade_base: 500`, `max_impact_pct: 1`, `keep_quote_reserve: 1000`,
`keep_base_reserve: 1000`, `buy_budget_quote: 5000`, `sell_budget_base: 5000`, `budget_period: daily`. Someone sells
TOKEN and the pool drops to 0.85: the script buys TOKEN with at most 500 BZE per trade, each trade cut where its
impact would pass 1%, one every 3 blocks until the pool is back at 0.9, at most 5000 BZE a day, and never below
1000 BZE in the wallet. Above 1.2 it sells TOKEN the same way.

**Run.** `cp conf/scripts/beezee_price_bands_token_bze.yml.example hb/conf/scripts/beezee_price_bands.yml`, replace
the TOKEN placeholder, set the band, the caps, the reserves and the budgets, fund the wallet with both tokens (plus BZE
for fees), then `start --v2 beezee_price_bands.yml`. `_token_bze` is a TOKEN/BZE template, `_mainnet` is for
BZE/USDC.inj and `_testnet` runs on testnet TBZE/TUSDC (see "AMM run guide"). Each documents every key; a test loads
them.

**Testnet.** Use a pair with BZE on one side that has a pool, and a second wallet.

- [ ] Set a band around the pool price and start the script: nothing is sent while the price is inside it.
- [ ] Push the price below the floor with a manual swap from the second wallet: the bot buys back to the floor (or
      as far as `max_trade_quote` and the budget allow), each trade within `max_impact_pct`, one trade per
      `cooldown_blocks`, and the BZE balance never goes below `keep_quote_reserve`.
- [ ] Repeat above the ceiling: the bot sells back to the ceiling, never below `keep_base_reserve`.
- [ ] With a small `buy_budget_quote`, it stops with "buy budget spent today" in `status`.

## Treasury TWAP

`scripts/beezee_treasury_twap.py` buys back (`side: buy`) or sells down (`side: sell`) `total_amount` of a pair's
base over `duration_hours`, one slice every `slice_interval_seconds` (default 300), without dumping or pumping the
price. The pair is traded in its own terms: a TOKEN/BZE TWAP buys or sells TOKEN against BZE and nothing is routed
through other pools (never BZE → USDC). A pair without BZE on either side is refused at start, since the fees and the
gas are paid in BZE. Upstream's `twap_executor` only takes the book; this script also uses the pool.

Every slice:

1. **Size.** The even slice is `total_amount` / number of slices. A slice is what is left over the slices left, never
   more than twice the even slice. With `catch_up: true` (default), what earlier slices did not trade (skipped or cut
   by the impact cap, the limit price or the reserve) is part of what is left, so it is spread over the later slices.
   With `catch_up: false` it is left over and reported at the end. Either way a refunded or failed part is still
   left to trade.
2. **Impact cap and limit price.** The slice is cut to the largest size whose own price impact is at most
   `max_impact_pct` (default 0.5): its average price against the venue's price before it, pool fee included. On the
   pool it is computed with `quote_swap`; on the book only levels within `max_impact_pct` of the best level are taken
   (`levels_to_take`). `limit_price` (optional) is never crossed: no buy above it, no sell below it, the pool's own
   fee included. If even the smallest size breaks either, or the price is already past the limit, the slice is
   skipped and the reason logged. A slice never trades bigger to make up for it.
3. **Reserves.** The input never comes out of `keep_quote_reserve` (buys) / `keep_base_reserve` (sells); when the
   input is BZE the taker fee and `estimated_gas_bze` come out of the part above the reserve too.
4. **Venue.** `venue: best` (default) prices the slice on both venues and sends the cheaper one per base (both pay
   one tradebin taker fee and the gas; the pool price includes its fee), never splitting a slice. `pool` always
   swaps (`amm_swap` with `slippage_pct`), `book` always takes the book (`fill_orders`). A book slice may be partly
   refunded by the chain; the refunded part stays in what is left.
5. **Accounting.** From the swap results and the fill events: base done and left, the average execution price
   against the reference TWAP (the pool price, or the book mid without a pool, sampled at every slice), the tradebin
   fees, the gas and the pool fees. When one entry of a book slice fails, the entries that filled still count.
6. **End.** At completion, at the deadline (after a slice still in flight settles) or on a halted pair, the script
   stops and logs a summary. Whatever is left is reported, never forced through. Run `stop` before starting it again
   (see "Stopping safely").

`status` shows the progress (done / total and the % of time elapsed), the next slice's size and the venue it would
take, the average execution price against the reference TWAP, the skipped slices with their reasons, the fees and gas,
and the reserves kept. The counters live in memory: after a restart, lower `total_amount` by what was already done.

**Buyback example.** A team wants to buy back 5,000 TOKEN over a week on a TOKEN/BZE pair around 1 BZE: `side: buy`,
`total_amount: 5000`, `duration_hours: 168`, `slice_interval_seconds: 600` (1,008 slices of about 5 TOKEN),
`max_impact_pct: 0.5`, `limit_price: 1.2`, `venue: best`, `keep_quote_reserve: 500`. Each slice buys about 5 TOKEN
wherever it is cheaper, never above 1.2 BZE, never moving the price more than 0.5%, and never spending the last
500 BZE. If TOKEN rises above 1.2 for a day, those slices are skipped and the later ones buy up to twice as much.

**Sell-down example.** A treasury sells 20,000 TOKEN over 24 hours: `side: sell`, `total_amount: 20000`,
`duration_hours: 24`, `slice_interval_seconds: 300` (288 slices of about 69.4 TOKEN), `max_impact_pct: 0.5`,
`limit_price: 0.8`, `catch_up: false`. A slice whose impact would pass 0.5% is cut; below 0.8 BZE nothing is sold;
with `catch_up: false` what was not sold is reported at the end instead of being pushed into later slices.

**Run.** `cp conf/scripts/beezee_treasury_twap_token_bze.yml.example hb/conf/scripts/beezee_treasury_twap.yml`,
replace the TOKEN placeholder, set the side, the amount, the period, the caps and the reserves, fund the wallet (plus
BZE for fees), then `start --v2 beezee_treasury_twap.yml`. `_token_bze` is a TOKEN/BZE template, `_mainnet` is for
BZE/USDC.inj and `_testnet` runs on testnet TBZE/TUSDC (see "AMM run guide"). Each documents every key; a test loads
them.

**Testnet.** Use a pair with BZE on one side, and a second wallet. On a pool-only pair (testnet LMN/TBZE) `venue: best`
trades on the pool alone; the venue comparison below needs a pair with both a pool and a book.

- [ ] Sell down a small total (e.g. 300 TOKEN) over 30 minutes (`duration_hours: 0.5`, `slice_interval_seconds: 300`,
      `venue: best`): one slice every 5 minutes, each within `max_impact_pct`.
- [ ] Move the book against the pool with the second wallet (a bid above the pool price, then none): the slices
      alternate venues as the prices differ.
- [ ] At the end, the summary (done, average price, fees) matches the trades in BZE Scan.

## AMM run guide

How to run the pool scripts and the pool-anchored controller above. Every one has three ready configs:

| Goal | Script / controller | Testnet (TBZE/TUSDC) | Mainnet (BZE/USDC.inj) | Template (TOKEN/BZE) |
|---|---|---|---|---|
| Keep the pool and the book in line | `scripts/beezee_pool_book_arb.py` | `conf/scripts/beezee_pool_book_arb_testnet.yml.example` | `…_mainnet.yml.example` | `…_token_bze.yml.example` |
| Quote on the book | `controllers/market_making/beezee_pool_anchored_pmm.py` | `conf/controllers/beezee_pool_anchored_pmm_testnet.yml.example` + `conf/scripts/beezee_pool_anchored_pmm_runner_testnet.yml.example` | `…_mainnet.yml.example` + `…_runner_mainnet.yml.example` | `…_token_bze.yml.example` |
| Volume on the pool | `scripts/beezee_pool_volume.py` | `conf/scripts/beezee_pool_volume_testnet.yml.example` | `…_mainnet.yml.example` | `…_token_bze.yml.example` |
| Price support | `scripts/beezee_price_bands.py` | `conf/scripts/beezee_price_bands_testnet.yml.example` | `…_mainnet.yml.example` | `…_token_bze.yml.example` |
| Treasury buyback / sell-down | `scripts/beezee_treasury_twap.py` | `conf/scripts/beezee_treasury_twap_testnet.yml.example` | `…_mainnet.yml.example` | `…_token_bze.yml.example` |

Each config says what to set and what one trade costs. TOKEN/BZE templates price, size and budget in BZE and never
route through USDC. Copy a config to `hb/conf/scripts/` (or `hb/conf/controllers/`) without `.example`, then
`start --v2 <file>.yml`; the controller starts through its runner.

### Which script for which goal

- **The pool and the book drift apart** (the book sells below the pool or buys above it): pool / order-book
  arbitrage. It takes the gap in one tx and holds nothing afterwards.
- **You want resting quotes on the book**: pool-anchored market making. It quotes around the pool price, which on most
  BZE pairs is deeper and harder to push than the book's own mid. With hedge mode it keeps the inventory flat.
- **You want volume**: pool volume prints it on the pool with round trips (cost: pool fee + 2 taker fees + gas per
  round trip). To print it on the order book instead, see "Volume target" (cost: 2 maker fees + gas per cycle).
- **You want to defend a price range**: price support bands. It buys below the floor and sells above the ceiling,
  within daily or total budgets.
- **A treasury buys back or sells down** a set amount over hours or days: treasury TWAP, one capped slice at a time on
  the cheaper venue.

### Pools and markets are separate venues

On BZE a liquidity pool and an order-book market are unrelated: a pool can exist with no market (the usual case for a
community token) and a market with no pool; a pair that has both has them by coincidence. The connector lists a pair
for every market and for every pool, and `status` shows each script's venues (`order book + pool`, `pool only`,
`order book only`). A pair keeps the same name whatever venues it has: a market's pair is named after the market, a
pool-only pair after the pool's two denoms (sorted, as the chain stores the pool), from the denoms' labels either way
(see "Denom labels").

- **Pool only**: pool volume, and bands and TWAP with `venue: pool`, run on any pool, with or without a market.
- **Book only**: volume target, TWAP with `venue: book` and upstream order-book strategies run on any market, with or
  without a pool.
- **Both**: the pool/book arbitrage and the pool-anchored controller (it quotes on the book and anchors on the pool)
  refuse to start, once and naming the missing venue, on a pair that lacks one. The controller still falls back to the
  book mid on a market without a pool, as documented above.
- `venue: best` (bands, TWAP) uses whatever venue the pair has and compares the two only when it has both. The bands'
  default `price_source` is the pool when the pair has one, else the book mid.
- On a pool-only pair the order-book calls (orders, cancels, `fill_orders`, `levels_to_take`) are refused with a clear
  error and never sent, and its order book stays empty; the pool calls work as on any pool. On a pair without a pool
  the pool calls return None or refuse, as before.

To trade a pool-only pair on the book too, create its market (`bzed tx tradebin create-market <base> <quote>`, fee
25,000 BZE on mainnet and testnet) and restart the client.

### Picking a pool and reading it against the book
- **BZE on one side.** Every AMM script pays the fees and the gas in BZE, so the arbitrage, pool volume, bands and
  TWAP scripts refuse a pair without BZE, and hedge mode needs it too.
- **Fee and depth.** `/bze/tradebin/all_liquidity_pools` on a REST endpoint lists every pool with its `fee` and
  reserves (in base units). The USDC pools charge 0.1%, most token pools 0.2–0.3%. On a constant-product pool a swap of
  `x` against a reserve `R` costs about `x / R` of price impact on top of the fee and moves the pool price about
  `2 × x / R`: keep trades to a small share of the reserve (`max_reserve_share_pct`, `max_impact_pct`).
- **Pool price against the book.** Every script's `status` shows the pool price in the pair's own terms (quote per
  base, the same units as the book), next to the best bid and ask where it uses the book. A pool price between the
  best bid and ask is normal. A best ask below the pool price, or a best bid above it, by more than the pool fee plus
  the fixed fees (2 × 0.1 BZE + gas, as a share of the trade) is an arbitrage the arb script takes. Quotes closer to
  the pool price than its fee can be arbitraged against you: the pool-anchored controller never places them.
- **A pool can point the other way.** A pool always orders its two denoms alphabetically, while a market's creator
  picks its base and quote, so a market and its pool can be inverses of each other: mainnet BZE/USDC.inj (pool
  `ibc/81DA…_ubze`, base USDC.inj) and testnet BZE/TUSDC (pool `factory/…/testusd_ubze`, base TUSDC) are like this.
  Read the pool in the pair's orientation, never in the pool's: with the pair's base B and quote Q, the pool price
  p = (pool reserve of Q) / (pool reserve of B), which is `reserve_base / reserve_quote` of the pool when the pool is
  inverted. Q into the pool pushes p up, B into it pushes p down. An ask on the book sells B and a bid buys B, and
  every size is in B (BZE on these pairs), whatever the pool's own base is.

### Funding a wallet and keeping reserves

- **One wallet per bot**, holding only what that bot may trade (see below). Fund it with both tokens of the pair, plus
  BZE for the fees.
- **What BZE pays for.** Gas on every tx (about 0.002–0.003 BZE at the 0.01 ubze minimum gas price, on mainnet and
  testnet), the 0.1 BZE tradebin taker fee for every swap message and every fill message (also a fill that matches
  nothing), and the 0.001 BZE maker fee for every order placed. A buy that spends the other token still pays its fees in
  BZE.
- **Reserves.** Each script keeps a floor it never spends: `reserve_base` / `reserve_quote` (arbitrage, pool volume),
  `keep_base_reserve` / `keep_quote_reserve` (bands, TWAP). On the BZE side the fees come out of the part above the
  reserve. The bands and TWAP scripts refuse to start when a reserve is larger than the balance. The controller has no
  reserve key: its `total_amount_quote` is the most it quotes, so fund the wallet with that plus the fees.
- **Testnet.** Fund a throwaway wallet with testnet BZE and swap part of it into TUSDC in testnet-dex (see
  "Testnet pair" below).

### One wallet per bot

Don't run two scripts or controllers on one wallet. They would share one balance (each script's reserves and budgets
assume nothing else spends it), one account sequence (two clients signing at once refuse each other's txs with
sequence mismatches), and one set of open orders (after a restart a bot adopts every open order on its pair, the other
bot's included; the arb script would take the controller's own quotes). Give every bot its own wallet, as
`hummingbot-deploy` does: one service = one market = one wallet.

### Stopping safely

- **Wait for the in-flight trade.** `status` shows a swap, arb, hedge or slice in flight. `stop` while nothing is in
  flight. A swap interrupted after its tx was broadcast is adopted on restart (`Adopted restored order …`) and settles
  from its tx: FILLED when the tx landed, FAILED when it failed or is never found. Only a swap stopped before its tx
  was accepted always ends FAILED, since whether it was sent is unknown. An adopted swap is not counted in the
  restarted script's volume or spend (its counters start from zero): check the wallet's last txs in the explorer
  before trusting the counters. The balances are read from the chain, so they are right either way.
- **After a script stops itself** (TWAP `stopped: completed` or at its deadline, bands or TWAP on a halt), the client
  still holds it as the running strategy. Run `stop` before the next `start`: otherwise `start` only answers
  "Invalid strategy. Start aborted."
- **Resting orders.** Only the controller rests orders; `stop` cancels them. Afterwards check in the dex app that the
  wallet has no order left on the book, and cancel any leftover there. The scripts hold no resting orders: swaps settle
  in their tx and taker fills are refunded at the end of their block.
- **Restart.** The scripts and the controller keep their counters in memory only, so a restart starts them from zero:

  | Script / controller | Forgotten on restart | Do before restarting |
  |---|---|---|
  | Arbitrage | open imbalances, the day's trades and profit | rebalance a kept imbalance by hand if you want it gone |
  | Pool-anchored MM | hedge counters, the fills seen, unhedged fills | nothing: the restart cancels the old quotes and places one new set |
  | Pool volume | the day's volume and fee spend | lower `daily_volume_quote` / `max_daily_fee_spend` by what was done today |
  | Bands | budget spent | with `budget_period: total`, lower the budgets by what was spent |
  | TWAP | amount done, slices, reference TWAP | lower `total_amount` (and `duration_hours`) by what was done |

  A `docker kill` leaves the controller's orders on the book; the restart cancels them before the first tick and the
  controller places a new set (see "Restart with open orders"). `stop` cancels them first.

### Known behaviour

- **Testnet gas.** The testnet minimum gas price is 0.01 ubze, like mainnet; 0.001 is refused as insufficient fees.
- **Crossing an order by hand.** A crossing order must be priced exactly at the best price on the other side: the
  chain refuses a buy above the best ask (or a sell below the best bid) with "A better price is available". Price test
  orders that should cross at the best level itself.
- **Inventory skew in pure market making.** Upstream `pure_market_making` with `inventory_skew_enabled` stops placing
  bids once the wallet holds about 86% base. Fund the wallet near the target split before expecting a two-sided book. The pool-anchored controller has no inventory skew.
- **One-sided market making on a thin book walks its own price.** A book-mid strategy that quotes on one side only
  counts its own orders in the mid, so each refresh moves its reference toward them. Seed both sides, or anchor to the
  pool with the pool-anchored controller.
- **A fill that matches nothing still pays the 0.1 BZE taker fee**, but no trade carries it: the balances are right
  (read from the chain), while the PnL and the trade history miss that fee.
- **Pool swaps in `history`.** A swap's trade holds the input amount net of the pool fee, with the pool fee shown
  separately as a flat fee in the input token: a 2 TUSDC sell shows as 1.996 TUSDC plus a 0.004 TUSDC fee. The
  balances are exact.
- **Arbitrage.** Its state (an arb in flight, open imbalances, the counters) is in memory only. When it buys on the pool
  and sells into bids, the swap may deliver up to `slippage_pct` less base than the fill sold; that drift is not
  recorded as an imbalance, it only shows in the balances.
- **Pool-anchored MM.** A halt is seen only through the pool's `halted` flag. On a pair without a pool the connector
  still refuses orders on a halted denom, but the controller keeps trying.
- **Pool volume.** A halted pool pauses the script and it resumes by itself once the halt is lifted. If a round trip's
  completion is ever missed, the next ones wait behind it until a restart.
- **Bands and TWAP.** A halt stops them; once it is lifted, `stop` and then `start` them again (a `start` without
  the `stop` is refused with "Invalid strategy. Start aborted.").
- **TWAP.** With `catch_up: false`, a part the book refunded still stays in what is left and is traded later. Once
  nothing is left, the remaining slices log a "nothing left" skip until the deadline.
- **Pair names.** Pairs are named from the denom labels (see "Denom labels"): BZE/USDC.inj is `BZE-USDC.INJ` with
  USDC in whole USDC, testnet BZE/TUSDC is `TBZE-TUSDC`. A config with a name from before labels does not start and
  the log gives the new name.
- **Pool volume price guard.** A large swap from someone else and its revert both move the pool price, so a push and
  revert between cycles can skip two cycles in a row, not one.
- **Startup log.** The one-time "Beezee connector not ready. Pending status flags: …" line can leave out
  `pools_initialized` when TWAP or bands start: the pools load right after it. This is harmless.

### Testnet pair

Testnet (`bzetestnet-3`, October 2026) has two order-book markets: `LMN-TUSDC`, which has no pool and no BZE side, so
none of the AMM scripts can use it, and **BZE/TUSDC** (`TBZE-TUSDC`), which the testnet configs use. Its market id is
`ubze/factory/bze1z3mkcr2jz424w6m49frgjmy9uhlrx69phqwg3l/testusd` (base testnet BZE, quote TUSDC). Its pool is
`factory/…/testusd_ubze` (fee 0.2%), whose base is TUSDC: the market was created opposite to the pool on purpose, to
match mainnet BZE/USDC.inj (see "A pool can point the other way" above). Other pools with BZE are LMN/TBZE (fee 0.1%)
and VDL/TBZE (fee 0.3%); the connector lists them as pool-only pairs.

Reading TBZE-TUSDC (October 2026: about 178,000 TUSDC and 234,000 TBZE in the pool):

- **p** is TUSDC per TBZE, about 0.763: the pool's `reserve_base / reserve_quote` (TUSDC / TBZE), since the pool is
  inverted. `status` shows it already in the pair's orientation.
- **TUSDC into the pool pushes p up**, TBZE into it pushes p down.
- **Asks sell TBZE, bids buy TBZE**, and every size (orders, fills, arb sizes, TWAP slices) is in TBZE. A row of an
  older checklist written for TUSDC-BZE has its asks and bids swapped and its sizes in TUSDC: translate it.
- **Arbitrage drift.** When the arb buys on the pool and sells into bids, the pool leg can deliver up to `slippage_pct`
  less than the fill sold; on this pair that drift lands on the TUSDC side.

The market exists, so skip creating it. On a new testnet, or for another pair, it is created once (25,000 testnet
BZE, paid by the creating wallet), base first:

```bash
bzed tx tradebin create-market ubze factory/bze1z3mkcr2jz424w6m49frgjmy9uhlrx69phqwg3l/testusd \
  --from <test wallet> --keyring-backend test --chain-id bzetestnet-3 \
  --node https://testnet-rpc.getbze.com:443 --gas 300000 --fees 3000ubze
```

Then restart the client: `status` shows `order book + pool` for the pair. The run itself follows "Testnet run" with the
config of the script.

## Connector behaviour on chain v8.2.0

- **Wallet modes.** `wallet` takes a hex private key. `wallet_mnemonic` takes a BIP39 mnemonic (optional passphrase)
  and derives the key on `hd_path`, default `m/44'/118'/0'/0/0`, the same as `bzed keys add --recover`. The
  derivation is pinned by a test against the address `bzed` v8.2.0 recovers from the public BIP39 test mnemonic.
  `delegated` and `delegated_mnemonic` take the same key forms for the bot key, plus `granter_address` (see
  "Delegated account"); their tx shape is pinned by a test against `bzed tx authz exec --fee-granter`.
- **Gas.** Every create and cancel is simulated first. Gas limit = `ceil(gas_used × gas_adjustment)` + the queue
  extra below, fee = `ceil(gas limit × gas_price)` in ubze. Defaults: `gas_price` `0.01` (the mainnet public-node
  minimum), `gas_adjustment` `1.3`. Both are network settings. A failed simulation is never broadcast. The fixed
  `create_order_gas_limit` / `cancel_order_gas_limit` settings are gone, and old configs that still have them load
  and ignore them.
- **Several orders in one block.** Tradebin queues every create and cancel and runs the queue at the end of the
  block. A create pays extra gas for each message already queued on its market in that block: the tradebin param
  `orderBookQueueMessageScanExtraGas` (5000) plus the store reads of that message (about 6,200 gas per message on
  testnet). The simulation runs on the last committed state, where the queue is empty, so it cannot see this. With
  `gas_adjustment` alone, the tenth single-create tx of a burst would need about 1.53× its simulated gas: the txs
  beyond 1.3× ran out of gas (code 11). So, for each create, the connector adds that per-message gas once for every
  message of its own still waiting for a block on the same market (its tx was broadcast but is not found in a block
  yet). The scan param is read from the chain with the trading fees. Messages of other accounts in the same block
  are not known in advance: the `gas_adjustment` slack covers about three of them for a single create. Once a block
  holds more than `orderBookExtraGasWindow` (100) queued tradebin messages, each further create also pays
  `orderBookQueueExtraGas` (25,000) per message over that window, which the connector cannot foresee: on such a busy
  block creates cost much more and may run out of gas. A create that runs out of gas fails like any other failed
  create tx.
- **Trading fees.** The tradebin maker/taker fees (flat amounts, in ubze on mainnet) are read from the chain params
  and added to every order's fee estimate, so PnL and profitability checks include them.
- **Halted markets.** A market whose base or quote denom is halted by governance is exit-only: the connector refuses
  new orders on it, cancels its open orders (and keeps cancelling any still open while the halt lasts), and trades
  again once the halt is lifted. The halted list is read at start and every 60 s, and a tx refused with
  `denom is halted` takes effect at once.
- **Order tracking.** The order book is asynchronous: a tx only queues the order, and the chain executes it in that
  block or a later one. The connector looks up each create/cancel tx for its queue `message_id`, then reads the blocks
  (RPC `/block_results`) for the events carrying that id: fills, the saved order and its id, the cancel. A resting
  order's later fills and its cancel are matched by its order id, so a cancel made outside the bot (or sent by a
  `stop` right before the bot exited) is seen in its block. An order with no outcome within 20 blocks of its tx is marked
  failed. A partial fill whose rest the chain refunds ends `CANCELED` with its fills. After a restart, missed fills
  are read back from the last processed block (up to 1000 blocks); beyond that the connector reconciles resting
  orders from the chain and logs a warning. A create that far behind takes its unclaimed order on the book, even a
  partly filled one (the difference booked as one fill), and fails only when the book has none. The RPC endpoint must
  serve `/block_results` for recent heights.
- **Restart with open orders.** After a crash, `docker kill` or a container restart, the bot keeps working with the
  previous run's orders instead of cancelling them, so the book never goes empty and no create fee is paid twice.
  Start the same strategy file again: the saved tracking state brings the orders back, and when the strategy starts,
  before its first tick, every open order on a pair it trades is adopted as if the strategy had placed it. PMM keeps
  an adopted level within `order_refresh_tolerance_pct` and replaces it otherwise, like its own orders, so a restart
  leaves one set of orders, never two. Orders still waiting for their chain id (killed right after the create tx) are
  adopted too. An order stopped before its create tx was even sent has no tx and no id: on the first update it takes
  an unclaimed order of this account on the book with its side, price and amount (or a smaller remaining amount, the
  difference booked as one fill), else it is failed with a warning and the strategy places a new one. Fills that
  happened while the bot was down are still read back and close or shrink the adopted order.
  An order on a pair the new config no longer trades is cancelled, logged at INFO. `max_order_age` counts from the
  restart for an adopted order. The adoption lives in the strategy base, so every strategy built on it gets it (tested
  with PMM); `beezee_volume_target.py` places its orders past that tracker, so it also counts the restored orders on
  its pair as its own, and its first cycle cancels a leg a crash left half-done. A V2 strategy with controllers
  (`v2_with_controllers`, e.g. the pool-anchored controller) is the exception: its controllers trade only through
  executors, which never own a restored order, so its restored orders are cancelled at start (logged at INFO) and the
  controllers place a new set on their first tick.
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
  it on the order's first fill only. It is the taker fee only when the fill crossed an order that rested before the
  create's tx block; crossing an order saved in that block or later (both legs of a same-tx self-fill) books the maker
  fee, as the chain charged. An order that rests and never fills still paid the maker fee, which Hummingbot
  cannot record without a fill.
