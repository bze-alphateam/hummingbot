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
   - gas price `0.01` (the chain minimum; a higher one works too), gas adjustment `1.3`;
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
`coingecko_id_quote` (1 when unset) and multiplied by `anchor_price_scale` to get the connector's price units: 1000000
for BZE/USDC.inj, whose quote the bot counts in base units. It refreshes every `anchor_refresh_seconds` (two requests;
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
`trading_pair: ULMN-TUSDC`, `anchor_price_scale: 1` and a small `daily_volume_quote`. The book needs a bid and an ask
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

- **Which pool.** A pair has at most one pool, whose id is its two denoms sorted and joined by `_`. Hummingbot pairs
  follow the order-book market (`base/quote`), so the pool's base can be the pair's quote: BZE-USDC (market
  `ubze/ibc/81DA…`) trades against pool `ibc/81DA…_ubze`. The connector reads reserves by denom, so every price and
  quote comes out in the pair's own orientation.
- **Refresh.** All pools are read once, then the pools of the traded pairs on every new block. A failed read keeps
  the last reserves and logs a warning. A pool is halted while either of its denoms is in the halted list (the same
  list the order book uses).
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
    The fill is read from the swap's `SwapEvent`: price in the pair's order-price units (quote per base), amounts
    before the pool fee, and fees = the pool fee (in the input token) + the tradebin taker fee (0.1 BZE on mainnet,
    read from the chain params, once per swap). So `OrderFilledEvent`, the trade history and PnL treat a swap like an
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

**Run.** `cp conf/scripts/beezee_pool_book_arb.yml.example hb/conf/scripts/beezee_pool_book_arb.yml`, set
`max_trade_base` and the reserves, fund the wallet with both tokens (plus BZE for fees), then
`start --v2 beezee_pool_book_arb.yml`. The example is for BZE/USDC.inj on mainnet and documents every key; a test
loads it.

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

**Run.** `cp conf/scripts/beezee_pool_volume.yml.example hb/conf/scripts/beezee_pool_volume.yml`, set
`daily_volume_quote` and `max_daily_fee_spend`, fund the wallet with both tokens (plus BZE for fees), then
`start --v2 beezee_pool_volume.yml`. The example is for BZE/USDC.inj on mainnet and documents every key; a test
loads it.

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

**Run.** Copy `conf/controllers/beezee_pool_anchored_pmm.yml.example` (BZE/USDC.inj, mainnet) or
`beezee_pool_anchored_pmm_token_bze.yml.example` (a TOKEN/BZE template, replace the placeholders) to
`conf/controllers/<name>.yml` and set `total_amount_quote`. Then `create --script-config v2_with_controllers`, list
`<name>.yml` under `controllers_config`, and `start --v2 <that script config>.yml`. Both examples document every key
and a test loads them.

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
   balance. A halted pair (the pool's halted flag, or a refusal saying so) stops it, logged once; restart it once the
   halt is lifted.

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

**Run.** `cp conf/scripts/beezee_price_bands.yml.example hb/conf/scripts/beezee_price_bands.yml`, replace the TOKEN
placeholder, set the band, the caps, the reserves and the budgets, fund the wallet with both tokens (plus BZE for
fees), then `start --v2 beezee_price_bands.yml`. The example is a TOKEN/BZE template that documents every key; a test
loads it.

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
   stops and logs a summary. Whatever is left is reported, never forced through.

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

**Run.** `cp conf/scripts/beezee_treasury_twap.yml.example hb/conf/scripts/beezee_treasury_twap.yml`, replace the TOKEN
placeholder, set the side, the amount, the period, the caps and the reserves, fund the wallet (plus BZE for fees),
then `start --v2 beezee_treasury_twap.yml`. The example is a TOKEN/BZE template that documents every key; a test loads
it.

**Testnet.** Use a pair with BZE on one side that has both a pool and a book, and a second wallet.

- [ ] Sell down a small total (e.g. 300 TOKEN) over 30 minutes (`duration_hours: 0.5`, `slice_interval_seconds: 300`,
      `venue: best`): one slice every 5 minutes, each within `max_impact_pct`.
- [ ] Move the book against the pool with the second wallet (a bid above the pool price, then none): the slices
      alternate venues as the prices differ.
- [ ] At the end, the summary (done, average price, fees) matches the trades in BZE Scan.

## Connector behaviour on chain v8.2.0

- **Wallet modes.** `wallet` takes a hex private key. `wallet_mnemonic` takes a BIP39 mnemonic (optional passphrase)
  and derives the key on `hd_path`, default `m/44'/118'/0'/0/0`, the same as `bzed keys add --recover`. The
  derivation is pinned by a test against the address `bzed` v8.2.0 recovers from the public BIP39 test mnemonic.
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
  its pair as its own, and its first cycle cancels a leg a crash left half-done.
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
