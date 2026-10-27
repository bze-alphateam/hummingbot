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
