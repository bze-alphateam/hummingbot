import unittest
from decimal import Decimal

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_signer import BeezeeSigner
from hummingbot.connector.exchange.beezee.beezee_utils import (
    DEFAULT_FEES,
    BeezeeConfigMap,
    BeezeeMainnetNetworkMode,
    BeezeeMnemonicWalletAccountMode,
    BeezeeTestnetNetworkMode,
    BeezeeToken,
    BeezeeWalletAccountMode,
    chain_amount_to_display,
    chain_price_to_display,
    derive_private_key_from_mnemonic,
    display_amount_to_chain,
    display_price_to_chain,
    market_from_data,
    minimum_order_size_for_price,
    normalize_hd_path,
    normalize_mnemonic,
    parse_coin,
    private_key_from_account_mode,
    token_from_metadata,
    unique_market_symbols,
)
from hummingbot.core.data_type.trade_fee import TokenAmount


class BeezeeUtilsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.base_metadata = {
            "symbol": "BZE",
            "name": "Beezee",
            "display": "bze",
            "denom_units": [
                {"denom": "ubze", "exponent": 0},
                {"denom": "bze", "exponent": 6},
            ],
        }
        self.quote_metadata = {
            "symbol": "USDC",
            "name": "USD Coin",
            "display": "usdc",
            "denom_units": [
                {"denom": "uusdc", "exponent": 0},
                {"denom": "usdc", "exponent": 6},
            ],
        }

    def test_token_from_metadata_uses_display_exponent(self):
        token = token_from_metadata("ubze", self.base_metadata)

        self.assertEqual("BZE", token.symbol)
        self.assertEqual(Decimal("0.000001"), token.quantum)

    def test_unique_market_symbols_disambiguates_duplicates(self):
        duplicate_base_metadata = {
            "factory/alt/ubze": {
                **self.base_metadata,
                "symbol": "BZE",
                "display": "altbze",
                "denom_units": [
                    {"denom": "factory/alt/ubze", "exponent": 0},
                    {"denom": "altbze", "exponent": 6},
                ],
            }
        }
        mapping = unique_market_symbols(
            markets=[
                ("ubze", "uusdc"),
                ("factory/alt/ubze", "uusdc"),
            ],
            metadatas={
                "ubze": self.base_metadata,
                "uusdc": self.quote_metadata,
                **duplicate_base_metadata,
            },
        )

        self.assertEqual("BZE-USDC", mapping["ubze/uusdc"])
        self.assertNotEqual("BZE-USDC", mapping["factory/alt/ubze/uusdc"])

    def test_market_amount_and_price_conversions(self):
        market = market_from_data(
            {"base": "ubze", "quote": "uusdc"},
            {"ubze": self.base_metadata, "uusdc": self.quote_metadata},
        )

        self.assertEqual("1230000", display_amount_to_chain(Decimal("1.23"), market.base))
        self.assertEqual(Decimal("1.23"), chain_amount_to_display("1230000", market.base))
        self.assertEqual("2.5", display_price_to_chain(Decimal("2.5"), market))
        self.assertEqual(Decimal("2.5"), chain_price_to_display("2.5", market))

    def test_minimum_order_size_uses_chain_rule(self):
        base = BeezeeToken(denom="ubze", symbol="BZE", display_name="Beezee", display_denom="bze", exponent=6)
        quote = BeezeeToken(denom="uusdc", symbol="USDC", display_name="USD Coin", display_denom="usdc", exponent=6)
        market = market_from_data(
            {"base": base.denom, "quote": quote.denom},
            {
                base.denom: self.base_metadata,
                quote.denom: self.quote_metadata,
            },
        )

        self.assertEqual(Decimal("0.000004"), minimum_order_size_for_price("0.5", market))
        self.assertEqual(
            Decimal("0.0000000001"),
            market.min_price_increment,
        )
        self.assertEqual(CONSTANTS.DEFAULT_PRICE_SIG_DIGITS, 10)

    def test_normalize_mnemonic_validates_word_count_and_whitespace(self):
        mnemonic = normalize_mnemonic("  abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about  ")

        self.assertEqual(
            "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
            mnemonic,
        )

    def test_normalize_hd_path_accepts_standard_cosmos_path(self):
        self.assertEqual("m/44'/118'/0'/0/0", normalize_hd_path("m/44'/118'/0'/0/0"))

    def test_derive_private_key_from_mnemonic_is_stable(self):
        mnemonic = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"

        private_key_a = derive_private_key_from_mnemonic(mnemonic=mnemonic)
        private_key_b = derive_private_key_from_mnemonic(mnemonic=mnemonic)

        self.assertEqual(private_key_a, private_key_b)
        self.assertEqual(64, len(private_key_a))

    def test_private_key_from_account_mode_derives_from_mnemonic_wallet(self):
        account_mode = BeezeeMnemonicWalletAccountMode(
            mnemonic="abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
        )

        private_key = private_key_from_account_mode(account_mode)

        self.assertIsNotNone(private_key)
        self.assertEqual(64, len(private_key))

    def test_config_map_accepts_testnet_network(self):
        config = BeezeeConfigMap(
            connector="beezee",
            network=BeezeeTestnetNetworkMode(
                rest_endpoint="https://testnet-rest.example",
                rpc_endpoint="https://testnet-rpc.example",
            ),
        )

        self.assertEqual(CONSTANTS.TESTNET_CHAIN_ID, config.network.chain_id)

    def test_mnemonic_derivation_matches_bzed_test_vector(self):
        # Expected address from `bzed keys add --recover --keyring-backend test` (bzed v8.2.0),
        # for the public BIP39 test mnemonic on the default path m/44'/118'/0'/0/0.
        account = BeezeeMnemonicWalletAccountMode(
            mnemonic="abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
        )

        signer = BeezeeSigner(private_key_from_account_mode(account), "bze")

        self.assertEqual("m/44'/118'/0'/0/0", account.hd_path)
        self.assertEqual("bze19rl4cm2hmr8afy4kldpxz3fka4jguq0apypkdz", signer.address)

    def test_parse_coin_accepts_coin_objects_and_legacy_strings(self):
        self.assertEqual(("ubze", 1000), parse_coin({"denom": "ubze", "amount": "1000"}))
        self.assertEqual(("ubze", 100000), parse_coin("100000ubze"))
        self.assertEqual(("factory/bze1x/mill", 5), parse_coin("5factory/bze1x/mill"))
        self.assertIsNone(parse_coin(None))
        self.assertIsNone(parse_coin("ubze"))

    def test_network_gas_defaults(self):
        network = BeezeeMainnetNetworkMode()

        self.assertEqual(Decimal("0.01"), network.gas_price)
        self.assertEqual(Decimal("1.3"), network.gas_adjustment)

    def test_saved_configs_with_fixed_gas_limits_still_load(self):
        # Same path as Hummingbot's load_connector_config_map_from_file for conf/connectors/beezee.yml.
        config = BeezeeConfigMap.model_validate({
            "connector": "beezee",
            "network": "mainnet",
            "account_type": {
                "name": "wallet",
                "private_key": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",  # noqa: mock
                "address": None,
                "create_order_gas_limit": 250000,
                "cancel_order_gas_limit": 200000,
            },
        })

        self.assertIsInstance(config.account_type, BeezeeWalletAccountMode)
        self.assertNotIn("create_order_gas_limit", config.account_type.model_dump())
        self.assertNotIn("cancel_order_gas_limit", config.account_type.model_dump())

    def test_chain_price_step_is_tradebin_minimum_for_any_exponents(self):
        # Tradebin v8.2.0 has no tick size: prices are 18-decimal LegacyDecs that must exceed 1e-10.
        # The connector quantizes to 10 chain-price decimals, i.e. a chain step of exactly 1e-10.
        for base_exponent, quote_exponent in ((6, 6), (6, 18), (18, 6), (0, 6)):
            with self.subTest(base_exponent=base_exponent, quote_exponent=quote_exponent):
                market = market_from_data(
                    {"base": "ubase", "quote": "uquote"},
                    {
                        "ubase": {"display": "base", "denom_units": [{"denom": "base", "exponent": base_exponent}]},
                        "uquote": {"display": "quote", "denom_units": [{"denom": "quote", "exponent": quote_exponent}]},
                    },
                )

                self.assertEqual(Decimal("1E-10"), market.min_price_increment * market.display_price_scaler)

    def test_default_fees_are_in_the_bze_symbol(self):
        self.assertEqual([TokenAmount("BZE", Decimal("0.001"))], DEFAULT_FEES.maker_fixed_fees)
        self.assertEqual([TokenAmount("BZE", Decimal("0.1"))], DEFAULT_FEES.taker_fixed_fees)
