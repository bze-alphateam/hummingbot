import hashlib
import logging
import re
import unittest
from decimal import Decimal
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from aioresponses import aioresponses

from hummingbot.connector.exchange.beezee import beezee_constants as CONSTANTS
from hummingbot.connector.exchange.beezee.beezee_denom_labels import BeezeeDenomLabels, DenomLabelError
from hummingbot.connector.exchange.beezee.beezee_exchange import BeezeeExchange
from hummingbot.connector.exchange.beezee.beezee_utils import (
    BeezeeConfigMap,
    BeezeeCustomNetworkMode,
    BeezeeReadOnlyAccountMode,
)

REST_URL = "https://rest.example"
RPC_URL = "https://rpc.example"
# USDC bridged from Injective: on beezee-1 its bank metadata has no display unit and an unreadable symbol.
USDC_INJ = "ibc/81DA528F4C5546208D1D50F84C2D0B388B45D37AA36FCA622A3CB2D8EF014838"  # noqa: mock
USDC_INJ_METADATA = {
    "symbol": "ERC20:0XA00C59FF5A080D2B954D0C75E46E22A0C371235A",
    "display": USDC_INJ,
    "denom_units": [{"denom": USDC_INJ, "exponent": 0}],
}
OLD_PAIR = "BZE-ERC20:0XA00C59FF5A080D2B954D0C75E46E22A0C371235A"
TESTUSD = "factory/bze1z3mkcr2jz424w6m49frgjmy9uhlrx69phqwg3l/testusd"
FAKE_BZE = "factory/bze1faker/ufake"


def _metadata(symbol: str, denom: str, exponent: int = 6) -> Dict[str, Any]:
    return {"symbol": symbol, "display": symbol.lower(),
            "denom_units": [{"denom": denom, "exponent": 0}, {"denom": symbol.lower(), "exponent": exponent}]}


METADATA = {
    "ubze": _metadata("BZE", "ubze"),
    USDC_INJ: USDC_INJ_METADATA,
    TESTUSD: _metadata("TUSDC", TESTUSD),
    # Anyone can create a factory token and call it BZE.
    FAKE_BZE: _metadata("BZE", FAKE_BZE),
}


def pool(base: str, quote_denom: str) -> Dict[str, Any]:
    return {"id": f"{base}_{quote_denom}", "base": base, "quote": quote_denom, "lp_denom": f"ulp_{base}_{quote_denom}",
            "fee": "0.001", "fee_dest": {"treasury": "0", "burner": "0", "providers": "1"},
            "reserve_base": "1000000000", "reserve_quote": "2000000000000"}


def make_config(chain_id: str = CONSTANTS.MAINNET_CHAIN_ID, denom_labels: Optional[Dict[str, str]] = None) -> BeezeeConfigMap:
    return BeezeeConfigMap(
        connector="beezee",
        network=BeezeeCustomNetworkMode(rest_endpoint=REST_URL, rpc_endpoint=RPC_URL, chain_id=chain_id),
        account_type=BeezeeReadOnlyAccountMode(address="bze1test"),
        denom_labels=denom_labels or {},
    )


class BeezeeDenomLabelTests(unittest.IsolatedAsyncioTestCase):
    """The connector against mocked REST: pair names, decimals and balances come from the denoms' labels."""

    def setUp(self) -> None:
        self.mock_api = aioresponses()
        self.mock_api.start()
        self.addCleanup(self.mock_api.stop)
        self.log_records: List[logging.LogRecord] = []

    def handle(self, record):
        self.log_records.append(record)

    def _mock_chain(self, markets: List[Dict[str, str]], pools: List[Dict[str, Any]]):
        for path, key, items in (
            (CONSTANTS.TRADING_PAIRS_PATH_URL, "market", markets),
            (CONSTANTS.ALL_LIQUIDITY_POOLS_PATH_URL, "list", pools),
        ):
            self.mock_api.get(re.compile(rf"^{re.escape(REST_URL + path)}(\?.*)?$"),
                              payload={key: items, "pagination": {"next_key": None}}, repeat=True)
        for denom, metadata in METADATA.items():
            self.mock_api.get(f"{REST_URL}{CONSTANTS.DENOM_METADATA_PATH_URL}/{quote(denom, safe='')}",
                              payload={"metadata": metadata}, repeat=True)
        self.mock_api.get(f"{REST_URL}{CONSTANTS.TRADEBIN_PARAMS_PATH_URL}", payload={"params": {
            "marketMakerFee": {"denom": "ubze", "amount": "1000"},
            "marketTakerFee": {"denom": "ubze", "amount": "100000"},
        }}, repeat=True)

    def _exchange(self, config: BeezeeConfigMap, trading_pairs: List[str]) -> BeezeeExchange:
        exchange = BeezeeExchange(connector_configuration=config, trading_pairs=trading_pairs, trading_required=False)
        exchange.logger().setLevel(1)
        exchange.logger().addHandler(self)
        self.addCleanup(exchange.logger().removeHandler, self)
        return exchange

    def _errors(self) -> List[str]:
        return [record.getMessage() for record in self.log_records if record.levelno == logging.ERROR]

    async def test_mainnet_usdc_inj_is_labelled_as_on_the_dex_with_its_registry_decimals(self):
        self._mock_chain(markets=[{"base": "ubze", "quote": USDC_INJ}], pools=[pool(USDC_INJ, "ubze")])
        exchange = self._exchange(make_config(), ["BZE-USDC.INJ"])

        self.assertEqual(["BZE-USDC.INJ"], await exchange.all_trading_pairs())
        await exchange._update_trading_rules()

        rule = exchange.trading_rules["BZE-USDC.INJ"]
        self.assertEqual("USDC.INJ", rule.buy_order_collateral_token)
        self.assertEqual("BZE", rule.sell_order_collateral_token)
        # 6 decimals from the asset list, not the metadata's base units.
        self.assertEqual(Decimal("0.000001"), rule.min_quote_amount_increment)
        self.assertEqual(("book", "pool"), exchange.trading_pair_venues("BZE-USDC.INJ"))
        self.assertTrue(exchange.status_dict["trading_pair_names_current"])
        self.assertEqual([], self._errors())

    async def test_an_old_pair_name_keeps_the_connector_not_ready_and_logs_the_new_name(self):
        self._mock_chain(markets=[{"base": "ubze", "quote": USDC_INJ}], pools=[])
        exchange = self._exchange(make_config(), [OLD_PAIR])

        await exchange._update_trading_rules()
        await exchange._update_trading_rules()

        self.assertFalse(exchange.status_dict["trading_pair_names_current"])
        errors = [message for message in self._errors() if OLD_PAIR in message]
        self.assertEqual(1, len(errors), "logged once, not on every refresh")
        self.assertIn(f"Beezee pair '{OLD_PAIR}' is now named 'BZE-USDC.INJ'", errors[0])

    async def test_an_override_wins_over_the_asset_list(self):
        self._mock_chain(markets=[{"base": "ubze", "quote": USDC_INJ}], pools=[])
        exchange = self._exchange(make_config(denom_labels={USDC_INJ: "usdc-inj"}), ["BZE-USDC_INJ"])

        self.assertEqual(["BZE-USDC_INJ"], await exchange.all_trading_pairs())
        await exchange._update_trading_rules()
        self.assertEqual(Decimal("0.000001"), exchange.trading_rules["BZE-USDC_INJ"].min_quote_amount_increment)
        self.assertTrue(exchange.status_dict["trading_pair_names_current"])

    async def test_a_metadata_label_never_takes_a_verified_one(self):
        self._mock_chain(markets=[{"base": FAKE_BZE, "quote": "ubze"}, {"base": "ubze", "quote": USDC_INJ}], pools=[])
        exchange = self._exchange(make_config(), ["BZE-USDC.INJ"])

        fake_label = f"BZE_{hashlib.sha1(FAKE_BZE.encode()).hexdigest()[:4].upper()}"
        self.assertEqual(sorted([f"{fake_label}-BZE", "BZE-USDC.INJ"]), sorted(await exchange.all_trading_pairs()))
        warnings = [record.getMessage() for record in self.log_records if record.levelno == logging.WARNING]
        self.assertTrue(any(FAKE_BZE in message and fake_label in message for message in warnings))

    async def test_a_token_without_an_asset_list_entry_keeps_its_metadata_symbol(self):
        self._mock_chain(markets=[{"base": "ubze", "quote": TESTUSD}], pools=[])
        exchange = self._exchange(make_config(), [])

        self.assertEqual(["BZE-TUSDC"], await exchange.all_trading_pairs())

    async def test_testnet_uses_the_testnet_dex_labels(self):
        self._mock_chain(markets=[{"base": "ubze", "quote": TESTUSD}], pools=[pool(TESTUSD, "ubze")])
        exchange = self._exchange(make_config(chain_id=CONSTANTS.TESTNET_CHAIN_ID), ["BZE-TUSDC"])

        self.assertEqual(["TBZE-TUSDC"], await exchange.all_trading_pairs())
        await exchange._update_trading_rules()
        self.assertFalse(exchange.status_dict["trading_pair_names_current"])
        self.assertTrue(any("is now named 'TBZE-TUSDC'" in message for message in self._errors()))


class BeezeeDenomLabelConfigTests(unittest.TestCase):
    def test_two_denoms_with_one_label_refuse_the_config(self):
        with self.assertRaisesRegex(ValueError, "'BZE' for .*ubze"):
            make_config(denom_labels={FAKE_BZE: "BZE"})

    def test_two_overrides_with_one_label_refuse_the_config(self):
        with self.assertRaisesRegex(ValueError, "must be unique"):
            make_config(denom_labels={FAKE_BZE: "FAKE", TESTUSD: "fake"})

    def test_an_empty_override_is_refused(self):
        with self.assertRaisesRegex(ValueError, "is empty"):
            make_config(denom_labels={FAKE_BZE: " "})

    def test_a_label_clash_is_checked_on_the_configured_chain_only(self):
        # TBZE is ubze's label on testnet only.
        self.assertEqual({FAKE_BZE: "TBZE"}, make_config(denom_labels={FAKE_BZE: "TBZE"}).denom_labels)
        with self.assertRaises(ValueError):
            make_config(chain_id=CONSTANTS.TESTNET_CHAIN_ID, denom_labels={FAKE_BZE: "TBZE"})

    def test_the_connector_refuses_to_start_on_a_clash(self):
        # A config built without validation: the connector checks the labels again.
        config = BeezeeConfigMap.model_construct(
            connector="beezee",
            network=BeezeeCustomNetworkMode(rest_endpoint=REST_URL, rpc_endpoint=RPC_URL, chain_id=CONSTANTS.MAINNET_CHAIN_ID),
            account_type=BeezeeReadOnlyAccountMode(address="bze1test"),
            denom_labels={FAKE_BZE: "USDC.INJ"},
        )
        with self.assertRaises(DenomLabelError):
            BeezeeExchange(connector_configuration=config, trading_pairs=[], trading_required=False)

    def test_labels_are_stable_for_the_whole_run(self):
        labels = BeezeeDenomLabels.for_chain(CONSTANTS.MAINNET_CHAIN_ID)
        first = labels.token(FAKE_BZE, METADATA[FAKE_BZE])
        self.assertEqual(first, labels.token(FAKE_BZE, METADATA[FAKE_BZE]))
        self.assertEqual("BZE", labels.token("ubze", METADATA["ubze"]).symbol)
        self.assertNotEqual("BZE", first.symbol)
