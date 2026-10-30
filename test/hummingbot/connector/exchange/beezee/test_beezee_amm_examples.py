import unittest
from pathlib import Path

import yaml

from controllers.market_making.beezee_pool_anchored_pmm import BeezeePoolAnchoredPMMConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2ConfigBase
from scripts.beezee_pool_book_arb import BeezeePoolBookArbConfig
from scripts.beezee_pool_volume import BeezeePoolVolumeConfig
from scripts.beezee_price_bands import BeezeePriceBandsConfig
from scripts.beezee_treasury_twap import BeezeeTreasuryTwapConfig

CONF = Path(__file__).resolve().parents[5] / "conf"

# Every AMM script and controller ships a testnet config, a BZE/USDC.inj mainnet config and a TOKEN/BZE template.
FLAVOUR_PAIRS = {
    "mainnet": "BZE-ERC20:0XA00C59FF5A080D2B954D0C75E46E22A0C371235A",
    "testnet": "TUSDC-BZE",
    "token_bze": "TOKEN-BZE",
}
AMM_CONFIGS = {
    "beezee_pool_book_arb": (CONF / "scripts", BeezeePoolBookArbConfig),
    "beezee_pool_volume": (CONF / "scripts", BeezeePoolVolumeConfig),
    "beezee_price_bands": (CONF / "scripts", BeezeePriceBandsConfig),
    "beezee_treasury_twap": (CONF / "scripts", BeezeeTreasuryTwapConfig),
    "beezee_pool_anchored_pmm": (CONF / "controllers", BeezeePoolAnchoredPMMConfig),
}


class AmmExampleTests(unittest.TestCase):
    def test_every_amm_script_and_controller_has_each_flavour(self):
        for name, (folder, _) in AMM_CONFIGS.items():
            with self.subTest(name=name):
                found = sorted(path.name for path in folder.glob(f"{name}_*.yml.example") if "_runner_" not in path.name)
                self.assertEqual(sorted(f"{name}_{flavour}.yml.example" for flavour in FLAVOUR_PAIRS), found)

    def test_every_example_loads_into_its_config_with_known_keys_only(self):
        for name, (folder, config_class) in AMM_CONFIGS.items():
            for flavour, pair in FLAVOUR_PAIRS.items():
                example = folder / f"{name}_{flavour}.yml.example"
                with self.subTest(example=example.name):
                    data = yaml.safe_load(example.read_text())
                    self.assertEqual(set(), set(data) - set(config_class.model_fields), "keys the config does not know")
                    config = config_class(**data)
                    self.assertEqual(pair, config.trading_pair)
                    self.assertIn("BZE", config.trading_pair.split("-"), "the fees and the gas are paid in BZE")
                    self.assertEqual("beezee", data.get("exchange", data.get("connector_name")))
                    if folder.name == "scripts":
                        self.assertEqual(f"{name}.py", config.script_file_name)

    def test_each_runner_lists_its_network_controller(self):
        for network in ("testnet", "mainnet"):
            example = CONF / "scripts" / f"beezee_pool_anchored_pmm_runner_{network}.yml.example"
            with self.subTest(example=example.name):
                data = yaml.safe_load(example.read_text())
                self.assertEqual(set(), set(data) - set(StrategyV2ConfigBase.model_fields), "keys the config does not know")
                config = StrategyV2ConfigBase(**data)
                self.assertEqual("v2_with_controllers.py", config.script_file_name)
                self.assertEqual([f"beezee_pool_anchored_pmm_{network}.yml"], config.controllers_config)
                self.assertTrue((CONF / "controllers" / f"beezee_pool_anchored_pmm_{network}.yml.example").exists())
