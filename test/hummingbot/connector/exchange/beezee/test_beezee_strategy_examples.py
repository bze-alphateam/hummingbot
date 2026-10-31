import logging
import shutil
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List

from hummingbot.client.config.config_helpers import (
    get_strategy_template_path,
    load_strategy_config_map_from_file,
    missing_required_configs_legacy,
    parse_cvar_value,
    yaml_parser,
)
from hummingbot.client.settings import required_exchanges
from hummingbot.strategy.pure_market_making.pure_market_making_config_map import (
    pure_market_making_config_map as pmm_config_map,
)

STRATEGIES_DIR = Path(__file__).resolve().parents[5] / "conf" / "strategies"
EXAMPLES = sorted(STRATEGIES_DIR.glob("*.yml.example"))

# The values the run guide promises for both networks.
COMMON_SETTINGS = {
    "strategy": "pure_market_making",
    "exchange": "beezee",
    "bid_spread": Decimal("1.0"),
    "ask_spread": Decimal("1.0"),
    "order_levels": 5,
    "order_level_spread": Decimal("0.5"),
    "order_refresh_time": 30.0,
    "order_refresh_tolerance_pct": Decimal("0.2"),
    "inventory_skew_enabled": True,
    "inventory_target_base_pct": Decimal("50"),
    "hanging_orders_enabled": False,
    "price_source": "current_market",
    "price_type": "mid_price",
    "filled_order_delay": 30.0,
}
# Pair names as the connector lists them from each network's denom metadata.
MARKETS = {
    "beezee_pmm_testnet.yml.example": "LMN-TUSDC",
    "beezee_pmm_bze_usdc.yml.example": "BZE-USDC.INJ",
}


class _ErrorRecorder(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.ERROR)
        self.messages: List[str] = []

    def emit(self, record: logging.LogRecord):
        self.messages.append(record.getMessage())


class BeezeeStrategyExamplesTests(unittest.IsolatedAsyncioTestCase):
    """Every `conf/strategies/*.yml.example` loads through the client's own `import` path without an error."""

    def setUp(self):
        super().setUp()
        self._reset_config_map()
        self.tmp_dir = Path(tempfile.mkdtemp())
        self.errors = _ErrorRecorder()
        logging.getLogger().addHandler(self.errors)

    def tearDown(self):
        logging.getLogger().removeHandler(self.errors)
        shutil.rmtree(self.tmp_dir, ignore_errors=True)
        self._reset_config_map()
        required_exchanges.discard("beezee")
        super().tearDown()

    @staticmethod
    def _reset_config_map():
        for cvar in pmm_config_map.values():
            cvar.value = None

    @staticmethod
    def _read_yml(path: Path) -> Dict[str, Any]:
        with open(path, encoding="utf-8") as stream:
            return dict(yaml_parser.load(stream))

    async def _load(self, example: Path):
        # `import` reads conf/strategies/<name>.yml; load a copy so a version upgrade could never rewrite the example.
        yml_path = self.tmp_dir / example.name[: -len(".example")]
        shutil.copy(example, yml_path)
        config_map = await load_strategy_config_map_from_file(yml_path)
        return config_map, self._read_yml(yml_path)

    def test_the_testnet_and_mainnet_examples_exist(self):
        self.assertEqual(sorted(MARKETS), [example.name for example in EXAMPLES])

    async def test_every_example_loads_into_the_pure_market_making_config_map(self):
        template = self._read_yml(Path(get_strategy_template_path("pure_market_making")))
        for example in EXAMPLES:
            with self.subTest(example=example.name):
                self._reset_config_map()
                self.errors.messages.clear()
                data = self._read_yml(example)

                unknown_keys = sorted(set(data) - set(pmm_config_map) - {"template_version"})
                self.assertEqual([], unknown_keys, "keys the strategy does not know (the loader would skip them)")
                not_in_template = sorted(set(data) - set(template))
                self.assertEqual([], not_in_template, "keys the template lacks (the loader would skip them)")
                self.assertEqual(template["template_version"], data["template_version"])

                config_map, data_after_load = await self._load(example)

                self.assertIs(pmm_config_map, config_map)
                self.assertEqual([], self.errors.messages)
                self.assertEqual(data, data_after_load, "the loader rewrote the file")
                self.assertEqual([], [cvar.key for cvar in missing_required_configs_legacy(config_map)])
                for key, value in data.items():
                    if key == "template_version":
                        continue
                    self.assertEqual(parse_cvar_value(config_map[key], value), config_map[key].value, key)
                    self.assertIsNotNone(config_map[key].value, key)

    async def test_every_example_has_the_documented_settings(self):
        for example in EXAMPLES:
            with self.subTest(example=example.name):
                self._reset_config_map()
                config_map, _ = await self._load(example)

                settings = {key: config_map[key].value for key in COMMON_SETTINGS}
                self.assertEqual(COMMON_SETTINGS, settings)
                self.assertEqual(MARKETS[example.name], config_map["market"].value)
                self.assertGreater(config_map["order_amount"].value, Decimal("0"))
