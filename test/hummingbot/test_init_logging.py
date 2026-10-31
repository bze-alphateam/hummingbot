import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import hummingbot
from hummingbot import init_logging

TEMPLATE = Path(hummingbot.__file__).parent / "templates" / "hummingbot_logs_TEMPLATE.yml"


class InitLoggingTests(unittest.TestCase):
    """init_logging runs on every `start`. dictConfig disabled every logger an earlier start had created that the config
    does not name, e.g. `hummingbot.strategy_v2.runnable_base` (the V2 controllers): after a `stop` and a `start` the
    pool-anchored controller's INFO lines (`Hedging …`, `Hedge swap settled`) never reached the log."""

    def setUp(self) -> None:
        self.prefix = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.prefix)
        (Path(self.prefix) / "conf").mkdir()
        shutil.copy(TEMPLATE, Path(self.prefix) / "conf" / "hummingbot_logs.yml")

    def _configured(self, **kwargs) -> dict:
        with patch("hummingbot.prefix_path", return_value=self.prefix), \
                patch("hummingbot.STRUCT_LOGGER_SET", True), \
                patch("logging.config.dictConfig") as dict_config:
            init_logging("hummingbot_logs.yml", MagicMock(logger_override_whitelist=[]), **kwargs)
        dict_config.assert_called_once()
        return dict_config.call_args.args[0]

    def test_existing_loggers_stay_enabled(self):
        self.assertIs(False, self._configured()["disable_existing_loggers"])
        self.assertIs(False, self._configured(strategy_file_path="conf_v2_controllers.yml")["disable_existing_loggers"])

    def test_the_template_keeps_the_root_logger_at_info_with_the_console_and_the_file(self):
        root = self._configured()["root"]
        self.assertEqual("INFO", root["level"])
        self.assertEqual(["console", "file_handler"], list(root["handlers"]))
