import unittest

from hummingbot.client.settings import AllConnectorSettings, ConnectorType, connectable_exchange_names


class BeezeeRegistrationTests(unittest.TestCase):
    def test_beezee_is_discovered_as_connectable_spot_exchange(self):
        settings = AllConnectorSettings.create_connector_settings()["beezee"]

        self.assertEqual(ConnectorType.Exchange, settings.type)
        self.assertFalse(settings.centralised)
        self.assertEqual("BZE-USDC", settings.example_pair)
        self.assertEqual("beezee", settings.config_keys.connector)
        self.assertIn("beezee", AllConnectorSettings.get_exchange_names())
        self.assertIn("beezee", connectable_exchange_names())
