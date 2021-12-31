import unittest

from src.mqtt_client import MqttConfKey
from src.mqtt_listener import MqttListener
from test.setup_test import SetupTest


class TestMqttListener(unittest.TestCase):

    @classmethod
    def create_listener(cls, skip_subscriptions):
        test_config_data = SetupTest.read_test_config()
        test_config_mqtt = test_config_data["mqtt"]
        test_config_mqtt[MqttConfKey.SUBSCRIPTIONS] = ["base1/#", "base2/#"]
        test_config_mqtt[MqttConfKey.SKIP_SUBSCRIPTION_REGEXES] = skip_subscriptions

        return MqttListener(test_config_mqtt)

    def test_accept_topic(self):
        listener = self.create_listener(["base1/exclude", "^base2/exclude"])

        self.assertTrue(listener._accept_topic("base1/include"))
        self.assertTrue(listener._accept_topic("base1/include/exclude"))

        self.assertFalse(listener._accept_topic("base1/exclude"))
        self.assertFalse(listener._accept_topic("base1/exclude2"))
        self.assertFalse(listener._accept_topic("base1/exclude/2"))

        self.assertFalse(listener._accept_topic("base2/exclude"))
        self.assertFalse(listener._accept_topic("base2/exclude2"))
        self.assertFalse(listener._accept_topic("base2/exclude/2"))

        self.assertTrue(listener._accept_topic("base1/include/base2/exclude"))

        pass
