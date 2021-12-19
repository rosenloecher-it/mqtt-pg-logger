import logging
import random
from typing import List, Set

import paho.mqtt.client as mqtt

from src.lifecycle_control import LifecycleControl, StatusNotification
from src.message import Message
from src.mqtt_client import MqttConfKey, MqttClient


_logger = logging.getLogger(__name__)


class MqttListener(MqttClient):

    def __init__(self, config):
        super().__init__(config)

        self._subscriptions = set()
        self._skip_subscription_regexes = set()

        self._messages = []  # type: List[Message]

        subscriptions = config.get(MqttConfKey.SUBSCRIPTIONS)

        with self._lock:
            self._skip_subscription_regexes = self.list_to_set(config.get(MqttConfKey.SKIP_SUBSCRIPTION_REGEXES))
            self._subscriptions = self.list_to_set(subscriptions)
            if not self._subscriptions:
                self._subscribed = True

    def try_to_subscribe(self) -> bool:
        """wait for getting mqtt connect callback called"""

        if not self._shutdown and not self._subscribed and self._is_connected:
            with self._lock:
                channels = [c for c in self._subscriptions]
            if channels:
                subs_qos = 1  # qos for subscriptions, not used, but necessary
                subscriptions = [(s, subs_qos) for s in channels]
                result, dummy = self._client.subscribe(subscriptions)
                if result != mqtt.MQTT_ERR_SUCCESS:
                    text = "could not subscripte to mqtt #{} ({})".format(result, subscriptions)
                    raise RuntimeError(text)

                self._subscribed = True
                LifecycleControl.notify(StatusNotification.MQTT_LISTENER_SUBSCRIBED)
                _logger.info("subscripted to MQTT channels (%s)", channels)

        return self._subscribed

    @classmethod
    def list_to_set(cls, items: List[str]) -> Set[str]:
        items_set = set()
        if items:
            for item in items:
                items_set.add(item)
        return items_set

    def _get_default_client_id(self):
        return f"pg_logger_{random.randint(1, 9999999999)}"

    def get_messages(self) -> List[Message]:
        with self._lock:
            messages = self._messages  # type: List[Message]
            self._messages = []
        return messages

    def _on_connect(self, mqtt_client, userdata, flags, rc):
        """MQTT callback is called when client connects to MQTT server."""
        if rc == 0:
            with self._lock:
                self._is_connected = True
            LifecycleControl.notify(StatusNotification.MQTT_LISTENER_CONNECTED)
            _logger.info("successfully connected to MQTT: flags=%s, rc=%s", flags, rc)
        else:
            _logger.error("connect to MQTT failed: flags=%s, rc=%s", flags, rc)

    def _on_disconnect(self, mqtt_client, userdata, rc):
        """MQTT callback for when the client disconnects from the MQTT server."""
        with self._lock:
            self._is_connected = False
        if rc == 0:
            _logger.info("disconnected from MQTT: rc=%s", rc)
        else:
            _logger.error("unexpectedly disconnected from MQTT broker: rc=%s", rc)

    def _on_message(self, mqtt_client, userdata, mqtt_message: mqtt.MQTTMessage):
        """MQTT callback when a message is received from MQTT server"""
        try:
            if mqtt_message is not None:
                message = Message.create(mqtt_message)
                message.time = self._now()
                _logger.debug('received mqtt message: "%s"', message)

                with self._lock:
                    # if self.store_retained or not message.retain:
                    if message.topic not in self._skip_subscription_regexes:  # TODP
                        self._messages.append(message)

        except Exception as ex:
            _logger.exception(ex)
