import logging
import re
from typing import List, Set

import paho.mqtt.client as mqtt

from src.lifecycle_control import LifecycleControl, StatusNotification
from src.message import Message
from src.mqtt_client import MqttConfKey, MqttClient, MqttException

_logger = logging.getLogger(__name__)


class MqttListener(MqttClient):

    def __init__(self, config):
        super().__init__(config)

        self._subscriptions = set()
        self._skip_subscription_regexes = []
        self._messages: List[Message] = []

        self._status_received_message_count = 0
        self._status_skipped_message_count = 0
        self._status_last_log = self._now()

        # MQTT V3 Protocol Specification: Do not use Message ID 0. It is reserved as an invalid Message ID.
        self._filter_message_id_0 = config.get(MqttConfKey.FILTER_MESSAGE_ID_0, False)

        skip_subscription_regexes = self.list_to_set(config.get(MqttConfKey.SKIP_SUBSCRIPTION_REGEXES))
        for skip_subscription_regex in skip_subscription_regexes:
            if skip_subscription_regex:
                self._skip_subscription_regexes.append(re.compile(skip_subscription_regex))

        subscriptions = config.get(MqttConfKey.SUBSCRIPTIONS)
        self._subscriptions = self.list_to_set(subscriptions)
        if not self._subscriptions:
            self._subscribed = True

    @property
    def is_connected(self):
        with self._lock:
            return self._is_connected and self._subscribed

    def connect(self):
        super().connect()

        time_step = 0.05
        time_counter = 0

        try:
            while LifecycleControl.should_proceed() and not self._shutdown:
                if self._try_to_subscribe():
                    break
                time_counter += LifecycleControl.sleep(time_step)
                if time_counter > 15:
                    raise MqttException("couldn't subscribe to MQTT topics... no connection?!")

        except Exception:
            self._subscribed = False
            raise

    def _try_to_subscribe(self) -> bool:
        """wait for getting mqtt connect callback called"""
        if not self._subscribed and self._is_connected:
            with self._lock:
                channels = [c for c in self._subscriptions]
            if channels:
                subs_qos = 1  # qos for subscriptions, not used, but necessary
                subscriptions = [(s, subs_qos) for s in channels]
                result, dummy = self._client.subscribe(subscriptions)
                if result != mqtt.MQTT_ERR_SUCCESS:
                    error_info = "{} (#{})".format(mqtt.error_string(result), result)
                    raise MqttException(f"could not subscribe to MQTT topics): {error_info}; topics: {channels}")

                self._subscribed = True
                LifecycleControl.notify(StatusNotification.MQTT_LISTENER_SUBSCRIBED)
                _logger.info("subscribed to MQTT topics (%s)", channels)

        return self._subscribed

    @classmethod
    def list_to_set(cls, items: List[str]) -> Set[str]:
        items_set = set()
        if items:
            for item in items:
                items_set.add(item)
        return items_set

    def get_messages(self) -> List[Message]:
        with self._lock:
            messages = self._messages
            self._messages = []
        return messages

    def _on_connect(self, mqtt_client, userdata, flags, rc):
        super()._on_connect(mqtt_client, userdata, flags, rc)

        if rc == 0:
            LifecycleControl.notify(StatusNotification.MQTT_LISTENER_CONNECTED)

    def _on_message(self, mqtt_client, userdata, mqtt_message: mqtt.MQTTMessage):
        """MQTT callback when a message is received from MQTT server"""
        try:
            if mqtt_message is not None:
                message = Message.create(mqtt_message)
                message.time = self._now()
                _logger.debug("message received: %s", message)

                accept_message = self._accept_topic(message.topic)

                with self._lock:
                    if accept_message:
                        if message.message_id <= 0 and self._filter_message_id_0:
                            accept_message = False
                    if accept_message:
                        self._messages.append(message)

                    self._status_received_message_count += 1
                    self._status_skipped_message_count += 0 if accept_message else 1
                    status_last_log = self._status_last_log

                if _logger.isEnabledFor(logging.INFO) and (self._now() - status_last_log).total_seconds() > 300:
                    with self._lock:
                        received_count = self._status_received_message_count
                        skipped_count = self._status_skipped_message_count
                        self._status_last_log = self._now()

                    if skipped_count > 0:
                        _logger.info("overall messages: received=%d; skipped=%d", received_count, skipped_count)
                    else:
                        _logger.info("overall messages: received=%d", received_count)

        except Exception as ex:
            _logger.exception(ex)

    def _accept_topic(self, topic) -> bool:
        accept = True
        # `self._skip_subscription_regexes` only used within threaded context, so no use of `self._lock`!
        for regex in self._skip_subscription_regexes:
            if regex.match(topic):
                accept = False
                _logger.debug('skipped topic: "%s"', topic)
                break

        return accept
