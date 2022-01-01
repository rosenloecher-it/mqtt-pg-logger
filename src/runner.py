import logging
import time

from src.lifecycle_control import LifecycleControl, StatusNotification
from src.mqtt_listener import MqttListener
from src.proxy_store import ProxyStore


_logger = logging.getLogger(__name__)


class Runner:

    def __init__(self, app_config):
        self._shutdown = False

        self._store = ProxyStore(app_config.get_database_config())

        self._mqtt = MqttListener(app_config.get_mqtt_config())
        self._mqtt.connect()

    def loop(self):
        """endless loop"""
        time_step = 0.05
        there_has_been_messages_to_notify = False

        try:
            while LifecycleControl.should_proceed():
                busy = False

                messages = self._mqtt.get_messages()
                if messages:
                    there_has_been_messages_to_notify = True
                    self._store.queue(messages)
                    busy = True

                if len(messages) == 0 and there_has_been_messages_to_notify:
                    there_has_been_messages_to_notify = False
                    LifecycleControl.notify(StatusNotification.RUNNER_QUEUE_EMPTIED)  # test related

                time.sleep(time_step / 100 if busy else time_step)

        except KeyboardInterrupt:
            # gets called without signal-handler
            _logger.debug("finishing...")

    def close(self):
        if self._mqtt is not None:
            self._mqtt.close()
            self._mqtt = None
        if self._store is not None:
            self._store.close()
            self._store = None
