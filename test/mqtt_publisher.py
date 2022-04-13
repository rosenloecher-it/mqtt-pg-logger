import logging
import random

from src.lifecycle_control import LifecycleControl, StatusNotification
from src.mqtt_client import MqttClient, MqttException

_logger = logging.getLogger(__name__)


class MqttPublisher(MqttClient):

    def __init__(self, config):
        super().__init__(config)

    def publish(self, topic: str, payload: str):
        with self._lock:
            if self._shutdown:
                return

            if not self._is_connected or self._connection_error_info:
                raise MqttException(self._connection_error_info or "MQTT is not connected!")

            return self._client.publish(
                topic=topic,
                payload=payload,
                qos=2,
                retain=False
            )

    @classmethod
    def get_default_client_id(cls):
        return f"pg_log_test_{random.randint(1, 9999999999)}"

    def _on_connect(self, mqtt_client, userdata, flags, rc):
        super()._on_connect(mqtt_client, userdata, flags, rc)

        if rc == 0:
            LifecycleControl.notify(StatusNotification.MQTT_PUBLISHER_CONNECTED)

    def _on_publish(self, mqtt_client, userdata, mid):
        """MQTT callback is invoked when message was successfully sent to the MQTT server."""
        _logger.debug("published MQTT message %s", str(mid))
