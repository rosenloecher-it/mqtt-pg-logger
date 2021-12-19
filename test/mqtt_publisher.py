import logging
import random

from src.lifecycle_control import LifecycleControl, StatusNotification
from src.mqtt_client import MqttClient


_logger = logging.getLogger(__name__)


class MqttPublisher(MqttClient):

    def __init__(self, config):
        super().__init__(config)

    def publish(self, topic: str, payload: str):
        if self._shutdown:
            return

        if not self._is_connected:
            raise RuntimeError("MQTT is not connected!")

        return self._client.publish(
            topic=topic,
            payload=payload,
            qos=2,
            retain=False
        )

    def _get_default_client_id(self):
        return MqttPublisher.get_default_client_id()

    @classmethod
    def get_default_client_id(cls):
        return f"pg_log_test_{random.randint(1, 9999999999)}"

    def _on_connect(self, mqtt_client, userdata, flags, rc):
        """MQTT callback is called when client connects to MQTT server."""
        if rc == 0:
            with self._lock:
                self._is_connected = True
            LifecycleControl.notify(StatusNotification.MQTT_PUBLISHER_CONNECTED)
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

    def _on_publish(self, mqtt_client, userdata, mid):
        """MQTT callback is invoked when message was successfully sent to the MQTT server."""
        _logger.debug("published MQTT message %s", str(mid))
