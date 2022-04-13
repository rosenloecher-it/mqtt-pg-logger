import datetime
import logging
import threading
from typing import Optional

import paho.mqtt.client as mqtt
from tzlocal import get_localzone


_logger = logging.getLogger(__name__)


class MqttConfKey:
    CLIENT_ID = "client_id"
    HOST = "host"
    PORT = "port"
    PASSWORD = "password"
    USER = "user"
    KEEPALIVE = "keepalive"
    PROTOCOL = "protocol"

    SSL_CA_CERTS = "ssl_ca_certs"
    SSL_CERTFILE = "ssl_certfile"
    SSL_INSECURE = "ssl_insecure"
    SSL_KEYFILE = "ssl_keyfile"

    FILTER_MESSAGE_ID_0 = "filter_message_id_0"

    SUBSCRIPTIONS = "subscriptions"
    SKIP_SUBSCRIPTION_REGEXES = "skip_subscription_regexes"

    TEST_SUBSCRIPTION_BASE = "test_subscription_base"  # Test only


SUBSCRIPTION_JSONSCHEMA = {
    "type": "array",
    "items": {"type": "string", "minLength": 1},
}


SKIP_SUBSCRIPTION_JSONSCHEMA = {
    "type": "array",
    "items": {
        "type": "string",
        "minLength": 1,
        "description": "If this regex matches the topic the message is skipped.",
        # "pattern": "[A-Za-z\/]*",  # no "#" at end
    },
}


MQTT_JSONSCHEMA = {
    "type": "object",
    "properties": {
        MqttConfKey.CLIENT_ID: {"type": "string", "minLength": 1},
        MqttConfKey.HOST: {"type": "string", "minLength": 1},
        MqttConfKey.KEEPALIVE: {"type": "integer", "minimum": 1},
        MqttConfKey.PORT: {"type": "integer"},
        MqttConfKey.PROTOCOL: {"type": "integer", "enum": [3, 4, 5]},
        MqttConfKey.SSL_CA_CERTS: {"type": "string", "minLength": 1},
        MqttConfKey.SSL_CERTFILE: {"type": "string", "minLength": 1},
        MqttConfKey.SSL_INSECURE: {"type": "boolean"},
        MqttConfKey.SSL_KEYFILE: {"type": "string", "minLength": 1},
        MqttConfKey.USER: {"type": "string", "minLength": 1},
        MqttConfKey.PASSWORD: {"type": "string"},

        MqttConfKey.FILTER_MESSAGE_ID_0: {
            "type": "boolean",
            "description": "Filter all messages with Message ID 0. Default: False. '0' is reserved as an invalid Message ID.",
        },

        MqttConfKey.SUBSCRIPTIONS: SUBSCRIPTION_JSONSCHEMA,
        MqttConfKey.SKIP_SUBSCRIPTION_REGEXES: SKIP_SUBSCRIPTION_JSONSCHEMA,
        MqttConfKey.TEST_SUBSCRIPTION_BASE: {
            "type": "string",
            "minLength": 1,
            "description": "For test only: base topic (get extended)",
            # "pattern": "[A-Za-z\/]*",  # no "#" at end
        }
    },
    "additionalProperties": False,
    "required": [MqttConfKey.HOST, MqttConfKey.PORT, MqttConfKey.SUBSCRIPTIONS],
}


class MqttException(Exception):
    pass


class MqttClient:

    DEFAULT_KEEPALIVE = 60
    DEFAULT_PORT = 1883
    DEFAULT_PORT_SSL = 8883
    DEFAULT_PROTOCOL = 4  # 5==MQTTv5, default: 4==MQTTv311, 3==MQTTv31
    DEFAULT_QUALITY = 1

    def __init__(self, config):

        self._host = None
        self._port = None
        self._keepalive = None

        self._client = None
        self._is_connected = False
        self._connection_error_info = None  # type: Optional[str]
        self._subscribed = False
        self._shutdown = False

        self._lock = threading.Lock()

        self._host = config[MqttConfKey.HOST]
        self._port = config.get(MqttConfKey.PORT)
        self._keepalive = config.get(MqttConfKey.KEEPALIVE, self.DEFAULT_KEEPALIVE)

        protocol = config.get(MqttConfKey.PROTOCOL, self.DEFAULT_PROTOCOL)
        client_id = config.get(MqttConfKey.CLIENT_ID)
        ssl_ca_certs = config.get(MqttConfKey.SSL_CA_CERTS)
        ssl_certfile = config.get(MqttConfKey.SSL_CERTFILE)
        ssl_keyfile = config.get(MqttConfKey.SSL_KEYFILE)
        ssl_insecure = config.get(MqttConfKey.SSL_INSECURE, False)
        is_ssl = ssl_ca_certs or ssl_certfile or ssl_keyfile
        user_name = config.get(MqttConfKey.USER)
        user_pwd = config.get(MqttConfKey.PASSWORD)
        subscriptions = config.get(MqttConfKey.SUBSCRIPTIONS)

        if not self._port:
            self._port = self.DEFAULT_PORT_SSL if is_ssl else self.DEFAULT_PORT

        if not self._host or not subscriptions:
            raise ValueError("mandatory mqtt configuration not found ({}, {})'!".format(MqttConfKey.HOST, MqttConfKey.SUBSCRIPTIONS))

        self._client = mqtt.Client(client_id=client_id, protocol=protocol)

        if is_ssl:
            self._client.tls_set(ca_certs=ssl_ca_certs, certfile=ssl_certfile, keyfile=ssl_keyfile)
            if ssl_insecure:
                _logger.info("disabling SSL certificate verification")
                self._client.tls_insecure_set(True)

        if user_name or user_pwd:
            self._client.username_pw_set(user_name, user_pwd)

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.on_publish = self._on_publish

        self._client.reconnect_delay_set()

    @property
    def is_connected(self):
        with self._lock:
            return self._is_connected

    def connect(self):
        self._client.connect_async(self._host, port=self._port, keepalive=self._keepalive)
        self._client.loop_start()
        _logger.debug("%s is connecting...", self.__class__.__name__)

    def close(self):
        self._shutdown = True
        if self._client is not None:
            self._client.loop_stop()
            self._client.disconnect()
            self._client.loop_forever()  # will block until disconnect complete
            self._client = None
            _logger.debug("%s was closed.", self.__class__.__name__)

    def ensure_connection(self):
        """
        Check for rarely unexpected disconnects, but when happens, it's not clear how to heal. At least the loop has to be restarted.
        Best to restart the whole app. Recognise a stopped service in system log.
        """
        with self._lock:
            is_connected = self._is_connected
            connection_error_info = self._connection_error_info

        if connection_error_info:
            raise MqttException(connection_error_info)  # leads to exit => restarted by systemd
        if not is_connected:
            raise MqttException("MQTT is not connected!")

    def _on_connect(self, _mqtt_client, _userdata, _flags, rc):
        """MQTT callback is called when client connects to MQTT server."""
        class_name = self.__class__.__name__
        if rc == 0:
            with self._lock:
                self._is_connected = True
            _logger.debug("%s was connected.", class_name)
        else:
            connection_error_info = f"{class_name} connection failed (#{rc}: {mqtt.error_string(rc)})!"
            _logger.error(connection_error_info)
            with self._lock:
                self._is_connected = False
                self._connection_error_info = connection_error_info

    def _on_disconnect(self, _mqtt_client, _userdata, rc):
        """MQTT callback for when the client disconnects from the MQTT server."""
        class_name = self.__class__.__name__
        connection_error_info = None
        if rc != 0:
            connection_error_info = f"{class_name} connection was lost (#{rc}: {mqtt.error_string(rc)}) => abort => restart!"

        with self._lock:
            self._is_connected = False
            if connection_error_info and not self._connection_error_info:
                self._connection_error_info = connection_error_info

        if rc == 0:
            _logger.debug("%s was disconnected.", class_name)
        else:
            _logger.error("%s was unexpectedly disconnected: %s", class_name, connection_error_info or "???")

    def _on_message(self, _mqtt_client, _userdata, mqtt_message: mqtt.MQTTMessage):
        """MQTT callback when a message is received from MQTT server"""

    def _on_publish(self, mqtt_client, userdata, mid):
        """MQTT callback is invoked when message was successfully sent to the MQTT server."""

    @classmethod
    def _now(cls) -> datetime:
        return datetime.datetime.now(tz=get_localzone())
