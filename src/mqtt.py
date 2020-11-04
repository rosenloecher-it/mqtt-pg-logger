

CLIENT_ID = "client_id"
HOST = "host"
PORT = "port"
PASSWORD = "password"

KEEPALIVE = "keepalive"
PROTOCOL = "protocol"

SSL_CA_CERTS = "ssl_ca_certs"
SSL_CERTFILE = "ssl_certfile"
SSL_INSECURE = "ssl_insecure"
SSL_KEYFILE = "ssl_keyfile"

SUBSCRIPTIONS = "subscriptions"
SKIP_SUBSCRIPTIONS = "skip_subscriptions"


SUBSCRIPTION_JSONSCHEMA = {
    "type": "array",
    "items": {"type": "string", "minLength": 1},
}


MQTT_JSONSCHEMA = {
    "type": "object",
    "properties": {
        CLIENT_ID: {"type": "string", "minLength": 1},
        HOST: {"type": "string", "minLength": 1},
        KEEPALIVE: {"type": "integer", "minimum": 1},
        PORT: {"type": "integer"},
        PROTOCOL: {"type": "integer", "enum": [3, 4, 5]},
        SSL_CA_CERTS: {"type": "string", "minLength": 1},
        SSL_CERTFILE: {"type": "string", "minLength": 1},
        SSL_INSECURE: {"type": "boolean"},
        SSL_KEYFILE: {"type": "string", "minLength": 1},
        PASSWORD: {"type": "string"},

        SUBSCRIPTIONS: SUBSCRIPTION_JSONSCHEMA,
        SKIP_SUBSCRIPTIONS: SUBSCRIPTION_JSONSCHEMA,
    },
    "additionalProperties": False,
    "required": [
        HOST,
        PORT,
        SUBSCRIPTIONS,
    ],
}


class Mqtt:

    def __init__(self):
        pass

    def open(self, config):
        pass

    def close(self):
        pass
