import os

import yaml
from jsonschema import validate

from src.app_logging import LOGGING_JSONSCHEMA
from src.database import DATABASE_JSONSCHEMA
from src.mqtt_client import MQTT_JSONSCHEMA


CONFIG_JSONSCHEMA = {
    "type": "object",
    "properties": {
        "database": DATABASE_JSONSCHEMA,
        "logging": LOGGING_JSONSCHEMA,
        "mqtt": MQTT_JSONSCHEMA,
    },
    "additionalProperties": False,
    "required": ["database", "mqtt"],
}


class AppConfig:

    def __init__(self, config_file):
        self._config_data = {}

        self.check_config_file_access(config_file)

        with open(config_file, 'r') as stream:
            file_data = yaml.unsafe_load(stream)

        self._config_data = {
            **{"database": {}, "logging": {}, "mqtt": {}},  # default
            **file_data
        }

        validate(file_data, CONFIG_JSONSCHEMA)

    def get_database_config(self):
        return self._config_data["database"]

    def get_logging_config(self):
        return self._config_data["logging"]

    def get_mqtt_config(self):
        return self._config_data["mqtt"]

    @classmethod
    def check_config_file_access(cls, config_file):
        if not os.path.isfile(config_file):
            raise FileNotFoundError('config file ({}) does not exist!'.format(config_file))

        permissions = oct(os.stat(config_file).st_mode & 0o777)[2:]
        if permissions != "600":
            extra = "change via 'chmod'. this config file may contain sensitive information."
            raise PermissionError(f"wrong config file permissions ({config_file}: expected 600, got {permissions})! {extra}")
