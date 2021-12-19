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

        if not os.path.isfile(config_file):
            raise FileNotFoundError('config file ({}) does not exist!'.format(config_file))
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

        # self.config[CONFKEY_MAIN] = {**main_section, **self.cli}
