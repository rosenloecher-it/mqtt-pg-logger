
DATABASE_JSONSCHEMA = {
    "type": "object",
    "properties": {
        "host": {"type": "string", "minLength": 1, "description": "Database host"},
        "port": {"type": "integer", "minimum": 1, "description": "Database port"},
        "user": {"type": "string", "minLength": 1, "description": "Database user"},
        "password": {"type": "string", "minLength": 1, "description": "Database password"},
        "database": {"type": "string", "minLength": 1, "description": "Database name"},
        "table_name": {"type": "string", "minLength": 1, "description": "Database table"},
    },
    "additionalProperties": False,
    "required": ["host", "user", "port", "password", "database"],
}


class Database:

    DEFAULT_TABLE_NAME = "mqtt_logs"

    def __init__(self):
        pass

    def open(self, config):
        pass

    def close(self):
        pass
