import abc
import datetime
import logging
from typing import Optional

import psycopg
from tzlocal import get_localzone


_logger = logging.getLogger(__name__)


class DatabaseConfKey:
    HOST = "host"
    USER = "user"
    PORT = "port"
    PASSWORD = "password"
    DATABASE = "database"
    TABLE_NAME = "table_name"
    TIMEZONE = "timezone"

    BATCH_SIZE = "batch_size"
    WAIT_MAX_SECONDS = "wait_max_seconds"
    CLEAN_UP_AFTER_DAYS = "clean_up_after_days"


DATABASE_JSONSCHEMA = {
    "type": "object",
    "properties": {
        DatabaseConfKey.HOST: {"type": "string", "minLength": 1, "description": "Database host"},
        DatabaseConfKey.PORT: {"type": "integer", "minimum": 1, "description": "Database port"},
        DatabaseConfKey.USER: {"type": "string", "minLength": 1, "description": "Database user"},
        DatabaseConfKey.PASSWORD: {"type": "string", "minLength": 1, "description": "Database password"},
        DatabaseConfKey.DATABASE: {"type": "string", "minLength": 1, "description": "Database name"},
        DatabaseConfKey.TABLE_NAME: {"type": "string", "minLength": 1, "description": "Database table "},
        DatabaseConfKey.TIMEZONE: {"type": "string", "minLength": 1, "description": "Predefined session timezone"},

        DatabaseConfKey.BATCH_SIZE: {
            "type": "integer", "minimum": 1,
            "description": "Database batch size: message are queued until batch size is reached"
        },
        DatabaseConfKey.WAIT_MAX_SECONDS: {
            "type": "integer", "minimum": 0,
            "description": "Wait (seconds) Queued messages are stored into database even the batch size is not reached."
        },
        DatabaseConfKey.CLEAN_UP_AFTER_DAYS: {
            "type": "integer",
            "description": "Delete entries older than <n> days. Deactivate clean up with values values <= 0."
        },
    },
    "additionalProperties": False,
    "required": [DatabaseConfKey.HOST, DatabaseConfKey.PORT, DatabaseConfKey.DATABASE],
}


class DatabaseException(Exception):
    pass


class Database(abc.ABC):

    DEFAULT_TABLE_NAME = "journal"

    def __init__(self, config):
        # runtime properties
        self._connection = None
        self._auto_commit = False
        self._last_connect_time = None  # type: Optional[datetime.datetime]

        # configuration
        self._connect_data = {
            "host": config[DatabaseConfKey.HOST],
            "port": config[DatabaseConfKey.PORT],
            "user": config[DatabaseConfKey.USER],
            "password": config.get(DatabaseConfKey.PASSWORD),
            "dbname": config[DatabaseConfKey.DATABASE],
        }

        self._table_name = config.get(DatabaseConfKey.TABLE_NAME, self.DEFAULT_TABLE_NAME)  # define by SQL scripts
        self._timezone = config.get(DatabaseConfKey.TIMEZONE)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    @property
    def is_connected(self):
        return bool(self._connection)

    def connect(self):
        if self._connection:
            self._connection.close()

        try:
            self._connection = psycopg.connect(**self._connect_data, autocommit=self._auto_commit)

            with self._connection.cursor() as cursor:
                time_zone = self._timezone if self._timezone else self.get_default_time_zone_name()
                stmt = "set timezone='{}'".format(time_zone)
                try:
                    cursor.execute(stmt)
                except Exception:
                    _logger.error("setting timezone failed (%s)!", stmt)
                    raise

            self._last_connect_time = self._now()

        except psycopg.OperationalError as ex:
            raise DatabaseException(str(ex)) from ex

    def close(self):
        try:
            if self._connection:
                self._connection.close()
        except Exception as ex:
            _logger.exception(ex)
        finally:
            self._connection = None

    @classmethod
    def get_default_time_zone_name(cls):
        local_timezone = get_localzone()
        if not local_timezone:
            local_timezone = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
        return str(local_timezone)

    @classmethod
    def _now(cls) -> datetime:
        """overwritable datetime.now for testing"""
        return datetime.datetime.now(tz=get_localzone())
