import datetime
import logging
from typing import Optional

from psycopg import sql

from src.database import Database, DatabaseConfKey
from src.lifecycle_control import LifecycleControl, StatusNotification

_logger = logging.getLogger(__name__)


class MessageStore(Database):

    DEFAULT_BATCH_SIZE = 100
    DEFAULT_WAIT_MAX_SECONDS = 10
    DEFAULT_CLEAN_UP_AFTER_DAYS = 14

    def __init__(self, config):
        super().__init__(config)

        self._batch_size = max(config.get(DatabaseConfKey.BATCH_SIZE, self.DEFAULT_BATCH_SIZE), 10000)
        self._clean_up_after_days = config.get(DatabaseConfKey.CLEAN_UP_AFTER_DAYS, self.DEFAULT_CLEAN_UP_AFTER_DAYS)

        self._last_clean_up_time = self._now()
        self._last_connect_time = None
        self._last_store_time = self._now()

    def connect(self):
        super().connect()

        LifecycleControl.notify(StatusNotification.MESSAGE_STORE_CONNECTED)

    def close(self):
        was_connection = bool(self._connection)

        super().close()

        if was_connection:
            LifecycleControl.notify(StatusNotification.MESSAGE_STORE_CLOSED)

    @property
    def last_clean_up_time(self) -> Optional[datetime.datetime]:
        return self._last_clean_up_time

    @property
    def last_connect_time(self) -> Optional[datetime.datetime]:
        return self._last_connect_time

    @property
    def last_store_time(self) -> Optional[datetime.datetime]:
        return self._last_store_time

    def store(self, messages):
        if not messages:
            return

        copy_statement = sql.SQL("COPY {} (message_id, topic, text, qos, retain, time) FROM STDIN") \
            .format(sql.Identifier(self._table_name))

        with self._connection.cursor() as cursor:
            with cursor.copy(copy_statement) as copy:
                for m in messages:
                    data = (m.message_id, m.topic, m.text, m.qos, m.retain, m.time)
                    copy.write_row(data)
            _logger.debug("%d row(s) inserted.", cursor.rowcount)

        self._last_store_time = self._now()

        self._connection.commit()

        LifecycleControl.notify(StatusNotification.MESSAGE_STORE_STORED)

    def clean_up(self):
        delete_statement = sql.SQL("DELETE FROM {} WHERE time < NOW() - INTERVAL '%s days'").format(sql.Identifier(self._table_name))

        with self._connection.cursor() as cursor:
            cursor.execute(delete_statement, (self._clean_up_after_days,))
            _logger.debug("%d row(s) cleaned up.", cursor.rowcount)

        self._connection.commit()
        self._last_clean_up_time = self._now()
