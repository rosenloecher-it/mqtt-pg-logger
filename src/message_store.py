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

        self._status_stored_message_count = 0
        self._status_last_log = self._now()

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
            cursor_rowcount = cursor.rowcount

        self._connection.commit()

        self._status_stored_message_count += cursor_rowcount
        _logger.debug("%d row(s) inserted.", cursor_rowcount)

        if _logger.isEnabledFor(logging.INFO) and (self._now() - self._status_last_log).total_seconds() > 300:
            self._status_last_log = self._now()
            _logger.info("overall messages: stored=%d", self._status_stored_message_count)

        LifecycleControl.notify(StatusNotification.MESSAGE_STORE_STORED)

    def clean_up(self):
        if self._clean_up_after_days <= 0:
            return  # skip

        time_limit = self._now() - datetime.timedelta(days=self._clean_up_after_days)

        delete_statement = sql.SQL("DELETE FROM {table} WHERE time < {time_limit}")\
            .format(table=sql.Identifier(self._table_name), time_limit=sql.Literal(time_limit))

        with self._connection.cursor() as cursor:
            cursor.execute(delete_statement)
            cursor_rowcount = cursor.rowcount

        self._connection.commit()

        _logger.info("clean up: %d row(s) deleted", cursor_rowcount)

        self._last_clean_up_time = self._now()
