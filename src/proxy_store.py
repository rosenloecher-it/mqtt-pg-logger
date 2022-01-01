import datetime
import logging
import threading
import time
from collections import deque
from typing import List

from tzlocal import get_localzone

from src.database import DatabaseException, DatabaseConfKey
from src.message import Message
from src.message_store import MessageStore


_logger = logging.getLogger(__name__)


class ProxyStore(threading.Thread):
    """A async proxy to MessageStore which handles batches, queuing"""

    RECONNECT_AFTER_SECONDS = 3600

    DEFAULT_BATCH_SIZE = 100
    DEFAULT_WAIT_MAX_SECONDS = 10

    QUEUE_LIMIT = 50000
    WAIT_AFTER_ERROR_SECONDS = 20
    FORCE_CLEAN_UP_AFTER_SECONDS = 3000
    LAZY_CLEAN_UP_AFTER_SECONDS = 300

    def __init__(self, config):
        threading.Thread.__init__(self)

        # runtime properties
        self._message_store = MessageStore(config)
        self._closing = False
        self._lock = threading.Lock()
        self._messages = deque()
        self._write_immediately = False

        self._last_error_text = None

        # configuration
        self._batch_size = min(config.get(DatabaseConfKey.BATCH_SIZE, self.DEFAULT_BATCH_SIZE), 10000)
        self._wait_max_seconds = min(config.get(DatabaseConfKey.WAIT_MAX_SECONDS, self.DEFAULT_WAIT_MAX_SECONDS), 60)

        super().start()

    def close(self):
        with self._lock:
            self._closing = True

    def _is_closing(self):
        with self._lock:
            return bool(self._closing)

    def queue(self, messages: List[Message], write_immediately=False):
        added = 0
        lost_messages = None

        with self._lock:
            if write_immediately and not self._write_immediately:
                self._write_immediately = True

            for message in messages:
                if len(self._messages) > self.QUEUE_LIMIT:
                    lost_messages = len(messages) - added
                    break
                self._messages.append(message)
                added += 1

        if lost_messages is not None:
            _logger.error("message queue limit (%d) reached => lost %d messages!", self.QUEUE_LIMIT, lost_messages)

    def _close_connection(self):
        try:
            self._message_store.close()
        except Exception as ex:
            _logger.exception(ex)

    def start(self):
        raise RuntimeError("started within constructor!")

    def run(self):
        step_time = 0.05
        last_loops_with_error_count = 0

        try:
            while not self._is_closing():
                busy = False
                if last_loops_with_error_count > 10:
                    time.sleep(2)

                try:
                    if self._check_connection():
                        busy = True
                    if self._should_store_messages():
                        if self._store_messages():
                            busy = True
                    if not busy:
                        if self._clean_up():
                            busy = True

                    if self._message_store.last_connect_time is not None:
                        diff_seconds = (self._now() - self._message_store.last_connect_time).total_seconds()
                        if diff_seconds > self.RECONNECT_AFTER_SECONDS:
                            _logger.debug(f"automatically closing connection after {self.RECONNECT_AFTER_SECONDS}s.")
                            self._close_connection()
                            busy = True

                    time.sleep(step_time / 100 if busy else step_time)
                    last_loops_with_error_count = 0

                except Exception as ex:
                    last_loops_with_error_count += 1
                    self._handle_exception_but_prevent_stacktrace(ex)
                    self._close_connection()
                    # proceed after connection loss

        except Exception as ex:
            _logger.exception(ex)
        finally:
            self._close_connection()

    def _check_connection(self) -> bool:
        """Separated to mock and test without threads"""

        if self._message_store.is_connected:
            return False
        self._message_store.connect()
        return True

    def _clean_up(self):
        """Separated to mock and test without threads"""
        if self._should_clean_up_items():
            self._message_store.clean_up()
            return True
        return False

    def _handle_exception_but_prevent_stacktrace(self, ex: Exception):
        error_text = str(ex)
        if error_text == self._last_error_text:
            _logger.error(error_text)
        else:
            self._last_error_text = error_text
            show_tracback = not isinstance(ex, DatabaseException)
            _logger.error(ex, exc_info=show_tracback)

    def _should_store_messages(self) -> bool:
        message_count = len(self._messages)
        if message_count == 0:
            return False

        if self._write_immediately:
            return True

        if message_count >= self._batch_size:
            return True

        diff_seconds = (self._now() - self._message_store.last_store_time).total_seconds()
        if diff_seconds > self._wait_max_seconds:
            return True

        return False

    def _store_messages(self) -> bool:
        messages = []

        with self._lock:
            while len(messages) < self._batch_size:
                try:
                    m = self._messages.popleft()
                    messages.append(m)
                except IndexError:
                    self._write_immediately = False
                    break

        if messages:
            self._message_store.store(messages)

        self._last_error_text = None

        return bool(messages)

    def _should_clean_up_items(self) -> bool:
        seconds_clean_up = (self._now() - self._message_store.last_clean_up_time).total_seconds()
        if seconds_clean_up >= self.FORCE_CLEAN_UP_AFTER_SECONDS:
            return True

        if len(self._messages) == 0 and seconds_clean_up > self.LAZY_CLEAN_UP_AFTER_SECONDS:
            seconds_since_last_store = (self._now() - self._message_store.last_store_time).total_seconds()
            return bool(seconds_since_last_store > 1)

        return False

    @classmethod
    def _now(cls) -> datetime:
        """overwritable datetime.now for testing"""
        return datetime.datetime.now(tz=get_localzone())
