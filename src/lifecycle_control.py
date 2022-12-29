import logging
import signal
import threading
import time
from enum import Enum


_logger = logging.getLogger(__name__)


class StatusNotification(Enum):
    MESSAGE_STORE_CLOSED = "MESSAGE_STORE_CLOSED"
    MESSAGE_STORE_CONNECTED = "MESSAGE_STORE_CONNECTED"
    MESSAGE_STORE_STORED = "MESSAGE_STORE_STORED"

    MQTT_LISTENER_CONNECTED = "MQTT_LISTENER_CONNECTED"
    MQTT_LISTENER_SUBSCRIBED = "MQTT_LISTENER_SUBSCRIBED"
    MQTT_PUBLISHER_CONNECTED = "MQTT_PUBLISHER_CONNECTED"

    RUNNER_QUEUE_EMPTIED = "RUNNER_QUEUE_EMPTIED"

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value


class LifecycleInstance:

    def __init__(self):
        self._lock = threading.Lock()
        self._proceed = True

        if threading.current_thread() is threading.main_thread():
            # integration tests run the service in a thread...
            signal.signal(signal.SIGINT, self._shutdown_signaled)
            signal.signal(signal.SIGTERM, self._shutdown_signaled)

    def _shutdown_signaled(self, sig, _frame):
        _logger.info("shutdown signaled (%s)", sig)
        self.shutdown()

    def should_proceed(self) -> bool:
        with self._lock:
            return self._proceed

    def shutdown(self):
        with self._lock:
            self._proceed = False

    def reset(self):
        """Assure that the class gets instantiated before the threads starts"""
        with self._lock:
            self._proceed = True

    def notify(self, status: StatusNotification):
        """Overwritten in test by a mock"""
        pass

    # noinspection
    def sleep(self, seconds: float) -> float:
        time.sleep(seconds)
        return seconds


class LifecycleControl:

    _instance: LifecycleInstance = None  # vs. MockedLifecycleInstance
    _creation_lock = threading.Lock()

    @classmethod
    def _create_instance(cls) -> LifecycleInstance:
        return LifecycleInstance()

    @classmethod
    def get_instance(cls) -> LifecycleInstance:
        if not LifecycleControl._instance:
            with LifecycleControl._creation_lock:
                # another thread could have created the instance before we acquired the lock.
                # So check that the instance is still nonexistent.
                if not LifecycleControl._instance:
                    LifecycleControl._instance = cls._create_instance()
        return LifecycleControl._instance

    @classmethod
    def should_proceed(cls) -> bool:
        return cls.get_instance().should_proceed()

    @classmethod
    def shutdown(cls):
        cls.get_instance().shutdown()

    @classmethod
    def reset(cls):
        """Assure that the class gets instantiated before the threads starts"""
        cls.get_instance().reset()

    @classmethod
    def notify(cls, status: StatusNotification):
        cls.get_instance().notify(status)

    @classmethod
    def sleep(cls, seconds: float) -> float:
        return cls.get_instance().sleep(seconds)
