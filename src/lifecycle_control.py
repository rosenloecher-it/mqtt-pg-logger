import logging
import signal
import threading
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


class LifecycleControl():

    _instance = None  # type: LifecycleControl  # vs. IntegrationControl
    _lock = threading.Lock()

    def __init__(self):
        self._proceed = True

        if threading.current_thread() is threading.main_thread():
            # integration tests run the service in a thread...
            signal.signal(signal.SIGINT, self._shutdown_signaled)
            signal.signal(signal.SIGTERM, self._shutdown_signaled)

    @classmethod
    def _create_instance(cls):
        return LifecycleControl()

    @classmethod
    def get_instance(cls):
        if not LifecycleControl._instance:
            with LifecycleControl._lock:
                # another thread could have created the instance before we acquired the lock.
                # So check that the instance is still nonexistent.
                if not LifecycleControl._instance:
                    LifecycleControl._instance = cls._create_instance()
        return LifecycleControl._instance

    def _shutdown_signaled(self, sig, _frame):
        _logger.info("shutdown signaled (%s)", sig)
        self._shutdown()

    def _should_proceed(self):
        with LifecycleControl._lock:
            return self._proceed

    @classmethod
    def should_proceed(cls) -> bool:
        instance = cls.get_instance()
        return instance._should_proceed()

    def _shutdown(self) -> bool:
        with LifecycleControl._lock:
            self._proceed = False

    @classmethod
    def shutdown(cls) -> bool:
        instance = cls.get_instance()
        instance._shutdown()

    def _reset(self):
        with LifecycleControl._lock:
            self._proceed = True

    @classmethod
    def reset(cls):
        """Assure that the class gets instantiated before the threads starts"""
        instance = cls.get_instance()
        instance._reset()

    def _notify(self, status: StatusNotification):
        """Overwritten in test by a mock"""
        pass

    @classmethod
    def notify(cls, status: StatusNotification):
        instance = cls.get_instance()
        instance._notify(status)
