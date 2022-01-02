import time
from typing import Optional

from src.lifecycle_control import LifecycleControl, StatusNotification, LifecycleInstance


class MockedLifecycleInstance(LifecycleInstance):

    WAIT_TIME = 0.05

    def __init__(self):
        super().__init__()

        self._set = set()
        self._ex = None  # type: Optional[Exception]

    def reset(self):
        super().reset()

        with self._lock:
            self._set.clear()

    def clear_notifications(self):
        with self._lock:
            self._set.clear()

    def has(self, status: StatusNotification) -> bool:
        with self._lock:
            return status in self._set

    def wait_for_notifications(self, notifications, time_out, description):
        missing_notifications = []

        time_waited = 0
        while True:
            time.sleep(self.WAIT_TIME)
            time_waited += self.WAIT_TIME
            if 0 < time_out < time_waited:
                raise RuntimeError(f"wait_for_notifications timed out: {time_out}s; {description}; missing: {missing_notifications}")

            with self._lock:
                if self._ex:
                    raise self._ex

                missing_notifications = []
                for notification in notifications:
                    if notification not in self._set:
                        missing_notifications.append(notification)
                        break

            if not missing_notifications:
                break

    def notify(self, status: StatusNotification):
        print(f"IntegrationControl got {status}.")
        with self._lock:
            self._set.add(status)

    def set_exception(self, ex: Exception):
        with self._lock:
            self._ex = ex


class MockedLifecycleControl(LifecycleControl):

    @classmethod
    def _create_instance(cls) -> MockedLifecycleInstance:
        return MockedLifecycleInstance()

    @classmethod
    def get_instance(cls) -> MockedLifecycleInstance:
        if not MockedLifecycleControl._instance:
            with MockedLifecycleControl._creation_lock:
                # another thread could have created the instance before we acquired the lock.
                # So check that the instance is still nonexistent.
                if not MockedLifecycleControl._instance:
                    MockedLifecycleControl._instance = cls._create_instance()
        return MockedLifecycleControl._instance
