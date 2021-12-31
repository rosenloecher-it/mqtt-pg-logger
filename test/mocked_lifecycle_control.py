import time
from typing import Optional

from src.lifecycle_control import LifecycleControl, StatusNotification


class MockedLifecycleControl(LifecycleControl):

    WAIT_TIME = 0.05

    def __init__(self):
        super().__init__()

        self._set = set()
        self._ex = None  # type: Optional[Exception]

    def _reset(self):
        super()._reset()

        with MockedLifecycleControl._lock:
            self._set.clear()

    def _clear_notifications(self):
        with MockedLifecycleControl._lock:
            self._set.clear()
            self._shutdown = False

    @classmethod
    def _create_instance(cls):
        return MockedLifecycleControl()

    @classmethod
    def clear_notifications(cls):
        instance = cls.get_instance()
        instance._clear_notifications()

    @classmethod
    def has(cls, status: StatusNotification) -> bool:
        instance = cls.get_instance()
        with MockedLifecycleControl._lock:
            return status in instance._set

    @classmethod
    def wait_for_notifications(cls, notifications, time_out, description):
        instance = cls.get_instance()

        missing_notifications = []

        time_waited = 0
        while True:
            time.sleep(cls.WAIT_TIME)
            time_waited += cls.WAIT_TIME
            if 0 < time_out < time_waited:
                raise RuntimeError(f"wait_for_notifications timed out: {time_out}s; {description}; missing: {missing_notifications}")

            with MockedLifecycleControl._lock:
                if instance._ex:
                    raise instance._ex

                missing_notifications = []
                for notification in notifications:
                    if notification not in instance._set:
                        missing_notifications.append(notification)
                        break

            if not missing_notifications:
                break

    def _notify(self, status: StatusNotification):
        print(f"IntegrationControl got {status}.")
        with MockedLifecycleControl._lock:
            self._set.add(status)

    @classmethod
    def set_exception(cls, ex: Exception):
        instance = cls.get_instance()
        with MockedLifecycleControl._lock:
            instance._ex = ex
