import threading
from typing import ClassVar

import time

from .protocol.base import BaseClient
from .storage.base import Operations
from .metric import Metric


class Meter:
    __slots__ = "_name", "value", "timestamp", "suffix"

    TLS = threading.local()

    def __init__(self, name, value=None, timestamp=None, suffix=None):
        self._name = name
        self.value = value
        self.timestamp = timestamp or int(time.time())
        self.suffix = suffix

    def send(self, operation: ClassVar[Operations]=Operations.add):
        if self.value and self.timestamp:
            if self.suffix:
                self._name = ".".join((self._name, self.suffix))

            self.TLS.client.add(
                Metric(self._name, self.value, self.timestamp),
                operation=operation
            )


class Counter(Meter):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._name = "%s.%s" % (self._name.rstrip("."), 'fail')
        else:
            self._name = "%s.%s" % (self._name.rstrip("."), 'ok')

        self.value = 1
        self.send(Operations.add)


class Timer(Meter):
    __slots__ = "_start_time",

    def __enter__(self):
        self._start_time = time.monotonic()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.value = time.monotonic() - self._start_time

        if exc_type:
            self._name = "%s.%s" % (self._name.rstrip("."), 'fail')
        else:
            self._name = "%s.%s" % (self._name.rstrip("."), 'ok')

        self.send(Operations.avg)


def set_client(client: BaseClient):
    Meter.TLS.client = client


__all__ = "Meter", "Counter", "Timer", "set_client",
