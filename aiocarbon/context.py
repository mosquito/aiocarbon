import threading
from typing import ClassVar

import time

from .protocol.base import BaseClient
from .protocol.buffer import BufferClient
from .storage.base import Operations
from .metric import Metric


class ClientStorage:
    def __init__(self):
        self._tls = threading.local()

    def get(self):
        client = getattr(self._tls, 'client', None)
        if not client:
            client = self._tls.client = BufferClient()
        return client

    def set(self, client: BaseClient):
        self._tls.client = client


class Meter:
    __slots__ = "_name", "value", "timestamp", "suffix"

    client_storage = ClientStorage()

    def __init__(
            self, name: str,
            value=None,
            timestamp: int = None,
            suffix: str = None
    ):
        self._name = name
        self.value = value
        self.timestamp = timestamp or int(time.time())
        self.suffix = suffix

    def send(self, operation: ClassVar[Operations] = Operations.add):
        if self.value and self.timestamp:
            if self.suffix:
                self._name = ".".join((self._name, self.suffix))

            self.client_storage.get().add(
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
    buffer_client = Meter.client_storage.get()
    Meter.client_storage.set(client)

    if hasattr(buffer_client, 'metrics_buffer'):
        while len(buffer_client.metrics_buffer):
            metric, operation = buffer_client.metrics_buffer.popleft()
            client.add(metric, operation)


__all__ = "Meter", "Counter", "Timer", "set_client",
