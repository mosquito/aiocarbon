import threading
import time
from types import TracebackType
from typing import Protocol

from .metric import Metric
from .protocol.buffer import BufferClient
from .storage import Operation, Operations


class MetricConsumer(Protocol):
    """The part of a client which the meters use."""

    def add(self, metric: Metric, operation: Operation | None = None) -> None:
        ...


class ClientStorage:
    """Keeps one metric consumer for each thread."""

    def __init__(self) -> None:
        self.tls = threading.local()

    def get(self) -> MetricConsumer:
        client: MetricConsumer | None = getattr(self.tls, "client", None)

        if client is None:
            client = self.tls.client = BufferClient()

        return client

    def set(self, client: MetricConsumer) -> None:
        self.tls.client = client


class Meter:
    __slots__ = "name", "suffix", "timestamp", "value"

    client_storage = ClientStorage()

    def __init__(
        self,
        name: str,
        value: float | None = None,
        timestamp: int | None = None,
        suffix: str | None = None,
    ) -> None:
        self.name = name
        self.value = value
        self.timestamp = timestamp or int(time.time())
        self.suffix = suffix

    def send(self, operation: Operation = Operations.add) -> None:
        if not (self.value and self.timestamp):
            return

        if self.suffix:
            self.name = ".".join((self.name, self.suffix))

        self.client_storage.get().add(
            Metric(self.name, self.value, self.timestamp),
            operation=operation,
        )

    def set_result_suffix(self, failed: bool) -> None:
        """Add the "ok" or the "fail" suffix to the metric name."""
        result = "fail" if failed else "ok"
        self.name = f"{self.name.rstrip('.')}.{result}"


class Counter(Meter):
    """Counts the events in the context block."""

    def __enter__(self) -> "Counter":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.set_result_suffix(exc_type is not None)
        self.value = 1
        self.send(Operations.add)


class Timer(Meter):
    """Measures the duration of the context block."""

    __slots__ = ("start_time",)

    def __enter__(self) -> "Timer":
        self.start_time = time.monotonic()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.value = time.monotonic() - self.start_time
        self.set_result_suffix(exc_type is not None)
        self.send(Operations.avg)


def set_client(client: MetricConsumer) -> None:
    """Set the client of this thread, and flush the buffered metrics to it."""
    previous = Meter.client_storage.get()
    Meter.client_storage.set(client)

    if isinstance(previous, BufferClient):
        while previous.metrics_buffer:
            metric, operation = previous.metrics_buffer.popleft()
            client.add(metric, operation)


__all__ = (
    "ClientStorage",
    "Counter",
    "Meter",
    "MetricConsumer",
    "Timer",
    "set_client",
)
