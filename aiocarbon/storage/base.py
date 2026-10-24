import abc
from collections.abc import Callable, Iterator, MutableMapping
from typing import Generic, TypeAlias, TypeVar

from aiocarbon.metric import Metric

MetricStore: TypeAlias = MutableMapping[int, float]
Operation: TypeAlias = Callable[[MetricStore, Metric], None]

StoreT = TypeVar("StoreT")


class Operations:
    """Rules that merge one sample into the store of a metric."""

    @staticmethod
    def add(store: MetricStore, metric: Metric) -> None:
        store[metric.timestamp] = (
            store.get(metric.timestamp, 0) + metric.value
        )

    @staticmethod
    def avg(store: MetricStore, metric: Metric) -> None:
        timestamp = metric.timestamp
        if timestamp in store:
            store[timestamp] = (store[timestamp] + metric.value) / 2
        else:
            store[timestamp] = metric.value

    @staticmethod
    def min(store: MetricStore, metric: Metric) -> None:
        timestamp = metric.timestamp
        if timestamp in store:
            store[timestamp] = min(store[timestamp], metric.value)
        else:
            store[timestamp] = metric.value

    @staticmethod
    def max(store: MetricStore, metric: Metric) -> None:
        timestamp = metric.timestamp
        if timestamp in store:
            store[timestamp] = max(store[timestamp], metric.value)
        else:
            store[timestamp] = metric.value


class BaseStorage(abc.ABC, Generic[StoreT]):
    def __init__(self) -> None:
        self.metrics: dict[str, StoreT] = {}

    @abc.abstractmethod
    def create_store(self) -> StoreT:
        """Return an empty store for one metric name."""
        raise NotImplementedError

    @abc.abstractmethod
    def __iter__(self) -> Iterator[Metric]:
        """Yield the metrics which are ready to send, and remove them."""
        raise NotImplementedError

    @abc.abstractmethod
    def add(self, metric: Metric, operation: Operation | None = None) -> None:
        """Store one sample."""
        raise NotImplementedError

    def get_metric(self, name: str) -> StoreT:
        """Return the store of this metric name, and create it if required."""
        store = self.metrics.get(name)

        if store is None:
            store = self.metrics[name] = self.create_store()

        return store

    def snapshot(self) -> tuple[tuple[str, StoreT], ...]:
        """Return the metric stores as a tuple.

        Iteration returns control to the caller, and the caller can add a new
        metric name. The snapshot makes a change of the dictionary safe.
        """
        return tuple(self.metrics.items())

    def poll(self) -> bool:
        """Return True when at least one metric has samples."""
        return any(self.metrics.values())


__all__ = (
    "BaseStorage",
    "MetricStore",
    "Operation",
    "Operations",
)
