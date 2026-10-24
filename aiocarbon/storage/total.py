import time
from collections import defaultdict
from collections.abc import Iterator

from aiocarbon.metric import Metric
from aiocarbon.storage.base import (
    BaseStorage,
    MetricStore,
    Operation,
    Operations,
)


class TotalStorage(BaseStorage[MetricStore]):
    """Adds up values of the same metrics with the same timestamps."""

    def create_store(self) -> MetricStore:
        return defaultdict(float)

    def add(self, metric: Metric, operation: Operation | None = None) -> None:
        operation = operation or Operations.add
        operation(self.get_metric(metric.name), metric)

    def __iter__(self) -> Iterator[Metric]:
        current_time = int(time.time())

        for name, metrics in self.snapshot():
            pending: list[tuple[int, float]] = []

            while metrics:
                timestamp, value = metrics.popitem()

                if timestamp >= current_time:
                    # The interval is not complete. Keep the sample.
                    pending.append((timestamp, value))
                    continue

                yield Metric(name=name, timestamp=timestamp, value=value)

            for timestamp, value in pending:
                metrics[timestamp] = metrics.get(timestamp, 0) + value


__all__ = ("TotalStorage",)
