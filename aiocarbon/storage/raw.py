import logging
import time
from collections.abc import Iterator

from aiocarbon.metric import Metric
from aiocarbon.storage.base import BaseStorage, Operation

log = logging.getLogger(__name__)

RawStore = list[tuple[int, float]]


class RawStorage(BaseStorage[RawStore]):
    """Saves metrics as is."""

    def create_store(self) -> RawStore:
        return []

    def add(self, metric: Metric, operation: Operation | None = None) -> None:
        self.get_metric(metric.name).append(
            (metric.timestamp, metric.value),
        )

    def __iter__(self) -> Iterator[Metric]:
        current_time = int(time.time())

        for name, metrics in self.snapshot():
            pending: RawStore = []

            for timestamp, value in metrics:
                if timestamp >= current_time:
                    # The interval is not complete. Keep the sample.
                    pending.append((timestamp, value))
                    continue

                yield Metric(name=name, timestamp=timestamp, value=value)

            metrics.clear()

            if not pending:
                continue

            metrics.extend(pending)
            log.debug(
                "%d metric(s) of %r were not sent, because they are newer "
                "than the current timestamp %d",
                len(pending), name, current_time,
            )


__all__ = ("RawStorage", "RawStore")
