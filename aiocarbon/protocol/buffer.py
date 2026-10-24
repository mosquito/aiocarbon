from collections import deque
from typing import NamedTuple

from aiocarbon.metric import Metric
from aiocarbon.storage import Operation


class BufferedMetric(NamedTuple):
    metric: Metric
    operation: Operation | None


class BufferClient:
    """Keeps the metrics until the application sets a carbon client.

    The buffer holds MAX_BUFFER_LEN metrics at most. When it is full, the
    oldest metric is dropped.
    """

    MAX_BUFFER_LEN = 10000

    def __init__(self) -> None:
        self.metrics_buffer: deque[BufferedMetric] = deque(
            (), maxlen=self.MAX_BUFFER_LEN,
        )

    def add(self, metric: Metric, operation: Operation | None = None) -> None:
        self.metrics_buffer.append(BufferedMetric(metric, operation))


__all__ = ("BufferClient", "BufferedMetric")
