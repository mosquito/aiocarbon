from typing import NamedTuple

from aiocarbon.metric import Metric
from aiocarbon.storage.base import Operations


class BufferedMetric(NamedTuple):

    metric: Metric
    operation: Operations


class BufferClient:

    def __init__(self):
        self.metrics_buffer = []

    def add(self, metric, operation):
        self.metrics_buffer.append(BufferedMetric(metric, operation))
