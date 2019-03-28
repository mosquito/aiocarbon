from collections import deque, namedtuple

from aiocarbon.metric import Metric
from aiocarbon.storage.base import Operations


BufferedMetric = namedtuple('BufferedMetric', ['metric', 'operation'])


class BufferClient:

    """
    Special case client for metrics buffering before initialization of
    proper carbon client completed
    """

    MAX_BUFFER_LEN = 10000

    def __init__(self):
        self.metrics_buffer = deque([], maxlen=self.MAX_BUFFER_LEN)

    def add(self, metric, operation):
        self.metrics_buffer.append(BufferedMetric(metric, operation))
