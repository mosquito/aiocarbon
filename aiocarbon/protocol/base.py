import abc
import asyncio
import logging
from collections import deque, defaultdict
from typing import Iterable, TypeVar

import itertools

from aiocarbon.metric import Metric

T = TypeVar('T')
log = logging.getLogger(__name__)


class BaseClient:
    SEND_PERIOD = 1

    def __init__(self, host: str, port: int = 2003,
                 loop: asyncio.AbstractEventLoop = None,
                 namespace: str=None):
        self.loop = loop or asyncio.get_event_loop()
        self._ns = namespace if namespace is not None else 'carbon'
        self._host = host
        self._port = port
        self._metrics = deque()
        self._lock = asyncio.Lock(loop=self.loop)
        self.__metrics = deque()

    async def run(self):
        while True:
            try:
                await self.send()
            except:
                log.exception("Exception while sending metrics")
            finally:
                await asyncio.sleep(self.SEND_PERIOD, loop=self.loop)

    async def __aenter__(self):
        await self._lock.acquire()
        while self._metrics:
            metric = self._metrics.popleft()
            self.__metrics.append(metric)

        return self.__metrics

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            while self.__metrics:
                self._metrics.appendleft(self.__metrics.pop())
        else:
            self.__metrics.clear()

        self._lock.release()

    def format_metric_name(self, metric: Metric):
        if self._ns:
            return ".".join((self._ns, metric.name))
        else:
            return metric.name

    @abc.abstractmethod
    def send(self):
        raise NotImplementedError

    @abc.abstractmethod
    def format_metric(self, metric: Metric):
        raise NotImplementedError

    def add(self, metric: Metric):
        self._metrics.append(metric)


def chunk_list(iterable: Iterable[T], size: int) -> Iterable[Iterable[T]]:
    iterable = iter(iterable)

    item = list(itertools.islice(iterable, size))

    while item:
        yield item
        item = list(itertools.islice(iterable, size))


def aggregate_metrics(metrics: Iterable[Metric]) -> Iterable[Metric]:
    result = defaultdict(float)

    for metric in metrics:  # type: Metric
        result[metric.name, int(metric.timestamp)] += metric.value

    for metric_ts, value in result.items():
        name, timestamp = metric_ts
        yield Metric(name=name, timestamp=timestamp, value=value)
