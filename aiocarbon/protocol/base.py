import abc
import asyncio
import logging
import re
import time

from collections import Counter
from typing import TypeVar, AsyncIterable

from immutables import Map

from aiocarbon.metric import Metric


T = TypeVar('T')
log = logging.getLogger(__name__)

CARBON_NS_INVALID_CHARS = re.compile(r'([^\w\d\.\-]+|(\.){2,})')


def strip_carbon_ns(string):
    return CARBON_NS_INVALID_CHARS.sub('_', string).strip('_').lower()


class Operations:
    @staticmethod
    def add(store: Counter, metric: Metric):
        store[metric.timestamp] += metric.value

    @staticmethod
    def avg(store: Counter, metric: Metric):
        store[metric.timestamp] = (store[metric.timestamp] + metric.value) / 2


class BaseClient:
    SEND_PERIOD = 1

    def __init__(self, host: str, port: int = 2003,
                 loop: asyncio.AbstractEventLoop = None,
                 namespace: str=None):
        self.loop = loop or asyncio.get_event_loop()
        self.namespace = namespace if namespace is not None else 'aiocarbon'
        self._host = host
        self._port = port
        self._metrics = Map()

        self.lock = asyncio.Lock(loop=self.loop)

    @property
    def namespace(self):
        return self._ns

    @namespace.setter
    def namespace(self, value):
        self._ns = strip_carbon_ns(value)

    async def run(self):
        while True:
            try:
                await self.send()
            except:
                log.exception("Exception while sending metrics")
            finally:
                await asyncio.sleep(self.SEND_PERIOD, loop=self.loop)

    def __iter__(self) -> AsyncIterable[Metric]:
        current_time = int(time.time())

        for name, metrics in self._metrics.items():  # type: Counter
            returning = list()

            while metrics:
                ts, value = metrics.popitem()

                if ts >= current_time:
                    returning.append((ts, value))
                    continue

                yield Metric(name=name, timestamp=ts, value=value)

            for ts, value in returning:
                metrics[ts] += value

    def format_metric_name(self, metric: Metric):
        if self._ns:
            return ".".join((self._ns, metric.name))
        else:
            return metric.name

    def poll(self):
        return any(self._metrics.values())

    @abc.abstractmethod
    def send(self):
        raise NotImplementedError

    @abc.abstractmethod
    def format_metric(self, metric: Metric):
        raise NotImplementedError

    def _get_metric(self, name):
        if name not in self._metrics:
            self._metrics = self._metrics.set(name, Counter())

        return self._metrics[name]

    def add(self, metric: Metric, operation=Operations.add):
        operation(self._get_metric(metric.name), metric)
