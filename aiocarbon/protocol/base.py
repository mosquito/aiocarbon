import abc
import asyncio
import logging
import time

from collections import Counter, OrderedDict, Callable
from typing import TypeVar, AsyncIterable

from aiocarbon.metric import Metric

T = TypeVar('T')
log = logging.getLogger(__name__)


class Operations:
    @staticmethod
    def add(store: Counter, metric: Metric):
        store[metric.timestamp] += metric.value

    @staticmethod
    def avg(store: Counter, metric: Metric):
        store[metric.timestamp] = (store[metric.timestamp] + metric.value) / 2


class DefaultOrderedDict(OrderedDict):
    # Source: http://stackoverflow.com/a/6190500/562769
    def __init__(self, default_factory=None, *a, **kw):
        if (default_factory is not None and
           not isinstance(default_factory, Callable)):
            raise TypeError('first argument must be callable')

        OrderedDict.__init__(self, *a, **kw)
        self.default_factory = default_factory

    def __getitem__(self, key):
        try:
            return OrderedDict.__getitem__(self, key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def __reduce__(self):
        if self.default_factory is None:
            args = tuple()
        else:
            args = self.default_factory,
        return type(self), args, None, None, self.items()

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return type(self)(self.default_factory, self)

    def __deepcopy__(self, memo):
        import copy
        return type(self)(self.default_factory,
                          copy.deepcopy(self.items()))

    def __repr__(self):
        return 'OrderedDefaultDict(%s, %s)' % (self.default_factory,
                                               OrderedDict.__repr__(self))


class BaseClient:
    SEND_PERIOD = 1

    def __init__(self, host: str, port: int = 2003,
                 loop: asyncio.AbstractEventLoop = None,
                 namespace: str=None):
        self.loop = loop or asyncio.get_event_loop()
        self._ns = namespace if namespace is not None else 'aiocarbon'
        self._host = host
        self._port = port

        self.lock = asyncio.Lock(loop=self.loop)
        self._metrics = DefaultOrderedDict(Counter)

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

    def add(self, metric: Metric, operation=Operations.add):
        operation(self._metrics[metric.name], metric)
