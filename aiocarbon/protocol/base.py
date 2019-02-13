import abc
import asyncio
import logging
import re

from aiocarbon.metric import Metric
from aiocarbon.storage.base import BaseStorage
from aiocarbon.storage import TotalStorage


log = logging.getLogger(__name__)

CARBON_NS_INVALID_CHARS = re.compile(r'([^\w\d\.\-]+|(\.){2,})')


def strip_carbon_ns(string):
    return CARBON_NS_INVALID_CHARS.sub('_', string).strip('_').lower()


class BaseClient:
    SEND_PERIOD = 1

    def __init__(self, host: str, port: int = 2003,
                 loop: asyncio.AbstractEventLoop = None,
                 storage: BaseStorage = None,
                 namespace: str=None):
        self.loop = loop or asyncio.get_event_loop()
        self.namespace = namespace if namespace is not None else 'aiocarbon'
        self._host = host
        self._port = port
        self._storage = storage or TotalStorage()
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

    def add(self, metric: Metric, operation=None):
        kwargs = {}
        if operation is not None:
            kwargs['operation'] = operation
        return self._storage.add(metric, **kwargs)
