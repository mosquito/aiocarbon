import abc
from typing import AsyncIterable

from aiocarbon.metric import Metric


class Operations:
    @staticmethod
    def add(store, metric: Metric):
        store[metric.timestamp] += metric.value

    @staticmethod
    def avg(store, metric: Metric):
        store[metric.timestamp] = (store[metric.timestamp] + metric.value) / 2


class BaseStorage:

    def __init__(self):
        self._metrics = None
        self._storage_class = None

    @abc.abstractmethod
    def __iter__(self) -> AsyncIterable[Metric]:
        raise NotImplementedError

    def _get_metric(self, name):
        if name not in self._metrics:
            self._metrics = self._metrics.set(name, self._storage_class())

        return self._metrics[name]

    def add(self, metric: Metric, operation=Operations.add):
        operation(self._get_metric(metric.name), metric)

    def poll(self):
        return any(self._metrics.values())
