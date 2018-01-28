import abc
from collections import defaultdict
from typing import Optional

import time

from .protocol.base import BaseClient
from .metric import Metric


class MeasurementBase:
    CLIENT = None       # type: BaseClient

    __slots__ = "_name",

    def __init__(self, name):
        self._name = name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        metric = self.get_metric()

        if metric:
            self.CLIENT.add(metric)

    @abc.abstractmethod
    def get_metric(self) -> Optional[Metric]:
        raise NotImplementedError


class Meter(MeasurementBase):
    VALUES = defaultdict(lambda: defaultdict(int))

    def get_metric(self) -> Metric:
        return Metric(self._name, self.value)

    @property
    def value(self):
        return self.VALUES[self.__class__][self._name]

    @value.setter
    def value(self, value):
        self.VALUES[self.__class__][self._name] = value

    def set(self, value):
        self.value = value


class Counter(MeasurementBase):
    COUNTERS = defaultdict(lambda: defaultdict(int))

    def get_metric(self) -> Optional[Metric]:
        return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            name = "%s.%s" % (self._name.rstrip("."), 'fail')
        else:
            name = "%s.%s" % (self._name.rstrip("."), 'ok')

        self.COUNTERS[self.__class__][name] += 1
        value = self.COUNTERS[self.__class__][name]

        self.CLIENT.add(Metric(name, value))


class Timer(MeasurementBase):
    TIMERS = defaultdict(lambda: defaultdict(int))

    def get_metric(self) -> Optional[Metric]:
        return None

    def __enter__(self):
        self._start_time = time.monotonic()
        return super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        delta = time.monotonic() - self._start_time

        if exc_type:
            name = "%s.%s" % (self._name.rstrip("."), 'fail')
        else:
            name = "%s.%s" % (self._name.rstrip("."), 'ok')

        self.CLIENT.add(Metric(name, delta))


def set_client(client: BaseClient):
    MeasurementBase.CLIENT = client


__all__ = "Meter", "Counter", "Timer", "set_client",
