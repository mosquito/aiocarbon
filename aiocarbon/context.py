import abc
import threading
from collections import defaultdict, Counter as _Counter
from typing import Optional

import time

from .protocol.base import BaseClient
from .metric import Metric


class MeasurementBase:
    __slots__ = "_name",

    TLS = threading.local()

    @property
    def client(self):
        return self.TLS.client

    def __init__(self, name):
        self._name = name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        metric = self.get_metric()

        if metric:
            self.client.add(metric)

    @abc.abstractmethod
    def get_metric(self) -> Optional[Metric]:
        raise NotImplementedError


class Meter(MeasurementBase):
    VALUES = defaultdict(_Counter)

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
    def get_metric(self) -> Optional[Metric]:
        return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            name = "%s.%s" % (self._name.rstrip("."), 'fail')
        else:
            name = "%s.%s" % (self._name.rstrip("."), 'ok')

        self.client.add(Metric(name, 1))


class Timer(MeasurementBase):
    TIMERS = defaultdict(_Counter)

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

        self.client.add(Metric(name, delta))


def set_client(client: BaseClient):
    MeasurementBase.TLS.client = client


__all__ = "Meter", "Counter", "Timer", "set_client",
