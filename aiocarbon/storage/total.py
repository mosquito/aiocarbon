import time
from collections import Counter
from typing import AsyncIterable

from immutables import Map

from aiocarbon.storage.base import BaseStorage
from aiocarbon.metric import Metric


class TotalStorage(BaseStorage):

    """ Adds up values of the same metrics with the same timestamps """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._metrics = Map()
        self._storage_class = Counter

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
