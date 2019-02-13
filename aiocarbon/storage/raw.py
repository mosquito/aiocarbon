import time
from typing import AsyncIterable

from immutables import Map

from aiocarbon.storage.base import BaseStorage
from aiocarbon.metric import Metric


class RawStorage(BaseStorage):

    """ Saves metrics as is """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._metrics = Map()
        self._storage_class = list

    def __iter__(self) -> AsyncIterable[Metric]:
        current_time = int(time.time())

        for name, metrics in self._metrics.items():  # type: Counter
            returning = list()

            for ts, value in metrics:

                if ts >= current_time:
                    returning.append((ts, value))
                    continue

                yield Metric(name=name, timestamp=ts, value=value)

            metrics.clear()
            metrics.extend(returning)

    def add(self, metric: Metric, operation=None):
        self._get_metric(metric.name).append((metric.timestamp, metric.value))
