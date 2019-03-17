import time
from typing import AsyncIterable
import logging

from immutables import Map

from aiocarbon.storage.base import BaseStorage
from aiocarbon.metric import Metric


log = logging.getLogger(__name__)


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

            if not returning:
                continue

            metrics.extend(returning)
            log.info("%d metric(s) of %r weren't sent yet because they're "
                     "newer than the current timestamp %d",
                     len(returning), name, current_time)

    def add(self, metric: Metric, operation=None):
        self._get_metric(metric.name).append((metric.timestamp, metric.value))
