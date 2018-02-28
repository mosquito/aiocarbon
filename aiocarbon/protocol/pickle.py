import asyncio
import logging
import pickle
import struct
from typing import Tuple

from aiocarbon.metric import Metric
from .base import BaseClient, chunk_list, aggregate_metrics

log = logging.getLogger(__name__)


class PickleClient(BaseClient):
    CHUNK_SIZE = 512

    async def send(self):
        async with self as metrics:
            reader, writer = await asyncio.open_connection(
                self._host, self._port, loop=self.loop
            )

            chunked = chunk_list(aggregate_metrics(metrics), self.CHUNK_SIZE)
            for chunk in chunked:
                payload = pickle.dumps(
                    [self.format_metric(m) for m in chunk],
                    protocol=2
                )

                header = struct.pack("!L", len(payload))
                writer.write(header)
                await writer.drain()

                writer.write(payload)

            await writer.drain()
            writer.close()
            reader.feed_eof()

    def format_metric(self, metric: Metric) -> Tuple[str, Tuple[float, int]]:
        return (
            self.format_metric_name(metric),
            (metric.timestamp, metric.value)
        )
