import asyncio
import logging

from aiocarbon.metric import Metric
from .base import BaseClient


log = logging.getLogger(__name__)


class TCPClient(BaseClient):
    CHUNK_SIZE = 512

    async def send(self):
        async with self.lock:
            reader, writer = await asyncio.open_connection(
                self._host, self._port, loop=self.loop
            )

            idx = 0
            for idx, metric in enumerate(self._storage):
                value = self.format_metric(metric)
                writer.write(value)

                if idx % self.CHUNK_SIZE == 0:
                    await writer.drain()

            log.debug('%d metric(s) were sent', idx + 1)

            await writer.drain()
            writer.close()
            reader.feed_eof()

    def format_metric(self, metric: Metric) -> bytes:
        if isinstance(metric.value, float):
            value = "%.12f" % metric.value
        else:
            value = metric.value

        return (
            "%s %s %s\n" % (
                self.format_metric_name(metric), value, metric.timestamp,
            )
        ).encode()
