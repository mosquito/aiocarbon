import asyncio
import logging

from aiocarbon.metric import Metric
from .base import BaseClient


log = logging.getLogger(__name__)


class TCPClient(BaseClient):
    async def send(self):
        async with self as metrics:
            reader, writer = await asyncio.open_connection(
                self._host, self._port, loop=self.loop
            )

            for metric in metrics:
                value = self.format_metric(metric)
                writer.write(value)

            await writer.drain()
            writer.close()
            reader.feed_eof()

    def format_metric(self, metric: Metric) -> bytes:
        return "{1} {0.value} {0.timestamp}\n".format(
            metric, self.format_metric_name(metric)
        ).encode()
