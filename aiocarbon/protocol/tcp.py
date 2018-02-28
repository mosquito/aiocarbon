import asyncio
import logging

from aiocarbon.metric import Metric
from .base import BaseClient, aggregate_metrics

log = logging.getLogger(__name__)


class TCPClient(BaseClient):
    async def send(self):
        async with self as metrics:
            reader, writer = await asyncio.open_connection(
                self._host, self._port, loop=self.loop
            )

            for metric in aggregate_metrics(metrics):
                value = self.format_metric(metric)
                writer.write(value)

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
