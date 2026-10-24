import asyncio
import logging

from aiocarbon.metric import Metric

from .base import BaseClient

log = logging.getLogger(__name__)


class TCPClient(BaseClient):
    CHUNK_SIZE = 512

    async def send(self) -> None:
        async with self.lock:
            _, writer = await asyncio.open_connection(self.host, self.port)

            try:
                count = 0
                for idx, metric in enumerate(self.storage):
                    writer.write(self.format_metric(metric))
                    count = idx + 1

                    if idx % self.CHUNK_SIZE == 0:
                        await writer.drain()

                log.debug("%d metric(s) were sent", count)
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

    def format_metric(self, metric: Metric) -> bytes:
        if isinstance(metric.value, float):
            value = f"{metric.value:.12f}"
        else:
            value = str(metric.value)

        name = self.format_metric_name(metric)
        return f"{name} {value} {metric.timestamp}\n".encode()


__all__ = ("TCPClient",)
