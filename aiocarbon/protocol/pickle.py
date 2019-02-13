import asyncio
import logging
import pickle
import struct
from typing import Tuple, List

from aiocarbon.metric import Metric
from .base import BaseClient

log = logging.getLogger(__name__)


class PickleClient(BaseClient):
    CHUNK_SIZE = 512

    async def __sender(self, data: List):
        payload = pickle.dumps(data, protocol=2)
        header = struct.pack("!L", len(payload))

        while True:
            try:
                reader, writer = await asyncio.open_connection(
                    self._host, self._port, loop=self.loop
                )

                writer.write(header)
                await writer.drain()
                writer.write(payload)
                await writer.drain()

                writer.close()
                reader.feed_eof()
            except:
                log.exception("Failed to send metrics")
            else:
                break

    async def send(self):
        async with self.lock:
            data = []

            for idx, metric in enumerate(self._storage):
                data.append(self.format_metric(metric))

                if idx % self.CHUNK_SIZE == 0:
                    await self.__sender(data)
                    data.clear()

            if data:
                await self.__sender(data)

    def format_metric(self, metric: Metric) -> Tuple[str, Tuple[float, int]]:
        return (
            self.format_metric_name(metric),
            (metric.timestamp, metric.value)
        )
