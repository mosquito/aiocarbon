import asyncio
import logging
import pickle
import struct

from aiocarbon.metric import Metric

from .base import BaseClient

log = logging.getLogger(__name__)

PickleMetric = tuple[str, tuple[int, float]]


class PickleClient(BaseClient):
    CHUNK_SIZE = 512

    # Carbon accepts the pickle protocol 2 only.
    PICKLE_PROTOCOL = 2

    SEND_ATTEMPTS = 3
    RETRY_DELAY = 0.5

    async def send_payload(self, data: list[PickleMetric]) -> None:
        """Send one batch of metrics. Retry SEND_ATTEMPTS times at most."""
        payload = pickle.dumps(data, protocol=self.PICKLE_PROTOCOL)
        header = struct.pack("!L", len(payload))

        for attempt in range(1, self.SEND_ATTEMPTS + 1):
            try:
                _, writer = await asyncio.open_connection(
                    self.host, self.port,
                )

                try:
                    writer.write(header)
                    writer.write(payload)
                    await writer.drain()
                finally:
                    writer.close()
                    await writer.wait_closed()
            except Exception:
                log.exception(
                    "Failed to send %d metric(s), attempt %d of %d",
                    len(data), attempt, self.SEND_ATTEMPTS,
                )

                if attempt == self.SEND_ATTEMPTS:
                    # All the attempts failed. Drop this batch, because the
                    # caller must not block the sending loop.
                    return

                await asyncio.sleep(self.RETRY_DELAY)
            else:
                log.debug("%d metric(s) were sent", len(data))
                return

    async def send(self) -> None:
        async with self.lock:
            data: list[PickleMetric] = []

            for metric in self.storage:
                data.append(self.format_metric(metric))

                if len(data) >= self.CHUNK_SIZE:
                    await self.send_payload(data)
                    data = []

            if data:
                await self.send_payload(data)

    def format_metric(self, metric: Metric) -> PickleMetric:
        return (
            self.format_metric_name(metric),
            (metric.timestamp, metric.value),
        )


__all__ = ("PickleClient", "PickleMetric")
