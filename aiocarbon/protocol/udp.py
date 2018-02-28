import asyncio
import logging
import socket

from aiocarbon.metric import Metric
from .base import BaseClient, chunk_list, aggregate_metrics

log = logging.getLogger(__name__)


class AsyncUDPSocket:

    __slots__ = (
        '__loop', '__sock', '__address',
        '__futures', '__closed', '__writer_added'
    )

    @staticmethod
    def create_future(loop):
        if hasattr(loop, 'create_future'):
            return loop.create_future()
        else:
            return asyncio.Future(loop=loop)

    def __init__(self, loop=None):
        self.__loop = asyncio.get_event_loop() if loop is None else loop
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__sock.setblocking(False)

        self.__futures = list()
        self.__closed = False
        self.__writer_added = False

    def sendto(self, data, host, port):
        data = data if isinstance(data, bytes) else str(data).encode('utf-8')
        future = self.create_future(self.__loop)

        destination = (data, host, port)
        self.__futures.append((destination, future))

        if not self.__writer_added:
            self.__loop.add_writer(self.__sock.fileno(), self.__sender)
            self.__writer_added = True

        return future

    def __sender(self):
        if not self.__futures:
            self.__loop.remove_writer(self.__sock.fileno())
            self.__writer_added = False
            return

        destination, future = self.__futures[0]
        data, host, port = destination

        try:
            self.__sock.sendto(data, (host, port))
        except (BlockingIOError, InterruptedError):
            return
        except Exception as exc:
            self.__abort(exc)
        else:
            self.__futures.pop(0)
            future.set_result(True)

    def __abort(self, exc):
        for future in (f for _, f in self.__futures if not f.done()):
            future.set_exception(exc)

        self.close()

    @property
    def is_closed(self):
        return self.__closed

    def close(self):
        if self.__closed:
            raise RuntimeError("Socket already closed")

        self.__closed = True
        self.__loop.remove_writer(self.__sock.fileno())
        self.__sock.close()

        for future in (f for _, f in self.__futures if not f.done()):
            future.set_exception(ConnectionError("Connection closed"))


class UDPClient(BaseClient):
    CHUNK_SIZE = 50

    def __init__(self, host: str, port: int = 2003, namespace: str=None,
                 loop: asyncio.AbstractEventLoop = None):
        super().__init__(host, port, namespace=namespace, loop=loop)
        self._socket = AsyncUDPSocket(loop=self.loop)

    async def send(self):
        async with self as metrics:
            chunked = chunk_list(
                aggregate_metrics(metrics),
                self.CHUNK_SIZE
            )

            for chunk in chunked:
                payload = b"".join(self.format_metric(m) for m in chunk)
                await self._socket.sendto(payload, self._host, self._port)

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
