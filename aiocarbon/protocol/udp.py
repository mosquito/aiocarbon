import asyncio
import io
import logging
import socket
from typing import Any

from aiocarbon.metric import Metric
from aiocarbon.storage import BaseStorage

from .base import BaseClient

log = logging.getLogger(__name__)

Destination = tuple[bytes, str, int]


async def resolver(
    host: str, port: int, loop: asyncio.AbstractEventLoop,
) -> tuple[socket.AddressFamily, str, int]:
    """Return the address family and the address for a UDP destination."""
    addr_info = await loop.getaddrinfo(host, port)

    family, _, _, _, host_info = next(
        info for info in addr_info if info[1] == socket.SOCK_DGRAM
    )

    return family, str(host_info[0]), port


class AsyncUDPSocket:
    """Sends UDP datagrams through a writer callback of the event loop."""

    __slots__ = ("closed", "loop", "pending", "sock", "writer_added")

    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self.sock: socket.socket | None = None
        self.pending: list[tuple[Destination, asyncio.Future[bool]]] = []
        self.closed = False
        self.writer_added = False

    async def sendto(self, data: bytes | str, host: str, port: int) -> bool:
        """Put one datagram in the queue, and wait for the send."""
        loop = self.loop = asyncio.get_running_loop()
        family, host, port = await resolver(host, port, loop)

        if self.sock is None:
            self.sock = socket.socket(family, socket.SOCK_DGRAM)
            self.sock.setblocking(False)

        payload = data if isinstance(data, bytes) else str(data).encode()
        future: asyncio.Future[bool] = loop.create_future()
        self.pending.append(((payload, host, port), future))

        if not self.writer_added:
            loop.add_writer(self.sock.fileno(), self.send_pending)
            self.writer_added = True

        return await future

    def send_pending(self) -> None:
        """Send the queued datagrams. The event loop calls this method."""
        if self.sock is None or self.loop is None:
            return

        if not self.pending:
            self.loop.remove_writer(self.sock.fileno())
            self.writer_added = False
            return

        destination, future = self.pending[0]
        data, host, port = destination

        try:
            self.sock.sendto(data, (host, port))
        except (BlockingIOError, InterruptedError):
            return
        except Exception as exc:
            self.abort(exc)
        else:
            self.pending.pop(0)
            future.set_result(True)

    def abort(self, exc: BaseException) -> None:
        """Fail all the queued datagrams, and close the socket."""
        for future in (f for _, f in self.pending if not f.done()):
            future.set_exception(exc)

        self.close()

    @property
    def is_closed(self) -> bool:
        return self.closed

    def close(self) -> None:
        if self.closed:
            raise RuntimeError("Socket already closed")

        self.closed = True

        if self.sock is not None:
            if self.loop is not None and self.writer_added:
                self.loop.remove_writer(self.sock.fileno())
                self.writer_added = False

            self.sock.close()

        for future in (f for _, f in self.pending if not f.done()):
            future.set_exception(ConnectionError("Connection closed"))


class UDPClient(BaseClient):
    SENDING_THRESHOLD = 1200

    def __init__(
        self,
        host: str,
        port: int = 2003,
        namespace: str | None = None,
        storage: BaseStorage[Any] | None = None,
    ) -> None:
        super().__init__(
            host, port, namespace=namespace, storage=storage,
        )

        self.socket = AsyncUDPSocket()

    async def send_part(self, data: bytes) -> None:
        await self.socket.sendto(data, self.host, self.port)
        log.debug("%d bytes were sent", len(data))

    async def send(self) -> None:
        async with self.lock:
            with io.BytesIO() as buffer:
                for metric in self.storage:
                    buffer.write(self.format_metric(metric))

                    if buffer.tell() > self.SENDING_THRESHOLD:
                        await self.send_part(buffer.getvalue())

                        buffer.seek(0)
                        buffer.truncate(0)

                if buffer.tell():
                    await self.send_part(buffer.getvalue())

    def format_metric(self, metric: Metric) -> bytes:
        if isinstance(metric.value, float):
            value = f"{metric.value:.12f}"
        else:
            value = str(metric.value)

        name = self.format_metric_name(metric)
        return f"{name} {value} {metric.timestamp}\n".encode()


__all__ = ("AsyncUDPSocket", "Destination", "UDPClient", "resolver")
