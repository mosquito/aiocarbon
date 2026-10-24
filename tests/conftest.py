import asyncio
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress

import pytest

from aiocarbon.protocol.tcp import TCPClient


class Server:
    """TCP server which collects every received byte."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.data = b""
        self.event = asyncio.Event()
        self.server: asyncio.Server | None = None
        self.connections: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self.handler, self.host, self.port,
        )

    async def stop(self) -> None:
        if self.server is None:
            return

        self.server.close()

        # Python 3.12 waits for the connection handlers in wait_closed(). A
        # cancelled client can leave a connection open, and then the handler
        # waits for data forever. Close the connections before the wait.
        for writer in tuple(self.connections):
            writer.close()

        await self.server.wait_closed()

    async def handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        self.connections.add(writer)

        try:
            while not reader.at_eof():
                chunk = await reader.read(4096)

                if not chunk:
                    break

                self.data += chunk

            if self.data:
                self.event.set()
        finally:
            self.connections.discard(writer)
            writer.close()

    async def wait_data(self) -> None:
        """Wait until a client sends data and closes the connection."""
        await self.event.wait()
        self.event = asyncio.Event()


@pytest.fixture
async def tcp_server(unused_tcp_port: int) -> AsyncIterator[Server]:
    server = Server("127.0.0.1", unused_tcp_port)
    await server.start()

    try:
        yield server
    finally:
        await server.stop()


@pytest.fixture
async def tcp_client(tcp_server: Server) -> AsyncIterator[TCPClient]:
    client = TCPClient(tcp_server.host, port=tcp_server.port, namespace="")
    task = asyncio.get_running_loop().create_task(client.run())

    try:
        yield client
    finally:
        task.cancel()

        with suppress(asyncio.CancelledError):
            await task


@pytest.fixture
def timestamp() -> Iterator[int]:
    yield int(time.time())
