import time
import asyncio

import pytest

from aiocarbon.protocol.tcp import TCPClient


@pytest.fixture()
def event_loop():
    asyncio.get_event_loop().close()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


class Server:
    def __init__(self, loop, host, port):
        self.loop = loop
        self.loop.run_until_complete(
            asyncio.start_server(
                self.handler, host, port, loop=loop
            )
        )

        self.host = host
        self.port = port
        self.data = b''
        self.event = asyncio.Event(loop=self.loop)

    async def handler(self, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter):
        while not reader.at_eof():
            self.data += await reader.read(1)

        if self.data:
            self.event.set()

    async def wait_data(self):
        await self.event.wait()
        self.event = asyncio.Event(loop=self.loop)


@pytest.fixture()
def tcp_server(event_loop, unused_tcp_port):
    server = Server(loop=event_loop, host='127.0.0.1', port=unused_tcp_port)
    yield server


@pytest.fixture()
async def tcp_client(event_loop, tcp_server):
    client = TCPClient(
        tcp_server.host,
        port=tcp_server.port,
        namespace='',
    )
    task = event_loop.create_task(client.run())
    yield client
    task.cancel()
    await asyncio.wait([task])


@pytest.fixture()
def timestamp():
    return int(time.time())
