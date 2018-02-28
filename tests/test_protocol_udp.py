import asyncio

import pytest
import time

from aiocarbon.metric import Metric
from aiocarbon.protocol.udp import UDPClient


pytestmark = pytest.mark.asyncio


class UDPServerProtocol(asyncio.DatagramProtocol):

    def __init__(self):
        self.queue = asyncio.Queue()
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.queue.put_nowait(data)

    def close(self):
        self.queue.put_nowait(None)


async def test_tcp_simple(event_loop: asyncio.AbstractEventLoop, random_port):
    protocol = UDPServerProtocol()
    await event_loop.create_datagram_endpoint(
        lambda: protocol,
        local_addr=('127.0.0.1', random_port)
    )

    client = UDPClient("127.0.0.1", port=random_port, namespace='')
    task = event_loop.create_task(client.run())

    now = time.time()

    metric = Metric(name='foo', value=42, timestamp=now)
    client.add(metric)

    data = await protocol.queue.get()

    lines = list(map(lambda x: x.decode(), filter(None, data.split(b'\n'))))

    assert len(lines) == 1

    for line in lines:
        name, value, ts = line.split(' ')
        value = float(value)
        ts = float(ts)

        assert name == 'foo'
        assert value == 42
        assert ts < time.time()

    task.cancel()
    await asyncio.wait([task])


async def test_tcp_many(event_loop: asyncio.AbstractEventLoop, random_port):
    count = 99991
    protocol = UDPServerProtocol()
    await event_loop.create_datagram_endpoint(
        lambda: protocol,
        local_addr=('127.0.0.1', random_port)
    )

    client = UDPClient("127.0.0.1", port=random_port, namespace='')
    now = time.time()

    for i in range(count):
        metric = Metric(name='foo', value=i, timestamp=now - i)
        client.add(metric)

    await client.send()
    await asyncio.sleep(1)

    protocol.close()

    data = b''
    chunk = await protocol.queue.get()

    while chunk is not None:
        data += chunk
        chunk = await protocol.queue.get()

    lines = list(map(lambda x: x.decode(), filter(None, data.split(b'\n'))))

    assert len(lines) == count

    for idx, line in enumerate(lines):
        name, value, ts = line.split(' ')
        value = float(value)
        ts = float(ts)

        assert name == 'foo'
        assert value == idx
        assert ts < time.time()
