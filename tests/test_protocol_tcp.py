import asyncio

import pytest
import time

from aiocarbon.metric import Metric
from aiocarbon.protocol.tcp import TCPClient


pytestmark = pytest.mark.asyncio


async def test_tcp_simple(tcp_client, tcp_server, event_loop):
    metric = Metric(name='foo', value=42)
    tcp_client.add(metric)

    await tcp_server.wait_data()

    name, value, ts = tcp_server.data.decode().strip().split(" ")

    assert name == metric.name
    assert int(float(value)) == int(metric.value)
    assert int(float(ts)) == int(metric.timestamp)


async def test_tcp_many(tcp_client, tcp_server, event_loop):
    now = int(time.time()) - 86400

    for i in range(199):
        metric = Metric(name='foo', value=42, timestamp=now + i)
        tcp_client.add(metric)

    await tcp_server.wait_data()

    for i in range(199):
        metric = Metric(name='foo', value=42, timestamp=now - i)
        tcp_client.add(metric)

    await tcp_server.wait_data()

    lines = list(filter(None, tcp_server.data.split(b"\n")))

    assert len(lines) == 398

    for line in lines:
        name, value, ts = line.decode().strip().split(" ")

        assert name == 'foo'
        assert int(float(value)) == 42


async def test_tcp_reconnect(event_loop: asyncio.AbstractEventLoop,
                             unused_tcp_port):

    async def handler(reader, writer):
        await reader.read(10)
        writer.close()
        reader.feed_eof()

    server = await asyncio.start_server(
        handler, '127.0.0.1', unused_tcp_port, loop=event_loop
    )

    client = TCPClient('127.0.0.1', port=unused_tcp_port, namespace='')
    count = 19907
    now = time.time() - 1

    for i in range(count):
        metric = Metric(name='foo', value=i, timestamp=now - i)
        client.add(metric)

    server.close()
    await server.wait_closed()

    with pytest.raises(ConnectionError):
        await client.send()

    event = asyncio.Event(loop=event_loop)

    data = b''

    async def handler(reader, writer):
        nonlocal data
        while not reader.at_eof():
            try:
                data += await reader.read(1024)
            except:
                break

        try:
            data += await reader.read(1024)
        except:
            pass

        event.set()

    server = await asyncio.start_server(
        handler, '127.0.0.1', unused_tcp_port, loop=event_loop
    )

    await client.send()
    await event.wait()

    server.close()
    await server.wait_closed()

    lines = sorted(
        map(
            lambda x: x.split(' '),
            filter(None, map(lambda x: x.decode(), data.split(b"\n")))
        ),
        key=lambda x: int(x[1])
    )

    for idx, (name, value, ts) in enumerate(lines):
        value = float(value)

        assert name == 'foo'
        assert idx == value

    assert len(lines) == count
