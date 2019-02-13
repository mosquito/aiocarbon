import asyncio
import pickle
import struct

import pytest
import time

from aiocarbon.metric import Metric
from aiocarbon.protocol.pickle import PickleClient


pytestmark = pytest.mark.asyncio


async def test_pickle_many(event_loop, unused_tcp_port):
    client = PickleClient('127.0.0.1', port=unused_tcp_port, namespace='')

    count = 9991
    now = time.time() - 1

    for i in range(count):
        metric = Metric(name='foo', value=i, timestamp=now - i)
        client.add(metric)

    data = list()
    event = asyncio.Event(loop=event_loop)

    async def handler(reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter):

        nonlocal data

        header_size = struct.calcsize('!L')

        while not reader.at_eof():
            try:
                header = await reader.readexactly(header_size)
            except asyncio.IncompleteReadError:
                break

            payload_size = struct.unpack("!L", header)[0]

            try:
                payload = await reader.readexactly(payload_size)
            except asyncio.IncompleteReadError:
                break

            for metric in pickle.loads(payload):
                data.append(metric)

        event.set()
        writer.close()
        reader.feed_eof()

    server = await asyncio.start_server(
        handler, '127.0.0.1', unused_tcp_port, loop=event_loop
    )

    await client.send()
    await asyncio.sleep(0.1)
    await event.wait()

    assert len(data) == count

    for idx, metric in enumerate(sorted(data, key=lambda x: x[1][1])):
        name, payload = metric
        ts, value = payload

        assert name == 'foo'
        assert value == idx

    server.close()


async def test_pickle_reconnect(event_loop: asyncio.AbstractEventLoop,
                                unused_tcp_port):

    async def handler(reader, writer):
        await reader.read(10)
        writer.close()
        reader.feed_eof()

    server = await asyncio.start_server(
        handler, '127.0.0.1', unused_tcp_port, loop=event_loop
    )

    client = PickleClient('127.0.0.1', port=unused_tcp_port, namespace='')

    count = 199991
    now = time.time() - 86400

    for i in range(count):
        metric = Metric(name='foo', value=i, timestamp=now - i)
        client.add(metric)

    server.close()
    await server.wait_closed()
    event = asyncio.Event()

    data = list()

    async def handler(reader, writer):
        nonlocal data
        while not reader.at_eof():
            try:
                header = await reader.readexactly(4)
            except asyncio.IncompleteReadError:
                break

            chunk_size = struct.unpack("!L", header)[0]

            try:
                payload = await reader.readexactly(chunk_size)
            except asyncio.IncompleteReadError:
                break

            for metric in pickle.loads(payload):
                data.append(metric)

        event.set()
        writer.close()
        reader.feed_eof()

    server = await asyncio.start_server(
        handler, '127.0.0.1', unused_tcp_port, loop=event_loop
    )

    await client.send()
    await asyncio.sleep(0.1)
    await event.wait()

    assert len(data) == count

    for idx, metric in enumerate(sorted(data, key=lambda x: x[1][1])):
        name, payload = metric
        ts, value = payload

        assert name == 'foo'
        assert value == idx

    server.close()
    await server.wait_closed()

