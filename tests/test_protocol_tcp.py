import asyncio
import time

import pytest

from aiocarbon.metric import Metric
from aiocarbon.protocol.tcp import TCPClient

from .conftest import Server


async def test_tcp_simple(tcp_client: TCPClient, tcp_server: Server) -> None:
    metric = Metric(name="foo", value=42)
    tcp_client.add(metric)

    await tcp_server.wait_data()

    name, value, ts = tcp_server.data.decode().strip().split(" ")

    assert name == metric.name
    assert int(float(value)) == int(metric.value)
    assert int(float(ts)) == int(metric.timestamp)


async def test_tcp_many(tcp_client: TCPClient, tcp_server: Server) -> None:
    now = int(time.time()) - 86400

    for i in range(199):
        tcp_client.add(Metric(name="foo", value=42, timestamp=now + i))

    await tcp_server.wait_data()

    for i in range(199):
        tcp_client.add(Metric(name="foo", value=42, timestamp=now - i))

    await tcp_server.wait_data()

    lines = list(filter(None, tcp_server.data.split(b"\n")))

    assert len(lines) == 398

    for line in lines:
        name, value, ts = line.decode().strip().split(" ")

        assert name == "foo"
        assert int(float(value)) == 42


async def test_tcp_reconnect(unused_tcp_port: int) -> None:
    async def reject(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        await reader.read(10)
        writer.close()

    server = await asyncio.start_server(reject, "127.0.0.1", unused_tcp_port)

    client = TCPClient("127.0.0.1", port=unused_tcp_port, namespace="")
    count = 19907
    now = time.time() - 1

    for i in range(count):
        client.add(Metric(name="foo", value=i, timestamp=now - i))

    server.close()
    await server.wait_closed()

    with pytest.raises(ConnectionError):
        await client.send()

    event = asyncio.Event()
    data = b""

    async def collect(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        nonlocal data

        try:
            while not reader.at_eof():
                chunk = await reader.read(4096)

                if not chunk:
                    break

                data += chunk

            event.set()
        finally:
            # Python 3.12 waits for every connection in wait_closed().
            writer.close()

    server = await asyncio.start_server(collect, "127.0.0.1", unused_tcp_port)

    await client.send()
    await event.wait()

    server.close()
    await server.wait_closed()

    lines = sorted(
        (line.decode().split(" ") for line in data.split(b"\n") if line),
        key=lambda parts: float(parts[1]),
    )

    assert len(lines) == count

    for idx, parts in enumerate(lines):
        assert parts[0] == "foo"
        assert float(parts[1]) == idx
