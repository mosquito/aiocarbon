import asyncio
import pickle
import struct
import time

from aiocarbon.metric import Metric
from aiocarbon.protocol.pickle import PickleClient, PickleMetric

HEADER_SIZE = struct.calcsize("!L")


class Collector:
    """Reads the pickle protocol of carbon, and keeps every metric."""

    def __init__(self, expected: int) -> None:
        self.expected = expected
        self.metrics: list[PickleMetric] = []
        self.event = asyncio.Event()

    async def handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        while not reader.at_eof():
            try:
                header = await reader.readexactly(HEADER_SIZE)
            except asyncio.IncompleteReadError:
                break

            payload_size = struct.unpack("!L", header)[0]

            try:
                payload = await reader.readexactly(payload_size)
            except asyncio.IncompleteReadError:
                break

            self.metrics.extend(pickle.loads(payload))

        if len(self.metrics) == self.expected:
            self.event.set()

        writer.close()


def assert_sorted_values(metrics: list[PickleMetric]) -> None:
    for idx, (name, payload) in enumerate(
        sorted(metrics, key=lambda metric: metric[1][1]),
    ):
        assert name == "foo"
        assert payload[1] == idx


async def test_pickle_many(unused_tcp_port: int) -> None:
    client = PickleClient("127.0.0.1", port=unused_tcp_port, namespace="")

    count = 9991
    now = time.time() - 1

    for i in range(count):
        client.add(Metric(name="foo", value=i, timestamp=now - i))

    collector = Collector(count)
    server = await asyncio.start_server(
        collector.handler, "127.0.0.1", unused_tcp_port,
    )

    await client.send()
    await collector.event.wait()

    assert_sorted_values(collector.metrics)

    server.close()
    await server.wait_closed()


async def test_pickle_reconnect(unused_tcp_port: int) -> None:
    async def reject(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
    ) -> None:
        await reader.read(10)
        writer.close()

    server = await asyncio.start_server(reject, "127.0.0.1", unused_tcp_port)

    client = PickleClient("127.0.0.1", port=unused_tcp_port, namespace="")

    count = 199991
    now = time.time() - 86400

    for i in range(count):
        client.add(Metric(name="foo", value=i, timestamp=now - i))

    server.close()
    await server.wait_closed()

    collector = Collector(count)
    server = await asyncio.start_server(
        collector.handler, "127.0.0.1", unused_tcp_port,
    )

    await client.send()
    await collector.event.wait()

    assert_sorted_values(collector.metrics)

    server.close()
    await server.wait_closed()
