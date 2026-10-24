import asyncio
import time

from aiocarbon.metric import Metric
from aiocarbon.protocol.udp import UDPClient
from aiocarbon.storage import TotalStorage


class UDPServerProtocol(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        super().__init__()

    def datagram_received(self, data: bytes, addr: object) -> None:
        self.queue.put_nowait(data)

    def close(self) -> None:
        self.queue.put_nowait(None)


async def test_udp_simple(unused_udp_port: int) -> None:
    protocol = UDPServerProtocol()
    await asyncio.get_running_loop().create_datagram_endpoint(
        lambda: protocol, local_addr=("127.0.0.1", unused_udp_port),
    )

    client = UDPClient(
        "127.0.0.1", port=unused_udp_port, namespace="",
        storage=TotalStorage(),
    )

    task = asyncio.get_running_loop().create_task(client.run())

    client.add(Metric(name="foo", value=42, timestamp=time.time()))

    data = await protocol.queue.get()
    assert data is not None

    lines = [line.decode() for line in data.split(b"\n") if line]

    assert len(lines) == 1

    for line in lines:
        name, value, ts = line.split(" ")

        assert name == "foo"
        assert float(value) == 42
        assert float(ts) < time.time()

    task.cancel()
    await asyncio.wait([task])


async def test_udp_many(unused_udp_port: int) -> None:
    count = 99991
    protocol = UDPServerProtocol()
    await asyncio.get_running_loop().create_datagram_endpoint(
        lambda: protocol, local_addr=("127.0.0.1", unused_udp_port),
    )

    client = UDPClient(
        "127.0.0.1", port=unused_udp_port, namespace="",
        storage=TotalStorage(),
    )
    now = time.time() - 5

    for i in range(count):
        client.add(Metric(name="foo", value=i, timestamp=now - i))

    await client.send()
    await asyncio.sleep(0.1)

    protocol.close()

    data = b""
    chunk = await protocol.queue.get()

    while chunk is not None:
        data += chunk
        chunk = await protocol.queue.get()

    lines = [line.decode() for line in data.split(b"\n") if line]

    assert len(lines) == count

    parsed = sorted(
        (line.split(" ") for line in lines),
        key=lambda parts: float(parts[1]),
    )

    for idx, (name, value, ts) in enumerate(parsed):
        assert name == "foo"
        assert float(value) == idx
        assert float(ts) < time.time()
