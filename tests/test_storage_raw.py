import asyncio

from aiocarbon.metric import Metric
from aiocarbon.protocol.tcp import TCPClient
from aiocarbon.storage import RawStorage

from .conftest import Server


async def test_send_metrics_with_same_ts(
    tcp_server: Server, tcp_client: TCPClient, timestamp: int,
) -> None:
    tcp_client.storage = RawStorage()
    metrics = [
        Metric(name="foo", value=44, timestamp=timestamp - 1),
        Metric(name="foo", value=11, timestamp=timestamp),
        Metric(name="foo", value=7, timestamp=timestamp),
        Metric(name="foo", value=24, timestamp=timestamp),
    ]

    for metric in metrics:
        tcp_client.add(metric)

    await asyncio.sleep(2)
    await tcp_server.wait_data()

    lines = list(filter(None, tcp_server.data.split(b"\n")))
    assert len(lines) == 4

    for metric in metrics:
        name, value, ts = lines.pop(0).decode().strip().split(" ")

        assert name == metric.name
        assert int(float(value)) == metric.value
        assert int(ts) == metric.timestamp
