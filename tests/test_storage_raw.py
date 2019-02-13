import asyncio
import pytest

from aiocarbon.metric import Metric
from aiocarbon.storage import RawStorage


pytestmark = pytest.mark.asyncio


async def test_send_metrics_with_same_ts(tcp_server, tcp_client, timestamp):
    tcp_client._storage = RawStorage()
    metrics = [
        Metric(name='foo', value=44, timestamp=timestamp-1),
        Metric(name='foo', value=11, timestamp=timestamp),
        Metric(name='foo', value=7, timestamp=timestamp),
        Metric(name='foo', value=24, timestamp=timestamp)]

    for metric in metrics:
        tcp_client.add(metric)

    await tcp_server.wait_data()

    lines = list(filter(None, tcp_server.data.split(b"\n")))
    assert len(lines) == 4

    for metric in metrics:
        line = lines.pop(0)
        name, value, ts = line.decode().strip().split(" ")

        assert name == metric.name
        assert int(value) == metric.value
        assert int(ts) == metric.timestamp
