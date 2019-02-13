import pytest

from aiocarbon.metric import Metric
from aiocarbon.storage import RawStorage


pytestmark = pytest.mark.asyncio


async def test_send_metrics_with_same_ts(tcp_server, tcp_client, timestamp):
    tcp_client._storage = RawStorage()

    for val in [11, 7, 24]:
        metric = Metric(name='foo', value=val, timestamp=timestamp)
        tcp_client.add(metric)

    await tcp_server.wait_data()

    lines = list(filter(None, tcp_server.data.split(b"\n")))
    assert len(lines) == 3

    for val in [24, 7, 11]:
        line = lines.pop()
        name, value, ts = line.decode().strip().split(" ")

        assert name == 'foo'
        assert int(value) == val
        assert int(ts) == timestamp
