import pytest

from aiocarbon.metric import Metric


pytestmark = pytest.mark.asyncio


async def test_sum_metrics_with_the_same_ts(tcp_server, tcp_client, timestamp):

    for val in [11, 7, 24]:
        metric = Metric(name='foo', value=val, timestamp=timestamp)
        tcp_client.add(metric)

    await tcp_server.wait_data()

    lines = list(filter(None, tcp_server.data.split(b"\n")))
    assert len(lines) == 1
    line = lines[0]
    name, value, ts = line.decode().strip().split(" ")

    assert name == 'foo'
    assert int(value) == 42
    assert int(ts) == timestamp
