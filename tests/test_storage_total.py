import math

import pytest

from aiocarbon.metric import Metric
from aiocarbon.storage.base import Operations

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    'operation,expected_value', [
        (Operations.add, 42),
        (Operations.avg, 16),
        (Operations.min, 9),
        (Operations.max, 22)
    ]
)
async def test_sum_metrics_with_the_same_ts(
    tcp_server, tcp_client, timestamp, operation, expected_value
):

    for val in [11, 9, 22]:
        metric = Metric(name='foo', value=val, timestamp=timestamp)
        tcp_client.add(metric, operation=operation)

    await tcp_server.wait_data()

    lines = list(filter(None, tcp_server.data.split(b"\n")))
    assert len(lines) == 1
    line = lines[0]
    name, value, ts = line.decode().strip().split(" ")

    assert name == 'foo'
    assert math.isclose(float(value), expected_value)
    assert int(ts) == timestamp
