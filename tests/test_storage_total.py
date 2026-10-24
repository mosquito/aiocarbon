import math

import pytest

from aiocarbon.metric import Metric
from aiocarbon.protocol.tcp import TCPClient
from aiocarbon.storage.base import Operation, Operations

from .conftest import Server


@pytest.mark.parametrize(
    "operation,expected_value", [
        (Operations.add, 42),
        (Operations.avg, 16),
        (Operations.min, 9),
        (Operations.max, 22),
    ],
)
async def test_sum_metrics_with_the_same_ts(
    tcp_server: Server,
    tcp_client: TCPClient,
    timestamp: int,
    operation: Operation,
    expected_value: float,
) -> None:
    for value in (11, 9, 22):
        tcp_client.add(
            Metric(name="foo", value=value, timestamp=timestamp),
            operation=operation,
        )

    await tcp_server.wait_data()

    lines = list(filter(None, tcp_server.data.split(b"\n")))
    assert len(lines) == 1

    name, value_text, ts = lines[0].decode().strip().split(" ")

    assert name == "foo"
    assert math.isclose(float(value_text), expected_value)
    assert int(ts) == timestamp
