import pytest

from aiocarbon import Meter, set_client


pytestmark = pytest.mark.asyncio


async def test_buffered_metrics_eventually_sent(tcp_server, tcp_client,
                                                timestamp):

    for val in [42, 33, 5]:
        Meter(name='init', value=val, timestamp=timestamp).send()

    set_client(tcp_client)

    await tcp_server.wait_data()

    lines = list(filter(None, tcp_server.data.split(b"\n")))
    assert len(lines) == 1
    line = lines[0]
    name, value, ts = line.decode().strip().split(" ")

    assert name == 'init'
    assert int(value) == 42 + 33 + 5
    assert int(ts) == timestamp
