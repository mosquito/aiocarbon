from aiocarbon import Meter, set_client
from aiocarbon.protocol.tcp import TCPClient

from .conftest import Server


async def test_buffered_metrics_eventually_sent(
    tcp_server: Server, tcp_client: TCPClient, timestamp: int,
) -> None:
    for value in (42, 33, 5):
        Meter(name="init", value=value, timestamp=timestamp).send()

    set_client(tcp_client)

    await tcp_server.wait_data()

    lines = list(filter(None, tcp_server.data.split(b"\n")))
    assert len(lines) == 1

    name, value_text, ts = lines[0].decode().strip().split(" ")

    assert name == "init"
    assert int(float(value_text)) == 42 + 33 + 5
    assert int(ts) == timestamp
