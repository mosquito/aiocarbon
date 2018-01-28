import asyncio
import socket
from contextlib import closing

import pytest


@pytest.fixture()
def event_loop():
    asyncio.get_event_loop().close()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture()
def random_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        return s.getsockname()[1]
