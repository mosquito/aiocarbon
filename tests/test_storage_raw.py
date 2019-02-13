import asyncio
import pytest

from aiocarbon.metric import Metric


pytestmark = pytest.mark.asyncio


async def test_send_metrics_with_same_ts(tcp_server, tcp_client, timestamp):
    await asyncio.sleep(0)
