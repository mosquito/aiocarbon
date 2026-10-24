import asyncio

import aiocarbon
from aiocarbon.protocol.base import BaseClient


async def send(client_class: type[BaseClient], port: int) -> None:
    client = aiocarbon.setup(
        host="127.0.0.1", port=port, client_class=client_class,
    )

    # Send the collected data every second.
    task = asyncio.get_running_loop().create_task(client.run())

    for _ in range(100):
        with aiocarbon.Counter("foo"), aiocarbon.Timer("bar"):
            await asyncio.sleep(0.1)

    for _ in range(100):
        with aiocarbon.Counter("foo"), aiocarbon.Timer("bar"):
            pass

    # Send the rest of the data before the exit.
    await client.send()

    task.cancel()
    await asyncio.wait([task])


async def main() -> None:
    await send(aiocarbon.TCPClient, 2003)
    await send(aiocarbon.UDPClient, 2003)
    await send(aiocarbon.PickleClient, 2004)


if __name__ == "__main__":
    asyncio.run(main())
