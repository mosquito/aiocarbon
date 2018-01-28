import asyncio
import aiocarbon


async def send(loop, client_class, port):
    client = aiocarbon.setup(
        host="127.0.0.1", port=port, client_class=client_class
    )

    # starts sending data every second
    task = loop.create_task(client.run())

    for _ in range(100):
        with aiocarbon.Counter("foo"), aiocarbon.Timer("bar"):
            await asyncio.sleep(0.1)

    for _ in range(100):
        with aiocarbon.Counter("foo"), aiocarbon.Timer("bar"):
            pass

    # sends data from buffer before exit
    await client.send()

    task.cancel()
    await asyncio.wait([task])


async def main(loop):
    await send(loop, aiocarbon.TCPClient, 2003)
    await send(loop, aiocarbon.UDPClient, 2003)
    await send(loop, aiocarbon.PickleClient, 2004)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
