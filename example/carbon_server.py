import asyncio
import pickle
import struct
from contextlib import suppress

HEADER_SIZE = struct.calcsize("!L")


async def tcp_handler(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
) -> None:
    host, port = writer.get_extra_info("peername")[:2]

    while not reader.at_eof():
        line = await reader.readline()

        if line:
            print(f"{host}:{port}", line.decode().strip())


async def pickle_handler(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
) -> None:
    host, port = writer.get_extra_info("peername")[:2]

    with suppress(asyncio.IncompleteReadError):
        while not reader.at_eof():
            header = await reader.readexactly(HEADER_SIZE)
            size = struct.unpack("!L", header)[0]

            for name, payload in pickle.loads(
                await reader.readexactly(size),
            ):
                timestamp, value = payload
                print(f"{host}:{port}", f"{name} {value:f} {timestamp}")


class UDPServerProtocol(asyncio.DatagramProtocol):
    def datagram_received(self, data: bytes, addr: object) -> None:
        for line in data.split(b"\n"):
            if line:
                print(addr, line.decode())


async def main() -> None:
    loop = asyncio.get_running_loop()

    await asyncio.start_server(tcp_handler, "127.0.0.1", 2003)
    await asyncio.start_server(pickle_handler, "127.0.0.1", 2004)
    await loop.create_datagram_endpoint(
        UDPServerProtocol, ("127.0.0.1", 2003),
    )

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
