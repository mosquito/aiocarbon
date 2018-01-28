import asyncio
import pickle
import struct


async def tcp_handler(reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter):

    addr = writer.get_extra_info('peername')
    while not reader.at_eof():
        try:
            line = await reader.readline()
            if line:
                print("%s:%d" % addr, line.decode().strip())
        except:
            break


async def pickle_handler(reader: asyncio.StreamReader,
                         writer: asyncio.StreamWriter):
    addr = writer.get_extra_info('peername')
    while not reader.at_eof():
        try:
            header = await reader.readexactly(4)
            size = struct.unpack("!L", header)[0]

            packet = pickle.loads(await reader.readexactly(size))

            for name, payload in packet:
                ts, value = payload
                print("%s:%d" % addr, "%s %f %s" % (name, value, ts))

        except asyncio.IncompleteReadError:
            return


class UDPServerProtocol(asyncio.DatagramProtocol):

    def __init__(self):
        self.queue = asyncio.Queue()
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        for line in data.split(b'\n'):
            if not line:
                continue

            print("%s:%s" % addr,  line.decode())

    def close(self):
        self.queue.put_nowait(None)


async def main(loop: asyncio.AbstractEventLoop):
    await asyncio.start_server(
        tcp_handler, '127.0.0.1', 2003, loop=loop
    )

    await asyncio.start_server(
        pickle_handler, '127.0.0.1', 2004, loop=loop
    )

    await loop.create_datagram_endpoint(
        UDPServerProtocol, ('127.0.0.1', 2003)
    )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.run_forever()
