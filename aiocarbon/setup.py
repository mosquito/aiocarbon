import asyncio
from typing import Type

from .protocol.base import BaseClient
from .protocol import PickleClient
from .context import set_client


def setup(host: str, port: int, client_class: Type[BaseClient]=PickleClient,
          namespace: str="carbon", loop: asyncio.AbstractEventLoop=None):

    loop = loop or asyncio.get_event_loop()
    client = client_class(host=host, port=port, namespace=namespace, loop=loop)
    set_client(client)

    return client
