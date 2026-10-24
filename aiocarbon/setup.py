from .context import set_client
from .protocol import PickleClient
from .protocol.base import BaseClient


def setup(
    host: str,
    port: int = 2003,
    client_class: type[BaseClient] = PickleClient,
    namespace: str = "carbon",
) -> BaseClient:
    """Create a carbon client, and make it the client of this thread.

    The caller must run the returned client with asyncio, for example
    ``asyncio.create_task(client.run())``.
    """
    client = client_class(host=host, port=port, namespace=namespace)
    set_client(client)

    return client


__all__ = ("setup",)
