import abc
import asyncio
import logging
import re
from typing import Any

from aiocarbon.metric import Metric
from aiocarbon.storage import BaseStorage, Operation, TotalStorage

log = logging.getLogger(__name__)

CARBON_NS_INVALID_CHARS = re.compile(r"([^\w\d\.\-]+|(\.){2,})")

DEFAULT_NAMESPACE = "aiocarbon"


def strip_carbon_ns(string: str) -> str:
    """Replace the characters which carbon does not accept in a name."""
    return CARBON_NS_INVALID_CHARS.sub("_", string).strip("_").lower()


class BaseClient(abc.ABC):
    SEND_PERIOD = 1

    def __init__(
        self,
        host: str,
        port: int = 2003,
        storage: BaseStorage[Any] | None = None,
        namespace: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.storage: BaseStorage[Any] = storage or TotalStorage()
        self.lock = asyncio.Lock()
        self.namespace = strip_carbon_ns(
            DEFAULT_NAMESPACE if namespace is None else namespace,
        )

    def set_namespace(self, namespace: str) -> None:
        """Set the metric name prefix. Invalid characters are replaced."""
        self.namespace = strip_carbon_ns(namespace)

    async def run(self) -> None:
        """Send the collected metrics every SEND_PERIOD seconds."""
        while True:
            try:
                await self.send()
            except Exception:
                log.exception("Exception while sending metrics")
            finally:
                await asyncio.sleep(self.SEND_PERIOD)

    def format_metric_name(self, metric: Metric) -> str:
        if self.namespace:
            return ".".join((self.namespace, metric.name))
        return metric.name

    @abc.abstractmethod
    async def send(self) -> None:
        """Send all the metrics which are ready."""
        raise NotImplementedError

    @abc.abstractmethod
    def format_metric(self, metric: Metric) -> Any:
        """Convert one metric to the wire format of this protocol."""
        raise NotImplementedError

    def add(self, metric: Metric, operation: Operation | None = None) -> None:
        self.storage.add(metric, operation)


__all__ = (
    "DEFAULT_NAMESPACE",
    "BaseClient",
    "strip_carbon_ns",
)
