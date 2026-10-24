import time
from typing import NamedTuple


class MetricBase(NamedTuple):
    name: str
    value: float
    timestamp: int


class Metric(MetricBase):
    """One metric sample. The default timestamp is the current time."""

    __slots__ = ()

    def __new__(
        cls, name: str, value: float, timestamp: float | None = None,
    ) -> "Metric":
        if not isinstance(value, int | float):
            raise TypeError(f"Value must be int or float, not {type(value)!r}")

        return super().__new__(cls, name, value, int(timestamp or time.time()))

    def __str__(self) -> str:
        return f"{self.name} {self.value} {self.timestamp}"

    def __repr__(self) -> str:
        return f"<Metric: {self.name} {self.value} {self.timestamp}>"


__all__ = ("Metric", "MetricBase")
