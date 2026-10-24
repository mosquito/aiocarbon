from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("aiocarbon")
except PackageNotFoundError:
    # The package is not installed. Tests and source checkouts use this value.
    __version__ = "0.0.0"

version_info: tuple[int, ...] = tuple(
    int(part) for part in __version__.split(".") if part.isdigit()
)


__all__ = ("__version__", "version_info")
