aiocarbon
=========

.. image:: https://github.com/mosquito/aiocarbon/actions/workflows/tests.yml/badge.svg
    :target: https://github.com/mosquito/aiocarbon/actions/workflows/tests.yml
    :alt: Tests

.. image:: https://coveralls.io/repos/github/mosquito/aiocarbon/badge.svg?branch=master
    :target: https://coveralls.io/github/mosquito/aiocarbon
    :alt: Coveralls

.. image:: https://img.shields.io/pypi/v/aiocarbon.svg
    :target: https://pypi.python.org/pypi/aiocarbon/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/wheel/aiocarbon.svg
    :target: https://pypi.python.org/pypi/aiocarbon/

.. image:: https://img.shields.io/pypi/pyversions/aiocarbon.svg
    :target: https://pypi.python.org/pypi/aiocarbon/

.. image:: https://img.shields.io/pypi/l/aiocarbon.svg
    :target: https://pypi.python.org/pypi/aiocarbon/

Client for feeding data to graphite. The package requires Python 3.11 or
newer, and has no runtime dependencies.

Installation
------------

.. code-block:: shell

    pip install aiocarbon

Example
-------

Counter example:

.. code-block:: python

    import asyncio

    import aiocarbon


    async def main():
        client = aiocarbon.setup(
            host="127.0.0.1", port=2003, client_class=aiocarbon.TCPClient
        )

        # Send the collected metrics every second.
        sender = asyncio.create_task(client.run())

        for _ in range(1000):
            with aiocarbon.Counter("foo"):
                await asyncio.sleep(0.1)

        # Send the rest of the metrics before the exit.
        await client.send()

        sender.cancel()


    if __name__ == "__main__":
        asyncio.run(main())

Clients
-------

* ``aiocarbon.TCPClient`` sends metrics in the plaintext protocol over TCP.
* ``aiocarbon.UDPClient`` sends metrics in the plaintext protocol over UDP.
* ``aiocarbon.PickleClient`` sends metrics in the pickle protocol, port 2004.

Each client aggregates the metrics in a storage. ``TotalStorage`` is the
default one, and it adds up the values of one metric in one second.
``RawStorage`` keeps every sample.

Development
-----------

The project uses `uv`_ for the environment, and `ruff`_, `mypy`_ and
`pytest`_ for the checks:

.. code-block:: shell

    uv sync
    uv run ruff check .
    uv run mypy
    uv run pytest

.. _uv: https://docs.astral.sh/uv/
.. _ruff: https://docs.astral.sh/ruff/
.. _mypy: https://mypy-lang.org/
.. _pytest: https://docs.pytest.org/
