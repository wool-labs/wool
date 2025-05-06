import asyncio
import importlib
import logging
from contextlib import contextmanager
from multiprocessing import cpu_count
from time import perf_counter

import click

import wool
from wool._client import WoolClient
from wool._pool import WoolPool
from wool._task import ping as _ping


class WorkerPoolCommand(click.core.Command):
    def __init__(self, *args, **kwargs):
        params = kwargs.pop("params", [])
        params = [
            click.Option(["--host", "-h"], type=str),
            click.Option(["--port", "-p"], type=int),
            click.Option(["--authkey", "-a"], type=str, callback=to_bytes),
            *params,
        ]
        super().__init__(*args, params=params, **kwargs)


@contextmanager
def timer():
    """Context manager to measure the execution time of a code block."""
    start = end = perf_counter()
    yield lambda: end - start
    end = perf_counter()


def to_bytes(context: click.Context, parameter: click.Parameter, value: str):
    """
    Convert the given value to bytes.

    Args:
        context (click.Context): Click context.
        parameter (click.Parameter): Click parameter.
        value (str): Value to convert.

    Returns:
        bytes: The converted value in bytes.
    """
    if value is None:
        return value
    return value.encode("utf-8")


def assert_nonzero(
    context: click.Context, parameter: click.Parameter, value: int
) -> int:
    """
    Assert that the given value is non-zero.

    Args:
        context (click.Context): Click context.
        parameter (click.Parameter): Click parameter.
        value (int): Value to check.

    Returns:
        int: The original value if it is non-zero.
    """
    if value is None:
        return value
    assert value >= 0
    return value


def debug(ctx, param, value):
    """
    Enable debugging with debugpy.

    Args:
        ctx (click.Context): Click context.
        param (click.Parameter): Click parameter.
        value (bool): Flag value indicating whether to enable debugging.
    """
    if not value or ctx.resilient_parsing:
        return

    import debugpy

    debugpy.listen(5678)
    click.echo("Waiting for debugger to attach...")
    debugpy.wait_for_client()
    click.echo("Debugger attached")


@click.group()
@click.option(
    "--debug",
    "-d",
    callback=debug,
    expose_value=False,
    help="Run with debugger listening on the specified port. Execution will block until the debugger is attached.",
    is_eager=True,
    type=int,
)
@click.option(
    "--verbosity",
    "-v",
    count=True,
    default=3,
    help="Verbosity level for logging.",
    type=int,
)
def cli(verbosity: int):
    """
    CLI command group with options for verbosity, debugging, and version.

    Args:
        verbosity (int): Verbosity level for logging.
        debug (bool): Flag to enable debugging.
        version (bool): Flag to display the version and exit.
    """
    match verbosity:
        case 4:
            wool.__log_level__ = logging.DEBUG
        case 3:
            wool.__log_level__ = logging.INFO
        case 2:
            wool.__log_level__ = logging.WARNING
        case 1:
            wool.__log_level__ = logging.ERROR

    logging.getLogger().setLevel(wool.__log_level__)
    logging.info(
        f"Set log level to {logging.getLevelName(wool.__log_level__)}"
    )


@cli.group()
def pool(): ...


@pool.command(cls=WorkerPoolCommand)
@click.option(
    "--breadth", "-b", type=int, default=cpu_count(), callback=assert_nonzero
)
@click.option(
    "modules",
    "--module",
    "-m",
    multiple=True,
    type=str,
    help="Python module containing workerpool task definitions to be executed by this pool.",
)
def up(host, port, authkey, breadth, modules):
    for module in modules:
        importlib.import_module(module)
    if not host:
        host = "localhost"
    else:
        raise NotImplementedError("Only localhost is supported for now")
    if not port:
        port = 0
    if not authkey:
        logging.warning("No authkey specified")
        authkey = b""
    workerpool = WoolPool(
        address=(host, port),
        breadth=breadth,
        authkey=authkey,
        log_level=wool.__log_level__,
    )
    workerpool.start()
    workerpool.join()


@pool.command(cls=WorkerPoolCommand)
@click.option(
    "--wait",
    "-w",
    is_flag=True,
    default=False,
    help="Wait for in-flight tasks to complete before shutting down.",
)
def down(host, port, authkey, wait):
    assert port
    if not host:
        host = "localhost"
    if not authkey:
        authkey = b""
    client = WoolClient(address=(host, port), authkey=authkey)
    client.stop(wait=wait)


@cli.command(cls=WorkerPoolCommand)
def ping(host, port, authkey):
    assert port
    if not host:
        host = "localhost"
    if not authkey:
        authkey = b""

    async def _():
        with timer() as t:
            await asyncio.create_task(_ping())

        print(f"Ping: {int((t() * 1000) + 0.5)} ms")

    with WoolClient(address=(host, port), authkey=authkey):
        asyncio.get_event_loop().run_until_complete(_())
