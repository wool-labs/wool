import asyncio
import importlib
import logging
from contextlib import contextmanager
from functools import partial
from multiprocessing import cpu_count
from time import perf_counter

import click

import wool
from wool._pool import WorkerPool
from wool._session import WorkerPoolSession
from wool._task import task

DEFAULT_PORT = 48800


# PUBLIC
class WorkerPoolCommand(click.core.Command):
    """
    Custom Click command class for worker pool commands.

    :param default_host: Default host address.
    :param default_port: Default port number.
    :param default_authkey: Default authentication key.
    """

    def __init__(
        self,
        *args,
        default_host="localhost",
        default_port=0,
        default_authkey=b"",
        **kwargs,
    ):
        params = kwargs.pop("params", [])
        params = [
            click.Option(["--host", "-h"], type=str, default=default_host),
            click.Option(["--port", "-p"], type=int, default=default_port),
            click.Option(
                ["--authkey", "-a"],
                type=str,
                default=default_authkey,
                callback=to_bytes,
            ),
            *params,
        ]
        super().__init__(*args, params=params, **kwargs)


@contextmanager
def timer():
    """
    Context manager to measure the execution time of a code block.

    :return: A function to retrieve the elapsed time.
    """
    start = end = perf_counter()
    yield lambda: end - start
    end = perf_counter()


def to_bytes(context: click.Context, parameter: click.Parameter, value: str):
    """
    Convert the given value to bytes.

    :param context: Click context.
    :param parameter: Click parameter.
    :param value: Value to convert.
    :return: The converted value in bytes.
    """
    if value is None:
        return b""
    return value.encode("utf-8")


def assert_nonzero(
    context: click.Context, parameter: click.Parameter, value: int
) -> int:
    """
    Assert that the given value is non-zero.

    :param context: Click context.
    :param parameter: Click parameter.
    :param value: Value to check.
    :return: The original value if it is non-zero.
    """
    if value is None:
        return value
    assert value >= 0
    return value


def debug(ctx, param, value):
    """
    Enable debugging mode with a specified port.

    :param ctx: The Click context object.
    :param param: The parameter being handled.
    :param value: The port number for the debugger.
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
    help=(
        "Run with debugger listening on the specified port. Execution will "
        "block until the debugger is attached."
    ),
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

    :param verbosity: Verbosity level for logging.
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
def pool():
    """
    CLI command group for managing worker pools.
    """
    pass


@pool.command(cls=partial(WorkerPoolCommand, default_port=DEFAULT_PORT))
@click.option(
    "--breadth", "-b", type=int, default=cpu_count(), callback=assert_nonzero
)
@click.option(
    "modules",
    "--module",
    "-m",
    multiple=True,
    type=str,
    help=(
        "Python module containing workerpool task definitions to be executed "
        "by this pool."
    ),
)
def up(host, port, authkey, breadth, modules):
    """
    Start a worker pool with the specified configuration.

    :param host: The host address for the worker pool.
    :param port: The port number for the worker pool.
    :param authkey: The authentication key for the worker pool.
    :param breadth: The number of worker processes in the pool.
    :param modules: Python modules containing task definitions.
    """
    for module in modules:
        importlib.import_module(module)
    if not authkey:
        logging.warning("No authkey specified")
    workerpool = WorkerPool(
        address=(host, port),
        breadth=breadth,
        authkey=authkey,
        log_level=wool.__log_level__,
    )
    workerpool.start()
    workerpool.join()


@pool.command(cls=partial(WorkerPoolCommand, default_port=DEFAULT_PORT))
@click.option(
    "--wait",
    "-w",
    is_flag=True,
    default=False,
    help="Wait for in-flight tasks to complete before shutting down.",
)
def down(host, port, authkey, wait):
    """
    Shut down the worker pool.

    :param host: The host address of the worker pool.
    :param port: The port number of the worker pool.
    :param authkey: The authentication key for the worker pool.
    :param wait: Whether to wait for in-flight tasks to complete.
    """
    assert port
    if not host:
        host = "localhost"
    if not authkey:
        authkey = b""
    with WorkerPoolSession(address=(host, port), authkey=authkey) as client:
        client.stop(wait=wait)


@cli.command(cls=partial(WorkerPoolCommand, default_port=DEFAULT_PORT))
def ping(host, port, authkey):
    """
    Ping the worker pool to check connectivity.

    :param host: The host address of the worker pool.
    :param port: The port number of the worker pool.
    :param authkey: The authentication key for the worker pool.
    """
    assert port
    if not host:
        host = "localhost"
    if not authkey:
        authkey = b""

    async def _():
        with timer() as t:
            await _ping()

        print(f"Ping: {int((t() * 1000) + 0.5)} ms")

    with WorkerPoolSession(address=(host, port), authkey=authkey):
        asyncio.get_event_loop().run_until_complete(_())


@task
async def _ping():
    """
    Asynchronous task to log a ping message.

    :return: None
    """
    logging.debug("Ping!")
