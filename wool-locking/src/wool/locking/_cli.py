import logging
from functools import partial

import click

import wool
from wool.locking._pool import LockPool
from wool.locking._pool import LockPoolSession

DEFAULT_PORT = 48900


@wool.cli.group("lock-pool")
def lock_pool():
    """
    CLI command group for managing lock pools.
    """
    pass


@lock_pool.command(
    cls=partial(wool.WorkerPoolCommand, default_port=DEFAULT_PORT)
)
def up(host, port, authkey):
    """
    Start a lock pool with the specified configuration.

    :param host: The host address for the lock pool.
    :param port: The port number for the lock pool.
    :param authkey: The authentication key for the lock pool.
    """
    if not authkey:
        logging.warning("No authkey specified")
    workerpool = LockPool(
        address=(host, port),
        authkey=authkey,
        log_level=wool.__log_level__,
    )
    workerpool.start()
    workerpool.join()


@lock_pool.command(
    cls=partial(wool.WorkerPoolCommand, default_port=DEFAULT_PORT)
)
@click.option(
    "--wait",
    "-w",
    is_flag=True,
    default=False,
    help="Wait for in-flight tasks to complete before shutting down.",
)
def down(host, port, authkey, wait):
    """
    Shut down the lock pool.

    :param host: The host address of the lock pool.
    :param port: The port number of the lock pool.
    :param authkey: The authentication key for the lock pool.
    :param wait: Whether to wait for in-flight tasks to complete.
    """
    assert port
    if not host:
        host = "localhost"
    if not authkey:
        authkey = b""
    with LockPoolSession(address=(host, port), authkey=authkey) as client:
        client.stop(wait=wait)
