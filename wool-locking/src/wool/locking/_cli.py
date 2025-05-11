import logging
from functools import partial

import click

import wool
from wool.locking._pool import LockPool, LockPoolSession

DEFAULT_PORT = 48900


@wool.cli.group("lock-pool")
def lock_pool():
    pass


@lock_pool.command(cls=partial(wool.PoolCommand, default_port=DEFAULT_PORT))
def up(host, port, authkey):
    if not authkey:
        logging.warning("No authkey specified")
    workerpool = LockPool(
        address=(host, port),
        authkey=authkey,
        log_level=wool.__log_level__,
    )
    workerpool.start()
    workerpool.join()


@lock_pool.command(cls=partial(wool.PoolCommand, default_port=DEFAULT_PORT))
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
    with LockPoolSession(address=(host, port), authkey=authkey) as client:
        client.stop(wait=wait)
