import asyncio
import datetime
import ipaddress
import os
import sys
import tempfile
import uuid
from collections.abc import Callable
from collections.abc import Coroutine
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from typing import NamedTuple

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

import wool

#: Tasks scheduled through `plant`, which owns their retention.
planted_tasks: list[asyncio.Task] = []

#: The running loop `record_peer_loop` last published.
peer_loop: asyncio.AbstractEventLoop | None = None

#: SANs covering the loopback addresses test workers bind to.
LOOPBACK_SANS = (
    x509.DNSName("localhost"),
    x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
)


class CertificateMaterial(NamedTuple):
    """PEM bytes for one certificate set, held in memory."""

    ca_pem: bytes
    key_pem: bytes
    cert_pem: bytes


class CertificateAuthority(NamedTuple):
    """A reusable signing authority, so several leaves share one bundle."""

    ca_pem: bytes
    key: object
    certificate: object


class CertificateFiles(NamedTuple):
    """Paths and PEM bytes for one certificate set written to disk."""

    ca_path: str
    key_path: str
    cert_path: str
    ca_pem: bytes
    key_pem: bytes
    cert_pem: bytes


def namespace_directory(namespace: str) -> Path:
    """Return the directory a `wool.LocalDiscovery` namespace's files live in.

    Mirrors the module's own choice between the two candidate roots —
    ``/dev/shm`` where Linux provides it, the temporary directory
    otherwise — rather than reaching into the module to ask.
    """
    shm = Path("/dev/shm")
    if sys.platform.startswith("linux") and os.access(shm, os.W_OK | os.X_OK):
        return shm.resolve() / f"wool-{namespace}"
    return Path(tempfile.gettempdir()).resolve() / f"wool-{namespace}"


def plant(coro: Coroutine[Any, Any, Any]) -> asyncio.Task:
    """Schedule ``coro`` on the running loop and hold a strong reference.

    ``asyncio`` references tasks only weakly, so a fire-and-forget task
    can be collected while still pending. The reference lives in
    `planted_tasks` until the task has finished and a later call prunes
    it, so the registry stays bounded in a long-lived process.
    """
    planted_tasks[:] = [task for task in planted_tasks if not task.done()]
    task = asyncio.get_running_loop().create_task(coro)
    planted_tasks.append(task)
    return task


def record_peer_loop() -> asyncio.AbstractEventLoop:
    """Publish the running loop through `peer_loop` and return it."""
    global peer_loop

    peer_loop = asyncio.get_running_loop()
    return peer_loop


async def park_then_plant(*factories: Callable[[], Coroutine[Any, Any, Any]]) -> None:
    """Await until cancelled, then plant each factory's coroutine in
    order and re-raise.

    The coroutine plants on cancellation rather than from a ``finally``
    clause, because a ``finally`` clause also runs when garbage
    collection closes a coroutine that was never cancelled, e.g., one
    stranded on a loop that has since shut down: planting there would
    schedule onto a closed loop, and observing there would report a
    cancellation that never happened.
    """
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        for factory in factories:
            plant(factory())
        raise


async def cancelling_peer(
    sweeps: int | None = 1,
    *,
    orphan: Callable[[], Coroutine[Any, Any, Any]] | None = None,
) -> None:
    """Cancel every other task on the loop for ``sweeps`` iterations, or
    on every iteration until the loop stops when ``sweeps`` is ``None``.

    Publishes the running loop through `record_peer_loop` and survives
    its own cancellation between sweeps. With ``orphan`` set it first
    plants that factory's coroutine and spares it from every sweep, so
    only another task on the loop can cancel the orphan.
    """
    current = asyncio.current_task()
    assert current is not None
    record_peer_loop()
    spared = {current}
    if orphan is not None:
        spared.add(plant(orphan()))
    remaining = sweeps
    while remaining is None or remaining > 0:
        for task in asyncio.all_tasks():
            if task not in spared:
                task.cancel()
        if remaining is not None:
            remaining -= 1
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            current.uncancel()


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


@contextmanager
def scoped_context() -> Generator[None]:
    """Test helper — bracket a block of Wool chain mutations.

    Per-test isolation lives in the ``pytest_pyfunc_call`` hook in
    ``tests/conftest.py``, which runs each sync test in a fresh
    `contextvars.copy_context` (async tests self-isolate via their
    task's context copy). With ``__chain__`` typed
    `~wool.runtime.context.chain.Chain` there is no settable
    "unarmed" value to install in place, so this manager no longer
    disarms; it is retained as a no-op scope around chain mutations.
    """
    yield


def context_is_unarmed() -> bool:
    """Test helper — return whether the current context carries no Wool state.

    A module-level, picklable function so it can be dispatched to a
    `~concurrent.futures.ProcessPoolExecutor` worker, where it
    proves a bare ``run_in_executor`` offload carries no Wool chain
    into a worker process.
    """
    return wool.__chain__.get(None) is None


def _private_key():
    """Generate a fresh RSA private key."""
    return rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )


def generate_authority(common_name="wool-test-ca"):
    """Generate a certificate authority that can issue several leaves.

    Pass the result as ``authority`` to `generate_ca_and_leaf` or
    `generate_certificate_files` to issue peers that chain to one trust
    bundle while carrying distinct names — the arrangement needed to
    tell workers apart by identity rather than by authority.
    """
    now = datetime.datetime.now(datetime.UTC)
    key = _private_key()
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256(), default_backend())
    )
    return CertificateAuthority(
        ca_pem=certificate.public_bytes(serialization.Encoding.PEM),
        key=key,
        certificate=certificate,
    )


def generate_ca_and_leaf(
    sans, *, common_name="wool-worker", self_signed=False, authority=None
):
    """Generate a leaf certificate carrying ``sans``, and the CA behind it.

    The leaf is granted both server and client extended key usages so it
    works on both sides of a mutual-TLS connection. By default the leaf
    is signed by a freshly generated CA; with ``self_signed=True`` the
    leaf signs itself and doubles as its own trust root (the degenerate
    single-certificate case), so the returned CA PEM is the leaf PEM.
    Pass ``authority`` — a `CertificateAuthority` from
    `generate_authority` — to issue under an existing CA instead, so
    several leaves share one trust bundle.

    Returns a `CertificateMaterial`, so callers bind the three PEMs by
    name — all three are `bytes`, and a permuted positional unpack
    mis-wires silently rather than raising.
    """
    now = datetime.datetime.now(datetime.UTC)
    leaf_key = _private_key()
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    if self_signed:
        issuer = subject
        signing_key = leaf_key
        ca_cert = None
    else:
        authority = authority if authority is not None else generate_authority()
        signing_key = authority.key
        issuer = authority.certificate.subject
        ca_cert = authority.certificate

    leaf_cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
        .add_extension(x509.SubjectAlternativeName(list(sans)), critical=False)
        .add_extension(
            x509.ExtendedKeyUsage(
                [
                    x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
                    x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
                ]
            ),
            critical=False,
        )
        .sign(signing_key, hashes.SHA256(), default_backend())
    )

    leaf_pem = leaf_cert.public_bytes(serialization.Encoding.PEM)
    key_pem = leaf_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    ca_pem = (
        leaf_pem if ca_cert is None else ca_cert.public_bytes(serialization.Encoding.PEM)
    )
    return CertificateMaterial(ca_pem=ca_pem, key_pem=key_pem, cert_pem=leaf_pem)


def write_certificate_files(directory, ca_pem, key_pem, cert_pem):
    """Write the given PEM material as ``ca.pem``, ``key.pem``, ``cert.pem``.

    Writes under *directory*, overwriting any previous material — calling
    it again with the same directory is an in-place rotation. Returns a
    `CertificateFiles` carrying both the file paths and the PEM bytes.
    """
    ca_path = directory / "ca.pem"
    key_path = directory / "key.pem"
    cert_path = directory / "cert.pem"
    ca_path.write_bytes(ca_pem)
    key_path.write_bytes(key_pem)
    cert_path.write_bytes(cert_pem)
    return CertificateFiles(
        str(ca_path), str(key_path), str(cert_path), ca_pem, key_pem, cert_pem
    )


def generate_certificate_files(
    directory, sans, *, common_name="wool-worker", authority=None
):
    """Generate a CA and leaf for ``sans`` and write them as PEM files.

    The generating layer over `write_certificate_files`: SANs in, paths
    out. Returns the same `CertificateFiles` that writing pre-generated
    material returns. ``authority`` issues under an existing CA — see
    `generate_ca_and_leaf`.
    """
    material = generate_ca_and_leaf(sans, common_name=common_name, authority=authority)
    return write_certificate_files(
        directory, material.ca_pem, material.key_pem, material.cert_pem
    )


#: Loop iterations `await_new_task` steps before it gives up.
_NEW_TASK_TICKS = 200


async def await_new_task(*, exclude=(), since=None):
    """Step the running loop until a task outside a snapshot appears.

    Returns the set of tasks that were not pending in the snapshot and
    are not in *exclude*, observed before any of them has taken its first
    step. The snapshot is taken on entry unless *since* supplies one
    taken earlier, in which case the loop is checked before it is first
    stepped, so a task created between the snapshot and the call is
    still caught unstepped. Raises `AssertionError` if nothing appears
    within `_NEW_TASK_TICKS` iterations, so an arrangement that stopped
    spawning the task it waits for cannot pass vacuously.

    .. rubric:: Implementation notes

    A task's first ``__step`` is queued behind the callbacks already
    ready when it is created, so the tick on which it first shows up in
    `asyncio.all_tasks` is the tick before its body runs under CPython's
    asyncio ready queue.
    """
    current = asyncio.current_task()
    excluded = set(exclude) | {current}
    baseline = asyncio.all_tasks() if since is None else since
    for tick in range(_NEW_TASK_TICKS):
        if tick or since is None:
            await asyncio.sleep(0)
        spawned = asyncio.all_tasks() - baseline - excluded
        if spawned:
            return spawned
    raise AssertionError(f"no new task appeared within {_NEW_TASK_TICKS} loop ticks")


async def drain_loop_tasks(*, exclude=(), timeout=5.0):
    """Cancel and await every pending task on the running loop.

    Spares the caller's own task and those in *exclude*. Performs one
    generation of the cancel-every-task shape that
    `WorkerService._destroy_worker_loop` repeats until the loop is quiet.
    Raises `AssertionError` if any cancelled task is still pending after
    *timeout* seconds, so a task that swallows its cancellation fails the
    test rather than hanging it.
    """
    current = asyncio.current_task()
    excluded = set(exclude) | {current}
    victims = [task for task in asyncio.all_tasks() if task not in excluded]
    if not victims:
        return
    for task in victims:
        task.cancel()
    _, pending = await asyncio.wait(victims, timeout=timeout)
    if pending:
        raise AssertionError(f"{len(pending)} task(s) still pending after the drain")
