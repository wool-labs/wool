from __future__ import annotations

import hashlib
import logging
import os
import ssl
import threading
from collections.abc import Callable
from contextvars import ContextVar
from contextvars import Token
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Protocol
from typing import runtime_checkable

if TYPE_CHECKING:
    import grpc

_log = logging.getLogger(__name__)

_current: ContextVar[WorkerCredentials | CredentialsProviderLike | None] = ContextVar(
    "worker_credentials", default=None
)


@dataclass(frozen=True)
class WorkerCredentials:
    """Container for worker TLS/mTLS credentials.

    Manages certificate files and provides credentials for both server-side
    (accepting connections) and client-side (making connections) operations.
    Designed for peer-to-peer networks where workers act as both servers
    and clients.

    **Mutual TLS (recommended for worker pools):**

    .. code-block:: python

        creds = WorkerCredentials.from_files(
            ca_path="certs/ca-cert.pem",
            key_path="certs/worker-key.pem",
            cert_path="certs/worker-cert.pem",
            mutual=True,  # Default: both parties authenticate
        )

        worker = LocalWorker("my-pool", credentials=creds)

    **One-way TLS (client anonymous):**

    .. code-block:: python

        creds = WorkerCredentials.from_files(
            ca_path="certs/ca-cert.pem",
            key_path="certs/worker-key.pem",
            cert_path="certs/worker-cert.pem",
            mutual=False,  # Server authenticated, client anonymous
        )

    :param ca_cert:
        PEM-encoded CA certificate for verifying peers.
    :param worker_key:
        PEM-encoded private key for this worker.
    :param worker_cert:
        PEM-encoded certificate for this worker.
    :param mutual:
        Whether to use mutual TLS (mTLS). If True (default), both server
        and client authenticate. If False, only server is authenticated.
    """

    ca_cert: bytes
    worker_key: bytes
    worker_cert: bytes
    mutual: bool = True

    @classmethod
    def from_files(
        cls,
        ca_path: str,
        key_path: str,
        cert_path: str,
        mutual: bool = True,
    ) -> WorkerCredentials:
        """Load credentials from PEM files.

        Reads certificate files from the filesystem and creates a
        WorkerCredentials instance. All files must be PEM-encoded.

        :param ca_path:
            Path to CA certificate file.
        :param key_path:
            Path to worker private key file.
        :param cert_path:
            Path to worker certificate file.
        :param mutual:
            Whether to use mutual TLS (mTLS). If True (default), both
            server and client authenticate. If False, only server is
            authenticated and clients remain anonymous at the transport
            layer.
        :returns:
            WorkerCredentials instance with loaded certificates.
        :raises FileNotFoundError:
            If any certificate file doesn't exist.
        :raises OSError:
            If any file cannot be read.
        """
        with open(ca_path, "rb") as f:
            ca_cert = f.read()
        with open(key_path, "rb") as f:
            worker_key = f.read()
        with open(cert_path, "rb") as f:
            worker_cert = f.read()

        return cls(
            ca_cert=ca_cert,
            worker_key=worker_key,
            worker_cert=worker_cert,
            mutual=mutual,
        )

    @classmethod
    def provider_from_files(
        cls,
        ca_path: str,
        key_path: str,
        cert_path: str,
        *,
        mutual: bool = True,
        identity: str | None = None,
        reload: bool = False,
    ) -> CredentialsProviderLike:
        """Build a credential provider backed by PEM files.

        A provider is the unit that worker pools, proxies, and worker
        processes consult to obtain the *current* credential material.
        This is the entry point for the two behaviours layered on top of
        the base `WorkerCredentials` model:

        - **Identity-based verification.** Pass ``identity`` to verify a
          discovered worker's server certificate against a stable logical
          identity (the certificate's subject-alternative name) rather
          than the dynamically assigned address it was dialed at. A single
          ``identity`` applies to every worker reached through the
          provider. When ``identity`` is ``None`` (default), verification
          falls back to the dialed address — exactly the legacy behaviour.

        - **Rotation without restart.** Pass ``reload=True`` to re-read the
          PEM files whenever they change on disk, so a long-running worker
          or pool adopts rotated certificates, keys, or certificate-
          authority bundles for subsequent connections without a process
          restart. When ``reload=False`` (default), the files are read once
          and the material is fixed for the provider's lifetime.

        :param ca_path:
            Path to the CA certificate file.
        :param key_path:
            Path to the worker private key file.
        :param cert_path:
            Path to the worker certificate file.
        :param mutual:
            Whether to use mutual TLS. If ``True`` (default), both server
            and client authenticate.
        :param identity:
            Expected server identity to verify discovered workers against,
            or ``None`` to verify against the dialed address.
        :param reload:
            Whether to re-read the PEM files when they change. If ``True``,
            returns a reloading provider; if ``False`` (default), returns a
            static provider that reads the files once.
        :returns:
            A `CredentialsProviderLike` resolving to the current
            credential material.
        :raises FileNotFoundError:
            If ``reload`` is ``False`` and any certificate file doesn't
            exist. A reloading provider defers reads until first resolved.
        :raises OSError:
            If ``reload`` is ``False`` and any file cannot be read.
        """
        if reload:
            return WorkerCredentialsProvider(
                _FileCredentialsReader(
                    ca_path,
                    key_path,
                    cert_path,
                    mutual=mutual,
                ),
                identity=identity,
                reloadable=True,
            )
        credentials = cls.from_files(
            ca_path=ca_path,
            key_path=key_path,
            cert_path=cert_path,
            mutual=mutual,
        )
        return WorkerCredentialsProvider(lambda: credentials, identity=identity)

    def server_credentials(self) -> grpc.ServerCredentials:
        """Build server credentials for accepting connections.

        Returns server credentials configured based on the ``mutual`` flag.
        Use when the worker is acting as a server accepting connections.

        **Mutual TLS (mutual=True):**
            Only clients with valid CA-signed certificates can connect.
            Provides mutual authentication at the transport layer.

        **One-way TLS (mutual=False):**
            Any client can establish an encrypted connection. The server's
            identity is verified, but **clients are NOT authenticated** at
            the transport layer. If using one-way TLS, you MUST implement
            authentication at a higher level (e.g., API keys, OAuth tokens)
            to verify client identity.

        :returns:
            Server credentials configured for mTLS or one-way TLS.

        .. warning::
            When ``mutual=False``, clients are not authenticated at the
            transport layer. Ensure application-level authentication is
            implemented.
        """
        import grpc

        return grpc.ssl_server_credentials(
            private_key_certificate_chain_pairs=[(self.worker_key, self.worker_cert)],
            root_certificates=self.ca_cert if self.mutual else None,
            require_client_auth=self.mutual,
        )

    def client_credentials(self) -> grpc.ChannelCredentials:
        """Build client credentials for making connections.

        Returns client credentials configured based on the ``mutual`` flag.
        Use when the worker is acting as a client connecting to servers.

        **Mutual TLS (mutual=True):**
            The worker presents its certificate to the server for mutual
            authentication. Required when connecting to servers configured
            with mutual TLS.

        **One-way TLS (mutual=False):**
            The worker verifies the server's certificate but does NOT
            present its own certificate. The worker remains anonymous to
            the server at the transport layer.

        :returns:
            Client credentials configured for mTLS or one-way TLS.
        """
        import grpc

        return grpc.ssl_channel_credentials(
            root_certificates=self.ca_cert,
            private_key=self.worker_key if self.mutual else None,
            certificate_chain=self.worker_cert if self.mutual else None,
        )


def _compute_fingerprint(credentials: WorkerCredentials, identity: str | None) -> str:
    """Compute a stable content fingerprint for credential material.

    The fingerprint changes if and only if the certificate-authority
    bundle, worker key, worker certificate, mutual-TLS flag, or expected
    identity changes.  It is the content-stable key under which client
    channels are pooled, so that unchanged material reuses a cached
    channel while rotated material yields a fresh one.

    :param credentials:
        The credential material to fingerprint.
    :param identity:
        The expected server identity, or ``None``.
    :returns:
        A hex SHA-256 digest over the material and identity.
    """
    hasher = hashlib.sha256()
    for part in (
        credentials.ca_cert,
        credentials.worker_key,
        credentials.worker_cert,
    ):
        # Length-prefix each field so distinct splits cannot collide.
        hasher.update(len(part).to_bytes(8, "big"))
        hasher.update(part)
    hasher.update(b"\x01" if credentials.mutual else b"\x00")
    hasher.update((identity or "").encode("utf-8"))
    return hasher.hexdigest()


def _normalize_identity(identity: str | None) -> str | None:
    """Collapse an empty or whitespace-only identity to ``None``.

    A blank identity would otherwise emit an empty
    ``grpc.ssl_target_name_override`` and fail verification opaquely;
    ``None`` instead selects the address-based path, the intended
    "no identity configured" behaviour.

    :param identity:
        The configured identity, or ``None``.
    :returns:
        The stripped identity, or ``None`` if blank.
    """
    if identity is None:
        return None
    identity = identity.strip()
    return identity or None


# public
@dataclass(frozen=True)
class CredentialsSnapshot:
    """An immutable, fingerprinted view of credential material.

    Resolved from a `CredentialsProviderLike`, a snapshot bundles the
    concrete `WorkerCredentials` with the expected ``identity`` to
    verify discovered workers against and a content ``fingerprint`` that
    changes only when the material or identity changes.

    :param credentials:
        The concrete credential material.
    :param identity:
        Expected server identity, or ``None`` to verify against the dialed
        address.
    :param fingerprint:
        Content fingerprint over the material and identity.
    """

    credentials: WorkerCredentials
    identity: str | None
    fingerprint: str

    @classmethod
    def of(
        cls, credentials: WorkerCredentials, identity: str | None = None
    ) -> CredentialsSnapshot:
        """Build a snapshot, computing its fingerprint.

        :param credentials:
            The concrete credential material.
        :param identity:
            Expected server identity, or ``None``.
        :returns:
            A snapshot whose fingerprint reflects *credentials* and
            *identity*.
        """
        return cls(
            credentials=credentials,
            identity=identity,
            fingerprint=_compute_fingerprint(credentials, identity),
        )


# public
@runtime_checkable
class CredentialsProviderLike(Protocol):
    """Protocol for objects that supply current credential material.

    A provider is the seam through which the runtime obtains credentials at
    the moment it needs them, so that rotated material can be adopted
    without restarting.  Implementations MUST be picklable: a provider
    crosses into worker subprocesses when supplied to a worker, and is
    re-resolved from the active `CredentialsContext` on the client
    side.
    """

    @property
    def reloadable(self) -> bool:
        """Whether the material may change over the provider's lifetime.

        A worker built from a reloadable provider serves rotating server
        credentials via a per-handshake fetcher; a non-reloadable provider
        takes the fixed static-credentials path.  Conforming providers
        declare this; the worker treats a provider that omits it as
        non-reloadable.
        """
        ...

    def resolve(self) -> CredentialsSnapshot:
        """Return the current credential snapshot.

        :returns:
            The current `CredentialsSnapshot`.
        """
        ...


# public
class WorkerCredentialsProvider:
    """A credential provider backed by a user-supplied fetch callback.

    The provider is a thin adapter: ``fetch`` returns the current
    `WorkerCredentials`, and the provider stamps the expected ``identity``
    onto a fingerprinted `CredentialsSnapshot`. Every source-specific
    concern — change detection, returning cached material when nothing has
    changed, validation, and failure handling — belongs to ``fetch``, which
    keeps the provider itself a pass-through. The file-backed factory
    `WorkerCredentials.provider_from_files` supplies exactly such a fetch.

    A non-reloadable provider resolves once when constructed and serves that
    fixed snapshot thereafter, so the fixed-material case needs no dedicated
    type — it is the general provider with a constant fetch:

    .. code-block:: python

        provider = WorkerCredentialsProvider(lambda: creds, reloadable=False)

    The lambda runs once in the constructing process and is dropped before
    the provider is pickled into a worker subprocess, so a non-reloadable
    ``fetch`` need not be picklable. A reloadable provider instead calls
    ``fetch`` on every resolution — including from the worker's per-handshake
    server fetcher — so a reloadable ``fetch`` MUST be an importable,
    picklable callable (a module-level function or a picklable object) and
    MUST be safe to call concurrently from gRPC handshake threads.

    :param fetch:
        A zero-argument callable returning the current `WorkerCredentials`.
    :param identity:
        Expected server identity to verify discovered workers against, or
        ``None`` to verify against the dialed address. Applies to every
        worker reached through the provider.
    :param reloadable:
        Whether ``fetch`` is consulted on every resolution. If ``False``
        (default), ``fetch`` is called once at construction and the
        resulting snapshot is fixed for the provider's lifetime.
    """

    def __init__(
        self,
        fetch: Callable[[], WorkerCredentials],
        *,
        identity: str | None = None,
        reloadable: bool = False,
    ) -> None:
        self._fetch = fetch
        self._identity = _normalize_identity(identity)
        self._reloadable = bool(reloadable)
        # A non-reloadable provider resolves eagerly so the snapshot — not
        # the callback — is what crosses into worker subprocesses (see
        # __getstate__); a reloadable provider defers to resolve().
        self._snapshot: CredentialsSnapshot | None = (
            None if self._reloadable else CredentialsSnapshot.of(fetch(), self._identity)
        )

    @property
    def identity(self) -> str | None:
        """The expected server identity, or ``None``."""
        return self._identity

    @property
    def reloadable(self) -> bool:
        """Whether the material can change over the provider's lifetime.

        A worker built from a reloadable provider serves rotating server
        credentials via a per-handshake fetcher; a non-reloadable provider
        takes the fixed static-credentials path.
        """
        return self._reloadable

    def resolve(self) -> CredentialsSnapshot:
        """Return the current credential snapshot.

        A non-reloadable provider returns the snapshot captured at
        construction. A reloadable provider calls ``fetch`` and stamps the
        identity onto a fresh snapshot; identical material yields an
        identical fingerprint, so unchanged credentials reuse the pooled
        channel and only the re-read is paid for by ``fetch``.

        :returns:
            The current `CredentialsSnapshot`.
        """
        if self._snapshot is not None:
            return self._snapshot
        return CredentialsSnapshot.of(self._fetch(), self._identity)

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        if not self._reloadable:
            # The fixed snapshot already rides along, so drop the callback:
            # a non-reloadable fetch (e.g., a lambda over in-memory material)
            # need not be picklable to cross into a worker subprocess.
            state["_fetch"] = None
        return state


def _validate_material(ca_path: str, key_path: str, cert_path: str) -> None:
    """Reject readable-but-malformed PEM by loading it through ``ssl``.

    `WorkerCredentials.from_files` reads raw bytes without parsing,
    so a non-atomic rotation that leaves a truncated or garbage — but
    readable — PEM would otherwise be cached as the current snapshot and
    only fail later, opaquely, at the handshake.  Loading the material into
    an `ssl.SSLContext` here parses the certificate, key, and CA
    bundle (and checks the key matches the certificate) using the same
    OpenSSL machinery the transport uses, so the reloading provider keeps
    its prior good material instead.  Stdlib ``ssl`` is used deliberately:
    validation must not add a third-party runtime dependency.

    :param ca_path:
        Path to the CA certificate bundle.
    :param key_path:
        Path to the worker private key.
    :param cert_path:
        Path to the worker certificate.
    :raises ssl.SSLError:
        If the certificate, key, or CA bundle is malformed (or the key does
        not match the certificate). A subclass of `OSError`.
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=cert_path, keyfile=key_path)
    context.load_verify_locations(cafile=ca_path)


# internal
class _FileCredentialsReader:
    """A picklable, stateful fetch that reads credentials from PEM files.

    Supplies the ``fetch`` for a reloadable `WorkerCredentialsProvider`
    built via `WorkerCredentials.provider_from_files`. Reads the
    certificate-authority, key, and certificate files, re-reading them when
    they change so a long-running worker or pool adopts rotated material
    without a restart. Unchanged files return the cached material (so the
    provider's fingerprint is stable and pooled channels are reused);
    changed files are re-read and re-validated.

    A transient read failure during rotation (e.g., a partially written
    file) returns the last good material rather than raising, so an
    in-flight rotation never tears the fleet down; genuinely invalid
    material that *can* be read surfaces later through the handshake-failure
    channel. The very first read has no last-good material to fall back on
    and propagates the error.

    The reader guards its own read-compare-write with a lock: the provider
    leaves synchronization to the fetch, and the worker's per-handshake
    server fetcher calls it from a gRPC C-core thread, off the asyncio loop,
    concurrently with client-side dispatch.

    :param ca_path:
        Path to the CA certificate file.
    :param key_path:
        Path to the worker private key file.
    :param cert_path:
        Path to the worker certificate file.
    :param mutual:
        Whether to use mutual TLS.
    """

    def __init__(
        self,
        ca_path: str,
        key_path: str,
        cert_path: str,
        *,
        mutual: bool = True,
    ) -> None:
        self._ca_path = os.fspath(ca_path)
        self._key_path = os.fspath(key_path)
        self._cert_path = os.fspath(cert_path)
        self._mutual = mutual
        self._cached_credentials: WorkerCredentials | None = None
        self._cached_signature: tuple[tuple[int, int, int, int], ...] | None = None
        # Excluded from __getstate__; recreated on unpickle.
        self._lock = threading.Lock()

    def __call__(self) -> WorkerCredentials:
        """Return the current credentials, re-reading the files on change.

        :returns:
            The current `WorkerCredentials`. Reuses the cached material when
            the files are unchanged, or on a failed re-read when prior
            material exists.
        :raises OSError:
            If the files cannot be read or are malformed (`ssl.SSLError` is
            an `OSError`) and no prior material exists.
        """
        with self._lock:
            try:
                # Cheap stat gate: only re-read when the files look changed.
                signature = self._signature()
            except OSError as exc:
                return self._fallback_or_raise(exc)
            if (
                signature == self._cached_signature
                and self._cached_credentials is not None
            ):
                return self._cached_credentials
            try:
                credentials = WorkerCredentials.from_files(
                    ca_path=self._ca_path,
                    key_path=self._key_path,
                    cert_path=self._cert_path,
                    mutual=self._mutual,
                )
                # Validate before caching so a readable-but-truncated PEM
                # never overwrites the last good material.
                _validate_material(self._ca_path, self._key_path, self._cert_path)
            except OSError as exc:
                return self._fallback_or_raise(exc)
            self._cached_credentials = credentials
            self._cached_signature = signature
            return credentials

    def _signature(self) -> tuple[tuple[int, int, int, int], ...]:
        """Return a cheap change signature for the files.

        Includes ``st_ino`` and ``st_ctime_ns`` alongside ``(mtime, size)``
        so an inode swap (atomic rename) and a same-size in-place content
        rewrite — which bumps ctime even if mtime is forced back — are both
        detected, not just an mtime/size change.
        """
        signature = []
        for path in (self._ca_path, self._key_path, self._cert_path):
            stat = os.stat(path)
            signature.append(
                (stat.st_mtime_ns, stat.st_size, stat.st_ino, stat.st_ctime_ns)
            )
        return tuple(signature)

    def _fallback_or_raise(self, exc: Exception) -> WorkerCredentials:
        """Return the last good credentials, logging, or re-raise if none."""
        if self._cached_credentials is not None:
            _log.warning(
                "Credential reload failed for %s; serving the last good "
                "material until it succeeds: %s",
                self._cert_path,
                exc,
            )
            return self._cached_credentials
        raise exc

    def __getstate__(self) -> dict:
        # Drop the cache so a pickled reader (crossing into a worker
        # subprocess) re-reads the live files rather than carrying material
        # from the originating process. The lock is process-local and not
        # picklable, so drop it too and recreate on unpickle.
        state = self.__dict__.copy()
        state["_cached_credentials"] = None
        state["_cached_signature"] = None
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._lock = threading.Lock()


def _coerce_provider(
    value: WorkerCredentials | CredentialsProviderLike | None,
) -> CredentialsProviderLike | None:
    """Normalize a credentials-or-provider value into a provider.

    A bare `WorkerCredentials` is wrapped in a non-reloadable
    `WorkerCredentialsProvider` with no identity, so it verifies against the
    dialed address; a provider is returned unchanged; ``None`` stays
    ``None``.  This is the single seam through which pools, proxies, and
    worker processes accept either form.

    :param value:
        A `WorkerCredentials`, a provider, or ``None``.
    :returns:
        A `CredentialsProviderLike`, or ``None``.
    """
    if value is None:
        return None
    # Branch on WorkerCredentials rather than isinstance(CredentialsProviderLike):
    # the latter is a runtime_checkable protocol, so a custom provider that
    # implements only resolve() (omitting the optional reloadable member)
    # would fail the check and be wrapped as if it were credentials. Treating
    # "not WorkerCredentials" as "already a provider" keeps such providers
    # working and defers the reloadable default to the worker.
    if isinstance(value, WorkerCredentials):
        return WorkerCredentialsProvider(lambda: value, reloadable=False)
    return value


class CredentialsContext:
    """Internal context manager for propagating credentials via ContextVar.

    Used by WorkerProcess._serve() to set credentials in worker subprocesses
    and by WorkerProxy.__init__() to resolve credentials from context. Carries
    either a `WorkerCredentials` or a `CredentialsProviderLike`.
    Not part of the public API.
    """

    def __init__(self, credentials: WorkerCredentials | CredentialsProviderLike) -> None:
        self._credentials = credentials
        self._token: Token | None = None

    def __enter__(self) -> CredentialsContext:
        self._token = _current.set(self._credentials)
        return self

    def __exit__(self, *_) -> None:
        if self._token is None:
            raise RuntimeError("__exit__ called without matching __enter__")
        _current.reset(self._token)
        self._token = None

    @classmethod
    def current(cls) -> WorkerCredentials | CredentialsProviderLike | None:
        """Get the current credentials or provider from the context.

        :returns:
            The active `WorkerCredentials` or
            `CredentialsProviderLike`, or ``None`` if no context is
            set.
        """
        return _current.get()
