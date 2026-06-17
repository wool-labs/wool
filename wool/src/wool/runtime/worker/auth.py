from __future__ import annotations

import hashlib
import logging
import os
import ssl
import threading
from contextvars import ContextVar
from contextvars import Token
from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import Protocol
from typing import runtime_checkable

if TYPE_CHECKING:
    import grpc

_log = logging.getLogger(__name__)

_current: ContextVar[WorkerCredentials | CredentialProviderLike | None] = ContextVar(
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
    ) -> CredentialProviderLike:
        """Build a credential provider backed by PEM files.

        A provider is the unit that worker pools, proxies, and worker
        processes consult to obtain the *current* credential material.
        This is the entry point for the two behaviours layered on top of
        the base :class:`WorkerCredentials` model:

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
            A :class:`CredentialProviderLike` resolving to the current
            credential material.
        :raises FileNotFoundError:
            If ``reload`` is ``False`` and any certificate file doesn't
            exist. A reloading provider defers reads until first resolved.
        :raises OSError:
            If ``reload`` is ``False`` and any file cannot be read.
        """
        if reload:
            return FileCredentialProvider(
                ca_path,
                key_path,
                cert_path,
                mutual=mutual,
                identity=identity,
            )
        credentials = cls.from_files(
            ca_path=ca_path,
            key_path=key_path,
            cert_path=cert_path,
            mutual=mutual,
        )
        return StaticCredentialProvider(credentials, identity=identity)

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
class CredentialSnapshot:
    """An immutable, fingerprinted view of credential material.

    Resolved from a :class:`CredentialProviderLike`, a snapshot bundles the
    concrete :class:`WorkerCredentials` with the expected ``identity`` to
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
    ) -> CredentialSnapshot:
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
class CredentialProviderLike(Protocol):
    """Protocol for objects that supply current credential material.

    A provider is the seam through which the runtime obtains credentials at
    the moment it needs them, so that rotated material can be adopted
    without restarting.  Implementations MUST be picklable: a provider
    crosses into worker subprocesses when supplied to a worker, and is
    re-resolved from the active :class:`CredentialContext` on the client
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

    def resolve(self) -> CredentialSnapshot:
        """Return the current credential snapshot.

        :returns:
            The current :class:`CredentialSnapshot`.
        """
        ...


# public
@dataclass(frozen=True)
class StaticCredentialProvider:
    """A provider that always resolves to fixed credential material.

    Wraps a single :class:`WorkerCredentials` instance and an optional
    expected ``identity``.  This is the back-compatible default: a bare
    :class:`WorkerCredentials` supplied to a pool, proxy, or worker is
    wrapped in a static provider with no identity, preserving the legacy
    address-based verification behaviour exactly.

    :param credentials:
        The fixed credential material.
    :param identity:
        Expected server identity, or ``None`` to verify against the dialed
        address.
    """

    credentials: WorkerCredentials
    identity: str | None = None
    _snapshot: CredentialSnapshot = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "identity", _normalize_identity(self.identity))
        object.__setattr__(
            self, "_snapshot", CredentialSnapshot.of(self.credentials, self.identity)
        )

    def resolve(self) -> CredentialSnapshot:
        """Return the fixed credential snapshot.

        :returns:
            The same :class:`CredentialSnapshot` on every call.
        """
        return self._snapshot

    @property
    def reloadable(self) -> bool:
        """Whether the material can change over the provider's lifetime.

        Always ``False`` for a static provider, so a worker built from one
        uses fixed server credentials.
        """
        return False


def _validate_material(ca_path: str, key_path: str, cert_path: str) -> None:
    """Reject readable-but-malformed PEM by loading it through ``ssl``.

    :class:`WorkerCredentials.from_files` reads raw bytes without parsing,
    so a non-atomic rotation that leaves a truncated or garbage — but
    readable — PEM would otherwise be cached as the current snapshot and
    only fail later, opaquely, at the handshake.  Loading the material into
    an :class:`ssl.SSLContext` here parses the certificate, key, and CA
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
        not match the certificate). A subclass of :class:`OSError`.
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=cert_path, keyfile=key_path)
    context.load_verify_locations(cafile=ca_path)


# public
class FileCredentialProvider:
    """A provider that re-reads PEM files as they are rotated.

    Resolves the current credential material from the certificate-
    authority, key, and certificate files on disk, re-reading them when
    they change so a long-running worker or pool adopts rotated material
    without a restart.  Unchanged files resolve to the same cached snapshot
    (so pooled channels are reused); changed files yield a new snapshot
    with a fresh fingerprint.

    A transient read failure during rotation (e.g. a partially written
    file) resolves to the last good snapshot rather than raising, so an
    in-flight rotation never tears the fleet down; genuinely invalid
    material that *can* be read surfaces later through the handshake-failure
    channel.  The very first resolution has no last-good snapshot to fall
    back on and propagates the error.

    :param ca_path:
        Path to the CA certificate file.
    :param key_path:
        Path to the worker private key file.
    :param cert_path:
        Path to the worker certificate file.
    :param mutual:
        Whether to use mutual TLS.
    :param identity:
        Expected server identity, or ``None`` to verify against the dialed
        address.
    """

    def __init__(
        self,
        ca_path: str,
        key_path: str,
        cert_path: str,
        *,
        mutual: bool = True,
        identity: str | None = None,
    ) -> None:
        self._ca_path = os.fspath(ca_path)
        self._key_path = os.fspath(key_path)
        self._cert_path = os.fspath(cert_path)
        self._mutual = mutual
        self._identity = _normalize_identity(identity)
        self._cached_snapshot: CredentialSnapshot | None = None
        self._cached_signature: tuple[tuple[int, int, int, int], ...] | None = None
        # The server-side fetcher consults resolve() from a gRPC C-core
        # thread, off the asyncio loop, concurrently with client-side
        # dispatch — so guard the read-compare-write with a threading (not
        # asyncio) lock. Excluded from __getstate__; recreated on unpickle.
        self._lock = threading.Lock()

    @property
    def identity(self) -> str | None:
        """The expected server identity, or ``None``."""
        return self._identity

    @property
    def reloadable(self) -> bool:
        """Whether the material can change over the provider's lifetime.

        Always ``True`` for a file provider, so a worker built from one
        serves rotating credentials via a per-handshake fetcher.
        """
        return True

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

    def resolve(self) -> CredentialSnapshot:
        """Return the current credential snapshot, re-reading on change.

        On a transient read failure or readable-but-malformed material
        during a rotation, a prior good snapshot is kept (and a warning is
        logged) rather than caching broken bytes, so an in-progress
        rotation never tears the fleet down nor silently adopts garbage.

        :returns:
            The current :class:`CredentialSnapshot`.  Reuses the cached
            snapshot when the files are unchanged, or on a failed re-read
            when a prior snapshot exists.
        :raises OSError:
            If the files cannot be read or are malformed (``ssl.SSLError``
            is an ``OSError``) and no prior snapshot exists.
        """
        with self._lock:
            try:
                # Cheap stat gate: only re-read when the files look changed.
                signature = self._signature()
            except OSError as exc:
                return self._fallback_or_raise(exc)
            if signature == self._cached_signature and self._cached_snapshot is not None:
                return self._cached_snapshot
            try:
                credentials = WorkerCredentials.from_files(
                    ca_path=self._ca_path,
                    key_path=self._key_path,
                    cert_path=self._cert_path,
                    mutual=self._mutual,
                )
                # Validate before caching so a readable-but-truncated PEM
                # never overwrites the last good snapshot.
                _validate_material(self._ca_path, self._key_path, self._cert_path)
            except OSError as exc:
                return self._fallback_or_raise(exc)
            snapshot = CredentialSnapshot.of(credentials, self._identity)
            self._cached_snapshot = snapshot
            self._cached_signature = signature
            return snapshot

    def _fallback_or_raise(self, exc: Exception) -> CredentialSnapshot:
        """Return the last good snapshot, logging, or re-raise if none."""
        if self._cached_snapshot is not None:
            _log.warning(
                "Credential reload failed for %s; serving the last good "
                "material until it succeeds: %s",
                self._cert_path,
                exc,
            )
            return self._cached_snapshot
        raise exc

    def __getstate__(self) -> dict:
        # Drop the cache so a pickled provider (e.g. crossing into a worker
        # subprocess) re-reads the live files rather than carrying a stale
        # snapshot from the originating process. The lock is process-local
        # and not picklable, so drop it too and recreate on unpickle.
        state = self.__dict__.copy()
        state["_cached_snapshot"] = None
        state["_cached_signature"] = None
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._lock = threading.Lock()


def _coerce_provider(
    value: WorkerCredentials | CredentialProviderLike | None,
) -> CredentialProviderLike | None:
    """Normalize a credentials-or-provider value into a provider.

    A bare :class:`WorkerCredentials` is wrapped in a
    :class:`StaticCredentialProvider` with no identity, preserving the
    legacy address-based verification behaviour; a provider is returned
    unchanged; ``None`` stays ``None``.  This is the single seam through
    which pools, proxies, and worker processes accept either form.

    :param value:
        A :class:`WorkerCredentials`, a provider, or ``None``.
    :returns:
        A :class:`CredentialProviderLike`, or ``None``.
    """
    if value is None:
        return None
    # Branch on WorkerCredentials rather than isinstance(CredentialProviderLike):
    # the latter is a runtime_checkable protocol, so a custom provider that
    # implements only resolve() (omitting the optional reloadable member)
    # would fail the check and be wrapped as if it were credentials. Treating
    # "not WorkerCredentials" as "already a provider" keeps such providers
    # working and defers the reloadable default to the worker.
    if isinstance(value, WorkerCredentials):
        return StaticCredentialProvider(value)
    return value


class CredentialContext:
    """Internal context manager for propagating credentials via ContextVar.

    Used by WorkerProcess._serve() to set credentials in worker subprocesses
    and by WorkerProxy.__init__() to resolve credentials from context. Carries
    either a :class:`WorkerCredentials` or a :class:`CredentialProviderLike`.
    Not part of the public API.
    """

    def __init__(self, credentials: WorkerCredentials | CredentialProviderLike) -> None:
        self._credentials = credentials
        self._token: Token | None = None

    def __enter__(self) -> CredentialContext:
        self._token = _current.set(self._credentials)
        return self

    def __exit__(self, *_) -> None:
        if self._token is None:
            raise RuntimeError("__exit__ called without matching __enter__")
        _current.reset(self._token)
        self._token = None

    @classmethod
    def current(cls) -> WorkerCredentials | CredentialProviderLike | None:
        """Get the current credentials or provider from the context.

        :returns:
            The active :class:`WorkerCredentials` or
            :class:`CredentialProviderLike`, or ``None`` if no context is
            set.
        """
        return _current.get()
