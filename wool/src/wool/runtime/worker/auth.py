from __future__ import annotations

import hashlib
import os
from collections.abc import Callable
from contextvars import ContextVar
from contextvars import Token
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import grpc

_current: ContextVar[WorkerCredentials | WorkerCredentialsProvider | None] = ContextVar(
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
        ca_path: str | os.PathLike[str],
        key_path: str | os.PathLike[str],
        cert_path: str | os.PathLike[str],
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

    def as_provider(self, *, identity: str | None = None) -> WorkerCredentialsProvider:
        """Adapt these fixed credentials into a non-reloadable provider.

        The result always resolves to this material, so it is the in-memory
        equivalent of supplying a bare `WorkerCredentials` (and what the
        runtime wraps one in). ``identity`` sets the stable logical identity
        — the certificate's subject-alternative name — to verify discovered
        workers against, instead of the address they were dialed at; with
        ``None`` (default) verification falls back to the dialed address.

        For material that changes over a process's lifetime — rotated files,
        a secrets manager — build a reloadable provider directly with a fetch
        callback instead, e.g.,
        ``WorkerCredentialsProvider(fetch, reloadable=True)``; the reload
        strategy then lives in ``fetch``.

        :param identity:
            Expected server identity, or ``None`` to verify against the
            dialed address.
        :returns:
            A non-reloadable `WorkerCredentialsProvider` over this material.
        """
        return WorkerCredentialsProvider(lambda: self, identity=identity)

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
class WorkerCredentialsSnapshot:
    """An immutable, fingerprinted view of credential material.

    Resolved from a `WorkerCredentialsProvider`, a snapshot bundles the
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
    ) -> WorkerCredentialsSnapshot:
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
class WorkerCredentialsProvider:
    """A credential provider backed by a user-supplied fetch callback.

    The provider is a thin adapter: ``fetch`` returns the current
    `WorkerCredentials`, and the provider stamps the expected ``identity``
    onto a fingerprinted `WorkerCredentialsSnapshot`. Every source-specific
    concern — change detection, returning cached material when nothing has
    changed, validation, and failure handling — belongs to ``fetch``, which
    keeps the provider itself a pass-through. `WorkerCredentials.as_provider`
    is the shorthand for the fixed-material case.

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
        self._snapshot: WorkerCredentialsSnapshot | None = (
            None
            if self._reloadable
            else WorkerCredentialsSnapshot.of(fetch(), self._identity)
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

    def resolve(self) -> WorkerCredentialsSnapshot:
        """Return the current credential snapshot.

        A non-reloadable provider returns the snapshot captured at
        construction. A reloadable provider calls ``fetch`` and stamps the
        identity onto a fresh snapshot; identical material yields an
        identical fingerprint, so unchanged credentials reuse the pooled
        channel and only the re-read is paid for by ``fetch``.

        :returns:
            The current `WorkerCredentialsSnapshot`.
        """
        if self._snapshot is not None:
            return self._snapshot
        return WorkerCredentialsSnapshot.of(self._fetch(), self._identity)

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        if not self._reloadable:
            # The fixed snapshot already rides along, so drop the callback:
            # a non-reloadable fetch (e.g., a lambda over in-memory material)
            # need not be picklable to cross into a worker subprocess.
            state["_fetch"] = None
        return state


def _coerce_provider(
    value: WorkerCredentials | WorkerCredentialsProvider | None,
) -> WorkerCredentialsProvider | None:
    """Normalize a credentials-or-provider value into a provider.

    A bare `WorkerCredentials` is adapted via `WorkerCredentials.as_provider`
    into a non-reloadable provider with no identity, so it verifies against
    the dialed address; a provider is returned unchanged; ``None`` stays
    ``None``.  This is the single seam through which pools, proxies, and
    worker processes accept either form.

    :param value:
        A `WorkerCredentials`, a provider, or ``None``.
    :returns:
        A `WorkerCredentialsProvider`, or ``None``.
    """
    if value is None:
        return None
    # WorkerCredentials is the one thing to wrap; anything else is assumed to
    # already be a provider. Providers are consumed structurally — resolve()
    # plus an optional reloadable the worker defaults off — so there is no
    # nominal protocol to isinstance-check here.
    if isinstance(value, WorkerCredentials):
        return value.as_provider()
    return value


class CredentialsContext:
    """Internal context manager for propagating credentials via ContextVar.

    Used by WorkerProcess._serve() to set credentials in worker subprocesses
    and by WorkerProxy.__init__() to resolve credentials from context. Carries
    either a `WorkerCredentials` or a `WorkerCredentialsProvider`.
    Not part of the public API.
    """

    def __init__(
        self, credentials: WorkerCredentials | WorkerCredentialsProvider
    ) -> None:
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
    def current(cls) -> WorkerCredentials | WorkerCredentialsProvider | None:
        """Get the current credentials or provider from the context.

        :returns:
            The active `WorkerCredentials` or
            `WorkerCredentialsProvider`, or ``None`` if no context is
            set.
        """
        return _current.get()
