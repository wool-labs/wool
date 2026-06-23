from __future__ import annotations

import os
from collections.abc import Callable
from contextvars import ContextVar
from contextvars import Token
from dataclasses import dataclass
from dataclasses import replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import grpc

_current: ContextVar[WorkerCredentials | WorkerCredentialsProvider | None] = ContextVar(
    "worker_credentials", default=None
)


# public
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
    :param identity:
        Expected server identity, i.e., the peer certificate's subject-
        alternative name to verify dialed workers against, or ``None``
        (default) to verify against the dialed address. A blank value
        normalizes to ``None``. Only consumed client-side; inert when
        presenting the worker's own server certificate.
    """

    ca_cert: bytes
    worker_key: bytes
    worker_cert: bytes
    mutual: bool = True
    identity: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "identity", _normalize_identity(self.identity))

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

        Equivalent to supplying a bare `WorkerCredentials`. ``identity``
        sets the stable logical identity to verify discovered workers against,
        instead of the address they were dialed at. Given an identity of ``None``
        (default), verification falls back to the dialed address.

        For credentials that change over a process's lifetime, build a
        reloadable provider directly with a factory callable instead, e.g.,
        ``WorkerCredentialsProvider(factory, reloadable=True)``; the reload
        strategy then lives in ``factory``.

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
class WorkerCredentialsProvider:
    """A credential provider backed by a user-supplied factory callable.

    The provider is a thin adapter: ``factory`` returns the current
    `WorkerCredentials`, and `resolve` hands them back with the provider's
    ``identity`` applied. Every source-specific concern, e.g., rotation,
    validation, failure handling, etc., belongs to ``factory``, which keeps
    the provider itself a pass-through. `WorkerCredentials.as_provider` is
    the shorthand for the fixed-material case.

    A non-reloadable provider resolves once when constructed and serves that
    fixed result thereafter.  The lambda runs once in the constructing process
    and is dropped before the provider is pickled into a worker subprocess,
    so a non-reloadable ``factory`` need not be picklable.

    A reloadable provider resolves on every dispatch (the resolved credentials
    key the channel pool) client-side, and with each TLS handshake server-side.
    A reloadable ``factory`` therefore MUST be an importable, picklable callable
    (a module-level function or a picklable object) and MUST be safe to call
    concurrently from gRPC handshake threads.

    :param factory:
        A zero-argument callable returning the current `WorkerCredentials`.
    :param identity:
        Expected server identity to verify discovered workers against,
        applied to every credential the provider yields (overriding any
        identity the credentials already carry). ``None`` (default) leaves
        the credentials' own identity untouched.
    :param reloadable:
        Whether ``factory`` is consulted on every resolution. If ``False``
        (default), ``factory`` is called once at construction and the result
        is fixed for the provider's lifetime.
    """

    def __init__(
        self,
        factory: Callable[[], WorkerCredentials],
        *,
        identity: str | None = None,
        reloadable: bool = False,
    ) -> None:
        self._factory = factory
        self._identity = _normalize_identity(identity)
        self._reloadable = bool(reloadable)
        # A non-reloadable provider resolves eagerly so the credentials,
        # not the callback, are what cross into worker subprocesses (see
        # __getstate__); a reloadable provider defers to resolve().
        self._cached: WorkerCredentials | None = (
            None if self._reloadable else self._apply(factory())
        )

    def __getstate__(self) -> dict:
        """Return the picklable state, dropping a non-reloadable factory.

        The fixed credentials already ride along in the state, so the callback
        is dropped: a non-reloadable ``factory`` (e.g., a lambda over in-memory
        material) need not be picklable to cross into a worker subprocess.
        """
        state = self.__dict__.copy()
        if not self._reloadable:
            state["_factory"] = None
        return state

    @property
    def identity(self) -> str | None:
        """The expected server identity, or ``None``."""
        return self._identity

    @property
    def reloadable(self) -> bool:
        """Whether the credentials can change over the provider's lifetime.

        A worker built from a reloadable provider serves rotating server
        credentials via a per-handshake fetcher; a non-reloadable provider
        takes the fixed static-credentials path.
        """
        return self._reloadable

    def resolve(self) -> WorkerCredentials:
        """Return the current credentials, with the provider's identity applied.

        A non-reloadable provider returns the credentials captured at
        construction. A reloadable provider calls ``factory`` each time;
        identical material compares equal, so unchanged credentials reuse the
        pooled channel and only the re-read is paid for by ``factory``.

        :returns:
            The current `WorkerCredentials`.
        """
        if self._cached is not None:
            return self._cached
        return self._apply(self._factory())

    def _apply(self, credentials: WorkerCredentials) -> WorkerCredentials:
        """Return ``credentials`` with the provider's identity applied.

        The provider's identity, when configured, is authoritative;
        otherwise the credentials keep whatever identity they carry.
        """
        if self._identity is None:
            return credentials
        return replace(credentials, identity=self._identity)


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
