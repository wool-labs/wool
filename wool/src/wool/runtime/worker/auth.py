"""Worker credential material and its resolution.

Provides `WorkerCredentials`, the PEM material a worker presents and
verifies against, and `WorkerCredentialsProvider`, which resolves that
material per use so a running pool can adopt rotated certificates.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from dataclasses import replace
from datetime import timedelta

import cloudpickle
import grpc

from wool.utilities.refreshing import Refreshing
from wool.utilities.throttle import Throttle

_log = logging.getLogger(__name__)

_current: ContextVar[WorkerCredentialsProvider | None] = ContextVar(
    "worker_credentials", default=None
)

# Default wall-clock interval between reloadable factory invocations.
# Rotation cadence is hours-to-days while dispatch rate can be hundreds
# per second, so even one second caps factory invocations at one per
# second per provider per process while bounding rotation adoption at
# ``window + resolve duration`` — invisible against any real rotation
# schedule.
_RESOLVE_DEBOUNCE = timedelta(seconds=1)

# Three consecutive unequal refreshes is well past what a real rotation
# and its retries produce, and sixty seconds is the floor on repeats. See
# `WorkerCredentialsProvider._note_churn` for what the warning means.
_CHURN_WARNING_THRESHOLD = 3
_CHURN_WARNING_INTERVAL_S = 60.0


# public
@dataclass(frozen=True)
class WorkerCredentials:
    """Container for worker TLS/mTLS credentials.

    The PEM material a worker presents and verifies against, in a form both
    sides of a peer-to-peer pool can use: a worker is a server to its
    callers and a client to its peers, and one instance serves both roles.

    :param ca_cert:
        PEM-encoded CA certificate for verifying peers.
    :param worker_key:
        PEM-encoded private key for this worker.
    :param worker_cert:
        PEM-encoded certificate for this worker.
    :param mutual:
        Whether to use mutual TLS (mTLS). If ``True`` (default), both
        server and client authenticate. If ``False``, only the server is
        authenticated.
    :param identity:
        Expected server identity, i.e., the peer certificate's subject-
        alternative name to verify dialed workers against, or ``None``
        (default) to verify against the dialed address. A blank value
        normalizes to ``None``. Only consumed client-side; inert when
        presenting the worker's own server certificate. A provider-level
        identity, when set, overrides this field — see
        `WorkerCredentialsProvider` for precedence.

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

        All three files must be PEM-encoded.

        :param ca_path:
            Path to CA certificate file.
        :param key_path:
            Path to worker private key file.
        :param cert_path:
            Path to worker certificate file.
        :param mutual:
            Whether to use mutual TLS (mTLS) — see `WorkerCredentials`.
        :returns:
            A `WorkerCredentials` over the loaded material.
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
        is forwarded to `WorkerCredentialsProvider`, whose ``identity``
        parameter documents the precedence.

        For credentials that change over a process's lifetime, build a
        reloadable provider directly with a factory callable instead; the
        reload strategy then lives in ``factory``.

        :param identity:
            Provider-level expected server identity — see
            `WorkerCredentialsProvider` for precedence.
        :returns:
            A non-reloadable `WorkerCredentialsProvider` over this material.

        **Configuring a pool with a provider:**

        .. code-block:: python

            import functools

            # Fixed material, i.e., what this method adapts.
            provider = WorkerCredentials.from_files(
                ca_path="certs/ca-cert.pem",
                key_path="certs/worker-key.pem",
                cert_path="certs/worker-cert.pem",
            ).as_provider(identity="wool-worker.svc")

            # Material that rotates out of band: a factory instead, called
            # again once per freshness interval.
            factory = functools.partial(
                WorkerCredentials.from_files,
                "certs/ca-cert.pem",
                "certs/worker-key.pem",
                "certs/worker-cert.pem",
            )
            provider = WorkerCredentialsProvider(
                factory, identity="wool-worker.svc", reloadable=True
            )

            # Either goes anywhere ``credentials=`` is accepted.
            async with WorkerPool(spawn=4, credentials=provider):
                ...
        """
        return WorkerCredentialsProvider(lambda: self, identity=identity)

    def _material(self) -> tuple[list[tuple[bytes, bytes]], bytes | None]:
        """Map this material onto what the gRPC server builders take.

        :returns:
            The key/certificate pairs, and the root certificates to
            verify clients against — ``None`` under one-way TLS, where
            clients are not authenticated.

        .. rubric:: Implementation notes

        The single home for the server-side key/cert/CA mapping, so the
        static and rotation-capable paths cannot disagree about it and a
        change to the ``mutual`` CA rule is made once.
        """
        return (
            [(self.worker_key, self.worker_cert)],
            self.ca_cert if self.mutual else None,
        )

    def server_credentials(self) -> grpc.ServerCredentials:
        """Build server credentials for accepting connections.

        Under mutual TLS only clients holding a CA-signed certificate can
        connect; under one-way TLS any client can establish an encrypted
        connection to this worker.

        :returns:
            Server credentials configured for mTLS or one-way TLS.

        .. warning::
            When ``mutual=False``, clients are **not authenticated** at the
            transport layer, so authentication MUST be implemented at a
            higher level (e.g., API keys, OAuth tokens) to establish who a
            caller is.
        """
        pairs, roots = self._material()
        return grpc.ssl_server_credentials(
            private_key_certificate_chain_pairs=pairs,
            root_certificates=roots,
            require_client_auth=self.mutual,
        )

    def server_certificate_configuration(self) -> grpc.ServerCertificateConfiguration:
        """Build the server certificate configuration for this material.

        Use when serving a rotation-capable gRPC server yourself: this is
        the per-handshake configuration `grpc.dynamic_ssl_server_credentials`
        asks its fetcher for, carrying the same material `server_credentials`
        would have fixed at startup.

        :returns:
            Certificate configuration for mTLS or one-way TLS.
        """
        pairs, roots = self._material()
        return grpc.ssl_server_certificate_configuration(pairs, root_certificates=roots)

    def client_credentials(self) -> grpc.ChannelCredentials:
        """Build client credentials for making connections.

        Under one-way TLS the worker verifies the server but presents no
        certificate of its own, so it remains anonymous to the server at
        the transport layer.

        :returns:
            Client credentials configured for mTLS or one-way TLS.
        """
        return grpc.ssl_channel_credentials(
            root_certificates=self.ca_cert,
            private_key=self.worker_key if self.mutual else None,
            certificate_chain=self.worker_cert if self.mutual else None,
        )

    def identity_channel_options(self) -> list[tuple[str, str]]:
        """Build the identity-derived secure-channel options.

        Use when building a gRPC channel yourself: these options verify the
        peer's certificate against the configured identity rather than the
        address it was dialed at, which is what lets a worker keep a stable
        logical identity across a dynamically assigned address. See
        `WorkerCredentials`'s ``identity`` field for the verification
        semantics.

        :returns:
            ``[("grpc.ssl_target_name_override", identity)]`` when an
            identity is configured, else an empty list.

        .. rubric:: Implementation notes

        Named for the identity it derives from, not for the channel it
        configures, to keep it distinct from
        `~wool.runtime.worker.base.ChannelOptions`, i.e., the message-size
        and keepalive settings a worker advertises.
        """
        if self.identity is None:
            return []
        return [("grpc.ssl_target_name_override", self.identity)]


# public
class WorkerCredentialsProvider:
    """A credential provider backed by a user-supplied factory callable.

    The provider adapts a source of credentials to the worker stack:
    ``factory`` returns the current `WorkerCredentials`, and `credentials`
    hands them back with the provider's ``identity`` applied. Where the
    material comes from (e.g., a file, a secrets manager, a lease) belongs
    to ``factory``. `WorkerCredentials.as_provider` is the shorthand for the
    fixed-material case.

    A non-reloadable provider resolves once when constructed and serves that
    result for its lifetime, so a broken ``factory`` fails at construction
    rather than at the first handshake. ``factory`` is cloudpickled with the
    provider, so it MUST be cloudpickle-serializable but need not be
    reachable by name — a lambda or closure is fine, which is what
    `WorkerCredentials.as_provider` relies on. A copy that crosses into a
    worker subprocess re-consults ``factory`` once on its first read rather
    than carrying the material, so a factory that closes over fixed material
    reproduces it exactly while one that reads its source anew may observe a
    later state.

    A ``factory`` whose output never compares equal defeats channel pooling,
    costing a TLS handshake and a pooled channel per change; consecutive
    unequal resolutions are warned about.

    A reloadable ``factory`` MUST be safe to call concurrently from gRPC
    handshake threads. It MUST NOT read its own provider's `credentials`:
    a recursive read would join the very invocation it is running inside
    and deadlock.

    :param factory:
        A zero-argument callable returning the current `WorkerCredentials`.
    :param identity:
        Expected server identity to verify discovered workers against.
        Identity is deliberately settable at two levels: on the material
        (the `WorkerCredentials.identity` field) and on the provider
        (this parameter, which `WorkerCredentials.as_provider` forwards
        to). Precedence: a configured provider identity is applied to
        every credential the provider yields, overriding any identity
        the credentials already carry; ``None`` (default) leaves the
        credentials' own identity untouched, and verification falls
        back to the dialed address only when the material carries no
        identity either.
    :param reloadable:
        Whether ``factory`` is consulted on every resolution. If ``False``
        (default), ``factory`` is called once at construction and the result
        is fixed for the provider's lifetime.
    :param fresh_for:
        How long resolved material is served before a refresh is triggered,
        bounding how often ``factory`` runs and, with it, how quickly a
        rotation is adopted. One second by default, which caps ``factory``
        at one call per second per provider per process while keeping
        rotation adoption invisible against any real rotation schedule.
        Ignored when not ``reloadable``. Lower it when credentials are
        short-lived or revocation must land quickly; raise it when
        ``factory`` is expensive.
    :param stale_for:
        How much longer past ``fresh_for`` material stays servable while
        refreshes are attempted; see `Refreshing`. ``None`` (default) means
        a persistently failing ``factory`` never fails a dispatch; set it to
        stop presenting material whose age exceeds what the deployment
        tolerates. Ignored when not ``reloadable``.
    :param timeout:
        How long a caller waits on a resolution another caller started
        before giving up on it and serving whatever is still servable.
        ``None`` (default) waits indefinitely. See `Refreshing` for what the
        bound does and does not cover.
    :raises:
        Whatever ``factory`` raises, when the provider is not ``reloadable``
        and the construction-time resolution fails.
    """

    def __init__(
        self,
        factory: Callable[[], WorkerCredentials],
        *,
        identity: str | None = None,
        reloadable: bool = False,
        fresh_for: timedelta = _RESOLVE_DEBOUNCE,
        stale_for: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> None:
        self._factory = factory
        self._identity = _normalize_identity(identity)
        self._reloadable = bool(reloadable)
        # The material each refresh replaced comes from Refreshing, which
        # owns it.
        self._churn_count: int = 0
        self._churn_throttle = Throttle(_CHURN_WARNING_INTERVAL_S)
        # One storage path for both kinds: a non-reloadable provider is a
        # Refreshing whose value never ages out, so ``factory`` runs exactly
        # once.
        self._refreshing: Refreshing[WorkerCredentials] = Refreshing(
            self._derive,
            fresh_for=fresh_for if self._reloadable else None,
            stale_for=stale_for if self._reloadable else None,
            timeout=timeout,
            on_refresh=self._note_churn,
            on_error=self._note_refresh_error,
        )
        if not self._reloadable:
            # Fail-fast resolution — see the class docstring.
            self._refreshing.get()

    def __getstate__(self) -> dict:
        """Return the picklable state, serializing the factory by value.

        .. rubric:: Implementation notes

        ``factory`` is cloudpickled rather than left to the ambient pickler
        because the provider crosses into a worker subprocess through
        `multiprocessing`, whose spawn path uses plain `pickle` — which
        cannot take the closures and lambdas a factory commonly is. Churn
        accounting is per-process and resets with the copy.
        """
        state = self.__dict__.copy()
        state["_factory"] = cloudpickle.dumps(self._factory)
        state["_churn_count"] = 0
        state["_churn_throttle"] = Throttle(_CHURN_WARNING_INTERVAL_S)
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore pickled state, deserializing the factory."""
        state = dict(state)
        state["_factory"] = cloudpickle.loads(state["_factory"])
        self.__dict__.update(state)

    @property
    def identity(self) -> str | None:
        """The expected server identity, or ``None``."""
        return self._identity

    @property
    def reloadable(self) -> bool:
        """Whether the credentials can change over the provider's lifetime."""
        return self._reloadable

    @classmethod
    def coerce(
        cls, credentials: WorkerCredentials | WorkerCredentialsProvider | None
    ) -> WorkerCredentialsProvider | None:
        """Return ``credentials`` as a provider.

        A bare `WorkerCredentials` is wrapped via `WorkerCredentials.as_provider`;
        an existing provider (i.e., any object exposing a ``credentials``
        attribute and a boolean ``reloadable``, which includes duck-typed
        providers) or ``None`` passes through unchanged. Anything else fails
        fast here rather than with an opaque `AttributeError` mid-dispatch.

        :param credentials:
            A bare value, a provider, or ``None``.
        :returns:
            A `WorkerCredentialsProvider`, or ``None`` when ``credentials``
            is ``None``.
        :raises TypeError:
            If ``credentials`` is none of the accepted shapes, e.g., a raw
            `grpc.ChannelCredentials`.

        .. rubric:: Implementation notes

        Public so third-party `WorkerFactory` and `WorkerLike`
        implementations accepting the same
        ``WorkerCredentials | WorkerCredentialsProvider | None`` union get
        the canonical normalization — and its fail-fast validation —
        without reimplementing it.
        """
        if credentials is None:
            return None
        if isinstance(credentials, WorkerCredentials):
            return credentials.as_provider()
        if hasattr(credentials, "credentials") and isinstance(
            getattr(credentials, "reloadable", None), bool
        ):
            return credentials
        raise TypeError(
            "credentials must be a WorkerCredentials, a provider exposing "
            "'credentials' and a boolean 'reloadable', or None; got "
            f"{type(credentials).__name__}. A raw grpc.ChannelCredentials is "
            "no longer accepted — wrap the PEM material in a "
            "WorkerCredentials instead."
        )

    @property
    def credentials(self) -> Refreshing[WorkerCredentials]:
        """The current credentials, as a resource that reads either way.

        Material carries the provider's ``identity``.

        .. rubric:: Implementation notes

        `Refreshing` documents the full contract; the two reads that matter
        here are ``await provider.credentials`` from an event loop, which
        never blocks it, and ``provider.credentials.get()`` from a thread
        with no loop — a gRPC handshake thread being the case this exists
        for. ``provider.credentials.refresh()`` bypasses the freshness
        interval when material must be adopted immediately, e.g., after a
        revocation.
        """
        return self._refreshing

    def _derive(self) -> WorkerCredentials:
        """Invoke ``factory`` and return its material with identity applied.

        The loader handed to `Refreshing`. Runs off the lock and off the
        event loop.
        """
        return self._apply(self._factory())

    def _note_churn(
        self, material: WorkerCredentials, previous: WorkerCredentials | None
    ) -> None:
        """Warn when consecutive refreshes keep producing unequal material.

        A single rotation is expected and rare; a ``factory`` whose output
        never compares equal defeats channel pooling, costing a TLS
        handshake and a pooled channel per change. Rate-limited through
        `Throttle` so a persistently churning factory does not flood the
        log; the consecutive-count threshold is a pre-filter ahead of it.

        .. rubric:: Implementation notes

        Reported through `Refreshing`'s ``on_refresh`` hook rather than by
        shadowing the previous material here, which is also why this
        accounting needs no lock of its own. The comparison is meaningful
        only because `WorkerCredentials` compares by value, which is why the
        detection lives here and not in `Refreshing`, whose values are
        arbitrary.
        """
        if previous is None:
            return
        if material != previous:
            self._churn_count += 1
        else:
            self._churn_count = 0
            # Closing the incident — see `Throttle.discard`.
            self._churn_throttle.discard()
            return
        if self._churn_count < _CHURN_WARNING_THRESHOLD:
            return
        emit, suppressed = self._churn_throttle.due()
        if not emit:
            return
        _log.warning(
            "Reloadable credential factory produced %d consecutive "
            "unequal snapshots; factory output that never compares "
            "equal defeats channel pooling — each change costs a TLS "
            "handshake and a pooled channel. Return cached material "
            "for unchanged inputs.%s",
            self._churn_count,
            f" ({suppressed} similar warnings suppressed)" if suppressed else "",
        )

    def _note_refresh_error(self, error: BaseException, served_stale: bool) -> None:
        """Log a refresh failure that previous material absorbed.

        Reported for `Refreshing`'s ``on_error`` hook. A failure that left
        previous material serving is invisible to callers, so the log is the
        only signal it happened. A first resolution has nothing to absorb it
        and raises to every waiting caller instead, which needs no log here.

        .. rubric:: Implementation notes

        Rotation is daily-scale and the previous certificate is
        overwhelmingly still valid, so failing dispatches over a transient
        factory blip would be strictly worse than serving on.
        """
        if served_stale:
            _log.warning(
                "Reloadable credential refresh failed; serving previous material",
                exc_info=error,
            )

    def _apply(self, credentials: WorkerCredentials) -> WorkerCredentials:
        """Return ``credentials`` with the provider's identity applied."""
        if self._identity is None:
            return credentials
        return replace(credentials, identity=self._identity)


@contextmanager
def credentials_scope(
    credentials: WorkerCredentials | WorkerCredentialsProvider,
) -> Iterator[None]:
    """Bind ``credentials`` as the ambient credentials for the enclosed scope.

    The value is normalized through `WorkerCredentialsProvider.coerce` on
    entry, so the scope always carries a `WorkerCredentialsProvider`; an
    unsupported shape raises `TypeError` here. The binding is reset on exit.

    .. rubric:: Implementation notes

    Normalizing on entry rather than on read is what lets
    `current_credentials` return the binding without re-coercing it.
    """
    token = _current.set(WorkerCredentialsProvider.coerce(credentials))
    try:
        yield
    finally:
        _current.reset(token)


def current_credentials() -> WorkerCredentialsProvider | None:
    """Return the ambient credential provider, or ``None`` if unset."""
    return _current.get()


def _normalize_identity(identity: str | None) -> str | None:
    """Collapse an empty or whitespace-only identity to ``None``.

    A blank identity would otherwise emit an empty
    ``grpc.ssl_target_name_override`` and fail verification opaquely;
    ``None`` instead selects the address-based path, the intended
    "no identity configured" behavior.

    :param identity:
        The configured identity, or ``None``.
    :returns:
        The stripped identity, or ``None`` if blank.
    """
    if identity is None:
        return None
    identity = identity.strip()
    return identity or None
