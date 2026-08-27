"""Worker credential material and its resolution.

Provides `WorkerCredentials`, the PEM material a worker presents and
verifies against, and `WorkerCredentialsProvider`, which resolves that
material per use so a running pool can adopt rotated certificates.
"""

from __future__ import annotations

import logging
import os
import warnings
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import timedelta

import cloudpickle
import grpc

from wool.runtime.worker.exceptions import IneffectivePeersWarning
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

    def as_provider(
        self,
        *,
        peers: str | Iterable[str] | Callable[[str], bool] | None = None,
    ) -> WorkerCredentialsProvider:
        """Adapt these fixed credentials into a non-reloadable provider.

        For credentials that change over a process's lifetime, build a
        reloadable provider directly with a factory callable instead; the
        reload strategy then lives in ``factory``.

        :param peers:
            The peer names the client accepts, passed along to the
            `WorkerCredentialsProvider` constructor directly.
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
            ).as_provider(peers="wool-worker.svc")

            # Material that rotates out of band: a factory instead, called
            # again once per freshness interval.
            factory = functools.partial(
                WorkerCredentials.from_files,
                "certs/ca-cert.pem",
                "certs/worker-key.pem",
                "certs/worker-cert.pem",
            )
            provider = WorkerCredentialsProvider(
                factory, peers="wool-worker.svc", reloadable=True
            )

            # Either goes anywhere ``credentials=`` is accepted.
            async with WorkerPool(
                spawn=4, identity="wool-worker.svc", credentials=provider
            ):
                ...
        """
        return WorkerCredentialsProvider(lambda: self, peers=peers)

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


# public
class WorkerCredentialsProvider:
    """A credential provider backed by a user-supplied factory callable.

    The provider mediates between a source of credential material and
    the two consumers of it, the worker server and the dispatch client.
    Three terms recur. *Material* is a `WorkerCredentials` value. A
    *resolution* is one invocation of ``factory``, which returns the
    current material. The *policy* is the admission rule compiled from
    ``peers``, which names the workers a client built on this provider
    will accept. Where material originates (e.g., a file, a secrets
    manager, a lease) is ``factory``'s concern alone; the provider
    prescribes no strategy for obtaining it. `credentials` exposes the
    current material, and `WorkerCredentials.as_provider` constructs the
    degenerate provider over fixed material.

    A provider is either fixed or reloadable. A fixed provider resolves
    exactly once, at construction, and serves that material for its
    lifetime; a failing ``factory`` therefore fails construction rather
    than the first handshake. A reloadable provider re-resolves on the
    schedule `Refreshing` governs, so rotated material is adopted
    without a restart.

    The provider is a value and crosses process boundaries by
    cloudpickle. ``factory`` MUST therefore be cloudpickle-serializable,
    though it need not be importable by name; a lambda or a closure
    satisfies the requirement, and `WorkerCredentials.as_provider` relies
    on exactly that. A copy carries ``factory`` rather than material and
    resolves once on its first read, so a factory closing over fixed
    material reproduces it exactly, whereas one that reads its source
    anew may observe a later state than the original did.

    Resolutions are compared by value. Material that compares equal
    across resolutions reuses pooled channels; material that does not
    incurs a TLS handshake and a new pooled channel, and consecutive
    unequal resolutions are reported by warning, since a ``factory``
    whose output never compares equal defeats pooling entirely.

    A reloadable ``factory`` MUST tolerate concurrent invocation from
    gRPC handshake threads, and MUST NOT read its own provider's
    `credentials`: such a read joins the resolution it is executing
    within and deadlocks.

    :param factory:
        A zero-argument callable returning the current `WorkerCredentials`.
    :param peers:
        The names this client accepts from the workers it dials, each
        verified in place of the dialed address: a single name, an
        iterable of names, or a predicate over a candidate name. ``None``
        (default), a blank name, and an empty iterable each leave the
        policy unconfigured, which admits every worker and pins nothing;
        the latter two are accompanied by an `IneffectivePeersWarning`.
        The policy is outbound only: it decides whom a client will dial,
        never whom a worker will serve. URI-form names such as SPIFFE IDs
        are not currently supported.

        A predicate MUST be cloudpickle-serializable, MUST be cheap and
        non-blocking, and MUST NOT raise; a predicate that raises is
        treated as having rejected the worker. Admission is the whole of
        what ``peers`` decides — see `WorkerProxy` for the two admission
        states and what each pins.
    :param reloadable:
        Whether ``factory`` is consulted on every resolution. When
        ``False`` (default), ``factory`` is invoked once at construction
        and the result is fixed for the provider's lifetime.
    :param fresh_for:
        The interval for which resolved material is served before a
        refresh is triggered; see `Refreshing`. It bounds the rate at which
        ``factory`` runs and, with it, the latency with which a rotation is
        adopted. The default of one second caps ``factory`` at one call per
        second per provider per process while keeping adoption latency
        negligible against any real rotation schedule. Ignored unless
        ``reloadable``. Lower it when material is short-lived or
        revocation must take effect promptly; raise it when ``factory``
        is expensive.
    :param stale_for:
        The interval beyond ``fresh_for`` for which material remains
        servable while refreshes are attempted; see `Refreshing`.
        ``None`` (default) means a persistently failing ``factory`` never
        fails a dispatch; set it to withdraw material whose age exceeds
        what the deployment tolerates. Ignored unless ``reloadable``.
    :param timeout:
        The interval a caller waits on a resolution another caller
        started before abandoning it and serving whatever remains
        servable; see `Refreshing`. ``None`` (default) waits indefinitely.
    :raises:
        Whatever ``factory`` raises, when the provider is not
        ``reloadable`` and the construction-time resolution fails.

    .. rubric:: Implementation notes

    ``peers`` compiles once into a `_PeerPolicy`, modelled on go-spiffe's
    ``Authorizer``; the predicate form is the extension point for pattern
    acceptance. A predicate executes synchronously inside the proxy's
    admission loop, whose ``async for`` is unguarded, so an exception
    escaping it would terminate that loop and leave the proxy admitting
    and evicting nothing for the remainder of its life. Reading a raising
    predicate as a rejection is the conservative interpretation of a
    policy that could not decide, and it keeps a caller-supplied seam
    from terminating the loop that invoked it.

    Nothing inbound consults the policy: a worker accepts any caller
    holding a certificate from its configured authority, and
    authenticating inbound callers by name is separate work. gRPC's
    client-side verifier never consults URI SANs (the forms it does
    consult are on `WorkerMetadata`'s ``identity``), which is why a
    URI-form name passes normalization and admission and then fails at
    the handshake.
    """

    def __init__(
        self,
        factory: Callable[[], WorkerCredentials],
        *,
        peers: str | Iterable[str] | Callable[[str], bool] | None = None,
        reloadable: bool = False,
        fresh_for: timedelta = _RESOLVE_DEBOUNCE,
        stale_for: timedelta | None = None,
        timeout: timedelta | None = None,
    ) -> None:
        self._factory = factory
        self._policy = _normalize_peers(peers)
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

        ``factory`` is cloudpickled here rather than left to the ambient
        pickler because the provider reaches a worker subprocess through
        `multiprocessing`, whose spawn path uses plain `pickle`. The policy
        takes the same path for the same reason: a predicate is commonly a
        lambda, and a `frozenset` survives cloudpickle unchanged, so one path
        serves both shapes. Churn accounting is per-process and resets with
        the copy.
        """
        state = self.__dict__.copy()
        state["_factory"] = cloudpickle.dumps(self._factory)
        state["_policy"] = cloudpickle.dumps(self._policy)
        state["_churn_count"] = 0
        state["_churn_throttle"] = Throttle(_CHURN_WARNING_INTERVAL_S)
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore pickled state, deserializing the factory and policy."""
        state = dict(state)
        state["_factory"] = cloudpickle.loads(state["_factory"])
        state["_policy"] = cloudpickle.loads(state["_policy"])
        self.__dict__.update(state)

    @property
    def peers(self) -> frozenset[str] | Callable[[str], bool] | None:
        """The policy's value, or ``None`` when no policy is configured."""
        return self._policy.value if self._policy is not None else None

    @property
    def reloadable(self) -> bool:
        """Whether the provider re-resolves after construction."""
        return self._reloadable

    @property
    def credentials(self) -> Refreshing[WorkerCredentials]:
        """The current material as a `Refreshing` resource.

        .. rubric:: Implementation notes

        The resource admits two reads. Awaiting it from an event loop
        returns the servable material without suspending on a slow
        ``factory``; calling `Refreshing.get` from a thread with no loop
        performs a due refresh itself and returns the result.
        `Refreshing.refresh` forces a new resolution when material must
        be adopted at once, e.g., after a revocation. `Refreshing` owns
        the full contract.

        The synchronous read exists for gRPC's per-handshake certificate
        fetcher, a callback the C-core invokes from its own thread, off
        any event loop.
        """
        return self._refreshing

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

    def accepts_peer(self, peer: str | None) -> bool:
        """Report whether a worker advertising ``peer`` is admitted by the policy.

        Vacuously ``True`` when no policy is configured. See `WorkerProxy`
        for the two admission states and what each pins.

        :param peer:
            The name a worker advertised, or ``None`` when it advertised
            none.
        :returns:
            ``True`` if the policy admits the worker, otherwise ``False``
        """
        if self._policy is None:
            return True
        if (peer := normalize_peer(peer)) is None:
            return False
        return self._policy.accepts(peer)

    def describe_peers(self) -> str:
        """Describe the accepted names for an admission diagnostic.

        :returns:
            The accepted names, a note that a predicate decides, or an empty
            string if no policy is configured.
        """
        return "" if self._policy is None else self._policy.render()

    def _derive(self) -> WorkerCredentials:
        """Invoke ``factory`` and return its material verbatim.

        The loader handed to `Refreshing`; it runs off the lock and off
        the event loop, and applies nothing to what ``factory`` returns.
        """
        return self._factory()

    def _note_churn(
        self, material: WorkerCredentials, previous: WorkerCredentials | None
    ) -> None:
        """Warn when consecutive resolutions keep producing unequal material.

        A single rotation is expected and rare; a ``factory`` whose output
        never compares equal defeats channel pooling. The warning is rate-limited
        through `Throttle` so a persistently churning factory does not flood the
        log, with the consecutive-count threshold as a pre-filter ahead of it.

        .. rubric:: Implementation notes

        Reported through `Refreshing`'s ``on_refresh`` hook rather than by
        shadowing the previous material here, which is also why this
        accounting needs no lock of its own. The comparison is meaningful
        only because `WorkerCredentials` compares by value, which is why
        the detection lives here and not in `Refreshing`, whose values are
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
            "unequal results; factory output that never compares "
            "equal defeats channel pooling — each change costs a TLS "
            "handshake and a pooled channel. Return cached material "
            "for unchanged inputs.%s",
            self._churn_count,
            f" ({suppressed} similar warnings suppressed)" if suppressed else "",
        )

    def _note_refresh_error(self, error: BaseException, served_stale: bool) -> None:
        """Log a refresh failure that previous material absorbed.

        Invoked through `Refreshing`'s ``on_error`` hook. A failure that
        left previous material serving is invisible to callers, so the
        log is the only signal that it occurred. A first resolution has
        nothing to absorb it and raises to every waiting caller instead,
        which needs no log here.

        .. rubric:: Implementation notes

        Rotation is daily-scale and the previous certificate is
        overwhelmingly still valid, so failing dispatches over a
        transient factory fault would be strictly worse than serving on.
        """
        if served_stale:
            _log.warning(
                "Reloadable credential refresh failed; serving previous material",
                exc_info=error,
            )


@dataclass(frozen=True)
class _PeerPolicy:
    """Which worker names a client accepts from the peers it dials.

    A policy admits any name it covers: one naming a single peer admits
    exactly that name, one naming several admits any of them, and a
    predicate admits whatever it answers ``True`` for.

    :param value:
        The accepted names as a frozenset, or a predicate over a
        candidate name.
    """

    value: frozenset[str] | Callable[[str], bool]

    def accepts(self, name: str) -> bool:
        """Report whether ``name`` is admitted by this policy."""
        if isinstance(self.value, frozenset):
            return name in self.value
        try:
            return bool(self.value(name))
        except Exception:
            # A raising predicate is a rejection — see the
            # `WorkerCredentialsProvider` implementation notes.
            _log.exception(
                "Peer-name predicate raised for %r; treating the peer as "
                "rejected. See WorkerCredentialsProvider's 'peers' parameter.",
                name,
            )
            return False

    def render(self) -> str:
        """Describe the accepted names for a diagnostic."""
        if isinstance(self.value, frozenset):
            return ", ".join(sorted(self.value))
        return "a peer-name predicate"


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


def normalize_peer(peer: str | None, *, parameter: str = "peer") -> str | None:
    """Collapse an empty or whitespace-only peer name to ``None``.

    A blank name is not a name; ``None`` is the "no name configured"
    state, whichever side of the connection the name belongs to.

    :param peer:
        The configured name, or ``None``.
    :param parameter:
        Which caller-facing parameter supplied the name. Names the side
        of the connection it belongs to, which decides what advice a
        rejection can honestly give: a caller who passed a collection to
        an accept-list wanted ``peers``, while one who passed a
        collection to an advertised ``identity`` wanted something else
        entirely and must not be sent across the connection to find it.
    :returns:
        The stripped name, or ``None`` if blank.
    :raises TypeError:
        If ``peer`` is neither a string nor ``None``.

    .. rubric:: Implementation notes

    The single home for normalizing one logical name, whatever supplied
    it. Routing every entry point through here is what makes a
    non-string fail the same way everywhere, rather than surfacing as an
    `AttributeError` from ``strip`` at whichever layer happened to
    receive it. On the client ``peer`` path a blank name would otherwise
    emit an empty ``grpc.ssl_target_name_override`` and fail verification
    opaquely, where ``None`` selects the address-based path.
    """
    if peer is None:
        return None
    if not isinstance(peer, str):
        hint = (
            " A collection of accepted names belongs in a provider's 'peers'."
            if parameter == "peer"
            else ""
        )
        raise TypeError(
            f"expected a single peer name or None for '{parameter}'; got "
            f"{type(peer).__name__}.{hint}"
        )
    peer = peer.strip()
    return peer or None


def _warn_unconfigured(peers: object) -> None:
    """Report a ``peers`` value that named nothing after normalization."""
    warnings.warn(
        f"peers={peers!r} names no peers after normalization, which leaves "
        "this provider unconfigured: advertisements are ignored and no name "
        "is pinned. Pass at least one name to gate on identity",
        IneffectivePeersWarning,
        stacklevel=4,
    )


def _normalize_peers(
    peers: str | Iterable[str] | Callable[[str], bool] | None,
) -> _PeerPolicy | None:
    """Compile ``peers`` into a policy, or ``None`` if none is configured.

    :param peers:
        A single name, an iterable of names, a predicate over a
        candidate name, or ``None``.
    :returns:
        The compiled `_PeerPolicy`, or ``None`` when nothing is
        configured.
    :raises TypeError:
        If ``peers`` is none of those shapes.

    .. rubric:: Implementation notes

    A blank name and an empty iterable both collapse to ``None`` — the
    "nothing configured" state — rather than to a policy accepting
    nothing, which would reject every peer and read as a silent outage.
    This extends the blank-collapses-to-None rule `normalize_peer`
    already applies to a single name.
    """
    if peers is None:
        return None
    if isinstance(peers, str):
        name = normalize_peer(peers, parameter="peers")
        if name is None:
            _warn_unconfigured(peers)
            return None
        return _PeerPolicy(frozenset({name}))
    if callable(peers):
        return _PeerPolicy(peers)
    if isinstance(peers, Iterable):
        names: set[str] = set()
        for each in peers:
            if (name := normalize_peer(each, parameter="peers")) is not None:
                names.add(name)
        if not names:
            _warn_unconfigured(peers)
            return None
        return _PeerPolicy(frozenset(names))
    raise TypeError(
        "peers must be a peer name, an iterable of names, a predicate "
        f"over a name, or None; got {type(peers).__name__}."
    )
