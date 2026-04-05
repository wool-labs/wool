import contextvars

import wool


def test_public_symbol_accessibility_should_import_every_symbol():
    """Test public symbol accessibility from wool package.

    Given:
        The wool package with a defined __all__ list
    When:
        Checking for accessibility of each public symbol
    Then:
        All symbols in __all__ should be importable from the package
    """
    # Arrange
    expected_symbols = wool.__all__

    # Act & assert
    for symbol in expected_symbols:
        assert hasattr(wool, symbol), f"Symbol '{symbol}' not found in wool package"


def test_public_api_completeness_should_match_expected_surface():
    """Test public API completeness of wool package.

    Given:
        The wool package public API definition
    When:
        Validating the complete __all__ list contents
    Then:
        It should match the expected public interface exactly
    """
    # Arrange
    expected_public_api = {
        "RpcError",
        "TransientRpcError",
        "UnexpectedResponse",
        "WorkerConnection",
        "ChainContention",
        "ChainSerializationError",
        "TaskFactoryDisplaced",
        "Context",
        "ContextVar",
        "ContextVarCollision",
        "copy_context",
        "RuntimeContext",
        "SerializationError",
        "SerializationWarning",
        "Token",
        "WoolError",
        "WoolWarning",
        "install_task_factory",
        "to_thread",
        "DispatchingLoadBalancerLike",
        "LoadBalancerContextLike",
        "LoadBalancerContextView",
        "LoadBalancerLike",
        "NoWorkersAvailable",
        "RoundRobinLoadBalancer",
        "Task",
        "TaskException",
        "current_task",
        "routine",
        "Serializer",
        "BackpressureContext",
        "AdvertiseHostError",
        "BackpressureLike",
        "BoundWorkerFactory",
        "LocalWorker",
        "LoopbackAdvertisementWarning",
        "Worker",
        "WorkerCredentials",
        "WoolError",
        "WoolWarning",
        "WorkerFactory",
        "WorkerLike",
        "WorkerPool",
        "WorkerProxy",
        "WorkerService",
        "Discovery",
        "DiscoveryEvent",
        "DiscoveryEventType",
        "DiscoveryLike",
        "DiscoveryPublisherLike",
        "DiscoverySubscriberLike",
        "IneffectiveLeaseWarning",
        "IneffectiveQuorumTimeoutWarning",
        "LanDiscovery",
        "LocalDiscovery",
        "PredicateFunction",
        "WorkerMetadata",
        "Factory",
        "UndefinedType",
    }

    # Act
    actual_public_api = set(wool.__all__)

    # Assert
    assert actual_public_api == expected_public_api


def test_removed_symbols_should_not_be_accessible():
    """Test that symbols removed in this PR are not accessible from wool.

    Given:
        The wool package after the stdlib-context refactor.
    When:
        Checking for the presence of each removed symbol by name.
    Then:
        It should not expose any of the four removed symbols as attributes.
    """
    # Arrange
    removed_names = [
        "Chain",
        "current_context",
        "create_task",
        "ContextAlreadyBound",
    ]

    # Act & assert
    for name in removed_names:
        assert not hasattr(wool, name), (
            f"Removed symbol '{name}' is still accessible on the wool package"
        )


def test_context_primitives_should_be_verbatim_stdlib_reexports():
    """Test wool.Context and wool.copy_context are the stdlib objects themselves.

    Given:
        The wool package, which builds on contextvars rather than
        replacing it.
    When:
        wool.Context and wool.copy_context are compared by identity to
        their contextvars counterparts.
    Then:
        Each should be the very same object — verbatim re-exports, not
        wool-native wrappers — so a contextvars-to-wool migration keeps
        stdlib Context semantics exactly.
    """
    # Arrange, act, & assert
    assert wool.Context is contextvars.Context
    assert wool.copy_context is contextvars.copy_context


def test_token_should_not_be_the_stdlib_token_alias():
    """Test wool.Token is a distinct wrapper, not the stdlib Token.

    Given:
        The wool package, whose wool.Token is a picklable wrapper rather
        than a re-export of contextvars.Token.
    When:
        wool.Token is compared by identity to contextvars.Token.
    Then:
        It should not be the very same object — wool.Token must never
        regress into an alias of the stdlib token.
    """
    # Arrange, act, & assert
    assert wool.Token is not contextvars.Token


def test_wool_error_should_subclass_exception_not_runtime_error():
    """Test WoolError descends from Exception and nothing narrower.

    Given:
        The wool.WoolError umbrella class.
    When:
        Its position in the builtin exception hierarchy is checked.
    Then:
        It should subclass Exception directly — not RuntimeError — so
        Wool-domain signals never match broad handlers written for
        stdlib runtime errors.
    """
    # Arrange, act, & assert
    assert issubclass(wool.WoolError, Exception)
    assert not issubclass(wool.WoolError, RuntimeError)


def test_serializer_singleton_should_return_same_instance():
    """Test wool.__serializer__ is a stable module-level singleton.

    Given:
        The wool package.
    When:
        wool.__serializer__ is accessed twice.
    Then:
        Each access should return the same instance.
    """
    # Arrange, act, & assert
    assert wool.__serializer__ is wool.__serializer__


def test_serializer_singleton_should_satisfy_serializer_protocol():
    """Test wool.__serializer__ satisfies the wool.Serializer protocol.

    Given:
        The wool.__serializer__ singleton and the wool.Serializer protocol.
    When:
        isinstance is evaluated against the runtime-checkable protocol.
    Then:
        It should return True.
    """
    # Arrange, act, & assert
    assert isinstance(wool.__serializer__, wool.Serializer)
