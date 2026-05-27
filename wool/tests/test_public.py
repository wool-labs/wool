import wool


def test_public_symbol_accessibility():
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


def test_public_api_completeness():
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
        "ContextDecodeWarning",
        "TaskFactoryDisplaced",
        "ContextVar",
        "ContextVarCollision",
        "RuntimeContext",
        "Token",
        "TokenLike",
        "install_task_factory",
        "to_thread",
        "LoadBalancerContextLike",
        "LoadBalancerLike",
        "NoWorkersAvailable",
        "RoundRobinLoadBalancer",
        "Task",
        "TaskException",
        "current_task",
        "routine",
        "Serializer",
        "BackpressureContext",
        "BackpressureLike",
        "LocalWorker",
        "Worker",
        "WorkerCredentials",
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
        "LanDiscovery",
        "LocalDiscovery",
        "PredicateFunction",
        "WorkerMetadata",
        "Factory",
    }

    # Act
    actual_public_api = set(wool.__all__)

    # Assert
    assert actual_public_api == expected_public_api


def test_removed_symbols_are_not_accessible():
    """Test that symbols removed in this PR are not accessible from wool.

    Given:
        The wool package after the stdlib-context refactor.
    When:
        Checking for the presence of each removed symbol by name.
    Then:
        It should not expose any of the five removed symbols as attributes.
    """
    # Arrange
    removed_names = [
        "Context",
        "current_context",
        "copy_context",
        "create_task",
        "ContextAlreadyBound",
    ]

    # Act & assert
    for name in removed_names:
        assert not hasattr(wool, name), (
            f"Removed symbol '{name}' is still accessible on the wool package"
        )


def test_serializer_singleton_identity():
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


def test_serializer_singleton_with_serializer_protocol():
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
