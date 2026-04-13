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
    expected_public_api = [
        "RpcError",
        "TransientRpcError",
        "UnexpectedResponse",
        "WorkerConnection",
        "Context",
        "ContextVar",
        "RuntimeContext",
        "Token",
        "current_context",
        "LoadBalancerContextLike",
        "LoadBalancerLike",
        "NoWorkersAvailable",
        "RoundRobinLoadBalancer",
        "Serializer",
        "Task",
        "TaskException",
        "current_task",
        "routine",
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
    ]

    # Act
    actual_public_api = wool.__all__

    # Assert
    assert actual_public_api == expected_public_api
