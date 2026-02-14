import wool


def test_public_symbol_accessibility():
    """
    GIVEN the wool package with a defined __all__ list
    WHEN checking for accessibility of each public symbol
    THEN all symbols in __all__ should be importable from the package.
    """
    # Arrange
    expected_symbols = wool.__all__

    # Act & Assert
    for symbol in expected_symbols:
        assert hasattr(wool, symbol), f"Symbol '{symbol}' not found in wool package"


def test_public_api_completeness():
    """
    GIVEN the wool package public API definition
    WHEN validating the complete __all__ list contents
    THEN it should match the expected public interface exactly.
    """
    # Arrange
    expected_public_api = [
        # Connection
        "RpcError",
        "TransientRpcError",
        "UnexpectedResponse",
        "WorkerConnection",
        # Context
        "RuntimeContext",
        # Load balancing
        "ConnectionResourceFactory",
        "LoadBalancerContextLike",
        "LoadBalancerLike",
        "NoWorkersAvailable",
        "RoundRobinLoadBalancer",
        # Work - New names (preferred)
        "Task",
        "WorkTaskEvent",
        "WorkTaskEventHandler",
        "WorkTaskEventType",
        "WorkTaskException",
        "current_task",
        "routine",
        "work",
        # Workers
        "LocalWorker",
        "Worker",
        "WorkerCredentials",
        "WorkerFactory",
        "WorkerLike",
        "WorkerPool",
        "WorkerProxy",
        "WorkerService",
        # Events
        "AsyncEventHandler",
        # Discovery
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
        # Typing
        "Factory",
    ]

    # Act
    actual_public_api = wool.__all__

    # Assert
    assert actual_public_api == expected_public_api
