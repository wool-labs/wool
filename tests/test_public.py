import wool


def test_public():
    # Get all symbols from the wool module
    for symbol in wool.__all__:
        assert hasattr(wool, symbol), (
            f"Symbol '{symbol}' not found in wool module"
        )

    assert set(wool.__all__) == {
        "WoolTaskException",
        "WoolFuture",
        "WoolTask",
        "WoolTaskEvent",
        "WoolTaskEventCallback",
        "WoolPool",
        "WoolClient",
        "current_task",
        "__log_format__",
        "__log_level__",
        "task",
    }
