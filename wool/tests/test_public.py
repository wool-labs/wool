import wool


def test_public():
    for symbol in wool.__all__:
        assert hasattr(wool, symbol), (
            f"Symbol '{symbol}' not found in wool package"
        )

    assert wool.__all__ == [
        "TaskException",
        "Future",
        "Task",
        "TaskEvent",
        "TaskEventCallback",
        "WorkerPool",
        "WorkerPoolSession",
        "WorkerPoolCommand",
        "Scheduler",
        "__log_format__",
        "__log_level__",
        "__wool_session__",
        "cli",
        "current_task",
        "pool",
        "session",
        "task",
    ]
