import wool.locking


def test_public():
    for symbol in wool.locking.__all__:
        assert hasattr(wool.locking, symbol), (
            f"Symbol '{symbol}' not found in wool.locking package"
        )

    assert wool.locking.__all__ == [
        "LockPool",
        "LockPoolSession",
        "lock",
        "pool",
        "session",
    ]
