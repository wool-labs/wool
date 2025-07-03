import debugpy
import pytest


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    if config.getoption("--wait-for-debugger"):
        print("Waiting for debugger to attach...")
        debugpy.listen(("0.0.0.0", 5678))
        debugpy.wait_for_client()
        print("Debugger attached.")


def pytest_addoption(parser):
    parser.addoption(
        "-D",
        "--wait-for-debugger",
        action="store_true",
        default=False,
        help="Wait for a debugpy client to attach before running tests",
    )
