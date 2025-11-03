import pytest
from pytest_mock import MockerFixture

from wool.core.discovery.base import WorkerInfo


@pytest.fixture
def dispatch_side_effect_factory():
    """Factory for creating dispatch side effect functions.

    Returns a function that creates make_dispatch_side_effect functions with
    the provided dependencies.
    """

    def factory(workers_attempted: list, side_effect_iterator, tasks_dispatched: list):
        """Create a make_dispatch_side_effect function.

        :param workers_attempted:
            List to track which workers attempted the dispatch.
        :param side_effect_iterator:
            Iterator over side effects (exceptions or return values).
        :param tasks_dispatched:
            List to track successfully dispatched task results.

        :returns:
            A function that creates dispatch side effect functions for workers.
        """

        def make_dispatch_side_effect(worker_info):
            async def dispatch_side_effect(task, *, timeout=None):
                del timeout
                workers_attempted.append(worker_info)
                side_effect = next(side_effect_iterator)
                if isinstance(side_effect, Exception):
                    raise side_effect
                else:
                    tasks_dispatched.append(task)
                    return side_effect

            return dispatch_side_effect

        return make_dispatch_side_effect

    return factory


@pytest.fixture
def mock_connection_resource_factory(mocker: MockerFixture):
    """Factory for creating mock connection resource factories.

    Returns a function that creates connection resource factories for workers.
    """

    def factory(worker_info: WorkerInfo, mock_workers: dict):
        """Create a connection resource factory for the specified worker.

        :param worker_info:
            The worker info for which to create the factory.
        :param mock_workers:
            Dictionary mapping worker info to mock connections.

        :returns:
            A callable that returns a mock connection resource.
        """
        mock_connection_resource = mocker.MagicMock(
            __aenter__=mocker.AsyncMock(return_value=mock_workers[worker_info]),
            __aexit__=mocker.AsyncMock(return_value=None),
        )
        return lambda: mock_connection_resource

    return factory
