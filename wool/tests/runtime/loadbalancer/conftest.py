import pytest


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

        def make_dispatch_side_effect(metadata):
            async def dispatch_side_effect(task, *, timeout=None):
                del timeout
                workers_attempted.append(metadata)
                side_effect = next(side_effect_iterator)
                if isinstance(side_effect, Exception):
                    raise side_effect
                else:
                    tasks_dispatched.append(task)
                    return side_effect

            return dispatch_side_effect

        return make_dispatch_side_effect

    return factory
