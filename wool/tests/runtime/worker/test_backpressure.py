from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.routine.task import Task
from wool.runtime.routine.task import WorkerProxyLike
from wool.runtime.worker.service import BackpressureContext
from wool.runtime.worker.service import BackpressureLike

from .conftest import PicklableMock


def _make_task():
    mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
    return Task(
        id=uuid4(),
        callable=lambda: None,
        args=(),
        kwargs={},
        proxy=mock_proxy,
    )


class TestBackpressureContext:
    """Tests for :class:`BackpressureContext` dataclass."""

    def test___init___with_valid_fields(self):
        """Test BackpressureContext instantiation.

        Given:
            An active task count and a Task instance
        When:
            BackpressureContext is instantiated
        Then:
            It should store both fields correctly
        """
        # Arrange
        task = _make_task()

        # Act
        ctx = BackpressureContext(active_task_count=3, task=task)

        # Assert
        assert ctx.active_task_count == 3
        assert ctx.task is task

    def test___init___is_frozen(self):
        """Test BackpressureContext is immutable.

        Given:
            A BackpressureContext instance
        When:
            An attribute is reassigned
        Then:
            It should raise FrozenInstanceError
        """
        # Arrange
        ctx = BackpressureContext(active_task_count=0, task=_make_task())

        # Act & Assert
        with pytest.raises(AttributeError):
            ctx.active_task_count = 5

    def test___eq___with_equal_fields(self):
        """Test BackpressureContext equality with identical fields.

        Given:
            Two BackpressureContext instances with the same active_task_count and task
        When:
            Compared with ==
        Then:
            It should return True
        """
        # Arrange
        task = _make_task()
        ctx_a = BackpressureContext(active_task_count=2, task=task)
        ctx_b = BackpressureContext(active_task_count=2, task=task)

        # Act & Assert
        assert ctx_a == ctx_b

    def test___eq___with_different_task_count(self):
        """Test BackpressureContext inequality with different active_task_count.

        Given:
            Two BackpressureContext instances with different active_task_count
        When:
            Compared with ==
        Then:
            It should return False
        """
        # Arrange
        task = _make_task()
        ctx_a = BackpressureContext(active_task_count=0, task=task)
        ctx_b = BackpressureContext(active_task_count=1, task=task)

        # Act & Assert
        assert ctx_a != ctx_b

    @given(count=st.integers(min_value=0, max_value=10_000))
    @settings(max_examples=50)
    def test___init___with_arbitrary_task_count(self, count):
        """Test BackpressureContext stores arbitrary non-negative active_task_count.

        Given:
            Any non-negative integer for active_task_count
        When:
            BackpressureContext is instantiated
        Then:
            It should store the value exactly
        """
        # Arrange
        task = _make_task()

        # Act
        ctx = BackpressureContext(active_task_count=count, task=task)

        # Assert
        assert ctx.active_task_count == count


class TestBackpressureLike:
    """Tests for :class:`BackpressureLike` protocol."""

    def test_sync_callable_satisfies_protocol(self):
        """Test sync callable satisfies BackpressureLike.

        Given:
            A sync function with the correct signature
        When:
            Checked against BackpressureLike
        Then:
            It should be recognized as an instance
        """

        # Arrange
        def hook(ctx: BackpressureContext) -> bool:
            return True

        # Act & Assert
        assert isinstance(hook, BackpressureLike)

    def test_async_callable_satisfies_protocol(self):
        """Test async callable satisfies BackpressureLike.

        Given:
            An async function with the correct signature
        When:
            Checked against BackpressureLike
        Then:
            It should be recognized as an instance
        """

        # Arrange
        async def hook(ctx: BackpressureContext) -> bool:
            return True

        # Act & Assert
        assert isinstance(hook, BackpressureLike)

    def test_callable_class_satisfies_protocol(self):
        """Test callable class instance satisfies BackpressureLike.

        Given:
            A class with a __call__ method
        When:
            An instance is checked against BackpressureLike
        Then:
            It should be recognized as an instance
        """

        # Arrange
        class Hook:
            def __call__(self, ctx: BackpressureContext) -> bool:
                return ctx.active_task_count >= 4

        # Act & Assert
        assert isinstance(Hook(), BackpressureLike)

    def test_non_callable_does_not_satisfy_protocol(self):
        """Test non-callable does not satisfy BackpressureLike.

        Given:
            A plain string (not callable)
        When:
            Checked against BackpressureLike
        Then:
            It should not be recognized as an instance
        """
        # Act & Assert
        assert not isinstance("not-a-hook", BackpressureLike)
