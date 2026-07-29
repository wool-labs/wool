from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from contextlib import ExitStack

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.utilities.noreentry import noreentry


# Test helpers (not fixtures)
class _SyncDummy:
    """Class with a sync @noreentry method."""

    @noreentry
    def run(self):
        return "ok"


class _AsyncDummy:
    """Class with an async @noreentry method."""

    @noreentry
    async def run(self):
        return "ok"


class _SyncContextDummy:
    """Sync context manager with both __enter__ and __exit__ guarded.

    Records the exception type handed to __exit__ so the arguments
    forwarded through the descriptor can be observed.
    """

    def __init__(self):
        self.exited_with = "not-exited"

    @noreentry
    def __enter__(self):
        return self

    @noreentry
    def __exit__(self, exc_type, exc, tb):
        self.exited_with = exc_type
        return False


class _AsyncContextDummy:
    """Async context manager with both __aenter__ and __aexit__ guarded.

    Records the exception type handed to __aexit__ so the arguments
    forwarded through the descriptor can be observed.
    """

    def __init__(self):
        self.exited_with = "not-exited"

    @noreentry
    async def __aenter__(self):
        return self

    @noreentry
    async def __aexit__(self, exc_type, exc, tb):
        self.exited_with = exc_type
        return False


class _SubclassedContextDummy(_SyncContextDummy):
    """Subclass inheriting a @noreentry-guarded __enter__ from its base."""


class _ArgumentDummy:
    """Class with a @noreentry method taking positional and keyword args."""

    @noreentry
    def run(self, first, *, second=None):
        return first, second


class _PayloadDummy:
    """Class with a @noreentry method echoing back its whole payload."""

    @noreentry
    def run(self, *args, **kwargs):
        return args, kwargs


class _MultiMethodDummy:
    """Class with multiple @noreentry methods."""

    @noreentry
    def first(self):
        return "first"

    @noreentry
    def second(self):
        return "second"


@noreentry
def _bare_function():
    """Module-level function decorated with @noreentry, owned by no class."""
    return "ok"


_payloads = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.text(),
    st.binary(),
    st.lists(st.integers()),
)

_keywords = st.text(
    alphabet=st.characters(min_codepoint=97, max_codepoint=122),
    min_size=1,
    max_size=8,
).filter(lambda name: name != "self")


class TestNoReentry:
    """Tests for the noreentry decorator."""

    def test_noreentry_sync_method_first_invocation(self):
        """Test sync method executes normally on first invocation.

        Given:
            A class with a sync @noreentry method.
        When:
            The method is called for the first time.
        Then:
            It should return normally.
        """
        # Arrange
        obj = _SyncDummy()

        # Act
        result = obj.run()

        # Assert
        assert result == "ok"

    def test_noreentry_sync_method_second_invocation_raises(self):
        """Test sync method raises RuntimeError on second invocation.

        Given:
            A class with a sync @noreentry method called once.
        When:
            The method is called a second time on the same instance.
        Then:
            It should raise RuntimeError.
        """
        # Arrange
        obj = _SyncDummy()
        obj.run()

        # Act & assert
        with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
            obj.run()

    @pytest.mark.asyncio
    async def test_noreentry_async_method_first_invocation(self):
        """Test async method executes normally on first invocation.

        Given:
            A class with an async @noreentry method.
        When:
            The method is awaited for the first time.
        Then:
            It should return normally.
        """
        # Arrange
        obj = _AsyncDummy()

        # Act
        result = await obj.run()

        # Assert
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_noreentry_async_method_second_invocation_raises(self):
        """Test async method raises RuntimeError on second invocation.

        Given:
            A class with an async @noreentry method awaited once.
        When:
            The method is awaited a second time on the same instance.
        Then:
            It should raise RuntimeError.
        """
        # Arrange
        obj = _AsyncDummy()
        await obj.run()

        # Act & assert
        with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
            await obj.run()

    def test_noreentry_separate_instances_independent(self):
        """Test instances track guard state independently.

        Given:
            Two instances of a class with a @noreentry method.
        When:
            The method is called on the first instance.
        Then:
            The method remains callable on the second instance.
        """
        # Arrange
        a = _SyncDummy()
        b = _SyncDummy()
        a.run()

        # Act
        result = b.run()

        # Assert
        assert result == "ok"

    def test_noreentry_error_message_qualname(self):
        """Test RuntimeError includes the method's qualified name.

        Given:
            A class with a @noreentry method called once.
        When:
            The method is called a second time.
        Then:
            The RuntimeError message should include the method's __qualname__.
        """
        # Arrange
        obj = _SyncDummy()
        obj.run()

        # Act & assert
        with pytest.raises(RuntimeError, match="_SyncDummy.run"):
            obj.run()

    def test_noreentry_preserves_coroutinefunction_check(self):
        """Test decorator preserves async function detection.

        Given:
            A class with an async @noreentry method.
        When:
            asyncio.iscoroutinefunction is called on the method.
        Then:
            It should return True.
        """
        # Act & assert
        assert asyncio.iscoroutinefunction(_AsyncDummy.run)

    def test_noreentry_preserves_wrapped_function_name(self):
        """Test decorator preserves the original function name.

        Given:
            A class with a @noreentry method.
        When:
            The decorated method's __name__ is inspected.
        Then:
            It should equal the original function name.
        """
        # Act & assert
        assert _SyncDummy.run.__name__ == "run"

    def test_noreentry_multiple_methods_independent(self):
        """Test guard on one method does not affect other methods.

        Given:
            A class with two @noreentry methods where the first
            has been guarded (called twice).
        When:
            The second method is called.
        Then:
            The second method executes normally.
        """
        # Arrange
        obj = _MultiMethodDummy()
        obj.first()
        with pytest.raises(RuntimeError):
            obj.first()

        # Act
        result = obj.second()

        # Assert
        assert result == "second"

    def test_noreentry_should_raise_type_error_when_called_unbound_without_instance(
        self,
    ):
        """Test an unbound call with no instance is rejected.

        Given:
            A @noreentry method accessed unbound through its class
        When:
            It is called with no arguments, so there is no instance to
            bind
        Then:
            It should raise TypeError.
        """
        # Arrange
        unbound = _SyncDummy.run

        # Act & assert
        with pytest.raises(
            TypeError, match="must be called with an instance of '_SyncDummy'"
        ):
            unbound()

    def test_noreentry_should_raise_type_error_when_bare_function_is_called(self):
        """Test a decorated module-level function rejects invocation.

        Given:
            A @noreentry-decorated module-level function, owned by no
            class
        When:
            The function is called
        Then:
            It should raise TypeError.
        """
        # Act & assert
        with pytest.raises(
            TypeError, match="only decorates methods, not bare functions"
        ):
            _bare_function()

    def test_noreentry_should_raise_type_error_when_called_with_foreign_instance(self):
        """Test an unbound call rejects an instance it does not own.

        Given:
            A @noreentry method accessed unbound through its class
        When:
            It is called with an object that is not an instance of the
            class that owns it
        Then:
            It should raise TypeError.
        """
        # Arrange
        unbound = _SyncDummy.run

        # Act & assert
        with pytest.raises(
            TypeError, match="must be called with an instance of '_SyncDummy'"
        ):
            unbound(_MultiMethodDummy())

    def test_noreentry_should_enter_context_when_guarded_enter_pushed_onto_stack(self):
        """Test a guarded sync manager enters through an ExitStack.

        Given:
            A sync context manager whose __enter__ is decorated with
            @noreentry
        When:
            The manager is pushed onto a contextlib.ExitStack
        Then:
            It should enter successfully and yield the manager.
        """
        # Arrange
        manager = _SyncContextDummy()

        # Act
        with ExitStack() as stack:
            entered = stack.enter_context(manager)

            # Assert
            assert entered is manager

    def test_noreentry_should_raise_when_guarded_enter_pushed_onto_stack_twice(self):
        """Test the guard still fires for a manager entered on a stack.

        Given:
            A sync context manager already entered through an ExitStack
        When:
            The same manager is pushed onto the stack a second time
        Then:
            It should raise RuntimeError.
        """
        # Arrange
        manager = _SyncContextDummy()

        # Act & assert
        with ExitStack() as stack:
            stack.enter_context(manager)
            with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
                stack.enter_context(manager)

    @pytest.mark.asyncio
    async def test_noreentry_should_enter_context_when_guarded_aenter_pushed_onto_stack(
        self,
    ):
        """Test a guarded async manager enters through an AsyncExitStack.

        Given:
            An async context manager whose __aenter__ is decorated with
            @noreentry
        When:
            The manager is pushed onto a contextlib.AsyncExitStack
        Then:
            It should enter successfully and yield the manager.
        """
        # Arrange
        manager = _AsyncContextDummy()

        # Act
        async with AsyncExitStack() as stack:
            entered = await stack.enter_async_context(manager)

            # Assert
            assert entered is manager

    @pytest.mark.asyncio
    async def test_noreentry_should_raise_when_guarded_aenter_pushed_onto_stack_twice(
        self,
    ):
        """Test the guard still fires for an async manager on a stack.

        Given:
            An async context manager already entered through an
            AsyncExitStack
        When:
            The same manager is pushed onto the stack a second time
        Then:
            It should raise RuntimeError.
        """
        # Arrange
        manager = _AsyncContextDummy()

        # Act & assert
        async with AsyncExitStack() as stack:
            await stack.enter_async_context(manager)
            with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
                await stack.enter_async_context(manager)

    def test_noreentry_should_enter_context_when_guarded_enter_used_in_with(self):
        """Test a guarded manager works in a plain with statement.

        Given:
            A sync context manager whose __enter__ is decorated with
            @noreentry
        When:
            The manager is used in a plain with statement
        Then:
            It should enter and yield the manager.
        """
        # Arrange
        manager = _SyncContextDummy()

        # Act
        with manager as entered:
            # Assert
            assert entered is manager

    @pytest.mark.asyncio
    async def test_noreentry_should_enter_context_when_guarded_aenter_used_in_with(self):
        """Test a guarded manager works in a plain async with statement.

        Given:
            An async context manager whose __aenter__ is decorated with
            @noreentry
        When:
            The manager is used in a plain async with statement
        Then:
            It should enter and yield the manager.
        """
        # Arrange
        manager = _AsyncContextDummy()

        # Act
        async with manager as entered:
            # Assert
            assert entered is manager

    def test_noreentry_should_forward_exception_details_when_exit_stack_unwinds(self):
        """Test a guarded __exit__ receives the exception forwarded to it.

        Given:
            A context manager whose __enter__ and __exit__ are both
            decorated with @noreentry
        When:
            An exception is raised inside an ExitStack body holding the
            manager
        Then:
            It should forward the exception details to __exit__ and let
            the exception propagate.
        """
        # Arrange
        manager = _SyncContextDummy()

        # Act
        with pytest.raises(ValueError, match="boom"):
            with ExitStack() as stack:
                stack.enter_context(manager)
                raise ValueError("boom")

        # Assert — contextlib invokes the guarded __exit__ unbound, so
        # the exception triple must survive the descriptor
        assert manager.exited_with is ValueError

    @pytest.mark.asyncio
    async def test_noreentry_should_forward_exception_details_when_async_stack_unwinds(
        self,
    ):
        """Test a guarded __aexit__ receives the exception forwarded to it.

        Given:
            An async context manager whose __aenter__ and __aexit__ are
            both decorated with @noreentry
        When:
            An exception is raised inside an AsyncExitStack body holding
            the manager
        Then:
            It should forward the exception details to __aexit__ and let
            the exception propagate.
        """
        # Arrange
        manager = _AsyncContextDummy()

        # Act
        with pytest.raises(ValueError, match="boom"):
            async with AsyncExitStack() as stack:
                await stack.enter_async_context(manager)
                raise ValueError("boom")

        # Assert
        assert manager.exited_with is ValueError

    def test_noreentry_should_raise_when_entered_by_statement_then_exit_stack(self):
        """Test the guard is shared across bound and unbound call forms.

        Given:
            A guarded context manager already entered with a plain with
            statement
        When:
            The same manager is pushed onto an ExitStack, which enters it
            through the unbound call form
        Then:
            It should raise RuntimeError, the guard being shared across
            both forms.
        """
        # Arrange
        manager = _SyncContextDummy()
        with manager:
            pass

        # Act & assert
        with ExitStack() as stack:
            with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
                stack.enter_context(manager)

    @pytest.mark.asyncio
    async def test_noreentry_should_raise_when_entered_by_async_with_then_stack(self):
        """Test the async guard is shared across bound and unbound forms.

        Given:
            A guarded async context manager already entered with a plain
            async with statement
        When:
            The same manager is pushed onto an AsyncExitStack, which
            enters it through the unbound call form
        Then:
            It should raise RuntimeError, the guard being shared across
            both forms.
        """
        # Arrange
        manager = _AsyncContextDummy()
        async with manager:
            pass

        # Act & assert
        async with AsyncExitStack() as stack:
            with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
                await stack.enter_async_context(manager)

    def test_noreentry_should_enter_context_when_subclass_pushed_onto_exit_stack(self):
        """Test a subclass inherits a guarded __enter__ usably.

        Given:
            A subclass of a context manager whose __enter__ is decorated
            with @noreentry on the base class
        When:
            An instance of the subclass is pushed onto an ExitStack
        Then:
            It should enter successfully and yield the manager.
        """
        # Arrange
        manager = _SubclassedContextDummy()

        # Act
        with ExitStack() as stack:
            entered = stack.enter_context(manager)

            # Assert
            assert entered is manager

    def test_noreentry_should_forward_arguments_when_called_unbound(self):
        """Test an unbound call forwards arguments past the instance.

        Given:
            A class with a @noreentry method taking a positional and a
            keyword-only argument
        When:
            The method is called unbound through its class with the
            instance first, followed by both arguments
        Then:
            It should return the result computed from both forwarded
            arguments.
        """
        # Arrange
        obj = _ArgumentDummy()

        # Act
        result = _ArgumentDummy.run(obj, "first", second="second")

        # Assert
        assert result == ("first", "second")

    @given(
        args=st.lists(_payloads, max_size=4),
        kwargs=st.dictionaries(_keywords, _payloads, max_size=4),
    )
    @settings(max_examples=50, deadline=None)
    def test_noreentry_should_forward_arbitrary_payload_when_called_unbound(
        self, args, kwargs
    ):
        """Test an unbound call forwards any payload past the instance.

        Given:
            Any tuple of positional arguments and any mapping of keyword
            arguments
        When:
            A guarded echo method is called unbound through its class
            with a fresh instance first, followed by the payload
        Then:
            It should return exactly the payload it was given.
        """
        # Arrange — a fresh instance per example, so no example inherits
        # another's exhausted guard
        obj = _PayloadDummy()

        # Act
        result = _PayloadDummy.run(obj, *args, **kwargs)

        # Assert
        assert result == (tuple(args), kwargs)

    @given(forms=st.lists(st.sampled_from(["bound", "unbound"]), min_size=1, max_size=6))
    @settings(max_examples=25, deadline=None)
    def test_noreentry_should_admit_only_the_first_call_when_call_forms_interleave(
        self, forms
    ):
        """Test at most one invocation survives any mix of call forms.

        Given:
            Any non-empty sequence of invocation forms drawn from the
            bound and unbound call shapes
        When:
            The sequence is applied to a single fresh instance
        Then:
            It should let exactly the first invocation through and raise
            RuntimeError for every later one, whatever the interleaving.
        """
        # Arrange — a fresh instance per example, so no example inherits
        # another's exhausted guard
        obj = _SyncDummy()

        def invoke(form):
            if form == "bound":
                return obj.run()
            return _SyncDummy.run(obj)

        # Act & assert
        assert invoke(forms[0]) == "ok"
        for form in forms[1:]:
            with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
                invoke(form)
