"""Stdlib parity pins for ``wool.ContextVar`` propagation into child tasks.

These tests pin BOTH stdlib ``contextvars`` behavior and the Wool
parity: a :class:`wool.ContextVar` value set in a parent context must
be visible inside child tasks created through every asyncio task-
spawning edge — :func:`asyncio.create_task`,
:meth:`loop.create_task`, :func:`asyncio.ensure_future`,
:func:`asyncio.gather`, and :class:`asyncio.TaskGroup`. Unlike a plain
:class:`contextvars.ContextVar` (which a child task observes under the
*same* :class:`contextvars.Context` copy), a Wool child task runs on a
freshly-minted ``chain_id`` — copy-on-fork — while still inheriting the
parent's variable values.

The value-propagation tests take the ``make_var`` fixture and run once
per variable type, so a single assertion proves the two propagate
identically; the copy-on-fork tests parametrize over the spawn
entrypoint, so one body pins every edge. The ``wool``-only tests
additionally pin copy-on-fork: each child, and each of two siblings,
forks onto a distinct chain.

Test classes group by parity concern rather than by a single
production class — this is a cross-cutting parity suite with no one
class under test. Every test additionally runs under both the default
``asyncio`` loop and uvloop, via the ``event_loop_policy`` fixture in
``conftest.py``.

A future change to CPython's task-context copy semantics, or to Wool's
task factory, fails here first.
"""

import asyncio
import contextvars
import threading
import types
import uuid
import warnings
from collections.abc import Callable
from collections.abc import Coroutine
from typing import Any
from typing import TypeVar

import pytest

import wool
from wool.runtime.context.exceptions import ChainContention
from wool.runtime.context.exceptions import TaskFactoryDisplaced
from wool.runtime.context.factory import install_task_factory
from wool.runtime.context.var import ContextVar

pytestmark = pytest.mark.stdlib_parity

_T = TypeVar("_T")

_CoroFactory = Callable[[], Coroutine[Any, Any, _T]]


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


class _Spawner:
    """One asyncio task-spawning entrypoint, driven uniformly.

    Each instance knows how to run a batch of coroutine factories
    through its entrypoint and return their results in order, so a
    single parametrized test body pins every spawn edge.
    """

    def __init__(self, name: str):
        self.name = name

    async def run(self, factories: list[_CoroFactory[_T]]) -> list[_T]:
        """Spawn one child per factory and return their results in order."""
        raise NotImplementedError  # pragma: no cover — overridden per edge


class _CreateTaskSpawner(_Spawner):
    async def run(self, factories):
        tasks = [asyncio.create_task(factory()) for factory in factories]
        return list(await asyncio.gather(*tasks))


class _LoopCreateTaskSpawner(_Spawner):
    async def run(self, factories):
        loop = asyncio.get_running_loop()
        tasks = [loop.create_task(factory()) for factory in factories]
        return list(await asyncio.gather(*tasks))


class _EnsureFutureSpawner(_Spawner):
    async def run(self, factories):
        futures = [asyncio.ensure_future(factory()) for factory in factories]
        return list(await asyncio.gather(*futures))


class _GatherSpawner(_Spawner):
    async def run(self, factories):
        return list(await asyncio.gather(*(factory() for factory in factories)))


class _TaskGroupSpawner(_Spawner):
    async def run(self, factories):
        tasks: list[asyncio.Task[Any]] = []
        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(factory()) for factory in factories]
        return [task.result() for task in tasks]


_SPAWNERS = [
    _CreateTaskSpawner("create_task"),
    _LoopCreateTaskSpawner("loop.create_task"),
    _EnsureFutureSpawner("ensure_future"),
    _GatherSpawner("gather"),
    _TaskGroupSpawner("TaskGroup"),
]


@pytest.fixture(params=_SPAWNERS, ids=lambda s: s.name)
def spawner(request) -> _Spawner:
    """Return one asyncio task-spawning entrypoint to pin under parity.

    Parametrizes a task-creation test over every spawn edge —
    :func:`asyncio.create_task`, :meth:`loop.create_task`,
    :func:`asyncio.ensure_future`, :func:`asyncio.gather`, and
    :class:`asyncio.TaskGroup` — so one test body pins them all.
    """
    return request.param


class TestTaskCreationValuePropagationParity:
    @pytest.mark.asyncio
    async def test_child_should_observe_scoped_value(self, spawner, make_var):
        """Test a context variable value is visible in a child task.

        Given:
            A context variable set in the parent.
        When:
            A child created through the spawn edge under test reads it.
        Then:
            It should observe the parent's value, identically for a
            stdlib and a wool variable and for every spawn edge.
        """
        # Arrange
        var = make_var("ct_value")
        var.set("parent")

        async def child() -> str:
            return var.get()

        # Act
        observed = await spawner.run([child])

        # Assert
        assert observed == ["parent"]


class TestTaskCreationCopyOnFork:
    @pytest.mark.asyncio
    async def test_child_should_fork_fresh_chain(self, spawner):
        """Test a child task forks onto a fresh chain.

        Given:
            A wool.ContextVar set in an armed parent with the task
            factory installed.
        When:
            A child created through the spawn edge under test reads the
            value and wool.__chain__.get().id.
        Then:
            It should observe the parent's value on a chain id distinct
            from the parent's, and leave the parent's own chain
            unchanged — copy-on-fork, for every spawn edge.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("fork_chain"))
        var.set("parent")
        parent = wool.__chain__.get(None)
        assert parent is not None

        async def child() -> tuple[str, uuid.UUID]:
            context = wool.__chain__.get(None)
            assert context is not None
            return var.get(), context.id

        # Act
        (observed,) = await spawner.run([child])

        # Assert
        assert observed[0] == "parent"
        assert observed[1] != parent.id
        after = wool.__chain__.get(None)
        assert after is not None
        assert after.id == parent.id

    @pytest.mark.asyncio
    async def test_siblings_should_fork_distinct_chains(self, spawner):
        """Test sibling child tasks each fork onto a distinct chain.

        Given:
            A wool.ContextVar set in an armed parent with the task
            factory installed.
        When:
            Two sibling children are created through the spawn edge
            under test.
        Then:
            It should give each sibling a chain id distinct from the
            parent's and from the other sibling's — copy-on-fork mints a
            fresh chain per task, never one chain shared across
            siblings, for every spawn edge.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("fork_siblings"))
        var.set("parent")
        parent = wool.__chain__.get(None)
        assert parent is not None

        async def child() -> uuid.UUID:
            context = wool.__chain__.get(None)
            assert context is not None
            return context.id

        # Act
        first, second = await spawner.run([child, child])

        # Assert
        assert first != parent.id
        assert second != parent.id
        assert first != second

    @pytest.mark.asyncio
    async def test_factory_should_be_dormant_when_unarmed(self, spawner):
        """Test the task factory is dormant when the context is unarmed.

        Given:
            The Wool task factory installed but no wool.ContextVar set
            (unarmed context).
        When:
            A child is created through the spawn edge under test.
        Then:
            It should observe no context inside the child — the factory
            is dormant when unarmed, for every spawn edge.
        """
        # Arrange
        install_task_factory()

        async def child() -> bool:
            return wool.__chain__.get(None) is None

        # Act
        (context_is_none,) = await spawner.run([child])

        # Assert
        assert context_is_none

    @pytest.mark.asyncio
    async def test_unarmed_factory_should_preserve_plain_contextvars(self, spawner):
        """Test the installed task factory leaves plain contextvars intact.

        Given:
            The Wool task factory installed, no wool.ContextVar ever set
            (an unarmed context), and a plain contextvars.ContextVar set
            in the parent.
        When:
            A child created through the spawn edge under test reads the
            plain variable and current_context.
        Then:
            It should observe the parent's plain value and no Wool
            context — installing the factory costs an unarmed context
            nothing, behaving as a plain contextvars.Context, for every
            spawn edge.
        """
        # Arrange
        install_task_factory()
        var: contextvars.ContextVar[str] = contextvars.ContextVar(_unique("unarmed"))
        var.set("parent")

        async def child() -> tuple[str, bool]:
            return var.get(), wool.__chain__.get(None) is None

        # Act
        ((observed_value, context_is_none),) = await spawner.run([child])

        # Assert
        assert observed_value == "parent"
        assert context_is_none


class TestTaskFactoryComposition:
    @pytest.mark.asyncio
    async def test_user_factory_should_fork_when_installed_before_wool(self):
        """Test a user factory installed before Wool's still yields copy-on-fork.

        Given:
            A user task factory installed on the loop, then Wool's task
            factory installed after it (composing over the user one),
            and an armed wool.ContextVar.
        When:
            A child task is created and reads the parent value and its
            own chain id.
        Then:
            It should observe the parent's value on a forked chain, and
            the user factory should have run — Wool composes over the
            user factory rather than displacing it.
        """
        # Arrange
        loop = asyncio.get_running_loop()
        user_factory_calls: list[bool] = []

        def user_factory(loop, coro, **kwargs):
            user_factory_calls.append(True)
            return asyncio.Task(coro, loop=loop, **kwargs)

        loop.set_task_factory(user_factory)
        install_task_factory()
        var = ContextVar(_unique("compose_value"))
        var.set("parent")
        parent = wool.__chain__.get(None)
        assert parent is not None

        async def child() -> tuple[str, uuid.UUID]:
            context = wool.__chain__.get(None)
            assert context is not None
            return var.get(), context.id

        # Act
        observed_value, child_chain = await asyncio.create_task(child())

        # Assert
        assert observed_value == "parent"
        assert child_chain != parent.id
        assert user_factory_calls

    @pytest.mark.asyncio
    async def test_stdlib_value_should_propagate_under_composition(self):
        """Test a stdlib ContextVar propagates under a composed factory.

        Given:
            A user task factory installed before Wool's, an armed
            wool.ContextVar, and a plain contextvars.ContextVar set in
            the parent.
        When:
            A child task created under the composed factory reads the
            plain variable.
        Then:
            It should observe the parent's stdlib value — composition
            preserves native contextvars propagation.
        """
        # Arrange
        loop = asyncio.get_running_loop()

        def user_factory(loop, coro, **kwargs):
            return asyncio.Task(coro, loop=loop, **kwargs)

        loop.set_task_factory(user_factory)
        install_task_factory()
        wool_var = ContextVar(_unique("compose_arm"))
        wool_var.set("arm")
        std_var: contextvars.ContextVar[str] = contextvars.ContextVar(
            _unique("compose_std")
        )
        std_var.set("parent")

        async def child() -> str:
            return std_var.get()

        # Act
        observed = await asyncio.create_task(child())

        # Assert
        assert observed == "parent"

    @pytest.mark.asyncio
    async def test_legacy_two_arg_inner_factory_should_raise_type_error(self):
        """Test a legacy 2-arg inner factory raises TypeError under composition.

        Given:
            A legacy task factory with the two-argument ``(loop, coro)``
            signature installed, then Wool's factory composed over it,
            and an armed wool.ContextVar so Wool wraps and forwards
            ``context=`` to the inner factory.
        When:
            A child task is created.
        Then:
            It should raise :class:`TypeError` — Wool always forwards
            ``context=`` and a legacy 2-arg factory cannot accept it.
        """
        # Arrange
        loop = asyncio.get_running_loop()

        def legacy_factory(loop, coro):
            return asyncio.Task(coro, loop=loop)

        loop.set_task_factory(legacy_factory)
        install_task_factory()
        var = ContextVar(_unique("legacy_arm"))
        var.set("arm")

        async def child() -> None:
            return None

        # Act & assert — clear the broken composed factory before exit
        # so the loop's own teardown does not route through it. The
        # legacy factory rejects the ``context=`` Wool forwards, so the
        # wrapped ``_forked_scope`` coroutine Wool built is never
        # awaited; that leaked-coroutine RuntimeWarning is incidental to
        # this error path and is filtered here so it does not mask the
        # TypeError under assertion.
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore", "coroutine .* was never awaited", RuntimeWarning
                )
                with pytest.raises(TypeError):
                    await asyncio.create_task(child())
        finally:
            loop.set_task_factory(None)

    @pytest.mark.asyncio
    async def test_factory_installed_after_wool_should_raise_displacement(self):
        """Test a factory installed after Wool's emits a displacement warning.

        Given:
            Wool's task factory installed on the loop, then a
            third-party factory installed after it (displacing Wool's).
        When:
            Wool's self-install path runs again (a wool.ContextVar.set).
        Then:
            It should raise :class:`TaskFactoryDisplaced` — copy-on-
            fork is silently lost for tasks created after the
            displacement.
        """
        # Arrange
        loop = asyncio.get_running_loop()
        install_task_factory()

        def third_party_factory(loop, coro, **kwargs):
            return asyncio.Task(coro, loop=loop, **kwargs)

        loop.set_task_factory(third_party_factory)
        var = ContextVar(_unique("displaced"))

        # Act & assert — the next Wool API contact self-checks the loop.
        with pytest.raises(TaskFactoryDisplaced, match="displaced"):
            var.set("arm")

    @pytest.mark.asyncio
    async def test_install_task_factory_should_be_idempotent(self):
        """Test install_task_factory is a no-op when already installed.

        Given:
            Wool's task factory installed on the loop.
        When:
            install_task_factory is called a second time.
        Then:
            It should leave the same Wool-wrapped factory in place — the
            second call is a no-op, not a re-wrap that would compose
            Wool's factory over itself.
        """
        # Arrange
        loop = asyncio.get_running_loop()
        install_task_factory()
        first = loop.get_task_factory()

        # Act
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            install_task_factory()
        second = loop.get_task_factory()

        # Assert
        assert first is second
        assert getattr(second, "__wool_wrapped__", False)


class TestChainContentionGuardBoundary:
    @pytest.mark.asyncio
    async def test_two_tasks_should_raise_chain_contention_when_sharing_armed_context(
        self,
    ):
        """Test two tasks handed one context that is later armed trip the guard.

        Pins ``assert_chain_owner``'s cross-task trigger boundary: the
        task factory's copy-on-fork cannot catch a context shared while
        still *unarmed*; the chain-owner guard catches the second runner
        once it is armed.

        Given:
            One unarmed contextvars.Context passed to two tasks (sharing
            an unarmed context is permitted, exactly as stdlib allows).
        When:
            The first task arms the context with a wool.ContextVar.set
            and the second task then touches a wool.ContextVar.
        Then:
            The second task should raise :class:`ChainContention` —
            two tasks cannot run one armed chain.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("guard_shared"))
        shared = contextvars.copy_context()
        loop = asyncio.get_running_loop()
        armed = asyncio.Event()
        first_can_finish = asyncio.Event()

        async def first() -> None:
            var.set("armed-by-first")
            armed.set()
            await first_can_finish.wait()

        async def second() -> BaseException | None:
            await armed.wait()
            try:
                var.get("fallback")
            except BaseException as exc:  # noqa: BLE001 — return it for assertion
                return exc
            finally:
                first_can_finish.set()
            return None

        # Act — both tasks are handed the SAME context object.
        first_task = loop.create_task(first(), context=shared)
        second_task = loop.create_task(second(), context=shared)
        observed, _ = await asyncio.gather(second_task, first_task)

        # Assert
        assert isinstance(observed, ChainContention)

    @pytest.mark.asyncio
    async def test_repassing_a_live_armed_context_should_raise_chain_contention(self):
        """Test re-passing a live armed context to create_task is rejected.

        Pins the factory's up-front rejection: an armed context already
        driving a live task cannot be handed to a second create_task.

        Given:
            An armed contextvars.Context owned by a running task.
        When:
            That same context is passed to loop.create_task while the
            owning task is still live.
        Then:
            It should raise :class:`ChainContention` from the
            factory itself, before the second task runs.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("guard_repass"))
        loop = asyncio.get_running_loop()
        holder_running = asyncio.Event()
        holder_can_finish = asyncio.Event()

        async def holder() -> None:
            var.set("armed")
            holder_running.set()
            await holder_can_finish.wait()

        async def noop() -> None:
            return None

        # Act & assert — the holder task arms the context it is handed,
        # then that live, armed context is re-passed to a second
        # create_task while the holder is still running. The context is
        # threaded in explicitly rather than read back via
        # Task.get_context(), which is Python 3.12+; the project
        # supports 3.11.
        holder_context = contextvars.copy_context()
        holder_task = loop.create_task(holder(), context=holder_context)
        await holder_running.wait()
        try:
            with pytest.raises(ChainContention, match="armed"):
                loop.create_task(noop(), context=holder_context)
        finally:
            holder_can_finish.set()
            await holder_task

    @pytest.mark.asyncio
    async def test_callback_on_the_owning_thread_should_not_raise(self):
        """Test a callback on the owning thread does not trip the guard.

        The negative boundary: cooperatively-scheduled work on the
        chain's owning thread shares the chain serially and must not
        raise.

        Given:
            An armed wool.ContextVar owned by the loop thread.
        When:
            A loop.call_soon callback on that same thread reads the
            variable.
        Then:
            It should observe the value without raising
            :class:`ChainContention` — a callback shares the chain
            but never runs concurrently with its owner.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("guard_callback"))
        var.set("scope")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[str] = loop.create_future()

        def read() -> None:
            if not done.done():
                try:
                    done.set_result(var.get())
                except BaseException as exc:  # noqa: BLE001
                    done.set_exception(exc)

        # Act
        loop.call_soon(read)
        observed = await asyncio.wait_for(done, timeout=5.0)

        # Assert
        assert observed == "scope"

    @pytest.mark.asyncio
    async def test_offloaded_code_that_never_touches_a_var_should_not_raise(self):
        """Test offloaded code that touches no wool.ContextVar does not raise.

        The negative boundary: the guard fires when a wool.ContextVar is
        read or written, not when a thread boundary is crossed.

        Given:
            An armed wool.ContextVar owned by the loop thread.
        When:
            asyncio.to_thread offloads a function that does NOT touch
            any wool.ContextVar.
        Then:
            It should return normally without raising
            :class:`ChainContention` — offloaded code that never
            enters the chain is never flagged.
        """
        # Arrange
        var = ContextVar(_unique("guard_untouched"))
        var.set("scope")

        def offloaded() -> str:
            return "no-var-touched"

        # Act
        observed = await asyncio.to_thread(offloaded)

        # Assert
        assert observed == "no-var-touched"


class TestNestedTaskDepth:
    @pytest.mark.asyncio
    async def test_grandchild_should_fork_chain_distinct_from_ancestors(self):
        """Test a grandchild task forks a chain distinct from its ancestors.

        Given:
            A wool.ContextVar set in an armed parent with the task
            factory installed.
        When:
            The parent spawns a child task, which spawns a grandchild
            task, each reading wool.__chain__.get().id.
        Then:
            It should give parent, child, and grandchild three distinct
            chain ids — copy-on-fork mints a fresh chain at every
            nesting level.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("nested_chain"))
        var.set("depth-0")
        parent = wool.__chain__.get(None)
        assert parent is not None

        async def grandchild() -> uuid.UUID:
            context = wool.__chain__.get(None)
            assert context is not None
            return context.id

        async def child() -> tuple[uuid.UUID, uuid.UUID]:
            context = wool.__chain__.get(None)
            assert context is not None
            grandchild_chain = await asyncio.create_task(grandchild())
            return context.id, grandchild_chain

        # Act
        child_chain, grandchild_chain = await asyncio.create_task(child())

        # Assert
        assert len({parent.id, child_chain, grandchild_chain}) == 3

    @pytest.mark.asyncio
    async def test_depth_0_value_should_be_visible_at_depth_3(self):
        """Test a value set at the root is visible three task levels deep.

        Given:
            A wool.ContextVar set in an armed parent with the task
            factory installed.
        When:
            The parent spawns a chain of child -> grandchild ->
            great-grandchild tasks, the innermost reading the variable.
        Then:
            It should observe the depth-0 value at depth 3 —
            copy-on-fork is transitive: each fork inherits its parent's
            bindings.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("nested_value"))
        var.set("root-value")

        async def great_grandchild() -> str:
            return var.get()

        async def grandchild() -> str:
            return await asyncio.create_task(great_grandchild())

        async def child() -> str:
            return await asyncio.create_task(grandchild())

        # Act
        observed = await asyncio.create_task(child())

        # Assert
        assert observed == "root-value"


class TestGeneratorBasedCoroutine:
    @pytest.mark.asyncio
    async def test_generator_based_coroutine_should_propagate_value(self):
        """Test a generator-based coroutine task propagates a wool value.

        Exercises the ``Coroutine | Generator`` arm of ``wool_factory``:
        a task built from a generator decorated with
        :func:`asyncio.coroutine`-style ``@types.coroutine``.

        Given:
            A wool.ContextVar set in an armed parent with the task
            factory installed, and a child built from a
            ``@types.coroutine`` generator function.
        When:
            A child task created from that generator-based coroutine
            reads the variable.
        Then:
            It should observe the parent's value — the task factory
            forks the generator-based coroutine onto a copy-on-fork
            chain exactly like a native ``async def`` coroutine.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("genbased_value"))
        var.set("parent")

        @types.coroutine
        def generator_based_child():
            # A bare ``yield`` makes this a generator; ``@types.coroutine``
            # marks it awaitable so asyncio's create_task path accepts it.
            yield
            return var.get()

        # Act
        observed = await asyncio.create_task(generator_based_child())

        # Assert
        assert observed == "parent"


class TestRunCoroutineThreadsafe:
    @pytest.mark.asyncio
    async def test_run_coroutine_threadsafe_should_propagate_value(self, make_var):
        """Test run_coroutine_threadsafe propagates a value from a foreign thread.

        Given:
            A context variable set inside a coroutine that a foreign
            thread submits via asyncio.run_coroutine_threadsafe.
        When:
            That coroutine reads the variable it set.
        Then:
            It should observe the value — the submitted coroutine runs
            as a task on the loop and sees its own writes, identically
            for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("rct_value")
        loop = asyncio.get_running_loop()
        ready = asyncio.Event()

        async def submitted() -> str:
            var.set("submitted-scope")
            return var.get()

        result: list[str] = []
        error: list[BaseException] = []

        def submit_from_foreign_thread() -> None:
            future = asyncio.run_coroutine_threadsafe(submitted(), loop)
            try:
                result.append(future.result(timeout=5.0))
            except BaseException as exc:  # noqa: BLE001
                error.append(exc)
            loop.call_soon_threadsafe(ready.set)

        # Act
        worker = threading.Thread(target=submit_from_foreign_thread)
        worker.start()
        await asyncio.wait_for(ready.wait(), timeout=5.0)
        worker.join(timeout=5.0)

        # Assert
        assert not error
        assert result == ["submitted-scope"]

    @pytest.mark.asyncio
    async def test_run_coroutine_threadsafe_should_fork_fresh_chain(self):
        """Test a run_coroutine_threadsafe coroutine forks its own chain.

        Given:
            An armed wool.ContextVar on the loop thread with the task
            factory installed.
        When:
            A foreign thread submits a coroutine via
            asyncio.run_coroutine_threadsafe that arms its own
            wool.ContextVar and reads wool.__chain__.get().id.
        Then:
            It should run on a chain owned by the loop thread without
            tripping :class:`ChainContention` — the coroutine is
            scheduled as a task on the loop, not run on the foreign
            thread.
        """
        # Arrange
        install_task_factory()
        loop = asyncio.get_running_loop()
        loop_var = ContextVar(_unique("rct_loop"))
        loop_var.set("loop-armed")
        ready = asyncio.Event()
        result: list[uuid.UUID] = []
        error: list[BaseException] = []

        async def submitted() -> uuid.UUID:
            var = ContextVar(_unique("rct_chain"))
            var.set("submitted")
            context = wool.__chain__.get(None)
            assert context is not None
            return context.id

        def submit_from_foreign_thread() -> None:
            future = asyncio.run_coroutine_threadsafe(submitted(), loop)
            try:
                result.append(future.result(timeout=5.0))
            except BaseException as exc:  # noqa: BLE001
                error.append(exc)
            loop.call_soon_threadsafe(ready.set)

        # Act
        worker = threading.Thread(target=submit_from_foreign_thread)
        worker.start()
        await asyncio.wait_for(ready.wait(), timeout=5.0)
        worker.join(timeout=5.0)

        # Assert
        assert not error
        assert len(result) == 1


class TestSchedulingEdgeExceptionPropagation:
    @pytest.mark.asyncio
    async def test_lookup_error_in_a_gather_child_should_surface(self):
        """Test a LookupError raised in a gather child surfaces to the awaiter.

        Given:
            A wool.ContextVar with no value and no default.
        When:
            A child coroutine run via asyncio.gather reads it.
        Then:
            The :class:`LookupError` should surface out of the gather
            awaiter — an exception raised in a scheduling-edge child
            propagates to the caller.
        """
        # Arrange
        var = ContextVar(_unique("gather_exc"))

        async def child() -> str:
            return var.get()

        # Act & assert
        with pytest.raises(LookupError):
            await asyncio.gather(child())

    @pytest.mark.asyncio
    async def test_lookup_error_in_a_done_callback_should_surface(self):
        """Test a LookupError raised in a done callback surfaces to the awaiter.

        Given:
            A wool.ContextVar with no value and no default, and a future
            whose done callback reads it.
        When:
            The future resolves and the done callback runs.
        Then:
            The read's :class:`LookupError` should surface onto the
            observer future — an exception raised on a done-callback
            scheduling edge propagates rather than being swallowed by
            the loop's exception handler.
        """
        # Arrange
        var = ContextVar(_unique("dc_exc"))
        loop = asyncio.get_running_loop()
        observed: asyncio.Future[object] = loop.create_future()

        def done_callback(_: object) -> None:
            if observed.done():
                return
            try:
                observed.set_result(var.get())
            except BaseException as exc:  # noqa: BLE001 — route it to the future
                observed.set_exception(exc)

        future: asyncio.Future[None] = loop.create_future()
        future.add_done_callback(done_callback)

        # Act & assert
        future.set_result(None)
        with pytest.raises(LookupError):
            await asyncio.wait_for(observed, timeout=5.0)


class TestRunInExecutorWithExplicitContext:
    @pytest.mark.asyncio
    async def test_run_in_executor_with_ctx_run_should_raise_chain_contention(self):
        """Test loop.run_in_executor with an explicit ctx.run raises ChainContention.

        Pins the raw ``loop.run_in_executor(None, ctx.run, fn)`` idiom:
        a copy_context() run on an executor thread.

        Given:
            An armed wool.ContextVar and a copy_context() of the armed
            context.
        When:
            loop.run_in_executor offloads ``ctx.run`` over a function
            that reads the variable.
        Then:
            It should raise :class:`ChainContention` — running the
            armed chain's copied context on an executor thread enters
            the chain off its owning thread.
        """
        # Arrange
        var = ContextVar(_unique("rie_ctxrun"))
        var.set("armed")
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()

        def read() -> str:
            return var.get("fallback")

        # Act & assert
        with pytest.raises(ChainContention):
            await loop.run_in_executor(None, ctx.run, read)

    @pytest.mark.asyncio
    async def test_run_in_executor_with_ctx_run_should_not_raise_when_unarmed(self):
        """Test run_in_executor with ctx.run on an unarmed context does not raise.

        Given:
            An unarmed contextvars.Context (no wool.ContextVar set) and
            a wool.ContextVar with a default.
        When:
            loop.run_in_executor offloads ``ctx.run`` over a function
            that reads the variable.
        Then:
            It should return the default without raising — an unarmed
            context carries no chain to enter.
        """
        # Arrange
        var = ContextVar(_unique("rie_unarmed"), default="d")
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()

        def read() -> str:
            return var.get()

        # Act
        observed = await loop.run_in_executor(None, ctx.run, read)

        # Assert
        assert observed == "d"
