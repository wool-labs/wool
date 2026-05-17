import asyncio
import contextvars
import uuid

import pytest
import pytest_asyncio

from tests.helpers import scoped_context
from wool.runtime.context import ConcurrentChainEntry
from wool.runtime.context import ContextVar
from wool.runtime.context import current_snapshot
from wool.runtime.context import install_task_factory
from wool.runtime.context import to_thread


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


@pytest_asyncio.fixture(autouse=True)
async def _reset_task_factory():
    """Restore the running loop's task factory after each test.

    Tests here install Wool's task factory on the event loop; clearing
    it on teardown keeps a failed assertion mid-test from leaving a
    factory installed for any later test that shares the loop.
    """
    yield
    asyncio.get_running_loop().set_task_factory(None)


class TestFactory:
    @pytest.mark.asyncio
    async def test_install_task_factory_with_no_existing_factory(self):
        """Test install_task_factory installs a factory on the running loop.

        Given:
            A running loop with no task factory installed.
        When:
            install_task_factory is called with no loop argument.
        Then:
            It should install a non-None task factory on the running loop.
        """
        # Arrange
        loop = asyncio.get_running_loop()
        loop.set_task_factory(None)

        # Act
        install_task_factory()

        # Assert
        assert loop.get_task_factory() is not None

    @pytest.mark.asyncio
    async def test_install_task_factory_already_installed(self):
        """Test install_task_factory does not double-wrap an installed factory.

        Given:
            A running loop where install_task_factory has already run.
        When:
            install_task_factory is called a second time.
        Then:
            It should leave the task factory object unchanged.
        """
        # Arrange
        loop = asyncio.get_running_loop()
        loop.set_task_factory(None)
        install_task_factory()
        first = loop.get_task_factory()

        # Act
        install_task_factory()

        # Assert
        assert loop.get_task_factory() is first

    @pytest.mark.asyncio
    async def test_install_task_factory_composes_with_existing_factory(self):
        """Test install_task_factory wraps an existing user factory.

        Given:
            A running loop with a user-supplied task factory that
            increments a counter.
        When:
            install_task_factory is called and a child task is created
            from an armed parent.
        Then:
            It should fork the child onto a fresh chain UUID (copy-on-
            fork still works) and increment the user factory's counter
            (the original factory still participated).
        """
        # Arrange
        counter = [0]

        def user_factory(
            loop: asyncio.AbstractEventLoop,
            coro,
            **kwargs,
        ) -> asyncio.Task:
            counter[0] += 1
            return asyncio.Task(coro, loop=loop, **kwargs)

        loop = asyncio.get_running_loop()
        loop.set_task_factory(user_factory)

        var = ContextVar(_unique("compose"))

        async def child() -> uuid.UUID:
            snapshot = current_snapshot()
            assert snapshot is not None
            return snapshot.chain_id

        # Act
        install_task_factory()
        var.set("x")
        parent_snapshot = current_snapshot()
        assert parent_snapshot is not None
        child_chain = await asyncio.create_task(child())

        # Assert
        assert child_chain != parent_snapshot.chain_id
        assert counter[0] == 1

    @pytest.mark.asyncio
    async def test_install_task_factory_user_factory_installed_after_drops_fork_wrapping(
        self,
    ):
        """Test that installing a user factory after Wool's drops copy-on-fork.

        Given:
            Wool's task factory installed on the running loop and a
            ContextVar armed in the parent context.
        When:
            A plain pass-through user factory replaces Wool's factory
            and a child task is created.
        Then:
            It should observe the parent's chain UUID — confirming that
            copy-on-fork is gone once Wool's factory is no longer last.
        """
        # Arrange
        var = ContextVar(_unique("install_order"))

        def user_factory(
            loop: asyncio.AbstractEventLoop,
            coro,
            **kwargs,
        ) -> asyncio.Task:
            return asyncio.Task(coro, loop=loop, **kwargs)

        async def child() -> uuid.UUID | None:
            snapshot = current_snapshot()
            return snapshot.chain_id if snapshot is not None else None

        install_task_factory()
        var.set("x")
        parent_snapshot = current_snapshot()
        assert parent_snapshot is not None

        # Act
        loop = asyncio.get_running_loop()
        loop.set_task_factory(user_factory)
        child_chain = await asyncio.create_task(child())

        # Assert
        assert child_chain == parent_snapshot.chain_id

    @pytest.mark.asyncio
    async def test_install_task_factory_child_inherits_parent_value(self):
        """Test a child task inherits the parent's ContextVar value.

        Given:
            An armed context with a ContextVar set and Wool's task
            factory installed.
        When:
            A child task created with asyncio.create_task reads the
            variable.
        Then:
            It should observe the parent's value.
        """
        # Arrange
        var = ContextVar(_unique("child_inherit"))

        async def child() -> str:
            return var.get()

        install_task_factory()
        var.set("parent-value")

        # Act
        observed = await asyncio.create_task(child())

        # Assert
        assert observed == "parent-value"

    @pytest.mark.asyncio
    async def test_install_task_factory_child_forks_chain_id(self):
        """Test a child task runs on a fresh chain id (copy-on-fork).

        Given:
            An armed parent context with Wool's task factory installed.
        When:
            A child task reads current_snapshot().chain_id.
        Then:
            It should differ from the parent's chain id.
        """
        # Arrange
        var = ContextVar(_unique("child_fork"))

        async def child() -> uuid.UUID:
            snapshot = current_snapshot()
            assert snapshot is not None
            return snapshot.chain_id

        install_task_factory()
        var.set("x")
        parent_snapshot = current_snapshot()
        assert parent_snapshot is not None

        # Act
        child_chain = await asyncio.create_task(child())

        # Assert
        assert child_chain != parent_snapshot.chain_id

    @pytest.mark.asyncio
    async def test_first_set_self_installs_the_task_factory(self):
        """Test a bare ContextVar.set self-installs the task factory.

        Given:
            A running loop with no task factory installed and no
            explicit install_task_factory call.
        When:
            A wool.ContextVar is set for the first time, arming the
            context, then a child task is created with
            asyncio.create_task.
        Then:
            The child should fork onto a chain id distinct from the
            parent's — the first set self-installed the factory, so
            copy-on-fork works without an explicit install_task_factory
            call.
        """
        # Arrange
        loop = asyncio.get_running_loop()
        loop.set_task_factory(None)
        var = ContextVar(_unique("self_install"))

        async def child() -> uuid.UUID:
            snapshot = current_snapshot()
            assert snapshot is not None
            return snapshot.chain_id

        # Act
        var.set("x")  # No explicit install_task_factory() call.
        parent_snapshot = current_snapshot()
        assert parent_snapshot is not None
        child_chain = await asyncio.create_task(child())

        # Assert
        assert child_chain != parent_snapshot.chain_id

    @pytest.mark.asyncio
    async def test_install_task_factory_child_mutation_isolated_from_parent(self):
        """Test a child task's variable mutation does not leak to the parent.

        Given:
            An armed parent context with a ContextVar set.
        When:
            A child task sets the variable to a new value.
        Then:
            It should leave the parent observing its own value.
        """
        # Arrange
        var = ContextVar(_unique("child_isolate"))

        async def child() -> None:
            var.set("child-value")

        install_task_factory()
        var.set("parent-value")

        # Act
        await asyncio.create_task(child())

        # Assert
        assert var.get() == "parent-value"

    @pytest.mark.asyncio
    async def test_install_task_factory_child_with_parent_token(self):
        """Test a child task cannot reset a Token minted in the parent chain.

        Given:
            An armed parent context whose set produced a Token.
        When:
            A child task attempts to reset that parent Token.
        Then:
            It should raise ValueError naming the different chain.
        """
        # Arrange
        var = ContextVar(_unique("child_token"))
        install_task_factory()
        token = var.set("x")

        async def child() -> None:
            var.reset(token)

        # Act & assert
        with pytest.raises(ValueError, match="different chain"):
            await asyncio.create_task(child())

    @pytest.mark.asyncio
    async def test_install_task_factory_unarmed_context_child(self):
        """Test a child task of an unarmed context stays unarmed.

        Given:
            An unarmed context with Wool's task factory installed.
        When:
            A child task reads current_snapshot.
        Then:
            It should observe None — an unarmed fork is a dormant
            no-op.
        """

        async def child() -> object:
            return current_snapshot()

        install_task_factory()

        # Act
        observed = await asyncio.create_task(child())

        # Assert
        assert observed is None

    @pytest.mark.asyncio
    async def test_install_task_factory_shared_context_across_concurrent_tasks(self):
        """Test the factory rejects one contextvars.Context shared by two tasks.

        Given:
            An armed context with Wool's task factory installed and a
            live task created with an explicit contextvars.Context.
        When:
            A second task is created with that same context object
            while the first is still running.
        Then:
            It should raise wool.ConcurrentChainEntry — two tasks
            cannot interleave on one context's chain snapshot.
        """
        # Arrange
        var = ContextVar(_unique("shared_ctx"))
        release = asyncio.Event()

        async def body() -> None:
            # Block on an event the test controls so the first task is
            # provably still live when the second is created.
            await release.wait()

        install_task_factory()
        var.set("x")
        loop = asyncio.get_running_loop()
        shared = contextvars.copy_context()
        first = loop.create_task(body(), context=shared)

        # Act & assert
        try:
            with pytest.raises(ConcurrentChainEntry):
                loop.create_task(body(), context=shared)
        finally:
            release.set()
            await first

    @pytest.mark.asyncio
    async def test_install_task_factory_shared_unarmed_context_across_concurrent_tasks(
        self,
    ):
        """Test the factory allows one unarmed context shared by two live tasks.

        Given:
            Wool's task factory installed and a live task created with
            an explicit, unarmed contextvars.Context.
        When:
            A second task is created with that same context object while
            the first is still running.
        Then:
            It should not raise — an unarmed context carries no chain to
            corrupt, so sharing it across concurrently-live tasks is
            permitted exactly as stdlib asyncio permits it.
        """
        # Arrange
        release = asyncio.Event()

        async def body() -> None:
            await release.wait()

        install_task_factory()
        loop = asyncio.get_running_loop()
        shared = contextvars.copy_context()
        first = loop.create_task(body(), context=shared)

        # Act
        try:
            second = loop.create_task(body(), context=shared)
        finally:
            release.set()

        # Assert
        await asyncio.gather(first, second)

    @pytest.mark.asyncio
    async def test_install_task_factory_late_armed_shared_context_trips_guard(self):
        """Test arming a shared unarmed context trips the owner-task guard.

        Given:
            Wool's task factory installed and one unarmed
            contextvars.Context shared by two concurrently-live tasks,
            one of which arms it with a wool.ContextVar.set.
        When:
            The other task touches a wool.ContextVar on that now-armed
            chain.
        Then:
            It should raise wool.ConcurrentChainEntry — the owner-task
            guard catches the second task entering a chain another live
            task owns, the case the creation-time rejection cannot see
            because the context was unarmed when both tasks were created.
        """
        # Arrange
        var = ContextVar(_unique("late_arm"))
        armed = asyncio.Event()
        release = asyncio.Event()

        async def arming_task() -> None:
            var.set("owner")
            armed.set()
            await release.wait()

        async def entering_task() -> str:
            await armed.wait()
            try:
                return var.get("fallback")
            finally:
                release.set()

        install_task_factory()
        loop = asyncio.get_running_loop()
        shared = contextvars.copy_context()
        owner = loop.create_task(arming_task(), context=shared)
        intruder = loop.create_task(entering_task(), context=shared)

        # Act & assert
        with pytest.raises(ConcurrentChainEntry):
            await intruder
        await owner

    @pytest.mark.asyncio
    async def test_install_task_factory_context_reuse_after_task_completes(self):
        """Test a contextvars.Context is reusable once its task has finished.

        Given:
            An armed context with Wool's task factory installed and a
            task created with an explicit contextvars.Context that has
            run to completion.
        When:
            A new task is created with that same context object.
        Then:
            It should not raise — the context is free once no live
            task holds it.
        """
        # Arrange
        var = ContextVar(_unique("reuse_ctx"))

        async def body() -> None:
            await asyncio.sleep(0)

        install_task_factory()
        var.set("x")
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()

        # Act & assert
        await loop.create_task(body(), context=ctx)
        await loop.create_task(body(), context=ctx)

    @pytest.mark.asyncio
    async def test_install_task_factory_warns_when_displaced_by_a_later_factory(self):
        """Test a displaced Wool factory surfaces a RuntimeWarning.

        Given:
            A running loop where Wool's task factory was self-installed
            by a first wool.ContextVar.set, then displaced by a
            third-party task factory installed after it.
        When:
            Wool's self-install path runs again — triggered by a first
            wool.ContextVar.set in a fresh unarmed context.
        Then:
            It should emit a RuntimeWarning — copy-on-fork is silently
            lost for tasks created since the displacement, so the
            displacement fails loud rather than passing unnoticed.
        """
        # Arrange
        loop = asyncio.get_running_loop()

        def user_factory(
            loop: asyncio.AbstractEventLoop,
            coro,
            **kwargs,
        ) -> asyncio.Task:
            return asyncio.Task(coro, loop=loop, **kwargs)

        # A first set self-installs Wool's factory and records the loop.
        with scoped_context():
            ContextVar(_unique("displace_seed")).set("x")
        # A third-party factory installed afterwards displaces Wool's.
        loop.set_task_factory(user_factory)

        # Act & assert — a later first-set re-enters the self-install
        # path, which finds the loop recorded but the factory no longer
        # Wool-wrapped.
        with scoped_context():
            with pytest.warns(RuntimeWarning, match="displaced"):
                ContextVar(_unique("displace_trigger")).set("y")

    @pytest.mark.asyncio
    async def test_install_task_factory_concurrent_tasks_with_default_contexts(self):
        """Test concurrent tasks with default per-task contexts are not rejected.

        Given:
            An armed context with Wool's task factory installed.
        When:
            Two child tasks are created concurrently without an
            explicit context argument.
        Then:
            It should not raise — the guard fires only on a shared
            explicit context, and asyncio copies the context per task.
        """
        # Arrange
        var = ContextVar(_unique("default_ctx"))

        async def body() -> str:
            await asyncio.sleep(0)
            return var.get()

        install_task_factory()
        var.set("x")

        # Act
        results = await asyncio.gather(
            asyncio.create_task(body()),
            asyncio.create_task(body()),
        )

        # Assert
        assert results == ["x", "x"]


class TestToThread:
    @pytest.mark.asyncio
    async def test_to_thread_with_positional_args(self):
        """Test wool.to_thread runs the callable and returns its result.

        Given:
            A blocking callable returning a value.
        When:
            wool.to_thread offloads it.
        Then:
            It should return the callable's result.
        """

        # Arrange
        def work(a: int, b: int) -> int:
            return a + b

        # Act
        result = await to_thread(work, 2, 3)

        # Assert
        assert result == 5

    @pytest.mark.asyncio
    async def test_to_thread_with_keyword_args(self):
        """Test wool.to_thread forwards keyword arguments to the callable.

        Given:
            A callable accepting a keyword argument.
        When:
            wool.to_thread is called with that keyword argument.
        Then:
            It should forward the keyword argument correctly.
        """

        # Arrange
        def work(*, label: str) -> str:
            return label.upper()

        # Act
        result = await to_thread(work, label="hi")

        # Assert
        assert result == "HI"

    @pytest.mark.asyncio
    async def test_to_thread_with_armed_context(self):
        """Test wool.to_thread carries the caller's ContextVar value into the thread.

        Given:
            An armed context with a ContextVar set.
        When:
            wool.to_thread offloads a function that reads the variable.
        Then:
            It should observe the caller's value.
        """
        # Arrange
        var = ContextVar(_unique("to_thread_value"))

        def read() -> str:
            return var.get()

        var.set("carried")

        # Act
        observed = await to_thread(read)

        # Assert
        assert observed == "carried"

    @pytest.mark.asyncio
    async def test_to_thread_with_armed_parent_chain(self):
        """Test wool.to_thread runs the offloaded function on a fresh chain.

        Given:
            An armed context whose chain id is known.
        When:
            wool.to_thread offloads a function reading
            current_snapshot().chain_id.
        Then:
            It should differ from the caller's chain id.
        """
        # Arrange
        var = ContextVar(_unique("to_thread_chain"))

        def read_chain() -> uuid.UUID:
            snapshot = current_snapshot()
            assert snapshot is not None
            return snapshot.chain_id

        var.set("x")
        caller_snapshot = current_snapshot()
        assert caller_snapshot is not None

        # Act
        offloaded_chain = await to_thread(read_chain)

        # Assert
        assert offloaded_chain != caller_snapshot.chain_id

    @pytest.mark.asyncio
    async def test_to_thread_mutation_in_worker_thread(self):
        """Test mutations made inside wool.to_thread do not propagate back.

        Given:
            An armed context with a ContextVar set.
        When:
            wool.to_thread offloads a function that resets the
            variable.
        Then:
            It should leave the caller observing its own value — the
            offloaded chain is detached, with no merge-back.
        """
        # Arrange
        var = ContextVar(_unique("to_thread_no_merge"))

        def mutate() -> None:
            var.set("thread-value")

        var.set("caller-value")

        # Act
        await to_thread(mutate)

        # Assert
        assert var.get() == "caller-value"

    @pytest.mark.asyncio
    async def test_to_thread_with_armed_context_and_guard(self):
        """Test wool.to_thread does not trip the concurrent-entry guard.

        Given:
            An armed context.
        When:
            wool.to_thread offloads a function that touches a
            ContextVar.
        Then:
            It should not raise wool.ConcurrentChainEntry.
        """
        # Arrange
        var = ContextVar(_unique("to_thread_guard"))

        def touch() -> str:
            return var.get()

        var.set("ok")

        # Act
        result = await to_thread(touch)

        # Assert
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_to_thread_with_unarmed_context(self):
        """Test wool.to_thread on an unarmed context offloads without arming.

        Given:
            An unarmed context.
        When:
            wool.to_thread offloads a function that reads
            current_snapshot.
        Then:
            It should observe None.
        """

        def read() -> object:
            return current_snapshot()

        # Act
        observed = await to_thread(read)

        # Assert
        assert observed is None
