import asyncio
import contextvars
import uuid

import pytest
import pytest_asyncio
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from tests.helpers import scoped_context
from wool.runtime.context import ChainContention
from wool.runtime.context import ContextVar
from wool.runtime.context import TaskFactoryDisplaced
from wool.runtime.context import current_context
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

    def test_install_task_factory_outside_running_loop_raises_runtime_error(self):
        """Test install_task_factory(loop=None) outside a loop raises clearly.

        Given:
            No running event loop in the calling scope.
        When:
            install_task_factory is called with no loop argument.
        Then:
            It should raise a RuntimeError whose message names
            install_task_factory and directs the caller to either run
            inside a running loop or pass ``loop=`` explicitly.
        """
        # Arrange, act & assert
        with pytest.raises(RuntimeError, match="install_task_factory"):
            install_task_factory()

    @pytest.mark.asyncio
    async def test_install_task_factory_inner_raises_closes_user_coroutine(
        self, recwarn
    ):
        """Test ``inner`` raising closes the wrapper and user coroutine.

        Given:
            Wool's factory installed and an inner factory that raises
            unconditionally; an armed parent so the child coroutine
            is wrapped in ``_forked_scope``.
        When:
            asyncio.create_task is called and the inner factory raises.
        Then:
            The exception should propagate, no "coroutine was never
            awaited" RuntimeWarning should be emitted at GC, and the
            user coroutine should be closed.
        """
        # Arrange
        import gc

        def failing_inner(
            loop: asyncio.AbstractEventLoop,
            coro,
            **kwargs,
        ) -> asyncio.Task:
            raise RuntimeError("inner refused the kwargs")

        loop = asyncio.get_running_loop()
        loop.set_task_factory(failing_inner)
        install_task_factory()

        var = ContextVar(_unique("inner_raises"))
        var.set("armed")

        async def user_coro() -> None:  # pragma: no cover — never awaited
            return None

        coro = user_coro()

        # Act
        try:
            with pytest.raises(RuntimeError, match="inner refused"):
                loop.create_task(coro)

            # Assert — collect to force any pending RuntimeWarning.
            del coro
            gc.collect()
            leaks = [
                w
                for w in recwarn.list
                if issubclass(w.category, RuntimeWarning)
                and "never awaited" in str(w.message)
            ]
            assert not leaks, f"unexpected coroutine-never-awaited warnings: {leaks}"
        finally:
            # Restore so teardown asyncgen shutdown can create tasks.
            loop.set_task_factory(None)

    @pytest.mark.asyncio
    async def test_install_task_factory_detects_wool_buried_under_third_party(self):
        """Test the idempotency check sees Wool buried under a third-party.

        Given:
            A loop where Wool's factory was installed first and a
            third-party factory was installed *over* it (a known
            ordering hazard).
        When:
            install_task_factory is called again.
        Then:
            It should detect the buried Wool layer via the
            ``__wool_inner__`` chain walk and skip the install rather
            than wrap into a ``wool → third-party → wool`` composition.
        """
        # Arrange — install Wool first, then a third-party factory
        # over it that explicitly preserves the inner chain attribute.
        loop = asyncio.get_running_loop()
        loop.set_task_factory(None)
        install_task_factory()
        wool_factory = loop.get_task_factory()

        def third_party(
            loop: asyncio.AbstractEventLoop,
            coro,
            **kwargs,
        ) -> asyncio.Task:
            return wool_factory(loop, coro, **kwargs)  # pyright: ignore[reportCallIssue]

        # Expose the inner chain so the walk can find Wool buried below.
        third_party.__wool_inner__ = wool_factory  # type: ignore[attr-defined]
        loop.set_task_factory(third_party)

        # Act — calling install_task_factory again should be a no-op.
        install_task_factory()

        # Assert — the third-party factory still sits on the loop,
        # unchanged. A naive outer-only check would have re-wrapped.
        assert loop.get_task_factory() is third_party

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
            context = current_context()
            assert context is not None
            return context.chain_id

        # Act
        install_task_factory()
        var.set("x")
        parent_context = current_context()
        assert parent_context is not None
        child_chain = await asyncio.create_task(child())

        # Assert
        assert child_chain != parent_context.chain_id
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
            context = current_context()
            return context.chain_id if context is not None else None

        install_task_factory()
        var.set("x")
        parent_context = current_context()
        assert parent_context is not None

        # Act
        loop = asyncio.get_running_loop()
        loop.set_task_factory(user_factory)
        child_chain = await asyncio.create_task(child())

        # Assert
        assert child_chain == parent_context.chain_id

    @pytest.mark.asyncio
    @given(
        parent_value=st.one_of(st.integers(), st.text(), st.lists(st.integers())),
        child_value=st.one_of(st.integers(), st.text(), st.lists(st.integers())),
    )
    @settings(
        max_examples=25,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    async def test_install_task_factory_copy_on_fork_child(
        self, parent_value, child_value
    ):
        """Test a child task forks the parent chain, inheriting then isolating state.

        Given:
            An armed parent context with Wool's task factory installed
            and a variable set to an arbitrary parent value.
        When:
            A child task created with asyncio.create_task reads the
            inherited value, reads its own chain id, then mutates the
            variable to an arbitrary child value.
        Then:
            The child should observe the parent's value, run on a chain
            id distinct from the parent's, and its mutation should not
            leak back — copy-on-fork inherits, forks, and isolates.
        """
        # Arrange
        var = ContextVar(_unique("copy_on_fork"))

        async def child() -> tuple[object, uuid.UUID]:
            inherited = var.get()
            context = current_context()
            assert context is not None
            var.set(child_value)
            return inherited, context.chain_id

        install_task_factory()
        var.set(parent_value)
        parent_context = current_context()
        assert parent_context is not None

        # Act
        inherited, child_chain = await asyncio.create_task(child())

        # Assert
        assert inherited == parent_value
        assert child_chain != parent_context.chain_id
        assert var.get() == parent_value

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
            context = current_context()
            assert context is not None
            return context.chain_id

        # Act
        var.set("x")  # No explicit install_task_factory() call.
        parent_context = current_context()
        assert parent_context is not None
        child_chain = await asyncio.create_task(child())

        # Assert
        assert child_chain != parent_context.chain_id

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
            A child task reads current_context.
        Then:
            It should observe None — an unarmed fork is a dormant
            no-op.
        """

        async def child() -> object:
            return current_context()

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
            It should raise wool.ChainContention — two tasks
            cannot interleave on one context's chain context.
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
            with pytest.raises(ChainContention):
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
        """Test arming a shared unarmed context trips the owning-task guard.

        Given:
            Wool's task factory installed and one unarmed
            contextvars.Context shared by two concurrently-live tasks,
            one of which arms it with a wool.ContextVar.set.
        When:
            The other task touches a wool.ContextVar on that now-armed
            chain.
        Then:
            It should raise wool.ChainContention — the owning-task
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
        with pytest.raises(ChainContention):
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
        """Test a displaced Wool factory raises TaskFactoryDisplaced.

        Given:
            A running loop where Wool's task factory was self-installed
            by a first wool.ContextVar.set, then displaced by a
            third-party task factory installed after it.
        When:
            Wool's self-install path runs again — triggered by a first
            wool.ContextVar.set in a fresh unarmed context.
        Then:
            It should raise TaskFactoryDisplaced — copy-on-fork is
            silently lost for tasks created since the displacement, so
            the displacement is surfaced rather than passing unnoticed.
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
            with pytest.raises(TaskFactoryDisplaced, match="displaced"):
                ContextVar(_unique("displace_trigger")).set("y")

    @pytest.mark.asyncio
    async def test_install_task_factory_direct_install_is_displacement_monitored(self):
        """Test a direct install_task_factory call registers the loop for displacement.

        Given:
            A running loop where Wool's task factory was installed by a
            direct install_task_factory() call — not a self-install from
            wool.ContextVar.set — then displaced by a third-party factory
            installed after it.
        When:
            Wool's self-install path runs again, triggered by a first
            wool.ContextVar.set in a fresh unarmed context.
        Then:
            It should raise TaskFactoryDisplaced — a direct install
            also records the loop, so direct-install (e.g. worker)
            loops are displacement-monitored like self-installed ones.
        """
        # Arrange
        loop = asyncio.get_running_loop()

        def user_factory(
            loop: asyncio.AbstractEventLoop,
            coro,
            **kwargs,
        ) -> asyncio.Task:
            return asyncio.Task(coro, loop=loop, **kwargs)

        # A direct install records the loop for displacement monitoring.
        install_task_factory(loop)
        # A third-party factory installed afterwards displaces Wool's.
        loop.set_task_factory(user_factory)

        # Act & assert — a first-set re-enters the self-install path,
        # which finds the loop recorded but the factory not Wool-wrapped.
        with scoped_context():
            with pytest.raises(TaskFactoryDisplaced, match="displaced"):
                ContextVar(_unique("direct_displace_trigger")).set("x")

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

    @pytest.mark.asyncio
    async def test_install_task_factory_cancel_before_step_closes_the_coroutine(
        self, recwarn
    ):
        """Test an armed task cancelled before its first step leaks no warning.

        Given:
            An armed parent context with Wool's task factory installed,
            and a child coroutine wrapped by the factory's forked
            scope.
        When:
            The child task is cancelled before the loop ever steps it,
            then awaited to completion.
        Then:
            No "coroutine was never awaited" RuntimeWarning should
            leak — the factory's release callback closes the un-stepped
            wrapped coroutine.
        """
        # Arrange
        var = ContextVar(_unique("cancel_before_step"))
        stepped = [False]

        async def child() -> None:
            stepped[0] = True

        install_task_factory()
        var.set("x")

        # Act — cancel before yielding control, so the task is never
        # stepped, then await it so its done-callback runs.
        child_task = asyncio.create_task(child())
        child_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await child_task
        # Force a GC cycle so any un-awaited coroutine would surface.
        import gc

        gc.collect()

        # Assert
        assert stepped[0] is False
        assert not [
            w
            for w in recwarn.list
            if issubclass(w.category, RuntimeWarning)
            and "never awaited" in str(w.message)
        ]


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
            current_context().chain_id.
        Then:
            It should differ from the caller's chain id.
        """
        # Arrange
        var = ContextVar(_unique("to_thread_chain"))

        def read_chain() -> uuid.UUID:
            context = current_context()
            assert context is not None
            return context.chain_id

        var.set("x")
        caller_context = current_context()
        assert caller_context is not None

        # Act
        offloaded_chain = await to_thread(read_chain)

        # Assert
        assert offloaded_chain != caller_context.chain_id

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
        """Test wool.to_thread does not trip the chain-contention guard.

        Given:
            An armed context.
        When:
            wool.to_thread offloads a function that touches a
            ContextVar.
        Then:
            It should not raise wool.ChainContention.
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
            current_context.
        Then:
            It should observe None.
        """

        def read() -> object:
            return current_context()

        # Act
        observed = await to_thread(read)

        # Assert
        assert observed is None


class TestTaskFactoryDisplaced:
    def test_task_factory_displaced_is_runtime_error_subclass(self):
        """Test TaskFactoryDisplaced is a RuntimeError subclass.

        Given:
            The TaskFactoryDisplaced exception class.
        When:
            Its subclass relationship to RuntimeError is checked.
        Then:
            It should be a subclass of RuntimeError — displacement is
            an unconditional structural failure, not a tunable warning;
            this lets callers catch it through the standard exception
            hierarchy.
        """
        # Arrange, act, & assert
        assert issubclass(TaskFactoryDisplaced, RuntimeError)

    def test_task_factory_displaced_is_re_exported_from_wool(self):
        """Test TaskFactoryDisplaced is re-exported on the wool package.

        Given:
            The wool package and the TaskFactoryDisplaced class.
        When:
            wool.TaskFactoryDisplaced is accessed.
        Then:
            It should be the same class as the one defined in
            wool.runtime.context.factory.
        """
        # Arrange, act, & assert
        assert wool.TaskFactoryDisplaced is TaskFactoryDisplaced
