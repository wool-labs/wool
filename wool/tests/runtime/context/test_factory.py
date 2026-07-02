import asyncio
import contextvars
import gc
import logging
import uuid
import weakref

import pytest
import pytest_asyncio
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from tests.helpers import scoped_context
from wool.runtime.context.exceptions import ChainContention
from wool.runtime.context.exceptions import TaskFactoryDisplaced
from wool.runtime.context.factory import context_is_armed
from wool.runtime.context.factory import install_task_factory
from wool.runtime.context.threading import to_thread
from wool.runtime.context.var import ContextVar


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


@pytest.mark.asyncio
async def test_install_task_factory_should_install_factory_when_none_exists():
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
async def test_install_task_factory_should_not_double_wrap_when_already_installed():
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


def test_install_task_factory_should_raise_runtime_error_when_outside_running_loop():
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
async def test_first_set_should_self_install_the_task_factory():
    """Test a bare ContextVar.set self-installs the task factory.

    Given:
        A running loop with no task factory installed and no
        explicit install_task_factory call.
    When:
        A wool.ContextVar is set for the first time, arming the
        chain, then a child task is created with
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
        context = wool.__chain__.get(None)
        assert context is not None
        return context.id

    # Act
    var.set("x")  # No explicit install_task_factory() call.
    parent_context = wool.__chain__.get(None)
    assert parent_context is not None
    child_chain = await asyncio.create_task(child())

    # Assert
    assert child_chain != parent_context.id


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
async def test_install_task_factory_should_inherit_then_isolate_state_on_fork(
    parent_value, child_value
):
    """Test a child task forks the parent chain, inheriting then isolating state.

    Given:
        An armed parent chain with Wool's task factory installed
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
        context = wool.__chain__.get(None)
        assert context is not None
        var.set(child_value)
        return inherited, context.id

    install_task_factory()
    var.set(parent_value)
    parent_context = wool.__chain__.get(None)
    assert parent_context is not None

    # Act
    inherited, child_chain = await asyncio.create_task(child())

    # Assert
    assert inherited == parent_value
    assert child_chain != parent_context.id
    assert var.get() == parent_value


@pytest.mark.asyncio
async def test_install_task_factory_should_stay_unarmed_when_parent_unarmed():
    """Test a child task of an unarmed chain stays unarmed.

    Given:
        An unarmed chain with Wool's task factory installed.
    When:
        A child task reads current_context.
    Then:
        It should observe None — an unarmed fork is a dormant
        no-op.
    """

    async def child() -> object:
        return wool.__chain__.get(None)

    install_task_factory()

    # Act
    observed = await asyncio.create_task(child())

    # Assert
    assert observed is None


@pytest.mark.asyncio
async def test_install_task_factory_should_raise_when_child_resets_parent_token():
    """Test a child task cannot reset a Token minted in the parent chain.

    Given:
        An armed parent chain whose set produced a Token.
    When:
        A child task attempts to reset that parent Token.
    Then:
        Stdlib's :meth:`contextvars.ContextVar.reset` raises
        :class:`ValueError` naming the different
        :class:`contextvars.Context` — the parent and the
        copy-on-fork child run in distinct contexts.
    """
    # Arrange
    var = ContextVar(_unique("child_token"))
    install_task_factory()
    token = var.set("x")

    async def child() -> None:
        var.reset(token)

    # Act & assert
    with pytest.raises(ValueError, match="different Context"):
        await asyncio.create_task(child())


@pytest.mark.asyncio
async def test_install_task_factory_should_wrap_an_existing_user_factory():
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
        context = wool.__chain__.get(None)
        assert context is not None
        return context.id

    # Act
    install_task_factory()
    var.set("x")
    parent_context = wool.__chain__.get(None)
    assert parent_context is not None
    child_chain = await asyncio.create_task(child())

    # Assert
    assert child_chain != parent_context.id
    assert counter[0] == 1


@pytest.mark.asyncio
async def test_install_task_factory_should_drop_fork_when_user_factory_installed_after():
    """Test that installing a user factory after Wool's drops copy-on-fork.

    Given:
        Wool's task factory installed on the running loop and a
        ContextVar armed in the parent chain.
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
        context = wool.__chain__.get(None)
        return context.id if context is not None else None

    install_task_factory()
    var.set("x")
    parent_context = wool.__chain__.get(None)
    assert parent_context is not None

    # Act
    loop = asyncio.get_running_loop()
    loop.set_task_factory(user_factory)
    child_chain = await asyncio.create_task(child())

    # Assert
    assert child_chain == parent_context.id


@pytest.mark.asyncio
async def test_install_task_factory_should_skip_when_wool_buried_under_third_party():
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
async def test_install_task_factory_should_close_coroutine_when_inner_raises(recwarn):
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
async def test_install_task_factory_should_close_coroutine_when_inner_raises_unarmed(
    recwarn,
):
    """Test ``inner`` raising in the unarmed branch closes the user coroutine.

    Given:
        Wool's factory installed over an inner factory that
        raises unconditionally, and an unarmed parent so the
        child coroutine is NOT wrapped in ``_forked_scope`` —
        the unarmed branch of the factory body, separate from
        the already-covered armed branch.
    When:
        asyncio.create_task is called from the unarmed parent
        and the inner factory raises.
    Then:
        The exception should propagate, no "coroutine was never
        awaited" RuntimeWarning should be emitted at GC, and the
        user coroutine should be closed by the unarmed-branch
        cleanup arm.
    """

    # Arrange
    def failing_inner(
        loop: asyncio.AbstractEventLoop,
        coro,
        **kwargs,
    ) -> asyncio.Task:
        raise RuntimeError("inner refused the kwargs")

    loop = asyncio.get_running_loop()
    loop.set_task_factory(failing_inner)
    install_task_factory()

    async def user_coro() -> None:  # pragma: no cover — never awaited
        return None

    coro = user_coro()

    # Act — no var.set, so the parent (and therefore the child)
    # is unarmed. The factory takes the no-wrap branch.
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
async def test_install_task_factory_should_close_coroutine_when_cancelled_before_step(
    recwarn,
):
    """Test an armed task cancelled before its first step leaks no warning.

    Given:
        An armed parent chain with Wool's task factory installed,
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
    gc.collect()

    # Assert
    assert stepped[0] is False
    assert not [
        w
        for w in recwarn.list
        if issubclass(w.category, RuntimeWarning) and "never awaited" in str(w.message)
    ]


@pytest.mark.parametrize("collect_between", [False, True], ids=["immediate", "after_gc"])
@pytest.mark.asyncio
async def test_install_task_factory_should_raise_when_context_shared_across_live_tasks(
    collect_between: bool,
):
    """Test the factory rejects one contextvars.Context shared by two tasks.

    Given:
        An armed chain with Wool's task factory installed and a
        live task created with an explicit contextvars.Context,
        optionally after a garbage collection while that task is in
        flight.
    When:
        A second task is created with that same context object
        while the first is still running.
    Then:
        It should raise wool.ChainContention — two tasks cannot
        interleave on one context's chain context, and a collection
        must not evict the live task's registry entry because the live
        task pins its own registry value.
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
        if collect_between:
            # Let the first task start, then force a collection: the
            # weak registry must keep the live task's entry.
            await asyncio.sleep(0)
            gc.collect()
        with pytest.raises(ChainContention):
            loop.create_task(body(), context=shared)
    finally:
        release.set()
        await first


@pytest.mark.asyncio
async def test_install_task_factory_should_not_raise_when_unarmed_context_shared():
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
async def test_install_task_factory_should_raise_when_shared_context_armed_late():
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
async def test_install_task_factory_should_not_raise_when_context_reused_after_task():
    """Test a contextvars.Context is reusable once its task has finished.

    Given:
        An armed chain with Wool's task factory installed and a
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
async def test_install_task_factory_should_not_raise_when_tasks_use_default_contexts():
    """Test concurrent tasks with default per-task contexts are not rejected.

    Given:
        An armed chain with Wool's task factory installed.
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
async def test_install_task_factory_should_raise_when_displaced_by_later_factory():
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
async def test_install_task_factory_should_monitor_displacement_when_direct_install():
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


def test_install_task_factory_should_not_warn_when_finalized_on_idle_loop(caplog):
    """Test the finalizer treats a non-running loop as teardown.

    Given:
        Wool's task factory installed on an event loop that is never
        started.
    When:
        The factory is replaced and dropped so it is garbage-collected
        while the loop is not running.
    Then:
        It should not warn that the factory was displaced — a finalizer
        firing on a non-running loop is normal teardown, not the
        displacement case that warns and poisons the loop.
    """
    # Arrange
    loop = asyncio.new_event_loop()
    try:
        install_task_factory(loop)

        # Act — drop Wool's factory so it is collected and its finalizer
        # fires while the loop is not running.
        with caplog.at_level(logging.WARNING, logger="wool.runtime.context.factory"):
            loop.set_task_factory(None)
            gc.collect()

        # Assert — no displacement warning was emitted for the teardown.
        assert not [r for r in caplog.records if "displaced" in r.getMessage().lower()]
    finally:
        loop.close()


@pytest.mark.asyncio
async def test_install_task_factory_should_raise_when_displacer_keeps_factory_alive():
    """Test displacement is caught at the next set when the finalizer cannot fire.

    Given:
        Wool's task factory installed, then displaced by a third-party
        factory that keeps the Wool factory object alive — so its
        finalizer never fires and the loop is not pre-flagged as
        displaced.
    When:
        A wool.ContextVar value is next set.
    Then:
        It should raise TaskFactoryDisplaced — the set-time factory
        inspection catches the displacement the finalizer missed.
    """
    # Arrange
    loop = asyncio.get_running_loop()
    install_task_factory(loop)
    stashed = loop.get_task_factory()  # strong ref so the finalizer cannot fire

    def user_factory(loop, coro, **kwargs) -> asyncio.Task:
        return asyncio.Task(coro, loop=loop, **kwargs)

    loop.set_task_factory(user_factory)

    # Act & assert
    with scoped_context():
        with pytest.raises(TaskFactoryDisplaced, match="displaced"):
            ContextVar(_unique("kept_alive_trigger")).set("x")
    assert stashed is not None  # keep the factory alive through the assert


@pytest.mark.asyncio
async def test_install_task_factory_should_raise_when_completion_sees_displacement():
    """Test a completing Wool task detects displacement as a backstop.

    Given:
        Wool's task factory installed with one Wool task in flight,
        then displaced by a third-party factory whose installation
        keeps the Wool factory object alive (so its finalizer never
        fires and the loop is not pre-flagged as displaced).
    When:
        The in-flight task completes.
    Then:
        A subsequent wool.ContextVar set should raise
        TaskFactoryDisplaced — the completing task is the backstop that
        notices the loop's factory is no longer Wool's.
    """
    # Arrange
    loop = asyncio.get_running_loop()
    install_task_factory(loop)
    stashed = loop.get_task_factory()  # keep Wool's factory alive

    def user_factory(loop, coro, **kwargs) -> asyncio.Task:
        return asyncio.Task(coro, loop=loop, **kwargs)

    release = asyncio.Event()

    async def in_flight() -> None:
        await release.wait()

    # A Wool-created task in flight; its done-callback is the backstop.
    # Left unarmed so the task body touches no wool.ContextVar and can
    # complete cleanly after the displacement, letting the done-callback
    # observe it rather than tripping the set-time check mid-task.
    task = loop.create_task(in_flight())

    # Act — displace Wool while the task is in flight, then let it finish
    # so its done-callback runs the displacement backstop.
    loop.set_task_factory(user_factory)
    release.set()
    await task
    await asyncio.sleep(0)

    # Assert
    with scoped_context():
        with pytest.raises(TaskFactoryDisplaced, match="displaced"):
            ContextVar(_unique("backstop_trigger")).set("y")
    assert stashed is not None  # keep the factory alive through the assert


@pytest.mark.asyncio
async def test_install_task_factory_should_detect_buried_wool_via_cached_layer():
    """Test the idempotency walk short-circuits on an already-cached layer.

    Given:
        Wool's factory buried beneath nested third-party wrappers,
        after Wool has already been detected once on the loop (so the
        inner wrapper is already memoized).
    When:
        install_task_factory is called again with a further wrapper
        stacked on top.
    Then:
        Wool should still be detected through the wrappers and the call
        should be idempotent — the loop's factory is left unchanged.
    """
    # Arrange — install Wool, bury it under a third party, and prime the
    # detection cache by detecting that buried layer once.
    loop = asyncio.get_running_loop()
    loop.set_task_factory(None)
    install_task_factory()
    wool_factory = loop.get_task_factory()

    def third_party(loop, coro, **kwargs) -> asyncio.Task:
        return wool_factory(loop, coro, **kwargs)  # pyright: ignore[reportCallIssue]

    third_party.__wool_inner__ = wool_factory  # type: ignore[attr-defined]
    loop.set_task_factory(third_party)
    install_task_factory()  # memoizes the third-party layer

    def outer(loop, coro, **kwargs) -> asyncio.Task:
        return third_party(loop, coro, **kwargs)

    outer.__wool_inner__ = third_party  # type: ignore[attr-defined]
    loop.set_task_factory(outer)

    # Act — the walk now hits the cached third-party layer mid-chain.
    install_task_factory()

    # Assert — Wool detected via the cache; the install was skipped.
    assert loop.get_task_factory() is outer


@pytest.mark.asyncio
async def test_install_task_factory_should_clear_reservation_when_armed_inner_raises(
    recwarn,
):
    """Test a failed armed task creation releases its context reservation.

    Given:
        Wool's factory composed over an inner factory that raises, and
        an armed Wool context passed explicitly to task creation.
    When:
        A task is created with that context and the inner factory
        raises.
    Then:
        The error should propagate, no "coroutine was never awaited"
        warning should be emitted, and the same context should remain
        reusable for a later task — its pending reservation was cleared
        rather than pinned.
    """

    # Arrange
    def failing_inner(loop, coro, **kwargs) -> asyncio.Task:
        raise RuntimeError("inner refused the kwargs")

    loop = asyncio.get_running_loop()
    loop.set_task_factory(failing_inner)
    install_task_factory()

    var = ContextVar(_unique("armed_reservation"))
    var.set("armed")
    armed_ctx = contextvars.copy_context()

    async def user_coro() -> None:  # pragma: no cover — never awaited
        return None

    first, second = user_coro(), user_coro()

    # Act & assert
    try:
        with pytest.raises(RuntimeError, match="inner refused"):
            loop.create_task(first, context=armed_ctx)
        # The reservation must be released: a second creation with the
        # same context reaches the (failing) inner factory again rather
        # than tripping the contention guard on a pinned slot.
        with pytest.raises(RuntimeError, match="inner refused"):
            loop.create_task(second, context=armed_ctx)

        del first, second
        gc.collect()
        leaks = [
            w
            for w in recwarn.list
            if issubclass(w.category, RuntimeWarning)
            and "never awaited" in str(w.message)
        ]
        assert not leaks, f"unexpected coroutine-never-awaited warnings: {leaks}"
    finally:
        loop.set_task_factory(None)


def test_install_task_factory_should_evict_task_when_release_stranded_and_gc():
    """Test a registered task is reclaimed when its release callback is stranded.

    Given:
        Wool's task factory installed on a dedicated event loop and an
        armed task registered in the chain-contention registry whose
        per-task release callback never fires — the loop is torn down
        before it can run, the worker-loop teardown scenario that
        strands the callback.
    When:
        The last strong reference to the task is dropped and a garbage
        collection is forced.
    Then:
        It should be reclaimed and its registry entry evicted — the
        registry holds tasks weakly, so worker bookkeeping stays bounded
        even when the release callback never runs, rather than leaking a
        done-but-pinned task per dispatch.
    """
    # Arrange
    loop = asyncio.new_event_loop()
    install_task_factory(loop)
    var = ContextVar(_unique("stranded_release"))
    tracker: dict[str, object] = {}

    async def arm_and_register() -> None:
        # Arm the chain, then register a child under the armed context.
        # The child is never stepped: run_until_complete returns the
        # moment this coroutine finishes, so the child's first step —
        # and therefore its release callback — never runs, exactly as
        # when a worker loop is torn down mid-dispatch.
        var.set("x")
        armed_context = contextvars.copy_context()

        async def child() -> None:  # pragma: no cover — never stepped
            return None

        async def probe() -> None:  # pragma: no cover — rejected before it runs
            return None

        task = loop.create_task(child(), context=armed_context)
        tracker["ref"] = weakref.ref(task)
        # Precondition: the chain-contention registry is private with no
        # public surface, so prove it actually tracks the task before
        # asserting eviction. While the task is live, re-passing its
        # armed context is rejected as chain contention — the public
        # proof that the entry exists. A strong-dict registry would
        # additionally pin the task alive past collection.
        try:
            loop.create_task(probe(), context=armed_context)
        except ChainContention:
            tracker["registered"] = True

    try:
        loop.run_until_complete(arm_and_register())
    finally:
        # Tear the loop down before the stranded release callback can
        # run. This drops the loop's scheduled first-step handle — the
        # only strong reference to the task besides the weak registry.
        loop.set_exception_handler(lambda _loop, _context: None)
        loop.close()

    # Act — collect now that the loop and every strong reference to the
    # task are gone.
    gc.collect()

    # Assert
    assert tracker.get("registered") is True
    assert tracker["ref"]() is None  # type: ignore[operator]


@pytest.mark.asyncio
async def test_install_task_factory_should_keep_pending_reservation_across_gc():
    """Test the pending-slot reservation survives a garbage collection.

    Given:
        Wool's task factory composed over an inner factory that, while
        it runs, forces a garbage collection and then re-enters task
        creation with the same armed context whose slot Wool has just
        reserved with its pending sentinel.
    When:
        A task is created with that armed context, driving the inner
        factory through the reservation window.
    Then:
        It should raise wool.ChainContention — the pending reservation
        is intact across the collection because the sentinel is a
        strongly-held module singleton — and the original task should
        still run to completion once the slot is populated.
    """
    # Arrange
    loop = asyncio.get_running_loop()
    var = ContextVar(_unique("pending_reservation"))
    var.set("x")
    armed_context = contextvars.copy_context()
    observed: dict[str, object] = {}

    async def probe() -> None:  # pragma: no cover — rejected before it runs
        return None

    def reservation_probe(
        inner_loop: asyncio.AbstractEventLoop,
        coro,
        **kwargs,
    ) -> asyncio.Task:
        # Wool reserved the armed context's slot with its pending
        # sentinel before delegating here. Force a collection, then try
        # to create a second task on the same armed context: the pending
        # reservation must still register as a live owner.
        gc.collect()
        try:
            inner_loop.create_task(probe(), context=armed_context)
        except ChainContention:
            observed["contention"] = True
        return asyncio.Task(coro, loop=inner_loop, **kwargs)

    loop.set_task_factory(reservation_probe)
    install_task_factory()

    async def body() -> int:
        return 7

    # Act
    result = await loop.create_task(body(), context=armed_context)

    # Assert
    assert observed.get("contention") is True
    assert result == 7


@pytest.mark.asyncio
async def test_to_thread_should_return_result_when_positional_args():
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
async def test_to_thread_should_forward_keyword_args():
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
async def test_to_thread_should_carry_caller_value_into_thread_when_armed_context():
    """Test wool.to_thread carries the caller's ContextVar value into the thread.

    Given:
        An armed chain with a ContextVar set.
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
async def test_to_thread_should_not_trip_contention_guard_when_armed_context():
    """Test wool.to_thread does not trip the chain-contention guard.

    Given:
        An armed chain.
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
async def test_to_thread_should_not_arm_when_unarmed_context():
    """Test wool.to_thread on an unarmed chain offloads without arming.

    Given:
        An unarmed chain.
    When:
        wool.to_thread offloads a function that reads
        current_context.
    Then:
        It should observe None.
    """

    def read() -> object:
        return wool.__chain__.get(None)

    # Act
    observed = await to_thread(read)

    # Assert
    assert observed is None


@pytest.mark.asyncio
async def test_to_thread_should_run_on_fresh_chain_when_armed_parent_chain():
    """Test wool.to_thread runs the offloaded function on a fresh chain.

    Given:
        An armed chain whose chain id is known.
    When:
        wool.to_thread offloads a function reading
        wool.__chain__.get(None).
    Then:
        It should differ from the caller's chain id.
    """
    # Arrange
    var = ContextVar(_unique("to_thread_chain"))

    def read_chain() -> uuid.UUID:
        context = wool.__chain__.get(None)
        assert context is not None
        return context.id

    var.set("x")
    caller_context = wool.__chain__.get(None)
    assert caller_context is not None

    # Act
    offloaded_chain = await to_thread(read_chain)

    # Assert
    assert offloaded_chain != caller_context.id


@pytest.mark.asyncio
async def test_to_thread_should_not_propagate_mutation_from_worker_thread():
    """Test mutations made inside wool.to_thread do not propagate back.

    Given:
        An armed chain with a ContextVar set.
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


def test_context_is_armed_should_return_true_when_armed_context():
    """Test context_is_armed returns True for a context carrying a context.

    Given:
        A contextvars.Context in which a wool.ContextVar has been set.
    When:
        context_is_armed is called on it.
    Then:
        It should return True — the context carries a non-None Wool
        context.
    """
    # Arrange
    var = ContextVar(_unique("armed_probe"))

    def _arm() -> None:
        var.set("x")

    armed_context = contextvars.copy_context()
    armed_context.run(_arm)

    # Act & assert
    assert context_is_armed(armed_context) is True


def test_context_is_armed_should_return_false_when_unarmed_context():
    """Test context_is_armed returns False for a context with no context.

    Given:
        A fresh contextvars.Context in which no wool.ContextVar has
        been set.
    When:
        context_is_armed is called on it.
    Then:
        It should return False — an unarmed context is
        indistinguishable from a plain contextvars.Context.
    """
    # Arrange
    unarmed_context = contextvars.Context()

    # Act & assert
    assert context_is_armed(unarmed_context) is False
