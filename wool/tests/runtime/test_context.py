import asyncio
import contextvars

import pytest
from hypothesis import given
from hypothesis import strategies as st

from wool.runtime.context import Context
from wool.runtime.context import ContextVar
from wool.runtime.context import ContextVarCollision
from wool.runtime.context import Token
from wool.runtime.context import build_frame_payload
from wool.runtime.context import current_context


@pytest.fixture(autouse=True)
def isolate_registry():
    from wool.runtime.context import _current_context
    from wool.runtime.context import _thread_context

    saved = dict(ContextVar._registry)
    ctx = _current_context()
    saved_data = dict(ctx._data)
    yield
    ctx._data.clear()
    ctx._data.update(saved_data)
    ContextVar._registry.clear()
    for k, v in saved.items():
        ContextVar._registry[k] = v
    if hasattr(_thread_context, "ctx"):
        _thread_context.ctx._data.clear()


class TestContextVar:
    def test___init___with_name_only(self):
        """Test ContextVar initialization with a name and no default.

        Given:
            A name string
        When:
            ContextVar is instantiated with the name
        Then:
            It should expose the name and raise LookupError on get() with no value set
        """
        # Act
        var = ContextVar("init_nameonly")

        # Assert
        assert var.name == "init_nameonly"
        with pytest.raises(LookupError):
            var.get()

    def test___init___with_default(self):
        """Test ContextVar initialization with a default value.

        Given:
            A name string and a default value
        When:
            ContextVar is instantiated with both
        Then:
            get() should return the default when no value is set
        """
        # Arrange
        var = ContextVar("init_withdefault", default=42)

        # Act & assert
        assert var.get() == 42

    def test___init___infers_namespace_from_caller(self):
        """Test ContextVar infers namespace from the caller's top-level package.

        Given:
            A ContextVar constructed from this test module
        When:
            No explicit namespace is provided
        Then:
            The namespace should be the top-level package of ``__name__``
        """
        # Arrange
        expected_ns = __name__.partition(".")[0]

        # Act
        var = ContextVar("inferred")

        # Assert
        assert var.namespace == expected_ns
        assert var.key == f"{expected_ns}:inferred"

    def test___init___accepts_explicit_namespace(self):
        """Test ContextVar uses an explicit namespace when provided.

        Given:
            A ContextVar constructed with namespace='myapp'
        When:
            No implicit inference is needed
        Then:
            The key should combine the explicit namespace with the name
        """
        # Act
        var = ContextVar("explicit", namespace="myapp")

        # Assert
        assert var.namespace == "myapp"
        assert var.key == "myapp:explicit"

    @given(
        name=st.text(
            alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
            min_size=1,
            max_size=12,
        ),
        namespace=st.text(
            alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
            min_size=1,
            max_size=12,
        ),
        default_a=st.one_of(st.integers(), st.text(), st.booleans()),
        default_b=st.one_of(st.integers(), st.text(), st.booleans()),
    )
    def test___init___raises_on_duplicate_key(
        self, name, namespace, default_a, default_b
    ):
        """Test duplicate-key construction raises regardless of default match.

        Given:
            Any non-empty name, namespace, and pair of default values
        When:
            A ContextVar is constructed, then a second with the identical key
        Then:
            The second construction raises ContextVarCollision whether or
            not the two defaults are equal
        """
        # The registry holds vars weakly, so we hold ``first`` for the
        # duration of the second construction; without that pin the
        # weakref would drop before the second call and no collision
        # would fire. The try/finally isolates each Hypothesis example
        # from its siblings since Hypothesis reuses the test function.
        key = f"{namespace}:{name}"
        ContextVar._registry.pop(key, None)
        try:
            # Arrange
            first = ContextVar(name, namespace=namespace, default=default_a)

            # Act & assert
            with pytest.raises(ContextVarCollision):
                ContextVar(name, namespace=namespace, default=default_b)

            # Hold ``first`` to the end so the collision actually had
            # something to collide with.
            assert first.key == key
        finally:
            ContextVar._registry.pop(key, None)

    def test___init___allows_same_name_in_different_namespace(self):
        """Test duplicate names across different namespaces are allowed.

        Given:
            A ContextVar 'shared' in namespace 'lib_a'
        When:
            A second ContextVar 'shared' is constructed in namespace 'lib_b'
        Then:
            Both should register without collision
        """
        # Act
        a = ContextVar("shared", namespace="lib_a")
        b = ContextVar("shared", namespace="lib_b")

        # Assert
        assert a is not b
        assert a.key == "lib_a:shared"
        assert b.key == "lib_b:shared"

    def test_get_with_explicit_default_fallback(self):
        """Test ContextVar.get returns the supplied fallback when unset.

        Given:
            A ContextVar with no class-level default and no value set
        When:
            get() is called with a fallback argument
        Then:
            It should return the fallback argument
        """
        # Arrange
        var = ContextVar("no_default")

        # Act
        value = var.get("fallback")

        # Assert
        assert value == "fallback"

    def test_set_returns_usable_token(self):
        """Test ContextVar.set returns a Token that can restore prior state.

        Given:
            A ContextVar with a value set
        When:
            A second set() is performed
        Then:
            The returned Token should reset to the prior value via reset()
        """
        # Arrange
        var = ContextVar("restorable", default="initial")
        first_token = var.set("second")

        # Act
        second_token = var.set("third")
        var.reset(second_token)

        # Assert
        assert var.get() == "second"
        var.reset(first_token)
        assert var.get() == "initial"

    def test_reset_rejects_used_token(self):
        """Test ContextVar.reset raises on a token already consumed.

        Given:
            A ContextVar and a Token that has already been used
        When:
            reset() is called with the same token again
        Then:
            It should raise RuntimeError
        """
        # Arrange
        var = ContextVar("once", default=0)
        token = var.set(1)
        var.reset(token)

        # Act & assert
        with pytest.raises(RuntimeError):
            var.reset(token)

    def test_reset_rejects_token_for_different_var(self):
        """Test ContextVar.reset rejects tokens minted by a different var.

        Given:
            Two distinct ContextVar instances, each with a set value
        When:
            reset() is called on one with the other's token
        Then:
            It should raise ValueError
        """
        # Arrange
        a = ContextVar("reset_a", default=0)
        b = ContextVar("reset_b", default=0)
        token = a.set(1)

        # Act & assert
        with pytest.raises(ValueError):
            b.reset(token)

    def test_reset_restores_old_value_in_different_context_scope(self):
        """Test reset restores old_value when invoked in a different Context scope.

        Given:
            A ContextVar set twice in the original Context, then a
            different wool.Context scope entered via Context.run
        When:
            reset(token) is invoked inside the scoped Context
        Then:
            The var should revert to the value captured in the token
        """
        # Arrange
        var = ContextVar("reset_fallback", default="initial")
        var.set("first")
        token = var.set("second")
        seeded = contextvars.copy_context()

        def body():
            var.set("outer-most")
            var.reset(token)
            return var.get()

        # Act
        result = seeded.run(body)

        # Assert
        assert result == "first"

    def test_reset_restores_unset_in_different_context_scope(self):
        """Test reset restores unset state when old_value was MISSING.

        Given:
            A previously-unset ContextVar whose set() Token captured
            MISSING as the old_value, then a different wool.Context
            scope entered via Context.run
        When:
            reset(token) is invoked in the scoped Context
        Then:
            The var should revert to unset; get(fallback) returns the
            supplied fallback
        """
        # Arrange
        var = ContextVar("reset_unset_fallback")
        token = var.set("briefly")
        seeded = contextvars.copy_context()

        def body():
            var.set("nested")
            var.reset(token)
            return var.get("<fallback>")

        # Act
        result = seeded.run(body)

        # Assert
        assert result == "<fallback>"

    @pytest.mark.asyncio
    async def test_reset_rejects_token_from_different_context(self):
        """Test reset raises ValueError when the token is from another Context.

        Given:
            A Token minted inside one Context (async run)
        When:
            reset() is called on the var inside a different Context
        Then:
            It should raise ValueError citing the Context mismatch
        """
        # Arrange
        var = ContextVar("context_check", default="start")
        ctx_a = Context()
        ctx_b = Context()
        captured: list[Token] = []

        async def capture():
            captured.append(var.set("a"))

        await ctx_a.run_async(capture())

        async def try_reset():
            with pytest.raises(ValueError, match="different wool.Context"):
                var.reset(captured[0])

        # Act & assert
        await ctx_b.run_async(try_reset())


class TestToken:
    pass


class TestContext:
    def test___new___allows_direct_instantiation(self):
        """Test Context() constructs an empty Context with a fresh id.

        Given:
            The Context class
        When:
            It is instantiated directly
        Then:
            The result should have a fresh id and no captured vars
        """
        # Act
        ctx = Context()

        # Assert
        assert ctx.id is not None
        assert len(ctx) == 0

    def test_run_seeds_vars_and_scopes_mutations(self):
        """Test Context.run seeds vars in a fresh stdlib Context and scopes mutations.

        Given:
            A Context captured with an initial var value
        When:
            Context.run() runs a function that mutates the var
        Then:
            Mutations should be visible inside the run and captured on exit
        """
        # Arrange
        var = ContextVar("run_seed", default="initial")
        var.set("seeded")
        ctx = current_context()

        def body():
            assert var.get() == "seeded"
            var.set("mutated")
            return var.get()

        # Act
        result = ctx.run(body)

        # Assert
        assert result == "mutated"
        assert ctx[var] == "mutated"

    def test_run_binds_context_for_sync_callers(self):
        """Test Context.run makes self.id the active Context id inside fn.

        Given:
            A Context constructed directly (sync caller, no asyncio task)
        When:
            Context.run invokes a function that reads current_context().id
        Then:
            The reported context id equals the Context's own id, not the
            process-default id
        """
        # Arrange
        ctx = Context()

        # Act
        observed = ctx.run(lambda: current_context().id)

        # Assert
        assert observed == ctx.id

    def test_run_snapshot_skips_vars_that_are_unset_or_set_then_reset(self):
        """Test Context.run's snapshot excludes vars that look unset at exit.

        Given:
            One var never set inside the run, one var that is set and
            then reset inside the run
        When:
            Context.run() returns and captures the post-run snapshot
        Then:
            Neither var should appear in the Context's captured vars
        """
        # Arrange
        untouched = ContextVar("untouched", default="x")
        set_then_reset = ContextVar("set_then_reset")
        ctx = Context()

        def body():
            token = set_then_reset.set("temp")
            set_then_reset.reset(token)

        # Act
        ctx.run(body)

        # Assert
        assert untouched not in ctx
        assert set_then_reset not in ctx

    def test_run_concurrent_entry_raises(self):
        """Test Context.run raises on re-entry.

        Given:
            A Context with a task currently running inside it
        When:
            A second run() is attempted
        Then:
            It should raise RuntimeError
        """
        # Arrange
        ctx = Context()

        def outer():
            with pytest.raises(RuntimeError):
                ctx.run(lambda: None)

        # Act & assert
        ctx.run(outer)

    def test_iter_yields_captured_vars(self):
        """Test Context iterates over captured ContextVar instances.

        Given:
            A Context with multiple captured vars
        When:
            It is iterated
        Then:
            The iterator should yield each captured var
        """
        # Arrange
        a = ContextVar("iter_a", default=0)
        b = ContextVar("iter_b", default=0)
        a.set(1)
        b.set(2)

        # Act
        ctx = current_context()

        # Assert
        assert set(iter(ctx)) == {a, b}

    def test_getitem_returns_captured_value(self):
        """Test Context[var] returns the captured value.

        Given:
            A Context with a captured var
        When:
            The Context is indexed by the var
        Then:
            The captured value should be returned
        """
        # Arrange
        var = ContextVar("get_item", default=0)
        var.set(1)
        ctx = current_context()

        # Act & assert
        assert ctx[var] == 1

    def test_contains_reports_membership(self):
        """Test `var in ctx` reports whether the var was captured.

        Given:
            A Context with one captured var and one uncaptured var
        When:
            Membership is tested for each
        Then:
            The captured var should be present and the uncaptured absent
        """
        # Arrange
        in_ctx = ContextVar("present_var", default=0)
        out_ctx = ContextVar("absent_var", default=0)
        in_ctx.set(1)

        # Act
        ctx = current_context()

        # Assert
        assert in_ctx in ctx
        assert out_ctx not in ctx

    def test_len_returns_captured_count(self):
        """Test len(ctx) returns the number of captured vars.

        Given:
            A Context with two captured vars
        When:
            len() is called on the Context
        Then:
            It should return 2
        """
        # Arrange
        a = ContextVar("len_a", default=0)
        b = ContextVar("len_b", default=0)
        a.set(1)
        b.set(2)

        # Act
        ctx = current_context()

        # Assert
        assert len(ctx) == 2

    def test_keys_values_items_expose_captured_pairs(self):
        """Test Context keys/values/items expose the captured mapping.

        Given:
            A Context with two captured vars
        When:
            keys(), values(), items() are called
        Then:
            Each accessor should return the expected captured pairs
        """
        # Arrange
        a = ContextVar("kvitems_a", default="")
        b = ContextVar("kvitems_b", default="")
        a.set("x")
        b.set("y")

        # Act
        ctx = current_context()

        # Assert
        assert set(ctx.keys()) == {a, b}
        assert set(ctx.values()) == {"x", "y"}
        assert dict(ctx.items()) == {a: "x", b: "y"}

    def test_repr_includes_id_and_var_count(self):
        """Test Context repr mentions id and number of vars.

        Given:
            A Context with one captured var
        When:
            repr() is called on it
        Then:
            The repr should contain "id=" and "vars=1"
        """
        # Arrange
        var = ContextVar("repr_var", default=0)
        var.set(1)
        ctx = current_context()

        # Act
        text = repr(ctx)

        # Assert
        assert "id=" in text
        assert "vars=1" in text

    @pytest.mark.asyncio
    async def test_run_async_seeds_and_scopes(self):
        """Test Context.run_async seeds vars and scopes mutations asynchronously.

        Given:
            A Context with a seeded var value
        When:
            run_async runs a coroutine that mutates the var
        Then:
            The mutation should be visible inside and captured on exit
        """
        # Arrange
        var = ContextVar("runa_seed", default="initial")
        var.set("seeded")
        ctx = current_context()

        async def body():
            assert var.get() == "seeded"
            var.set("mutated")
            return var.get()

        # Act
        result = await ctx.run_async(body())

        # Assert
        assert result == "mutated"
        assert ctx[var] == "mutated"

    @pytest.mark.asyncio
    async def test_run_async_snapshot_skips_vars_unset_or_set_then_reset(self):
        """Test Context.run_async's snapshot excludes vars that look unset at exit.

        Given:
            One var never set inside the coroutine, one var set and then
            reset inside the coroutine
        When:
            Context.run_async returns and captures the post-run snapshot
        Then:
            Neither var should appear in the Context's captured vars
        """
        # Arrange
        untouched = ContextVar("async_untouched", default="x")
        set_then_reset = ContextVar("async_set_then_reset")
        ctx = Context()

        async def body():
            token = set_then_reset.set("temp")
            set_then_reset.reset(token)

        # Act
        await ctx.run_async(body())

        # Assert
        assert untouched not in ctx
        assert set_then_reset not in ctx

    @pytest.mark.asyncio
    async def test_run_async_snapshot_preserves_seeded_value(self):
        """Test Context.run_async captures seeded values after the coroutine runs.

        Given:
            A Context seeded with a var value and a coroutine that
            doesn't touch that var
        When:
            Context.run_async runs the coroutine
        Then:
            The captured snapshot still holds the seeded value — the
            var was set (in the seeded Context) and counts as modified
        """
        # Arrange
        var = ContextVar("runa_preserve_seed", default="initial")
        var.set("seeded")
        ctx = current_context()

        async def body():
            return var.get()

        # Act
        result = await ctx.run_async(body())

        # Assert
        assert result == "seeded"
        assert ctx[var] == "seeded"

    @pytest.mark.asyncio
    async def test_run_async_snapshot_reflects_worker_mutations(self):
        """Test Context.run_async captures worker-side mutations on top of seed values.

        Given:
            A Context seeded with two vars and a coro that modifies one
        When:
            Context.run_async runs the coroutine
        Then:
            The captured snapshot reflects the mutation for the touched
            var while the untouched var still shows the seed value
        """
        # Arrange
        untouched = ContextVar("runa_untouched_seed", default="initial")
        touched = ContextVar("runa_touched_seed", default="initial")
        untouched.set("seed_untouched")
        touched.set("seed_touched")
        ctx = current_context()

        async def body():
            touched.set("mutated")

        # Act
        await ctx.run_async(body())

        # Assert
        assert ctx[untouched] == "seed_untouched"
        assert ctx[touched] == "mutated"

    @pytest.mark.asyncio
    async def test_run_async_concurrent_entry_raises(self):
        """Test Context.run_async raises on re-entry while a task is running.

        Given:
            A Context with a run_async task currently pending
        When:
            A concurrent run_async is attempted
        Then:
            The second call should raise RuntimeError
        """
        # Arrange
        ctx = Context()

        async def slow():
            await asyncio.sleep(0.01)

        task = asyncio.create_task(ctx.run_async(slow()))
        await asyncio.sleep(0)

        second = slow()

        # Act & assert
        try:
            with pytest.raises(RuntimeError):
                await ctx.run_async(second)
        finally:
            second.close()

        await task

    def test_build_frame_payload_raises_typeerror_for_unpicklable_value(self):
        """Test build_frame_payload raises TypeError naming the offending var.

        Given:
            A ContextVar set to an unpicklable value (a local generator
            function object)
        When:
            build_frame_payload() is called to snapshot the current vars
        Then:
            TypeError is raised with a message naming the offending var
            key, per the public serialization contract that
            non-serializable values surface a TypeError at dispatch
            time.
        """
        # Arrange
        var = ContextVar("ctx001_unpicklable")

        def _local_gen():
            yield 1

        var.set(_local_gen())

        # Act & assert
        with pytest.raises(TypeError, match=var.key):
            build_frame_payload()

    @pytest.mark.asyncio
    async def test_current_context_omits_var_after_reset_to_unset(self):
        """Test current_context omits vars removed by reset.

        Given:
            A ContextVar set then immediately reset inside
            Context.run_async, removing it from the Context's data
        When:
            current_context() is invoked mid-run after the reset
        Then:
            The returned Context does not contain the var — reset
            popped it from _data so the snapshot excludes it.
        """
        # Arrange
        var = ContextVar("ctx005_reset_then_probe")
        ctx = Context()
        observed: list[Context] = []

        async def body():
            token = var.set("temporary")
            var.reset(token)
            observed.append(current_context())

        # Act
        await ctx.run_async(body())

        # Assert
        assert len(observed) == 1
        assert var not in observed[0]


def test_current_context_captures_set_vars_with_id():
    """Test current_context() captures explicitly-set vars and active id.

    Given:
        A ContextVar with an explicit value set
    When:
        current_context() is called
    Then:
        The returned Context should contain the var with its value
        and a non-None id
    """
    # Arrange
    var = ContextVar("cur_ctx", default=0)
    var.set(1)

    # Act
    ctx = current_context()

    # Assert
    assert ctx[var] == 1
    assert ctx.id is not None


class TestPublicTypeShape:
    def test_token_repr_includes_var_key(self):
        """Test Token repr includes the owning var's key.

        Given:
            A Token produced by set() on a ContextVar
        When:
            repr() is called on it
        Then:
            The repr should include the var's full key
        """
        # Arrange
        var = ContextVar("repr_token_var")
        token = var.set("x")

        # Act
        text = repr(token)

        # Assert
        assert var.key in text

    def test_contextvar_repr_includes_key(self):
        """Test ContextVar repr includes the full key.

        Given:
            A ContextVar with a name
        When:
            repr() is called on it
        Then:
            The repr should include 'namespace:name'
        """
        # Arrange
        var = ContextVar("repr_cv")

        # Act
        text = repr(var)

        # Assert
        assert f"'{var.key}'" in text
