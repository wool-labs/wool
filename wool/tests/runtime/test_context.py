import asyncio
import contextvars
import sys
import types

import cloudpickle
import pytest

from wool.runtime.context import _UNSET
from wool.runtime.context import Context
from wool.runtime.context import ContextVar
from wool.runtime.context import Token
from wool.runtime.context import _apply_vars
from wool.runtime.context import _dumps
from wool.runtime.context import _loads
from wool.runtime.context import _snapshot_vars
from wool.runtime.context import _UnsetType
from wool.runtime.context import current_context
from wool.runtime.context import dispatch_timeout


@pytest.fixture
def fresh_module():
    """Create a synthetic module registered in sys.modules and drop it on teardown."""
    name = "tests_fresh_contextvar_mod"
    module = types.ModuleType(name)
    sys.modules[name] = module
    yield module
    sys.modules.pop(name, None)


class TestUnsetType:
    def test___new___returns_singleton_instance(self):
        """Test _UnsetType returns the same instance on every construction.

        Given:
            The _UnsetType class
        When:
            It is instantiated multiple times
        Then:
            It should return the same singleton instance each time
        """
        # Act
        first = _UnsetType()
        second = _UnsetType()

        # Assert
        assert first is second
        assert first is _UNSET

    def test___repr___returns_unset_literal(self):
        """Test _UnsetType repr returns the string 'UNSET'.

        Given:
            The _UNSET singleton
        When:
            repr() is called on it
        Then:
            It should return the string 'UNSET'
        """
        # Act & assert
        assert repr(_UNSET) == "UNSET"

    def test___bool___returns_false(self):
        """Test _UnsetType is falsy.

        Given:
            The _UNSET singleton
        When:
            It is evaluated in a boolean context
        Then:
            It should be falsy
        """
        # Act & assert
        assert not _UNSET

    def test___reduce___roundtrip_preserves_singleton_identity(self):
        """Test cloudpickle roundtrip returns the same singleton.

        Given:
            The _UNSET singleton
        When:
            It is serialized via cloudpickle.dumps and deserialized via cloudpickle.loads
        Then:
            The deserialized value should be the same singleton instance
        """
        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(_UNSET))

        # Assert
        assert restored is _UNSET


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
        # Arrange, act
        var = ContextVar("example")

        # Assert
        assert var.name == "example"
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
        # Arrange, act
        var = ContextVar("example", default=42)

        # Assert
        assert var.get() == 42

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
        a = ContextVar("a", default=0)
        b = ContextVar("b", default=0)
        token = a.set(1)

        # Act & assert
        with pytest.raises(ValueError):
            b.reset(token)

    def test_pickle_roundtrip_via_module_binding(self, fresh_module):
        """Test cloudpickle roundtrips a module-bound ContextVar to the same instance.

        Given:
            A ContextVar assigned to a module attribute
        When:
            It is pickled and unpickled via cloudpickle
        Then:
            The unpickled object should be the same module-attribute instance
        """
        # Arrange
        var = ContextVar("shipped")
        fresh_module.shipped = var

        # Act
        restored = _loads(_dumps(var))

        # Assert
        assert restored is var


class TestToken:
    def test_pickle_roundtrip_preserves_reset_semantics(self, fresh_module):
        """Test cloudpickle roundtrips a Token and it still restores prior value.

        Given:
            A module-bound ContextVar and a Token from set()
        When:
            The token is pickled, unpickled, and used with reset()
        Then:
            The ContextVar should return to its pre-set value
        """
        # Arrange
        var = ContextVar("tok", default="original")
        fresh_module.tok = var
        token = var.set("mutated")

        # Act
        restored_token = _loads(_dumps(token))
        assert var.get() == "mutated"
        var.reset(restored_token)

        # Assert
        assert var.get() == "original"


class Test_snapshot_vars:
    def test_omits_vars_with_no_explicit_value(self, fresh_module):
        """Test _snapshot_vars omits vars with no explicit value.

        Given:
            A module-bound ContextVar with no value set
        When:
            _snapshot_vars() is called
        Then:
            The resulting dict should not contain that var's key
        """
        # Arrange
        var = ContextVar("snapshot_unset")
        fresh_module.snapshot_unset = var

        # Act
        result = _snapshot_vars()

        # Assert
        key = f"{fresh_module.__name__}:snapshot_unset"
        assert key not in result

    def test_emits_set_value_under_module_attr_key(self, fresh_module):
        """Test _snapshot_vars emits mod:attr keys with serialized values.

        Given:
            A module-bound ContextVar with a value set
        When:
            _snapshot_vars() is called
        Then:
            The dict should contain 'mod:attr' → cloudpickled value
        """
        # Arrange
        var = ContextVar("snap_value")
        fresh_module.snap_value = var
        var.set({"x": 1})

        # Act
        result = _snapshot_vars()

        # Assert
        key = f"{fresh_module.__name__}:snap_value"
        assert key in result
        assert cloudpickle.loads(result[key]) == {"x": 1}


class Test_apply_vars:
    def test_restores_value_on_target_var(self, fresh_module):
        """Test _apply_vars restores a serialized value onto the local var.

        Given:
            A module-bound ContextVar and a snapshot dict with its key
        When:
            _apply_vars is called with the dict
        Then:
            get() on the var should return the deserialized value
        """
        # Arrange
        var = ContextVar("apply_target")
        fresh_module.apply_target = var
        snapshot = {
            f"{fresh_module.__name__}:apply_target": cloudpickle.dumps("restored")
        }

        # Act
        _apply_vars(snapshot)

        # Assert
        assert var.get() == "restored"

    def test_skips_unknown_module(self, caplog):
        """Test _apply_vars warns and skips entries for unknown modules.

        Given:
            A snapshot dict keyed by an absent module
        When:
            _apply_vars is called
        Then:
            It should log a warning and not raise
        """
        # Arrange
        snapshot = {"no_such_module:nope": cloudpickle.dumps("x")}

        # Act
        _apply_vars(snapshot)

        # Assert — no exception; no assertion needed beyond completion


class TestContext:
    def test___new___direct_instantiation_rejected(self):
        """Test Context.__new__ forbids direct construction.

        Given:
            The Context class
        When:
            A caller attempts Context()
        Then:
            It should raise TypeError
        """
        # Act & assert
        with pytest.raises(TypeError):
            Context()

    def test_run_seeds_vars_and_scopes_mutations(self, fresh_module):
        """Test Context.run seeds vars and scopes inner mutations.

        Given:
            A Context captured with a ContextVar set to a known value
        When:
            Context.run mutates the var inside the callable
        Then:
            The mutation should be observable from within the callable,
            and the Context's own snapshot should reflect the mutation
        """
        # Arrange
        var = ContextVar("run_seed", default="before")
        fresh_module.run_seed = var
        token = var.set("outer")
        ctx = current_context()
        var.reset(token)

        # Act
        def body():
            assert var.get() == "outer"
            var.set("inner")
            return var.get()

        result = ctx.run(body)

        # Assert
        assert result == "inner"
        assert ctx[var] == "inner"

    def test_run_concurrent_entry_raises(self, fresh_module):
        """Test Context.run rejects re-entry while already active.

        Given:
            A Context whose run() body tries to invoke ctx.run again
        When:
            The inner ctx.run fires
        Then:
            It should raise RuntimeError
        """
        # Arrange
        var = ContextVar("reentry", default=0)
        fresh_module.reentry = var
        var.set(1)
        ctx = current_context()

        def inner():
            return "never"

        def outer():
            ctx.run(inner)

        # Act & assert
        with pytest.raises(RuntimeError):
            ctx.run(outer)

    @pytest.mark.asyncio
    async def test_run_async_seeds_vars(self, fresh_module):
        """Test Context.run_async seeds vars into the task's context.

        Given:
            A Context captured with a ContextVar set to a known value
        When:
            Context.run_async runs a coroutine that reads the var
        Then:
            The coroutine should observe the seeded value
        """
        # Arrange
        var = ContextVar("async_seed", default="before")
        fresh_module.async_seed = var
        token = var.set("outer")
        ctx = current_context()
        var.reset(token)

        async def coro():
            return var.get()

        # Act
        result = await ctx.run_async(coro())

        # Assert
        assert result == "outer"

    @pytest.mark.asyncio
    async def test_run_async_concurrent_entry_raises(self, fresh_module):
        """Test Context.run_async rejects concurrent entry.

        Given:
            A Context whose run_async body awaits a second run_async on itself
        When:
            The inner run_async fires
        Then:
            It should raise RuntimeError
        """
        # Arrange
        var = ContextVar("async_reentry", default=0)
        fresh_module.async_reentry = var
        var.set(1)
        ctx = current_context()

        async def inner():
            return "never"

        async def outer():
            await ctx.run_async(inner())

        # Act & assert
        with pytest.raises(RuntimeError):
            await ctx.run_async(outer())

    def test_pickle_roundtrip_preserves_id_and_vars(self, fresh_module):
        """Test Context pickles preserving lineage id and var values.

        Given:
            A Context captured with a module-bound ContextVar value
        When:
            It is pickled and unpickled via cloudpickle
        Then:
            The restored Context should equal by id and expose the same vars
        """
        # Arrange
        var = ContextVar("ctx_pickle")
        fresh_module.ctx_pickle = var
        var.set("captured")
        ctx = current_context()

        # Act
        restored = _loads(_dumps(ctx))

        # Assert
        assert restored.id == ctx.id
        assert restored[var] == "captured"


class Test_current_context:
    def test_captures_set_vars_with_lineage(self, fresh_module):
        """Test current_context returns a Context with the current lineage and vars.

        Given:
            A module-bound ContextVar with a value set in the current context
        When:
            current_context() is called
        Then:
            The returned Context should contain the var with its value
            and carry a non-nil lineage UUID
        """
        # Arrange
        var = ContextVar("cur")
        fresh_module.cur = var
        var.set("value")

        # Act
        ctx = current_context()

        # Assert
        assert ctx[var] == "value"
        assert ctx.id.int != 0


class TestDispatchTimeoutContextVar:
    def test_default_is_none(self):
        """Test dispatch_timeout has no default value assigned.

        Given:
            The dispatch_timeout contextvar
        When:
            Read via .get() in a fresh context
        Then:
            It should return None
        """
        # Act & assert
        assert dispatch_timeout.get() is None

    def test_set_and_reset_restores_default(self):
        """Test dispatch_timeout.set is reversible via reset.

        Given:
            dispatch_timeout set to a float
        When:
            The token is reset
        Then:
            .get() should return None again
        """
        # Arrange
        token = dispatch_timeout.set(5.0)

        # Act
        dispatch_timeout.reset(token)

        # Assert
        assert dispatch_timeout.get() is None


class TestPublicTypeShape:
    def test_token_repr_includes_var_name(self, fresh_module):
        """Test Token repr surfaces the underlying var name.

        Given:
            A Token obtained from a ContextVar.set call
        When:
            repr() is called on it
        Then:
            The string should include the var's name
        """
        # Arrange
        var = ContextVar("repr_var")
        fresh_module.repr_var = var
        token = var.set(1)

        # Act
        result = repr(token)

        # Assert
        assert "repr_var" in result
        assert isinstance(token, Token)

    def test_contextvar_repr_includes_name(self):
        """Test ContextVar repr includes its name.

        Given:
            A ContextVar instance
        When:
            repr() is called on it
        Then:
            The string should include the var's name
        """
        # Arrange, act
        var = ContextVar("repr_form")

        # Assert
        assert "repr_form" in repr(var)


@pytest.mark.asyncio
async def test_await_is_continuous_mutations_leak_to_caller(fresh_module):
    """Test plain await on a coroutine mutating a var leaks to the caller.

    Given:
        A module-bound ContextVar and a coroutine that mutates it
    When:
        The coroutine is awaited directly (no create_task)
    Then:
        The caller should observe the mutation after the await
    """
    # Arrange
    var = ContextVar("continuity", default="before")
    fresh_module.continuity = var

    async def coro():
        var.set("after")

    # Act
    await coro()

    # Assert
    assert var.get() == "after"


@pytest.mark.asyncio
async def test_create_task_forks_context_isolating_mutations(fresh_module):
    """Test asyncio.create_task forks the context, isolating mutations.

    Given:
        A module-bound ContextVar and a coroutine that mutates it
    When:
        The coroutine is wrapped in asyncio.create_task
    Then:
        The caller's context should not observe the mutation
    """
    # Arrange
    var = ContextVar("forked", default="before")
    fresh_module.forked = var

    async def coro():
        var.set("child_only")

    # Act
    await asyncio.create_task(coro())

    # Assert
    assert var.get() == "before"


@pytest.mark.asyncio
async def test_create_task_with_explicit_context_writes_to_context(fresh_module):
    """Test asyncio.create_task with explicit context lands mutations there.

    Given:
        A stdlib Context and a coroutine that mutates a module-bound var
    When:
        The coroutine is scheduled with context=<explicit>
    Then:
        The explicit context should contain the mutation post-run
    """
    # Arrange
    var = ContextVar("explicit", default="before")
    fresh_module.explicit = var
    ctx = contextvars.copy_context()

    async def coro():
        var.set("from_task")

    # Act
    await asyncio.create_task(coro(), context=ctx)

    # Assert — caller's current context is unchanged
    assert var.get() == "before"
    # and ctx holds the mutation
    assert ctx[var._stdlib] == "from_task"
