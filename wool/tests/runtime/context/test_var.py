import asyncio
import contextvars
import copy
import gc
import pickle
import threading
import uuid

import cloudpickle
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from tests.helpers import scoped_context
from wool import protocol
from wool.runtime.context import ContextVar
from wool.runtime.context import ContextVarCollision
from wool.runtime.context import Token
from wool.runtime.context import current_context
from wool.runtime.context.base import Context

# ``_install_context`` is the private install primitive. The one
# test that needs raw install (without ``Context.mount``'s owner
# restamping) seeds an ownerless armed chain to verify the
# task-adoption path on first :meth:`ContextVar.set`. This is a
# deliberate test-infrastructure import.
from wool.runtime.context.base import _install_context
from wool.runtime.context.manifest import _ContextManifest

dumps = wool.__serializer__.dumps
loads = cloudpickle.loads

# Hypothesis sentinel for the default-ladder test — distinguishes "no
# argument supplied" from a supplied ``None``, which is itself a valid
# default value.
_NOTHING = object()


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


class TestContextVar:
    def test___init___with_name_only(self):
        """Test ContextVar initialization with a name and no default.

        Given:
            A name string.
        When:
            ContextVar is instantiated with the name.
        Then:
            It should expose the name and raise LookupError on get()
            with no value set.
        """
        # Act
        var = ContextVar(_unique("init_nameonly"))

        # Assert
        assert var.name.startswith("init_nameonly")
        with pytest.raises(LookupError):
            var.get()

    def test___init___with_default(self):
        """Test ContextVar initialization with a default value.

        Given:
            A name string and a default value.
        When:
            ContextVar is instantiated with both.
        Then:
            get() should return the default when no value is set.
        """
        # Arrange
        var = ContextVar(_unique("init_withdefault"), default=42)

        # Act & assert
        assert var.get() == 42

    def test___init___infers_namespace_from_caller(self):
        """Test ContextVar infers namespace from the caller's top-level package.

        Given:
            A ContextVar constructed from this test module.
        When:
            No explicit namespace is provided.
        Then:
            The namespace should be the top-level package of __name__.
        """
        # Arrange
        expected_ns = __name__.partition(".")[0]

        # Act
        var = ContextVar(_unique("inferred"))

        # Assert
        assert var.namespace == expected_ns

    def test___init___accepts_explicit_namespace(self):
        """Test ContextVar uses an explicit namespace when provided.

        Given:
            A ContextVar constructed with an explicit namespace.
        When:
            No implicit inference is needed.
        Then:
            The key should combine the explicit namespace with the name.
        """
        # Act
        var = ContextVar("explicit", namespace=_unique("ns"))

        # Assert
        assert var.namespace.startswith("ns")
        assert var.name == "explicit"

    def test___init___with_a_registered_key(self):
        """Test re-declaring an already-registered key raises ContextVarCollision.

        Given:
            A ContextVar already registered under a key.
        When:
            ContextVar is invoked again with the identical key.
        Then:
            It should raise ContextVarCollision — keys must be unique
            within a namespace.
        """
        # Arrange
        namespace = _unique("dup_same")
        first = ContextVar("v", namespace=namespace, default=1)

        # Act & assert
        with pytest.raises(ContextVarCollision):
            ContextVar("v", namespace=namespace, default=1)
        assert first.name == "v"

    @given(
        name=st.text(
            alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
            min_size=1,
            max_size=12,
        ),
        default_a=st.one_of(st.integers(), st.text(), st.booleans()),
        default_b=st.one_of(st.integers(), st.text(), st.booleans()),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test___init___with_duplicate_key(self, name, default_a, default_b):
        """Test duplicate-key construction raises regardless of default match.

        Given:
            Any non-empty name and pair of default values.
        When:
            A ContextVar is constructed, then a second with the
            identical key.
        Then:
            The second construction raises ContextVarCollision whether
            or not the two defaults are equal — distinct declarations
            of the same key collide.
        """
        # A unique namespace per example guarantees the registry slot
        # is free without needing teardown between Hypothesis examples.
        namespace = f"test_duplicate_{uuid.uuid4().hex}"

        # The registry holds vars weakly, so we hold ``first`` for the
        # duration of the second construction.
        first = ContextVar(name, namespace=namespace, default=default_a)

        with pytest.raises(ContextVarCollision):
            ContextVar(name, namespace=namespace, default=default_b)

        assert first.namespace == namespace
        assert first.name == name

    def test___init___with_same_name_in_different_namespace(self):
        """Test duplicate names across different namespaces are allowed.

        Given:
            A ContextVar in one namespace.
        When:
            A second ContextVar with the same name is constructed in a
            different namespace.
        Then:
            Both should register without collision.
        """
        # Arrange
        ns_a = _unique("lib_a")
        ns_b = _unique("lib_b")

        # Act
        a = ContextVar("shared", namespace=ns_a)
        b = ContextVar("shared", namespace=ns_b)

        # Assert
        assert a is not b
        assert (a.namespace, a.name) == (ns_a, "shared")
        assert (b.namespace, b.name) == (ns_b, "shared")

    def test___init___stub_promotion_with_default(self):
        """Test ContextVar.__new__ promotes a stub and seeds the supplied default.

        Given:
            A wire-decoded context that creates a default-less stub for
            a key, then a ContextVar construction for the same key with
            an explicit default.
        When:
            ContextVar is constructed with default="promoted" for the
            stub's key.
        Then:
            get() should return "promoted" — the stub is promoted in
            place and the supplied default is adopted.
        """
        # Arrange
        var_namespace = uuid.uuid4().hex
        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.vars.add(
            namespace=var_namespace,
            name="stub_promote",
            value=dumps("wire-value"),
        )
        # Decoding seeds a default-less stub for the key.
        _decoded = _ContextManifest.from_protobuf(pb, serializer=wool.__serializer__)  # noqa: F841

        # Act
        var = ContextVar("stub_promote", namespace=var_namespace, default="promoted")

        # Assert
        assert var.get() == "promoted"

    @given(
        ctor_default=st.one_of(st.none(), st.just(_NOTHING), st.integers(), st.text()),
        get_arg=st.one_of(st.none(), st.just(_NOTHING), st.integers(), st.text()),
        value_set=st.booleans(),
        armed=st.booleans(),
        set_value=st.one_of(st.integers(), st.text()),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_get_resolves_the_default_ladder(
        self, ctor_default, get_arg, value_set, armed, set_value
    ):
        """Test ContextVar.get walks the bound-value, get-arg, ctor-default ladder.

        Given:
            Any combination of a constructor default (present or
            absent), a get fallback argument (present or absent),
            whether the variable itself was set, and whether the
            context was independently armed by a sibling variable.
        When:
            get() is called — with or without the fallback argument.
        Then:
            It should return the highest-priority rung available: a set
            value first, then the supplied fallback, then the
            constructor default, raising LookupError only when none of
            the three is present — and an unset variable on an armed
            context still falls through the ladder rather than raising.
        """
        # Arrange
        ctor_kwargs = {} if ctor_default is _NOTHING else {"default": ctor_default}
        var = ContextVar(_unique("ladder"), **ctor_kwargs)

        # Act & assert
        with scoped_context():
            if armed:
                # Arm the context via a sibling so ``var`` itself stays
                # unset on an armed chain — the armed-unset rung.
                ContextVar(_unique("ladder_sibling")).set("arms-it")
            if value_set:
                var.set(set_value)
            args = () if get_arg is _NOTHING else (get_arg,)
            if value_set:
                assert var.get(*args) == set_value
            elif get_arg is not _NOTHING:
                assert var.get(*args) == get_arg
            elif ctor_default is not _NOTHING:
                assert var.get(*args) == ctor_default
            else:
                with pytest.raises(LookupError):
                    var.get(*args)

    def test_get_distinguishes_a_none_default_from_an_unset_var(self):
        """Test ContextVar.get treats a None default as a real default, not unset.

        Given:
            One ContextVar with no constructor default and one with an
            explicit default=None, both unset.
        When:
            get(None) is called on the first and get() on the second.
        Then:
            Both return None rather than raising LookupError — a
            supplied or constructor None is a real default, distinct
            from "no default supplied", mirroring
            contextvars.ContextVar.get.
        """
        # Arrange
        no_ctor_default = ContextVar(_unique("supplied_none"))
        ctor_none_default = ContextVar(_unique("ctor_none"), default=None)

        # Act & assert
        assert no_ctor_default.get(None) is None
        assert ctor_none_default.get() is None

    @pytest.mark.parametrize(
        "non_str_name",
        [42, None, ("ns", "name"), b"bytes-name"],
        ids=["int", "none", "tuple", "bytes"],
    )
    def test___new___with_a_non_str_name(self, non_str_name):
        """Test ContextVar construction rejects a non-str name.

        Given:
            A name argument that is not a str — an int, None, a tuple,
            or bytes.
        When:
            ContextVar is constructed with that argument.
        Then:
            It should raise TypeError — the documented contract is that
            a context variable name must be a str.
        """
        # Act & assert
        with pytest.raises(TypeError, match="name must be a str"):
            ContextVar(non_str_name)

    def test_get_with_more_than_one_positional_argument(self):
        """Test ContextVar.get rejects more than one positional argument.

        Given:
            An armed ContextVar.
        When:
            get is called with two positional arguments.
        Then:
            It should raise TypeError — get accepts at most one
            fallback argument, mirroring contextvars.ContextVar.get.
        """
        # Arrange
        var = ContextVar(_unique("get_too_many_args"))

        # Act & assert
        with scoped_context():
            with pytest.raises(TypeError, match="at most 1 argument"):
                var.get("a", "b")

    def test_get_returns_set_value(self):
        """Test ContextVar.get returns the value installed by set.

        Given:
            A ContextVar with a value set in the active context.
        When:
            get() is called.
        Then:
            It should return the set value.
        """
        # Arrange
        var = ContextVar(_unique("get_set"))

        # Act
        with scoped_context():
            var.set("value")

            # Assert
            assert var.get() == "value"

    def test_set_arms_an_unarmed_context(self):
        """Test the first set arms the context with a context.

        Given:
            A fresh, unarmed context where current_context is None.
        When:
            ContextVar.set is called for the first time.
        Then:
            current_context should return a Context.
        """
        # Arrange
        var = ContextVar(_unique("set_arms"))
        with scoped_context():
            assert current_context() is None

            # Act
            var.set("x")

            # Assert
            assert current_context() is not None

    def test_set_returns_token(self):
        """Test ContextVar.set returns a Token.

        Given:
            A ContextVar in an armed context.
        When:
            set is called.
        Then:
            It should return a Token instance.
        """
        # Arrange
        var = ContextVar(_unique("set_token"))

        # Act
        with scoped_context():
            token = var.set("x")

            # Assert
            assert isinstance(token, Token)

    def test_set_rebuilds_the_context_on_each_call(self):
        """Test each set rebuilds a new immutable context.

        Given:
            A wool.ContextVar set once on an armed context.
        When:
            set is called a second time.
        Then:
            It should install a context distinct by identity from the
            first — set rebuilds the immutable context rather than
            mutating it in place.
        """
        # Arrange
        var = ContextVar(_unique("set_cow"))

        # Act
        with scoped_context():
            var.set("first")
            first_context = current_context()
            var.set("second")
            second_context = current_context()

            # Assert
            assert first_context is not None
            assert second_context is not None
            assert second_context is not first_context

    def test_reset_with_first_set_token(self):
        """Test the first set's Token restores the var to its default on reset.

        Given:
            A ContextVar with a constructor default and a single set
            that captures a Token.
        When:
            reset(token) is called.
        Then:
            var.get() should return the constructor default.
        """
        # Arrange
        var = ContextVar(_unique("restore_default"), default="initial")

        # Act
        with scoped_context():
            token = var.set("x")
            var.reset(token)

            # Assert
            assert var.get() == "initial"

    def test_reset_with_nested_set_token(self):
        """Test a nested set's Token restores the outer set's value on reset.

        Given:
            A ContextVar set to "outer" and then set again to "inner"
            capturing the inner Token.
        When:
            reset(inner_token) is called.
        Then:
            var.get() should return "outer" — Tokens stack, each
            restoring only the value replaced by its own set.
        """
        # Arrange
        var = ContextVar(_unique("restore_outer"), default="d")

        # Act
        with scoped_context():
            var.set("outer")
            inner_token = var.set("inner")
            var.reset(inner_token)

            # Assert
            assert var.get() == "outer"

    def test_reset_with_used_token(self):
        """Test ContextVar.reset raises on a token already consumed.

        Given:
            A ContextVar and a Token that has already been used.
        When:
            reset() is called with the same token again.
        Then:
            It should raise RuntimeError — the reset routes through the
            backing variable's native single-use bit.
        """
        # Arrange
        var = ContextVar(_unique("once"), default=0)

        # Act & assert
        with scoped_context():
            token = var.set(1)
            var.reset(token)
            with pytest.raises(RuntimeError, match="has already been used"):
                var.reset(token)

    def test_reset_with_token_for_different_var(self):
        """Test ContextVar.reset rejects tokens minted by a different var.

        Given:
            Two distinct ContextVar instances, each with a set value.
        When:
            reset() is called on one with the other's token.
        Then:
            It should raise ValueError naming the different ContextVar.
        """
        # Arrange
        a = ContextVar(_unique("reset_a"), default=0)
        b = ContextVar(_unique("reset_b"), default=0)

        # Act & assert
        with scoped_context():
            token = a.set(1)
            with pytest.raises(
                ValueError, match="Token was created by a different ContextVar"
            ):
                b.reset(token)

    def test_reset_with_token_from_different_chain(self):
        """Test ContextVar.reset rejects a token minted in a different chain.

        Given:
            A ContextVar and a Token minted in one armed context,
            then a separate armed context (a fresh chain).
        When:
            reset(token) is called under the second chain.
        Then:
            It should raise ValueError naming the different chain.
        """
        # Arrange
        var = ContextVar(_unique("reset_chain"), default="d")

        # Act & assert
        with scoped_context():
            token = var.set("a")
        with scoped_context():
            var.set("b")
            with pytest.raises(
                ValueError, match="Token was created in a different chain"
            ):
                var.reset(token)

    @pytest.mark.parametrize(
        "not_a_token",
        ["a-string", 42, None],
        ids=["str", "int", "none"],
    )
    def test_reset_with_a_non_token_argument(self, not_a_token):
        """Test ContextVar.reset rejects an argument that is not a Token.

        Given:
            An armed ContextVar and an argument that is not a
            wool.Token — a string, an int, or None.
        When:
            reset() is called with that argument.
        Then:
            It should raise TypeError naming wool.Token — the type
            check guards reset before any token-state inspection.
        """
        # Arrange
        var = ContextVar(_unique("reset_not_token"))

        # Act & assert
        with scoped_context():
            var.set("x")
            with pytest.raises(TypeError, match="wool.Token"):
                var.reset(not_a_token)

    def test_reset_with_a_used_token_for_a_different_var(self):
        """Test ContextVar.reset reports a wrong-var token before a used token.

        Given:
            A Token that has already been consumed by a reset on its own
            ContextVar, passed to reset on a different ContextVar — so
            the token is BOTH already-used AND created by a different
            var.
        When:
            reset() is called with that token on the other var.
        Then:
            It should raise ValueError for the wrong var, not
            RuntimeError for the used token — the different-ContextVar
            guard is a pure token-vs-self check and runs before the
            context-scoped used-token check.
        """
        # Arrange
        a = ContextVar(_unique("reset_used_a"), default=0)
        b = ContextVar(_unique("reset_used_b"), default=0)

        # Act & assert
        with scoped_context():
            token = a.set(1)
            a.reset(token)
            with pytest.raises(
                ValueError, match="Token was created by a different ContextVar"
            ):
                b.reset(token)

    def test_reset_restores_unset_state(self):
        """Test reset restores unset state when old_value was MISSING.

        Given:
            A previously-unset ContextVar whose set() Token captured
            MISSING as the old_value.
        When:
            reset(token) is invoked.
        Then:
            The var should revert to unset; get(fallback) returns the
            supplied fallback.
        """
        # Arrange
        var = ContextVar(_unique("reset_unset"))

        # Act
        with scoped_context():
            token = var.set("briefly")
            var.reset(token)

            # Assert
            assert var.get("<fallback>") == "<fallback>"

    def test_reset_in_a_copied_context_rejects_the_token(self):
        """Test resetting a token in a copy_context copy is rejected.

        Given:
            A ContextVar set in one context, and a copy of that context
            taken via contextvars.copy_context.
        When:
            The token is reset inside the copy, then reset again in the
            original context.
        Then:
            The reset in the copy should raise ValueError — the reset
            delegates to the backing variable's native reset, and stdlib
            rejects a token reset in a different contextvars.Context
            than the one it was minted in — while the reset in the
            original context still succeeds.
        """
        # Arrange
        var = ContextVar(_unique("reset_copy_rejects"))

        # Act & assert
        with scoped_context():
            token = var.set("x")
            copy_ctx = contextvars.copy_context()
            with pytest.raises(ValueError, match="different Context"):
                copy_ctx.run(var.reset, token)
            var.reset(token)
            assert var.get("<unset>") == "<unset>"

    def test_reset_rejects_a_token_used_in_another_process(self):
        """Test ContextVar.reset rejects a token reset in another process.

        Given:
            A token whose id has been recorded in the active context's
            external_used log — the cross-process used signal, as
            merged back from a worker.
        When:
            reset() is called with that token.
        Then:
            It should raise RuntimeError — single-use holds across the
            wire, not only within the local process.
        """
        # Arrange
        var = ContextVar(_unique("reset_external_used"))

        # Act & assert
        with scoped_context():
            token = var.set("x")
            armed = current_context()
            assert armed is not None
            armed.evolve(external_used={token.id: (var.namespace, var.name)}).mount()
            with pytest.raises(RuntimeError, match="Token has already been used"):
                var.reset(token)

    def test_set_clears_a_prior_reset_signal(self):
        """Test re-setting a variable clears its reset-to-no-value signal.

        Given:
            A ContextVar reset to no value, leaving its key in the
            active context's reset_vars set.
        When:
            The variable is set again.
        Then:
            Its key should be absent from the context's reset_vars —
            a re-set variable is no longer in a reset state.
        """
        # Arrange
        var = ContextVar(_unique("set_clears_reset"))

        # Act & assert
        with scoped_context():
            token = var.set("first")
            var.reset(token)
            after_reset = current_context()
            assert after_reset is not None
            assert (var.namespace, var.name) in after_reset.reset_vars
            var.set("second")
            after_set = current_context()
            assert after_set is not None
            assert (var.namespace, var.name) not in after_set.reset_vars

    def test_pickle_roundtrip_when_var_registered(self):
        """Test the wool serializer roundtrips a ContextVar to the same instance.

        Given:
            A ContextVar registered in the process-wide registry.
        When:
            It is dumped via the wool serializer and loaded back.
        Then:
            The unpickled instance should be the same object as the
            original.
        """
        # Arrange
        var = ContextVar(_unique("shipped"))

        # Act
        restored = loads(dumps(var))

        # Assert
        assert restored is var

    def test_pickle_roundtrip_does_not_write_to_receiver_context(self):
        """Test the wool serializer roundtrip of a ContextVar carries identity only.

        Given:
            A ContextVar set to a specific value in the current context.
        When:
            The var is dumped and loaded inside a fresh, unarmed
            contextvars.Context.
        Then:
            The receiver's context stays unarmed and var.get falls back
            to the constructor default — the pickle path is a key-only
            payload, never a value context, so the value never reaches
            the receiver's backing variable.
        """
        # Arrange
        var = ContextVar(_unique("pickle_with_value"), default="default_value")

        # Act
        with scoped_context():
            var.set("sender_value")
            pickled = dumps(var)

        observed: list[object] = []

        def _receive() -> None:
            restored = loads(pickled)
            observed.append(restored.get())
            observed.append(current_context())

        # A fresh contextvars.Context carries neither the wool context
        # nor the variable's backing contextvars.ContextVar value.
        contextvars.Context().run(_receive)

        # Assert
        assert observed[0] == "default_value"
        assert observed[1] is None

    def test_contextvar_pickle_reload_after_wire_decode_seeds_default(self):
        """Test loading a cloudpickled ContextVar with a default seeds an existing stub.

        Given:
            A receiver process where a wire protocol.Context carrying a
            value for a ContextVar key is decoded first — creating a
            default-less stub — and a cloudpickle dump of a ContextVar
            for the same key with a non-Undefined default is loaded
            second, after the originating ContextVar has been released.
        When:
            cloudpickle.loads runs over the pickle.
        Then:
            It should return a ContextVar for that key whose get falls
            back to the pickled default — the second ingress folded the
            default into the stub rather than discarding it.
        """
        # Arrange
        var_namespace = uuid.uuid4().hex
        var = ContextVar(
            "wire_then_pickle", namespace=var_namespace, default="from-pickle"
        )
        pickled_var = dumps(var)
        del var
        gc.collect()

        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.vars.add(
            namespace=var_namespace,
            name="wire_then_pickle",
            value=dumps("from-wire"),
        )
        # Bound to keep the decoded manifest alive so its stub_pins anchor
        # holds the registered stub through the loads() call below.
        _decoded = _ContextManifest.from_protobuf(pb, serializer=wool.__serializer__)  # noqa: F841

        # Act
        restored_var = loads(pickled_var)

        # Assert
        assert restored_var.get() == "from-pickle"

    def test_repr_includes_namespace_and_name(self):
        """Test ContextVar repr includes the namespace and name.

        Given:
            A ContextVar with a name.
        When:
            repr() is called on it.
        Then:
            It should include the namespace and the name.
        """
        # Arrange
        var = ContextVar(_unique("repr_cv"))

        # Act
        text = repr(var)

        # Assert
        assert f"namespace={var.namespace!r}" in text
        assert f"name={var.name!r}" in text

    def test_vanilla_pickle_copy_and_deepcopy_are_rejected(self):
        """Test wool.ContextVar rejects pickle, cloudpickle, copy.copy, and deepcopy.

        Given:
            A wool.ContextVar.
        When:
            pickle.dumps, cloudpickle.dumps, copy.copy, and
            copy.deepcopy are each invoked on it.
        Then:
            All four raise TypeError — wool.__serializer__ remains the
            only valid serialization channel.
        """
        # Arrange
        match = "wool.ContextVar cannot be pickled"
        var = ContextVar(_unique("var_pickle_rejection"))

        # Act & assert
        with pytest.raises(TypeError, match=match):
            pickle.dumps(var)
        with pytest.raises(TypeError, match=match):
            cloudpickle.dumps(var)
        with pytest.raises(TypeError, match=match):
            copy.copy(var)
        with pytest.raises(TypeError, match=match):
            copy.deepcopy(var)

    def test_get_does_not_arm_an_unarmed_context(self):
        """Test ContextVar.get on an unarmed context leaves it unarmed.

        Given:
            A fresh unarmed context and a ContextVar with a default.
        When:
            get() is called.
        Then:
            current_context should still be None — a read never arms a
            context.
        """
        # Arrange
        var = ContextVar(_unique("get_no_arm"), default="d")

        # Act
        with scoped_context():
            var.get()

            # Assert
            assert current_context() is None

    @given(payload=st.text() | st.integers() | st.lists(st.integers()))
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_set_get_round_trip_with_arbitrary_value(self, payload):
        """Test set followed by get round-trips an arbitrary value.

        Given:
            Any text, integer, or list-of-integers value.
        When:
            The value is set on a ContextVar and read back via get.
        Then:
            get should return a value equal to the one set.
        """
        # Arrange
        var = ContextVar(_unique("rt_set_get"))

        # Act & assert
        with scoped_context():
            var.set(payload)
            assert var.get() == payload

    @pytest.mark.asyncio
    async def test_set_adopts_an_armed_chain_with_no_live_owning_task(self):
        """Test ContextVar.set adopts ownership of an ownerless armed chain.

        Given:
            An armed chain with no owner task — the shape a wire-
            decoded context has before its worker mounts it — entered
            by an asyncio task created with no Wool task factory
            installed (so the chain is not forked).
        When:
            The running task calls set on a wool.ContextVar.
        Then:
            The active context's _owning_task should become the running
            task — a new runner adopts an ownerless armed chain so a
            later concurrent task still fails loud via the owning-task
            guard.
        """
        # Arrange — clear any task factory so the child task does not
        # fork the chain onto a fresh owner.
        loop = asyncio.get_running_loop()
        loop.set_task_factory(None)
        var = ContextVar(_unique("set_adoption"))

        with scoped_context():
            # An armed chain with _owning_task=None — the shape of a
            # context decoded off the wire before a worker mounts it.
            _install_context(
                Context(
                    chain_id=uuid.uuid4(),
                    _owning_thread=threading.get_ident(),
                    _owning_task=None,
                )
            )
            armed = current_context()
            assert armed is not None
            assert armed._owning_task is None

            async def adopt() -> object:
                # Act — the running task sets the var on the ownerless
                # armed chain.
                var.set("adopted")
                context = current_context()
                assert context is not None
                ref = context._owning_task
                return None if ref is None else ref()

            try:
                task = asyncio.ensure_future(adopt())
                _owning_task = await task
            finally:
                loop.set_task_factory(None)

            # Assert — the running task adopted ownership.
            assert _owning_task is task

    def test_set_value_propagates_into_seeded_contextvars_context(self):
        """Test a set value is visible inside a stdlib Context copied after the set.

        Given:
            A ContextVar set to a value, then a stdlib contextvars.Context
            copied from the live context.
        When:
            The var is read inside that copied Context via Context.run.
        Then:
            It should observe the set value — Wool chain state rides in a
            stdlib contextvars.ContextVar and copies with stdlib semantics.
        """
        # Arrange
        var = ContextVar(_unique("propagate_seeded"))

        # Act
        with scoped_context():
            var.set("seeded")
            seeded = contextvars.copy_context()
            observed = seeded.run(var.get)

        # Assert
        assert observed == "seeded"
