import contextvars
import copy
import gc
import pickle
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
from wool.runtime.context import current_snapshot
from wool.runtime.context import decode_snapshot

dumps = wool.__serializer__.dumps
loads = cloudpickle.loads


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
            A wire-decoded snapshot that creates a default-less stub for
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
        _decoded = decode_snapshot(pb)  # noqa: F841

        # Act
        var = ContextVar("stub_promote", namespace=var_namespace, default="promoted")

        # Assert
        assert var.get() == "promoted"

    def test_get_with_an_explicit_default_on_an_unset_var(self):
        """Test ContextVar.get returns the supplied fallback when unset.

        Given:
            A ContextVar with no constructor default and no value set.
        When:
            get() is called with a fallback argument.
        Then:
            It should return the fallback argument.
        """
        # Arrange
        var = ContextVar(_unique("no_default"))

        # Act
        value = var.get("fallback")

        # Assert
        assert value == "fallback"

    def test_get_prefers_explicit_default_over_constructor_default(self):
        """Test ContextVar.get prefers a supplied fallback over the constructor default.

        Given:
            A ContextVar with a constructor default and no value set.
        When:
            get() is called with a different fallback argument.
        Then:
            It should return the supplied fallback, not the constructor
            default.
        """
        # Arrange
        var = ContextVar(_unique("get_priority"), default="ctor")

        # Act & assert
        assert var.get("explicit") == "explicit"

    def test_get_with_a_constructor_default_and_no_value(self):
        """Test ContextVar.get returns the constructor default when unset.

        Given:
            A ContextVar with a constructor default, no value set, and
            no fallback argument.
        When:
            get() is called.
        Then:
            It should return the constructor default — the middle rung
            of the get fallback ladder, between a supplied fallback and
            LookupError.
        """
        # Arrange
        var = ContextVar(_unique("ctor_default"), default="ctor")

        # Act & assert
        assert var.get() == "ctor"

    def test_get_with_a_supplied_none_default(self):
        """Test ContextVar.get returns None when None is supplied as the default.

        Given:
            A ContextVar with no constructor default and no value set.
        When:
            get(None) is called.
        Then:
            It should return None, not raise LookupError — get
            distinguishes a supplied None default from no default
            supplied, mirroring contextvars.ContextVar.get.
        """
        # Arrange
        var = ContextVar(_unique("supplied_none"))

        # Act & assert
        assert var.get(None) is None

    def test_get_with_a_constructor_none_default(self):
        """Test ContextVar.get returns None when the constructor default is None.

        Given:
            A ContextVar constructed with default=None and no value set.
        When:
            get() is called with no fallback argument.
        Then:
            It should return None, not raise LookupError — a constructor
            default of None is a real default, mirroring
            contextvars.ContextVar.
        """
        # Arrange
        var = ContextVar(_unique("ctor_none"), default=None)

        # Act & assert
        assert var.get() is None

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

    def test_get_with_armed_context_and_unset_var_returns_supplied_default(self):
        """Test ContextVar.get on an armed context falls back for an unset var.

        Given:
            A context armed by setting one ContextVar, and a second
            ContextVar that has never been set.
        When:
            get(default) is called on the second var.
        Then:
            It should return the supplied default without raising.
        """
        # Arrange
        armed_var = ContextVar(_unique("arm_trigger"))
        unset_var = ContextVar(_unique("unset_in_armed"), default="ctor_default")
        no_default_var = ContextVar(_unique("no_default_in_armed"))

        # Act & assert
        with scoped_context():
            armed_var.set("arms-it")
            assert unset_var.get("supplied") == "supplied"
            with pytest.raises(LookupError):
                no_default_var.get()

    def test_set_arms_an_unarmed_context(self):
        """Test the first set arms the context with a snapshot.

        Given:
            A fresh, unarmed context where current_snapshot is None.
        When:
            ContextVar.set is called for the first time.
        Then:
            current_snapshot should return a Snapshot.
        """
        # Arrange
        var = ContextVar(_unique("set_arms"))
        with scoped_context():
            assert current_snapshot() is None

            # Act
            var.set("x")

            # Assert
            assert current_snapshot() is not None

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

    def test_set_rebuilds_the_snapshot_on_each_call(self):
        """Test each set rebuilds a new immutable snapshot.

        Given:
            A wool.ContextVar set once on an armed context.
        When:
            set is called a second time.
        Then:
            It should install a snapshot distinct by identity from the
            first — set rebuilds the immutable snapshot rather than
            mutating it in place.
        """
        # Arrange
        var = ContextVar(_unique("set_cow"))

        # Act
        with scoped_context():
            var.set("first")
            first_snapshot = current_snapshot()
            var.set("second")
            second_snapshot = current_snapshot()

            # Assert
            assert first_snapshot is not None
            assert second_snapshot is not None
            assert second_snapshot is not first_snapshot

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
            It should raise RuntimeError naming the used token.
        """
        # Arrange
        var = ContextVar(_unique("once"), default=0)

        # Act & assert
        with scoped_context():
            token = var.set(1)
            var.reset(token)
            with pytest.raises(RuntimeError, match="Token has already been used"):
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
        """Test ContextVar.reset reports a used token before a wrong-var token.

        Given:
            A Token that has already been consumed by a reset on its own
            ContextVar, passed to reset on a different ContextVar — so
            the token is BOTH already-used AND created by a different
            var.
        When:
            reset() is called with that token on the other var.
        Then:
            It should raise RuntimeError for the used token, not
            ValueError for the wrong var — the used-token guard is
            checked first.
        """
        # Arrange
        a = ContextVar(_unique("reset_used_a"), default=0)
        b = ContextVar(_unique("reset_used_b"), default=0)

        # Act & assert
        with scoped_context():
            token = a.set(1)
            a.reset(token)
            with pytest.raises(RuntimeError, match="Token has already been used"):
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

    def test_pickle_roundtrip_does_not_write_to_receiver_snapshot(self):
        """Test the wool serializer roundtrip of a ContextVar carries identity only.

        Given:
            A ContextVar set to a specific value in the current context.
        When:
            The var is dumped and loaded inside a fresh unarmed context.
        Then:
            The receiver's snapshot stays unarmed and var.get falls back
            to the constructor default — the pickle path is a key-only
            payload, never a value snapshot.
        """
        # Arrange
        var = ContextVar(_unique("pickle_with_value"), default="default_value")

        # Act
        with scoped_context():
            var.set("sender_value")
            pickled = dumps(var)

        observed: list[object] = []
        with scoped_context():
            restored = loads(pickled)
            observed.append(restored.get())
            observed.append(current_snapshot())

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
        # Bound to keep the decoded snapshot alive so its stub_pins anchor
        # holds the registered stub through the loads() call below.
        _decoded = decode_snapshot(pb)  # noqa: F841

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
            current_snapshot should still be None — a read never arms a
            context.
        """
        # Arrange
        var = ContextVar(_unique("get_no_arm"), default="d")

        # Act
        with scoped_context():
            var.get()

            # Assert
            assert current_snapshot() is None

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


def test_no_wool_contextvar_constructions_outside_runtime_context():
    """Test wool source has no wool.ContextVar constructor calls outside the subpackage.

    Given:
        The wool source tree under src/wool/.
    When:
        Each module is parsed and walked for Call nodes whose target
        resolves to wool.ContextVar via any import alias.
    Then:
        No constructor call is found outside wool/runtime/context/ —
        _infer_namespace only skips frames within that submodule, so a
        wool-internal construction beyond it would misattribute the
        var's namespace.
    """
    # Architectural guardrail, not a behavior test: this lints the wool
    # source tree for misplaced wool.ContextVar constructions rather than
    # exercising ContextVar's own runtime behavior.
    import ast
    import importlib.util
    from pathlib import Path

    # Arrange
    spec = importlib.util.find_spec("wool")
    assert spec is not None and spec.origin is not None
    src_root = Path(spec.origin).resolve().parent

    def _resolves_to_contextvar(name: str, imports: dict[str, str]) -> bool:
        target = imports.get(name)
        return target in {
            "wool.ContextVar",
            "wool.runtime.context.ContextVar",
            "wool.runtime.context.var.ContextVar",
        }

    def _check_call(
        call: ast.Call, imports: dict[str, str], offending: list[int]
    ) -> None:
        func = call.func
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            if imports.get(func.value.id) == "wool" and func.attr == "ContextVar":
                offending.append(call.lineno)
        elif isinstance(func, ast.Name) and _resolves_to_contextvar(func.id, imports):
            offending.append(call.lineno)

    def _scan(file: Path) -> list[int]:
        tree = ast.parse(file.read_text(), filename=str(file))
        imports: dict[str, str] = {}
        offending: list[int] = []
        for stmt in tree.body:
            if isinstance(stmt, ast.Import):
                for alias in stmt.names:
                    imports[alias.asname or alias.name.partition(".")[0]] = alias.name
                continue
            if isinstance(stmt, ast.ImportFrom) and stmt.module:
                for alias in stmt.names:
                    imports[alias.asname or alias.name] = f"{stmt.module}.{alias.name}"
                continue
            for node in ast.walk(stmt):
                if isinstance(node, ast.Call):
                    _check_call(node, imports, offending)
        return offending

    # Act
    offenders: list[tuple[Path, int]] = []
    runtime_context = src_root / "runtime" / "context"
    for py_file in src_root.rglob("*.py"):
        if py_file.is_relative_to(runtime_context):
            continue
        for lineno in _scan(py_file):
            offenders.append((py_file.relative_to(src_root), lineno))

    # Assert
    assert not offenders, (
        "wool.ContextVar constructed outside wool.runtime.context — "
        "_infer_namespace's skip predicate would misattribute the "
        "namespace:\n" + "\n".join(f"  {f}:{ln}" for f, ln in offenders)
    )
