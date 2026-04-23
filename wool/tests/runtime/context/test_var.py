import contextvars
import gc
import uuid

import cloudpickle
import pytest
from hypothesis import given
from hypothesis import strategies as st

import wool
from tests.helpers import scoped_context
from wool.runtime.context import Context
from wool.runtime.context import ContextVar
from wool.runtime.context import ContextVarCollision
from wool.runtime.context import Token
from wool.runtime.context import current_context

dumps = wool.__serializer__.dumps
loads = cloudpickle.loads


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
        assert var.name == "inferred"

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
        assert var.name == "explicit"

    @given(
        name=st.text(
            alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
            min_size=1,
            max_size=12,
        ),
        default_a=st.one_of(st.integers(), st.text(), st.booleans()),
        default_b=st.one_of(st.integers(), st.text(), st.booleans()),
    )
    def test___init___with_duplicate_key(self, name, default_a, default_b):
        """Test duplicate-key construction raises regardless of default match.

        Given:
            Any non-empty name and pair of default values
        When:
            A ContextVar is constructed, then a second with the identical key
        Then:
            The second construction raises ContextVarCollision whether or
            not the two defaults are equal
        """
        # A unique namespace per example guarantees the registry slot
        # is free without needing teardown between Hypothesis examples
        # (which share a single test-function invocation). The
        # collision still fires within the example because both
        # ContextVar calls use the same namespace + name.
        namespace = f"test_duplicate_{uuid.uuid4().hex}"

        # The registry holds vars weakly, so we hold ``first`` for the
        # duration of the second construction; without that pin the
        # weakref would drop before the second call and no collision
        # would fire.
        first = ContextVar(name, namespace=namespace, default=default_a)

        with pytest.raises(ContextVarCollision):
            ContextVar(name, namespace=namespace, default=default_b)

        # Hold ``first`` to the end so the collision actually had
        # something to collide with.
        assert first.namespace == namespace
        assert first.name == name

    def test___init___with_same_name_in_different_namespace(self):
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
        assert (a.namespace, a.name) == ("lib_a", "shared")
        assert (b.namespace, b.name) == ("lib_b", "shared")

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

    def test_set_token_when_reset_from_first_set(self):
        """Test the first set's Token restores the var to its default on reset.

        Given:
            A ContextVar with a constructor default and a single set
            that captures a Token
        When:
            reset(token) is called
        Then:
            var.get() should return the constructor default
        """
        # Arrange
        var = ContextVar("restore_default", default="initial")
        token = var.set("x")

        # Act
        var.reset(token)

        # Assert
        assert var.get() == "initial"

    def test_set_token_restores_outer_value_when_reset_from_nested_set(self):
        """Test a nested set's Token restores the outer set's value on reset.

        Given:
            A ContextVar set to "outer" and then set again to "inner"
            capturing the inner Token
        When:
            reset(inner_token) is called
        Then:
            var.get() should return "outer" — Tokens stack, each
            restoring only the value replaced by its own set
        """
        # Arrange
        var = ContextVar("restore_outer", default="d")
        var.set("outer")
        inner_token = var.set("inner")

        # Act
        var.reset(inner_token)

        # Assert
        assert var.get() == "outer"

    def test_reset_with_used_token(self):
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

    def test_reset_with_token_for_different_var(self):
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

    def test_reset_with_token_from_different_in_process_context(self):
        """Test reset rejects a token minted in a different in-process Context.

        Given:
            A ContextVar and a Token minted by var.set(...) inside
            ctx_a.run(...) — the token still holds its in-memory
            Context reference (distinct from the cross-process
            scenario covered elsewhere, which tests the UUID fallback
            after pickling)
        When:
            var.reset(token) is called inside ctx_b.run(...) where
            ctx_b is a different wool.Context in the same process
        Then:
            ValueError is raised with a message naming the different
            wool.Context — the in-process identity check fires, not
            the UUID fallback
        """
        # Arrange
        var = ContextVar("inprocess_reset", default="d")
        ctx_a = Context()
        ctx_b = Context()
        captured: list[Token] = []

        def inside_a():
            captured.append(var.set("a_value"))

        ctx_a.run(inside_a)
        token = captured[0]

        def inside_b():
            var.reset(token)

        # Act & assert
        with pytest.raises(ValueError, match="different wool.Context"):
            ctx_b.run(inside_b)

    def test_reset_with_reconstituted_token_when_chain_id_differs(self):
        """Test reset raises ValueError for a reconstituted Token whose
        originating chain id does not match the current Context.

        Given:
            A ContextVar, a Token minted in Context A and pickled,
            the original Token then released and garbage-collected
            so the process-wide token registry's weak entry drops,
            and a fresh scoped_context(B) block whose id differs
            from A
        When:
            The pickled bytes are loaded (producing a reconstituted
            Token with _context=None and _context_id=A) and
            var.reset(restored) is called under Context B
        Then:
            ValueError is raised with a message naming the different
            wool.Context — the UUID fallback check on the
            reconstituted branch fires because object-identity
            comparison isn't available
        """
        # Arrange
        var = ContextVar("reconstituted_chain_mismatch", default="d")
        pickled: bytes

        def mint_and_release() -> bytes:
            with scoped_context():
                token = var.set("x")
                return wool.__serializer__.dumps(token)

        pickled = mint_and_release()
        gc.collect()

        # Act & assert
        with scoped_context():
            restored = cloudpickle.loads(pickled)
            with pytest.raises(ValueError, match="different wool.Context"):
                var.reset(restored)

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

    def test_pickle_roundtrip_when_var_registered(self):
        """Test cloudpickle roundtrips a ContextVar to the same registered instance.

        Given:
            A ContextVar registered in the process-wide registry
        When:
            It is pickled and unpickled via dumps / loads
        Then:
            The unpickled instance should be the same object as the original
        """
        # Arrange
        var = ContextVar("shipped")

        # Act
        restored = loads(dumps(var))

        # Assert
        assert restored is var

    def test_pickle_roundtrip_when_key_unregistered_on_receiver(self):
        """Test loads() creates a stub when the var key is not registered.

        Given:
            Pickle bytes of a ContextVar whose registry slot has been
            reclaimed via GC (the var instance was constructed inside
            a nested Context.run block, pickled while bound to a
            value, and released on block exit so the
            WeakValueDictionary entry can drop)
        When:
            loads(pickled) is called inside a fresh Context scope
        Then:
            The restored var's key and default match the original,
            but the receiver's Context contains no entry for the var
            and get(fallback) returns the fallback — pickle preserves
            identity only and never writes to the receiver's Context
        """
        # Arrange — pickle inside a nested Context.run so the
        # ephemeral var's strong ref in that Context's data dict
        # drops with the scope, letting the WeakValueDictionary slot
        # clear on gc.collect().
        captured: list[tuple[bytes, tuple[str, str]]] = []

        def pickle_ephemeral():
            ephemeral = ContextVar(
                "stub_unknown",
                namespace="test_stub_create",
                default="d",
            )
            ephemeral.set("sender_value")
            captured.append((dumps(ephemeral), (ephemeral.namespace, ephemeral.name)))

        Context().run(pickle_ephemeral)
        gc.collect()

        pickled, original_key = captured[0]

        observed: list[tuple[ContextVar, Context]] = []

        def in_fresh():
            restored = loads(pickled)
            observed.append((restored, current_context()))

        # Act
        Context().run(in_fresh)

        # Assert
        assert len(observed) == 1
        restored, receiver_ctx = observed[0]
        assert (restored.namespace, restored.name) == original_key
        assert restored.get("fallback") == "fallback"
        assert restored not in receiver_ctx

    def test_pickle_roundtrip_does_not_write_to_receiver_context(self):
        """Test pickling a ContextVar does not propagate its value via reduce.

        Given:
            A ContextVar set to a specific value in the current
            context
        When:
            The var is pickled and unpickled inside a fresh
            wool.Context
        Then:
            The receiver's Context contains no binding for the var
            and var.get() falls back through the receiver Context's
            empty data to the constructor default — pickle is a key-
            only payload; state propagation rides on the wire-context
            path, not the reduce tuple
        """
        # Arrange
        var = ContextVar("pickle_with_value", default="default_value")
        var.set("sender_value")
        pickled = dumps(var)

        # Act
        observed_get: list[object] = []
        observed_ctx: list[Context] = []

        def in_fresh():
            restored = loads(pickled)
            observed_get.append(restored.get())
            observed_ctx.append(current_context())

        Context().run(in_fresh)

        # Assert
        assert observed_get == ["default_value"]
        assert var not in observed_ctx[0]

    def test_pickle_roundtrip_when_var_unset(self):
        """Test pickling a never-set ContextVar leaves the receiver clean.

        Given:
            A ContextVar that has never been set in the current
            context
        When:
            The var is pickled and unpickled inside a fresh
            wool.Context
        Then:
            The receiver's var.get() returns the class-level default
            and the receiver's Context does not contain the var
        """
        # Arrange
        var = ContextVar("pickle_no_value", default="default_value")
        pickled = dumps(var)

        # Act
        observed_get: list[object] = []
        observed_ctx: list[Context] = []

        def in_fresh():
            restored = loads(pickled)
            observed_get.append(restored.get())
            observed_ctx.append(current_context())

        Context().run(in_fresh)

        # Assert
        assert observed_get == ["default_value"]
        assert var not in observed_ctx[0]

    def test_later_declaration_after_preloaded_var(self):
        """Test a later ContextVar declaration promotes a pre-existing stub.

        Given:
            A stub produced by unpickling an unknown-key pickle —
            the registry holds a stub after loads()
        When:
            ContextVar(name, namespace=ns, default=<v>) is constructed
            with the same (namespace, name)
        Then:
            The declaration returns the same instance as the restored
            stub (no ContextVarCollision raised), and the declaration's
            default value is observed via var.get() in a fresh Context
            distinct from the stub's wire-value receiver Context
        """
        # Arrange — pickle inside a nested Context.run, let gc clear
        # the slot, then unpickle to create a stub.
        captured: list[bytes] = []

        def pickle_ephemeral():
            ephemeral = ContextVar(
                "stub_promoted",
                namespace="test_stub_promote",
                default="d",
            )
            ephemeral.set("wire_value")
            captured.append(dumps(ephemeral))

        Context().run(pickle_ephemeral)
        gc.collect()

        pickled = captured[0]

        # Hold a strong reference to the receiver Context so it
        # outlives the pickle load, ensuring the later
        # ContextVar(...) declaration runs against the registry
        # state the load produced rather than landing on state
        # already reclaimed by GC.
        receiver = Context()
        stub_capture: list[ContextVar] = []

        def in_receiver():
            stub_capture.append(loads(pickled))

        receiver.run(in_receiver)
        restored = stub_capture[0]

        # Act — authoritative declaration with a new default.
        promoted = ContextVar(
            "stub_promoted",
            namespace="test_stub_promote",
            default="auth_default",
        )

        # Assert — same instance (stub was promoted, not replaced).
        assert promoted is restored

        # And the promoted default is observed in a fresh Context
        # (distinct from the receiver Context that holds wire_value).
        observed_default: list[object] = []

        def in_fresh():
            observed_default.append(promoted.get())

        Context().run(in_fresh)

        assert observed_default == ["auth_default"]

    def test_repr_includes_namespace_and_name(self):
        """Test ContextVar repr includes the namespace and name.

        Given:
            A ContextVar with a name
        When:
            repr() is called on it
        Then:
            The repr should include the namespace and the name
        """
        # Arrange
        var = ContextVar("repr_cv")

        # Act
        text = repr(var)

        # Assert
        assert f"namespace={var.namespace!r}" in text
        assert f"name={var.name!r}" in text

    def test_vanilla_pickle_copy_and_deepcopy_are_rejected(self):
        """Test wool.ContextVar rejects pickle, cloudpickle, copy.copy,
        and copy.deepcopy.

        Given:
            A wool.ContextVar
        When:
            pickle.dumps, cloudpickle.dumps, copy.copy, and
            copy.deepcopy are each invoked on it
        Then:
            All four raise TypeError. ContextVar identity lives in
            the process-wide var_registry, and a vanilla restore
            outside Wool's dispatch path bypasses the stub-promotion
            and collision-detection that ContextVar._reconstitute
            relies on — wool.__serializer__ remains the only valid
            serialization channel.
        """
        # Arrange
        import copy as _copy
        import pickle

        match = "wool.ContextVar cannot be pickled"
        var = ContextVar("var_pickle_rejection")

        # Act & assert
        with pytest.raises(TypeError, match=match):
            pickle.dumps(var)
        with pytest.raises(TypeError, match=match):
            cloudpickle.dumps(var)
        with pytest.raises(TypeError, match=match):
            _copy.copy(var)
        with pytest.raises(TypeError, match=match):
            _copy.deepcopy(var)


@pytest.mark.asyncio
async def test_contextvar_pickle_reload_after_wire_decode_seeds_default():
    """Test loading a cloudpickled :class:`ContextVar` with a default
    seeds the receiver's stub when an earlier wire decode created it
    without one.

    Given:
        A receiver process where a wire :class:`protocol.Context`
        carrying a value for a :class:`ContextVar` key is decoded
        first — creating a default-less stub via
        :meth:`Context.from_protobuf` — and a cloudpickle dump of a
        :class:`ContextVar` for the same key with a non-Undefined
        default is loaded second, after the originating
        :class:`ContextVar` has been released so the registry no
        longer holds it.
    When:
        :func:`cloudpickle.loads` runs over the pickle.
    Then:
        It should return a :class:`ContextVar` for that key whose
        :meth:`get` falls back to the pickled default — the second
        ingress folded the default into the stub rather than silently
        discarding it.
    """
    # Arrange
    from wool import protocol

    var_namespace = uuid.uuid4().hex

    var = ContextVar("wire_then_pickle", namespace=var_namespace, default="from-pickle")
    pickled_var = dumps(var)
    del var
    gc.collect()

    pb = protocol.Context(id=uuid.uuid4().hex)
    pb.vars.add(
        namespace=var_namespace,
        name="wire_then_pickle",
        value=dumps("from-wire"),
    )
    # Bound to keep the wire-side Context alive so its stub anchor
    # holds the registered stub through the loads() call below.
    _secondary = Context.from_protobuf(pb)  # noqa: F841

    # Act
    restored_var = loads(pickled_var)

    # Assert
    assert restored_var.get() == "from-pickle"


def test_no_wool_contextvar_constructions_outside_runtime_context():
    """Test wool source has no ``wool.ContextVar(...)`` constructor
    calls outside the ``wool.runtime.context`` subpackage.

    Given:
        The wool source tree under ``src/wool/``
    When:
        Each module is parsed and walked for ``Call`` nodes whose
        target resolves to ``wool.ContextVar`` (via any import alias
        — ``wool.ContextVar``, ``from wool import ContextVar``,
        ``from wool.runtime.context import ContextVar``, or the
        deeper ``wool.runtime.context.var.ContextVar``)
    Then:
        No constructor call is found outside ``wool/runtime/context/``
        — ``_infer_namespace`` only skips frames within that
        submodule, so a wool-internal construction beyond it would
        silently misattribute the var into ``wool``'s namespace
        rather than the user's package. Adding such a construction
        without first broadening the inference (or passing
        ``namespace=`` explicitly) would be a latent footgun this
        test catches at test time
    """
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
            # ``wool.ContextVar(...)`` style — bare alias resolves to wool.
            if imports.get(func.value.id) == "wool" and func.attr == "ContextVar":
                offending.append(call.lineno)
        elif isinstance(func, ast.Name) and _resolves_to_contextvar(func.id, imports):
            offending.append(call.lineno)

    def _scan(file: Path) -> list[int]:
        tree = ast.parse(file.read_text(), filename=str(file))
        # Flow-sensitive: walk the top-level body in source order, mutating
        # the import map as Imports are encountered so that calls earlier
        # in the file resolve against earlier imports — a later import that
        # rebinds the same alias (e.g. ``from contextvars import ContextVar``
        # after a wool import of the same name) does not retroactively mask
        # an earlier wool construction.
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
        "_infer_namespace's skip predicate (`wool.runtime.context.*`) "
        "would misattribute the namespace. Either pass `namespace=` "
        "explicitly or broaden the skip predicate before adding such "
        "a construction:\n" + "\n".join(f"  {f}:{ln}" for f, ln in offenders)
    )
