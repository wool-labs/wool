"""Stdlib parity pins for the headline v2 ``wool.ContextVar`` guarantees.

The v2 context model backs each :class:`wool.ContextVar` with its own
stdlib :class:`contextvars.ContextVar`, so the surface guarantees that
distinguished v2 — ``reset`` rejected across a different
:class:`contextvars.Context`, single-use ``Token`` enforcement,
``copy_context()`` propagation and copy-on-write isolation, and the
``1 + N`` armed-context width — are now *native* stdlib behaviors.

These tests pin each of those side-by-side with a stdlib
:class:`contextvars.ContextVar` via the ``make_var`` fixture, so one
assertion proves the wool variable behaves identically. The
``1 + N``-width and async-generator value tests round out the v2
contract. The loop-driven tests additionally run under both the
default ``asyncio`` loop and uvloop, via the ``event_loop_policy``
fixture in ``conftest.py``; the ``1 + N``-width tests are pure
:class:`contextvars.Context` enumeration and need no running loop.
"""

import contextvars
import uuid

import pytest

import wool
from wool.runtime.context.var import ContextVar

pytestmark = pytest.mark.stdlib_parity


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


def _wool_owned(context: contextvars.Context) -> list[contextvars.ContextVar]:
    """Return the wool-owned stdlib variables enumerated by *context*.

    Every wool-owned :class:`contextvars.ContextVar` carries an
    ``__wool`` name prefix (``__wool_chain__`` for the chain
    context, ``__wool_var__:{ns}:{name}`` for each backing), so a
    plain iteration over the context distinguishes them from
    application variables.
    """
    return [var for var in context if var.name.startswith("__wool")]


def _wool_owned_in_a_fresh_context(work) -> list[str]:
    """Run *work* in a brand-new Chain and list the wool-owned variables.

    A fresh :class:`contextvars.Context` carries none of the backing
    variables earlier work on the running thread may have left set, so
    the result is exactly the wool-owned variables *work* itself binds —
    making the count immune to cross-test backing-variable leakage.
    """
    holder: list[list[str]] = []

    def _runner() -> None:
        work()
        copied = contextvars.copy_context()
        holder.append([v.name for v in _wool_owned(copied)])

    contextvars.Context().run(_runner)
    return holder[0]


class TestUnarmedChainLoudFail:
    def test_get_should_raise_lookup_error_when_chain_unarmed(self):
        """Test the Wool chain variable raises LookupError when unarmed.

        Given:
            A fresh contextvars.Context in which Wool's chain variable was
            never armed, mirroring a stdlib contextvars.ContextVar declared
            without a default.
        When:
            wool.__chain__.get() is read with no default argument.
        Then:
            It should raise LookupError exactly as the defaultless stdlib
            variable does — the chain variable lost its default, so unarmed
            access fails loudly rather than returning None, the contract
            behind every .get(None) read.
        """
        # Arrange
        stdlib_var: contextvars.ContextVar[object] = contextvars.ContextVar(
            _unique("parity_no_default")
        )

        def _assert_loud() -> None:
            with pytest.raises(LookupError):
                stdlib_var.get()
            with pytest.raises(LookupError):
                wool.__chain__.get()

        # Act & assert — in a fresh context neither defaultless variable
        # has a value, so a bare ``get()`` raises.
        contextvars.Context().run(_assert_loud)


class TestResetCrossContextRejection:
    @pytest.mark.asyncio
    async def test_reset_should_raise_value_error_when_in_foreign_context(
        self, make_var
    ):
        """Test reset of a token in a different Chain raises ValueError.

        This is the headline v2 fix: a token is bound to the
        :class:`contextvars.Context` it was minted in, and resetting it
        elsewhere is rejected natively.

        Given:
            A context variable set in the current context, yielding a
            token.
        When:
            The token is reset inside a copy_context().run() — a
            different contextvars.Context than the one it was minted
            in.
        Then:
            It should raise :class:`ValueError`, identically for a
            stdlib and a wool variable.
        """
        # Arrange
        var = make_var("reset_xctx")
        token = var.set("value")
        foreign = contextvars.copy_context()

        # Act & assert
        with pytest.raises(ValueError):
            foreign.run(lambda: var.reset(token))

    @pytest.mark.asyncio
    async def test_reset_should_restore_prior_value_when_in_minting_context(
        self, make_var
    ):
        """Test reset of a token in its own Chain restores the prior value.

        Given:
            A context variable set in the current context, yielding a
            token.
        When:
            The token is reset in the same contextvars.Context it was
            minted in.
        Then:
            It should restore the variable's prior state without
            raising, identically for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("reset_same")
        token = var.set("value")

        # Act
        var.reset(token)

        # Assert
        assert var.get("unset") == "unset"


class TestResetWrongVariableRejection:
    @pytest.mark.asyncio
    async def test_reset_should_raise_value_error_when_token_from_another_variable(
        self, make_var
    ):
        """Test resetting variable B with variable A's token raises ValueError.

        Given:
            Two distinct context variables, A set to yield a token.
        When:
            B.reset is called with A's token.
        Then:
            It should raise :class:`ValueError` — a token is bound to
            the variable that minted it, identically for a stdlib and a
            wool variable.
        """
        # Arrange
        var_a = make_var("wrong_a")
        var_b = make_var("wrong_b")
        token_a = var_a.set("a")

        # Act & assert
        with pytest.raises(ValueError):
            var_b.reset(token_a)


class TestTokenSingleUse:
    @pytest.mark.asyncio
    async def test_resetting_a_token_twice_should_raise_runtime_error(self, make_var):
        """Test resetting the same token twice raises RuntimeError.

        Given:
            A context variable set once, yielding a token, then reset
            with that token.
        When:
            The same token is reset a second time.
        Then:
            It should raise :class:`RuntimeError` — tokens are
            single-use, identically for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("single_use")
        token = var.set("value")
        var.reset(token)

        # Act & assert
        with pytest.raises(RuntimeError):
            var.reset(token)


class TestCopyContextPropagation:
    @pytest.mark.asyncio
    async def test_value_set_before_copy_should_be_visible_in_ctx_run(self, make_var):
        """Test a value set before copy_context() is visible inside ctx.run.

        Given:
            A context variable set before a copy_context() is taken.
        When:
            A function is invoked through the copy's run().
        Then:
            It should observe the pre-copy value — copy_context()
            snapshots the bindings, identically for a stdlib and a wool
            variable.
        """
        # Arrange
        var = make_var("copy_visible")
        var.set("before-copy")
        copy = contextvars.copy_context()

        # Act
        observed = copy.run(var.get)

        # Assert
        assert observed == "before-copy"

    @pytest.mark.asyncio
    async def test_set_inside_ctx_run_should_not_leak_out(self, make_var):
        """Test a set inside copy_context().run() does not leak to the caller.

        Given:
            A context variable set in the caller, then a copy_context()
            taken.
        When:
            The variable is set to a new value inside the copy's run().
        Then:
            The caller should still observe its original value — native
            contextvars copy-on-write isolates the copy's write,
            identically for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("copy_isolate")
        var.set("caller")
        copy = contextvars.copy_context()

        def mutate() -> str:
            var.set("inside-run")
            return var.get()

        # Act
        inside = copy.run(mutate)

        # Assert
        assert inside == "inside-run"
        assert var.get() == "caller"


class TestCopyContextWidth:
    def test_unarmed_context_should_carry_no_wool_variables(self):
        """Test an unarmed context enumerates no wool-owned variables.

        Given:
            A brand-new context in which a wool.ContextVar is declared
            but never set.
        When:
            A copy_context() of that context is enumerated.
        Then:
            It should carry no wool-owned contextvars.ContextVar — an
            unarmed context is indistinguishable from a plain
            contextvars.Context.
        """

        # Arrange
        def declare_but_do_not_set() -> None:
            ContextVar(_unique("width_unarmed"))

        # Act
        wool_owned = _wool_owned_in_a_fresh_context(declare_but_do_not_set)

        # Assert
        assert wool_owned == []

    @pytest.mark.parametrize("n", [1, 2, 3])
    def test_armed_context_should_enumerate_one_plus_n_variables(self, n):
        """Test an armed context enumerates 1 + N wool-owned variables.

        Given:
            A brand-new context armed with N distinct bound
            wool.ContextVars.
        When:
            A copy_context() of that context is enumerated for its
            wool-owned variables.
        Then:
            It should enumerate exactly 1 + N — the one context
            variable plus one backing variable per bound wool.ContextVar.
        """
        # Arrange
        bound = [ContextVar(_unique(f"width_armed_{index}")) for index in range(n)]

        def arm() -> None:
            for index, var in enumerate(bound):
                var.set(f"v{index}")

        # Act
        wool_owned = _wool_owned_in_a_fresh_context(arm)

        # Assert
        assert len(wool_owned) == 1 + n
        assert wool_owned.count("__wool_chain__") == 1
        backing = [name for name in wool_owned if name != "__wool_chain__"]
        assert len(backing) == n


class TestAsyncGeneratorValuePropagation:
    @pytest.mark.asyncio
    async def test_value_should_be_visible_across_asend_athrow_and_aclose(
        self, make_var
    ):
        """Test a context variable is visible across every async-gen resumption.

        Given:
            A context variable set before an async generator is driven.
        When:
            The generator is resumed via __anext__, asend, athrow, and
            finally aclose.
        Then:
            It should observe the scope's value at every resumption
            point — an async generator runs in the context active at
            each step, identically for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("agen_value")
        var.set("scope")
        observed: list[tuple[str, str]] = []

        async def generator():
            observed.append(("anext", var.get()))
            yield 1
            observed.append(("asend", var.get()))
            try:
                yield 2
            except ValueError:
                observed.append(("athrow", var.get()))
            try:
                yield 3
            except GeneratorExit:
                observed.append(("aclose", var.get()))
                raise

        gen = generator()

        # Act
        await gen.__anext__()
        await gen.asend("payload")
        await gen.athrow(ValueError("boom"))
        await gen.aclose()

        # Assert
        assert observed == [
            ("anext", "scope"),
            ("asend", "scope"),
            ("athrow", "scope"),
            ("aclose", "scope"),
        ]

    @pytest.mark.asyncio
    async def test_asend_should_return_next_yield_value(self, make_var):
        """Test asend resumes the generator and returns the next yield.

        Given:
            A context variable set before an async generator that
            echoes the variable on each yield is driven.
        When:
            The generator is resumed with asend.
        Then:
            It should return the scope's value as the next yielded
            item — value propagation rides asend's resumption,
            identically for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("agen_asend")
        var.set("scope")

        async def generator():
            while True:
                yield var.get()

        gen = generator()
        await gen.__anext__()

        # Act
        resumed = await gen.asend(None)

        # Assert
        assert resumed == "scope"

        # Cleanup
        await gen.aclose()
