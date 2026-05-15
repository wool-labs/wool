"""Stdlib parity pins for ``async-generator.aclose`` semantics.

These tests assert observations about CPython's own
``await agen.aclose()`` behavior. They are intentionally NOT tests of
any wool code — they pin stdlib semantics so that a future change in
CPython's async-generator close protocol fails here first, signaling
that the paired :func:`wool.runtime.routine.task.routine_scope`
regression tests (and the helper's contract) may need to be revisited.

Companion to :class:`tests.runtime.routine.test_task.TestRoutineScope`,
which exercises ``routine_scope`` against the same parity assumptions.
"""

import asyncio

import pytest


class TestAsyncGenAcloseParity:
    @pytest.mark.asyncio
    async def test_aclose_propagates_internal_cancelled(self):
        """Test ``aclose`` propagates internal CancelledError.

        Given:
            A direct ``asyncio`` async generator that raises
            :class:`asyncio.CancelledError` during aclose unwind
            while the awaiting task's ``cancelling()`` count is 0.
        When:
            ``await agen.aclose()`` is invoked after one iteration.
        Then:
            It should raise :class:`asyncio.CancelledError`.
        """

        # Arrange
        async def naughty_gen():
            try:
                yield 1
                yield 2
            except GeneratorExit:
                raise asyncio.CancelledError()

        agen = naughty_gen()
        await agen.__anext__()

        # Act & assert
        with pytest.raises(asyncio.CancelledError):
            await agen.aclose()

    @pytest.mark.asyncio
    async def test_aclose_raises_runtime_error_when_yielding_during_ge(self):
        """Test ``aclose`` raises RuntimeError when the routine yields during GeneratorExit.

        Given:
            A direct ``asyncio`` async generator that catches
            :class:`GeneratorExit` and yields a value (a PEP 525
            protocol violation).
        When:
            ``await agen.aclose()`` is invoked after one iteration.
        Then:
            It should raise
            ``RuntimeError("async generator ignored GeneratorExit")``.
        """

        # Arrange
        async def yielding_gen():
            try:
                yield 1
                yield 2
            except GeneratorExit:
                yield "rude"

        agen = yielding_gen()
        await agen.__anext__()

        # Act & assert
        with pytest.raises(RuntimeError, match="ignored GeneratorExit"):
            await agen.aclose()
