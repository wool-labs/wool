import asyncio
import contextvars
import uuid

import pytest

from tests.helpers import scoped_context
from wool.runtime.context import ContextVar


@pytest.fixture(
    params=["asyncio", "uvloop"],
    ids=["asyncio", "uvloop"],
)
def event_loop_policy(request):
    """Parametrize every parity test over the asyncio and uvloop event loops.

    Wool advertises context propagation "across every conformant event loop —
    uvloop included". ``call_soon``/``call_later``, ``run_in_executor``, and
    ``set_task_factory`` are loop-implemented, so a parity claim is only
    substantiated when each test runs under both the default ``asyncio`` loop
    and uvloop's Cython reimplementation. pytest-asyncio honors this fixture
    when constructing the per-test loop.
    """
    if request.param == "uvloop":
        uvloop = pytest.importorskip("uvloop")
        return uvloop.EventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()


@pytest.fixture(
    params=["stdlib", "wool"],
    ids=["stdlib", "wool"],
)
def make_var(request):
    """Return a factory constructing a stdlib or wool context variable.

    Parametrizes a value-propagation parity test over both variable types so
    a single assertion proves ``wool.ContextVar`` propagates identically to
    ``contextvars.ContextVar`` across the scheduling edge under test. The
    factory takes a name stem and appends a process-unique suffix to avoid
    ``wool.ContextVar`` registry collisions across tests.
    """
    if request.param == "wool":
        return lambda stem: ContextVar(f"{stem}_{uuid.uuid4().hex}")
    return lambda stem: contextvars.ContextVar(f"{stem}_{uuid.uuid4().hex}")


@pytest.fixture(autouse=True)
def isolated_context():
    """Run each parity test under a fresh, unarmed Wool context.

    Resets the wool-owned snapshot ``contextvars.ContextVar`` so a
    :meth:`wool.ContextVar.set` in one test does not leak its armed
    snapshot into the next.
    """
    with scoped_context():
        yield
