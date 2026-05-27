import asyncio
import contextvars
import uuid

import pytest
import pytest_asyncio

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

    Resets the wool-owned context ``contextvars.ContextVar`` so a
    :meth:`wool.ContextVar.set` in one test does not leak its armed
    context into the next.
    """
    with scoped_context():
        yield


@pytest_asyncio.fixture(autouse=True)
async def reset_task_factory():
    """Clear the running loop's task factory after each parity test.

    Several parity tests install Wool's task factory, a user factory,
    or a deliberately-broken legacy factory on the per-test event loop.
    pytest-asyncio's loop teardown (``loop.shutdown_asyncgens``) routes
    its own task creation through whatever factory is left installed —
    a broken factory left in place crashes teardown. Clearing the
    factory on teardown keeps a test's factory mutation from poisoning
    loop teardown or any later test sharing the loop.
    """
    yield
    try:
        asyncio.get_running_loop().set_task_factory(None)
    except RuntimeError:  # pragma: no cover — no running loop for a sync test
        pass
