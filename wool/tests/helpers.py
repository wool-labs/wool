from contextlib import contextmanager
from typing import Iterator
from uuid import UUID

from wool import protocol
from wool.runtime.context import Context
from wool.runtime.context import attached


@contextmanager
def scoped_context(id: UUID | None = None) -> Iterator[Context]:
    """Test helper — install a wool.Context for the duration of the block.

    Mints a fresh chain id by default. Pass *id* to construct a
    Context with a specific chain id, used by tests that exercise
    chain-id-dependent semantics (e.g. ContextVar.reset's same-id
    fallback). The id-bearing path goes through the public
    ``Context.from_protobuf`` rather than the private
    ``_reconstitute`` builder, since wool deliberately does not
    expose an in-process id-only constructor (that would invite
    duplicate-id Contexts and undercut the single-task-per-Context
    invariant). On exit the prior scope's Context is restored.

    Attaches without claiming the single-task guard so tests can
    invoke ``Context.run`` / ``attached(ctx)`` on the yielded
    Context themselves.
    """
    if id is None:
        ctx = Context()
    else:
        ctx = Context.from_protobuf(protocol.Context(id=id.hex))
    with attached(ctx, guarded=False):
        yield ctx
