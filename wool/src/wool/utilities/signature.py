"""Signature questions asked as a caller would ask them.

Provides `accepts_kwarg`, `presupplies_kwarg`, `requires_kwarg`, and
`unbindable_call`: predicates over a specific call a caller intends to
make rather than over a callable's declaration, so a callable that
cannot be inspected is never refused on a guess.
"""

from __future__ import annotations

import inspect
from typing import Any
from typing import Callable


def accepts_kwarg(
    fn: Callable[..., Any], name: str, /, *args: Any, **kwargs: Any
) -> bool:
    """Report whether a callable accepts the specified keyword argument.

    Answers one question for a caller holding an optional value: may I
    pass this keyword, given everything else I am already passing? The
    call is the unit of the question — ``*args`` and ``**kwargs`` are
    the rest of the call, and the answer can differ between two calls
    to the same callable.

    A true answer says only that the value *can* be delivered, never
    what the callable does with it, and never that the call as a whole
    is satisfiable — some other argument may still be missing.

    :param fn:
        The callable to check.
    :param name:
        The parameter name to pass.
    :param args:
        Positional arguments the caller will also pass.
    :param kwargs:
        Other keyword arguments the caller will also pass.
    :returns:
        True when ``name`` can be delivered alongside ``*args`` and
        ``**kwargs`` — the callable declares it, or absorbs it through
        ``**kwargs``, and nothing already given collides with it. False
        when it cannot, and False when the signature cannot be read at
        all — the safe default, since a callable that cannot be
        inspected cannot be shown to accept anything.

    .. rubric:: Implementation notes

    The check binds rather than inspecting parameter kinds, because no
    rule keyed to `inspect.Parameter.kind` is correct. A
    positional-or-keyword parameter collides with a forwarded
    positional at one argument count and binds cleanly at another, so
    its classification is a property of the call, not of the
    declaration.

    It binds *partially*, which is what keeps the question about
    ``name`` alone. A complete bind fails whenever any other required
    parameter is unsatisfied, so probing one keyword would report on
    another: a callable declaring two required keywords would be found
    to accept neither, each probe defeated by the one it omits.
    """
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    try:
        signature.bind_partial(*args, **kwargs, **{name: None})
    except TypeError:
        return False
    return True


def presupplies_kwarg(fn: Callable[..., Any], name: str, /) -> bool:
    """Report whether a callable has already bound a value for a keyword.

    The complement to `accepts_kwarg` for a caller deciding whether it
    *should* pass a value rather than whether it *can*. A
    `functools.partial` carrying the keyword has had that argument
    decided by whoever built it; passing another would silently replace
    it, since a partial composes as ``{**bound, **call_site}``.

    Only a bound value counts, never a declared default. The
    distinction is the point: a default says "use this if you pass
    nothing", while a bound value says the question is already settled.
    Nested partials count, however deep the keyword was bound.

    :param fn:
        The callable to check.
    :param name:
        The parameter name to look for.
    :returns:
        True when ``name`` already has a value bound to it. False for
        anything else, including a parameter merely declared with a
        default, and including a callable that is not a partial at all.

    .. rubric:: Implementation notes

    A bound keyword and a declared default are indistinguishable through
    `inspect.signature`, which renders a partial's bound keyword as that
    parameter's default, so this reads ``functools.partial.keywords``
    directly. `functools.partial` flattens when nested, so one lookup
    sees every bound keyword.
    """
    return name in (getattr(fn, "keywords", None) or {})


def requires_kwarg(
    fn: Callable[..., Any], name: str, /, *args: Any, **kwargs: Any
) -> bool:
    """Report whether omitting a keyword would break the call.

    The complement of `accepts_kwarg`, and not its negation: a callable
    may accept a keyword, require it, both, or neither. A caller that
    only sometimes has a value needs both answers — one to know it may
    pass, the other to know it must.

    :param fn:
        The callable to check.
    :param name:
        The parameter name to omit.
    :param args:
        Positional arguments the caller will pass.
    :param kwargs:
        Other keyword arguments the caller will pass.
    :returns:
        True when omitting ``name`` would raise `TypeError` **for want
        of it** — the callable declares it, gives it no default, and
        nothing in ``*args``/``**kwargs`` already supplies it. False
        otherwise, including when the signature cannot be read and when
        the caller's own arguments do not fit, neither of which is a
        statement about ``name``.

    .. rubric:: Implementation notes

    Answered by inspecting the parameter rather than by attempting the
    call, because a failed bind does not say *which* argument it wanted.
    Reading any `TypeError` as evidence about ``name`` would report a
    callable that requires some unrelated parameter as requiring this
    one — a diagnostic naming the wrong keyword, and one the caller
    cannot act on.
    """
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    parameter = signature.parameters.get(name)
    if parameter is None or parameter.default is not inspect.Parameter.empty:
        return False
    if parameter.kind in (
        inspect.Parameter.VAR_POSITIONAL,
        inspect.Parameter.VAR_KEYWORD,
    ):
        # `*args`/`**kwargs` absorb rather than demand; omitting them
        # is always legal however they are named.
        return False
    try:
        bound = signature.bind_partial(*args, **kwargs)
    except TypeError:
        return False
    # Already supplied — by a pre-bound `functools.partial`, or
    # positionally by the caller — so it need not be passed again.
    return name not in bound.arguments


def unbindable_call(fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> str | None:
    """Return why a whole call would fail, or ``None`` when it would bind.

    Where `accepts_kwarg` and `requires_kwarg` each answer about one
    parameter and say nothing about the rest, this asks whether the
    call is satisfiable at all. A caller that has finished deciding
    which keywords to pass uses it to fail early, and with a message
    naming the argument actually at fault, rather than letting a
    `TypeError` surface later from a frame the user cannot see.

    :param fn:
        The callable to check.
    :param args:
        Positional arguments the caller will pass.
    :param kwargs:
        Keyword arguments the caller will pass.
    :returns:
        The binding failure's message when the call would raise
        `TypeError`, and ``None`` when it would bind. Also ``None`` when
        the signature cannot be read: an uninspectable callable offers
        nothing to check against, and refusing one on a guess is the
        behaviour this module exists to avoid.
    """
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return None
    try:
        signature.bind(*args, **kwargs)
    except TypeError as error:
        return str(error)
    return None
