"""The Wool signal roots.

Provides `WoolError` and `WoolWarning`: the umbrella base classes
under which Wool's own typed exceptions and warnings descend.
"""

from __future__ import annotations


# public
class WoolError(Exception):
    """Base class for every Wool-domain runtime exception.

    Catching `WoolError` matches every typed signal Wool raises from
    its own runtime with a single ``except`` clause::

        try:
            ...
        except wool.WoolError:
            ...
    """


# public
class WoolWarning(Warning):
    """Base class for Wool-domain warnings.

    Filtering on `WoolWarning` matches every Wool warning category
    with a single filter — for example, promoting all of them to
    errors::

        import warnings
        import wool

        warnings.filterwarnings("error", category=wool.WoolWarning)
    """
