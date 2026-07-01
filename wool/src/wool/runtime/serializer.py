from __future__ import annotations

import io
from typing import Any
from typing import Protocol
from typing import runtime_checkable

import cloudpickle


# public
@runtime_checkable
class Serializer(Protocol):
    """Protocol for pluggable serialization of dispatch payloads.

    Implementations must be hashable.  A class that overrides
    ``__eq__`` without supplying a compatible ``__hash__`` (which
    Python silently sets to ``None``) fails ``isinstance(obj,
    Serializer)``, because the runtime-checkable protocol consults
    ``hasattr`` and ``hasattr(obj, "__hash__")`` returns ``False``
    when ``__hash__`` is ``None``.
    """

    def __hash__(self) -> int: ...

    def dumps(self, obj: Any) -> bytes:
        """Serialize *obj* to bytes for transport across a worker boundary.

        :param obj:
            The object to serialize.  Implementations decide which payloads
            are supported.
        :returns:
            The serialized representation of *obj*.
        """
        ...

    def loads(self, data: bytes) -> Any:
        """Deserialize *data* produced by a matching :meth:`dumps` call.

        :param data:
            Bytes previously produced by :meth:`dumps`.
        :returns:
            The reconstructed object.
        """
        ...


class _WoolPickler(cloudpickle.Pickler):
    """Cloudpickle-based pickler that honors the ``__wool_reduce__`` protocol.

    For objects whose type defines ``__wool_reduce__``, the method's return
    value (a standard ``(callable, args)`` reduce tuple) is used in place of
    the standard reduction protocol.  All other objects are handled exactly
    as :class:`cloudpickle.Pickler` would handle them.

    The override fires before ``__reduce_ex__``, so a type that pairs
    ``__wool_reduce__`` with a ``__reduce_ex__`` guard (raising
    :exc:`TypeError`) can be guarded against vanilla pickling while still
    serializing correctly through this pickler.
    """

    def reducer_override(self, obj: Any) -> Any:
        if hasattr(type(obj), "__wool_reduce__"):
            return obj.__wool_reduce__()
        return super().reducer_override(obj)


class CloudpickleSerializer:
    """Default :class:`Serializer` implementation.

    Used by :class:`~wool.runtime.routine.task.Task.to_protobuf` when the
    caller does not provide an explicit serializer.  Semantically the
    contract is cloudpickle's: callables and arguments must be
    cloudpickle-picklable.  The fact that ``dumps`` routes through Wool's
    internal pickler — and therefore respects the ``__wool_reduce__``
    protocol on guarded Wool types — is an implementation detail.
    """

    def __hash__(self) -> int:
        """Return a constant hash; all instances are interchangeable."""
        return hash(CloudpickleSerializer)

    def __eq__(self, other: object) -> bool:
        """Return True for any other CloudpickleSerializer; instances are interchangeable."""
        return isinstance(other, CloudpickleSerializer)

    def dumps(self, obj: Any) -> bytes:
        """Serialize *obj* via Wool's internal pickler.

        Honors the ``__wool_reduce__`` protocol on guarded Wool types and
        otherwise reduces via cloudpickle.
        """
        buffer = io.BytesIO()
        _WoolPickler(buffer).dump(obj)
        return buffer.getvalue()

    def loads(self, data: bytes) -> Any:
        """Deserialize *data* via :func:`cloudpickle.loads`."""
        return cloudpickle.loads(data)
