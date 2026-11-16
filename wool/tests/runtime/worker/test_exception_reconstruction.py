import asyncio
import pickle
import subprocess
import sys
import threading
from functools import partial
from uuid import UUID

import grpc
import pytest

from wool.protocol.exceptions import ProtobufImportError
from wool.runtime.context.exceptions import ChainContention
from wool.runtime.context.exceptions import ChainSerializationError
from wool.runtime.context.exceptions import SerializationError
from wool.runtime.context.exceptions import SerializationWarning
from wool.runtime.context.exceptions import _reconstruct_chain_contention
from wool.runtime.loadbalancer.base import NoWorkersAvailable
from wool.runtime.worker.connection import HandshakeError
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.frame import ExceptionResponseFrame
from wool.runtime.worker.frame import Frame
from wool.runtime.worker.session import Rejected

_CASES = [
    pytest.param(
        partial(
            ChainContention,
            chain_id=UUID(int=7),
            kind="thread",
            owning_thread=1,
            current_thread=2,
        ),
        ("chain_id", "kind", "owning_thread", "current_thread"),
        id="chain-contention",
    ),
    *[
        pytest.param(
            partial(cls, grpc.StatusCode.INTERNAL, "worker detail"),
            ("code", "details"),
            id=cls.__name__,
        )
        for cls in (RpcError, TransientRpcError, HandshakeError)
    ],
    pytest.param(
        partial(SerializationError, "boom", cause=TypeError("cause"), value_repr="<x>"),
        ("cause", "value_repr"),
        id="serialization-error",
    ),
    pytest.param(
        partial(
            ChainSerializationError,
            SerializationWarning("first", var_key=("example", "first")),
            SerializationWarning("second", direction="encode", cause=TypeError("bad")),
        ),
        ("warnings",),
        id="chain-serialization-error",
    ),
    pytest.param(partial(Rejected, KeyError("key")), ("original",), id="rejected"),
    pytest.param(
        partial(ProtobufImportError, ImportError("no module example")),
        (),
        id="protobuf-import-error",
    ),
    pytest.param(NoWorkersAvailable, (), id="unchanged-no-workers"),
]


def _assert_field(actual, expected):
    if isinstance(expected, BaseException):
        assert type(actual) is type(expected)
        assert str(actual) == str(expected)
        assert actual.args == expected.args
        for field in ("cause", "value_repr", "var_key", "direction", "original_type"):
            if hasattr(expected, field):
                _assert_field(getattr(actual, field), getattr(expected, field))
    elif isinstance(expected, tuple):
        assert len(actual) == len(expected)
        for left, right in zip(actual, expected):
            _assert_field(left, right)
    else:
        assert actual == expected


def _marshalled(error):
    frame = ExceptionResponseFrame.for_send(error, wire_chain_manifest=None)
    return Frame.from_protobuf(frame.to_protobuf()).payload


def _force_fallback(error):
    cause = RuntimeError("unpicklable side-channel state")
    cause.lock = threading.Lock()
    error.__cause__ = cause
    # ImportError's reduction omits __cause__; its own state still forces
    # a genuine pickle failure, as does the cause for custom reductions.
    error.lock = threading.Lock()
    with pytest.warns(SerializationWarning):
        return _marshalled(error)


@pytest.mark.parametrize("factory,fields", _CASES)
@pytest.mark.parametrize("roundtrip", ["args", "pickle", "primary", "fallback"])
def test_exception_reconstruction_preserves_class_message_and_fields(
    factory, fields, roundtrip
):
    """Structured exception identity survives both worker serialization paths."""
    error = factory()
    message = str(error)
    if roundtrip == "args":
        restored = type(error)(*error.args)
    elif roundtrip == "pickle":
        restored = pickle.loads(pickle.dumps(error))
    elif roundtrip == "primary":
        restored = _marshalled(error)
    else:
        restored = _force_fallback(error)

    assert type(restored) is type(error)
    assert str(restored) == message
    for field in fields:
        _assert_field(getattr(restored, field), getattr(error, field))


@pytest.mark.asyncio
@pytest.mark.parametrize("fallback", [False, True])
async def test_task_contention_keeps_diagnostic_reprs_across_multiple_hops(fallback):
    task = asyncio.create_task(asyncio.sleep(0))
    try:
        error = ChainContention(
            chain_id=UUID(int=8), kind="task", owning_task=task, current_task=task
        )
        assert error.owning_task is task
        assert error.current_task is task
        message = str(error)
        expected_task = repr(task)
        roundtrip = _force_fallback if fallback else _marshalled
        for _ in range(2):
            error = roundtrip(error)
            assert type(error) is ChainContention
            assert error.owning_task == expected_task
            assert error.current_task == expected_task
            assert str(error) == message
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("kind", ["serialization", "rejected", "warnings"])
def test_unpicklable_attachments_on_structured_exception_arguments(kind):
    cause = ValueError("original cause")
    cause.lock = threading.Lock()
    if kind == "serialization":
        error = SerializationError("boom", cause=cause, value_repr="<x>")
    elif kind == "rejected":
        error = Rejected(cause)
    else:
        error = ChainSerializationError(
            SerializationWarning("first", cause=cause, var_key=("ns", "name")),
            SerializationWarning("second", direction="encode"),
        )

    with pytest.warns(SerializationWarning):
        restored = _marshalled(error)

    assert type(restored) is type(error)
    assert str(restored) == str(error)
    if kind == "serialization":
        assert type(restored.cause) is ValueError
        assert str(restored.cause) == "original cause"
        assert restored.value_repr == "<x>"
        assert restored.cause is restored.args[1]
    elif kind == "rejected":
        assert type(restored.original) is ValueError
        assert str(restored.original) == "original cause"
        assert restored.original is restored.args[0]
    else:
        assert len(restored.warnings) == 2
        assert restored.warnings[0].var_key == ("ns", "name")
        assert type(restored.warnings[0].cause) is ValueError
        assert str(restored.warnings[0].cause) == "original cause"
        assert restored.warnings[1].direction == "encode"


def test_cyclic_unpicklable_exception_attributes_are_bounded():
    cause = ValueError("original cause")
    cause.cycle = cause
    cause.lock = threading.Lock()
    error = SerializationError("boom", cause=cause)

    with pytest.warns(SerializationWarning):
        restored = _marshalled(error)

    assert type(restored) is SerializationError
    assert type(restored.cause) is ValueError
    assert str(restored.cause) == "original cause"


@pytest.mark.parametrize(
    "code,details,message",
    [
        (None, None, ""),
        (None, "details", "details"),
        (grpc.StatusCode.INTERNAL, None, "INTERNAL"),
        (grpc.StatusCode.INTERNAL, "", "INTERNAL: "),
    ],
)
def test_rpc_error_message_variants_reconstruct(code, details, message):
    error = RpcError(code, details)
    restored = type(error)(*error.args)
    assert str(restored) == str(error) == message
    assert restored.code is code
    assert restored.details == details


class _LegacyChainContention:
    def __init__(self, kind):
        self.kind = kind

    def __reduce__(self):
        return _reconstruct_chain_contention, (
            UUID(int=9),
            self.kind,
            1,
            2,
            "<Task pending>",
            "<Task pending>",
        )


@pytest.mark.parametrize("kind", ["thread", "task", "create_task"])
def test_legacy_chain_contention_reductions_still_load(kind):
    # This reduction names the module-level helper emitted by older versions,
    # rather than going through the current ChainContention.__reduce__.
    restored = pickle.loads(pickle.dumps(_LegacyChainContention(kind)))
    assert type(restored) is ChainContention
    assert restored.chain_id == UUID(int=9)
    assert restored.kind == kind
    assert restored.owning_thread == 1
    assert restored.current_thread == 2
    assert restored.owning_task == "<Task pending>"
    assert restored.current_task == "<Task pending>"
    assert str(pickle.loads(pickle.dumps(restored))) == str(restored)


@pytest.mark.subprocess_probe
def test_branching_exception_cycles_do_not_expand_during_fallback():
    code = """
import threading
import warnings
from wool.runtime.context.exceptions import SerializationError, SerializationWarning
from wool.runtime.worker.frame import ExceptionResponseFrame, Frame

cause = ValueError('original cause')
cause.first = cause
cause.second = cause
cause.lock = threading.Lock()
error = SerializationError('boom', cause=cause)
with warnings.catch_warnings():
    warnings.simplefilter('ignore', SerializationWarning)
    frame = ExceptionResponseFrame.for_send(error, wire_chain_manifest=None)
    restored = Frame.from_protobuf(frame.to_protobuf()).payload
assert type(restored) is SerializationError
assert type(restored.cause) is ValueError
assert restored.cause.first is restored.cause
assert restored.cause.second is restored.cause
argument = ValueError()
argument.args = (argument,)
argument.lock = threading.Lock()
error = SerializationError('cyclic arguments', cause=argument)
with warnings.catch_warnings():
    warnings.simplefilter('ignore', SerializationWarning)
    frame = ExceptionResponseFrame.for_send(error, wire_chain_manifest=None)
    restored = Frame.from_protobuf(frame.to_protobuf()).payload
assert type(restored) is RuntimeError
assert str(restored) == 'SerializationError: cyclic arguments'
print('bounded')
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "bounded"


def test_intrinsically_unpicklable_nonexception_argument_still_demotes():
    error = SerializationError(threading.Lock())
    with pytest.warns(SerializationWarning):
        restored = _marshalled(error)
    assert type(restored) is RuntimeError
    assert str(restored).startswith("SerializationError:")
