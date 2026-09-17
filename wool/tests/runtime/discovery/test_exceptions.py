import pickle
import threading
import uuid

import pytest
from hypothesis import example
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from wool.runtime.context.exceptions import SerializationWarning
from wool.runtime.discovery.exceptions import DiscoveryBlockExhausted
from wool.runtime.discovery.exceptions import DiscoveryCapacityExhausted
from wool.runtime.discovery.exceptions import DiscoveryNamespaceInUse
from wool.runtime.discovery.exceptions import DiscoveryNamespaceNotFound
from wool.runtime.discovery.exceptions import DiscoveryWorkerNotFound
from wool.runtime.worker.frame import ExceptionResponseFrame
from wool.runtime.worker.frame import Frame

#: Every discovery exception, the arguments its fields are built from,
#: and the names those fields carry. Drives the contracts in
#: `TestDiscoveryExceptionReconstruction` that hold across every class;
#: each class's own property coverage lives in its own suite.
_RECONSTRUCTION_CASES = [
    pytest.param(DiscoveryCapacityExhausted, (128,), ("capacity",), id="capacity"),
    pytest.param(
        DiscoveryCapacityExhausted, (None,), ("capacity",), id="capacity-unknown"
    ),
    pytest.param(DiscoveryBlockExhausted, (2048,), ("size",), id="block"),
    pytest.param(DiscoveryBlockExhausted, (None,), ("size",), id="block-unknown"),
    pytest.param(
        DiscoveryWorkerNotFound, (uuid.UUID(int=7),), ("uid",), id="worker-not-found"
    ),
    pytest.param(DiscoveryWorkerNotFound, (None,), ("uid",), id="worker-unknown"),
    pytest.param(
        DiscoveryNamespaceInUse, ("ns", "seg"), ("namespace", "segment"), id="in-use"
    ),
    pytest.param(
        DiscoveryNamespaceInUse,
        (None, None),
        ("namespace", "segment"),
        id="in-use-unknown",
    ),
    pytest.param(DiscoveryNamespaceNotFound, ("ns",), ("namespace",), id="not-found"),
    pytest.param(
        DiscoveryNamespaceNotFound, (None,), ("namespace",), id="not-found-unknown"
    ),
]


def _marshalled(error):
    """Round-trip error through a worker's exception response frame.

    Returns the decoded payload. The exception crosses the same
    serializer a real dispatch uses.
    """
    frame = ExceptionResponseFrame.for_send(error, wire_chain_manifest=None)
    return Frame.from_protobuf(frame.to_protobuf()).payload


class TestDiscoveryCapacityExhausted:
    @given(capacity=st.one_of(st.none(), st.integers()))
    @example(capacity=128)
    @example(capacity=None)
    @settings(max_examples=100)
    def test___init___should_expose_its_capacity_across_the_argument_domain(
        self, capacity
    ):
        """Test field exposure and message content over the argument.

        Given:
            Any optional capacity.
        When:
            A DiscoveryCapacityExhausted is constructed from it.
        Then:
            It should expose the capacity unchanged and name it in the
            message, omitting the clause entirely where it is None.
        """
        # Act
        error = DiscoveryCapacityExhausted(capacity)

        # Assert
        assert error.capacity == capacity

        message = str(error)
        assert "No available slots in discovery registry" in message
        if capacity is None:
            assert "capacity" not in message
        else:
            assert f"capacity {capacity}" in message

    @given(capacity=st.one_of(st.none(), st.integers()))
    @settings(max_examples=50)
    def test___reduce___should_preserve_its_capacity_across_a_boundary(self, capacity):
        """Test reconstruction survives pickling.

        Given:
            Any optional capacity.
        When:
            A DiscoveryCapacityExhausted is pickled and unpickled.
        Then:
            It should restore the capacity and its args.
        """
        # Arrange
        error = DiscoveryCapacityExhausted(capacity)

        # Act
        restored = pickle.loads(pickle.dumps(error))

        # Assert
        assert restored.capacity == capacity
        assert restored.args == error.args

    @given(capacity=st.one_of(st.none(), st.integers()))
    @settings(max_examples=25)
    def test___init___should_survive_the_worker_exception_serializer(self, capacity):
        """Test the exception survives the worker exception serializer.

        Given:
            Any optional capacity.
        When:
            A DiscoveryCapacityExhausted is encoded in an exception
            response frame and decoded.
        Then:
            It should arrive as the same class with its capacity and the
            same message.
        """
        # Arrange
        error = DiscoveryCapacityExhausted(capacity)

        # Act
        restored = _marshalled(error)

        # Assert
        assert isinstance(restored, DiscoveryCapacityExhausted)
        assert restored.capacity == capacity
        assert str(restored) == str(error)


class TestDiscoveryBlockExhausted:
    @given(size=st.one_of(st.none(), st.integers()))
    @example(size=2048)
    @example(size=None)
    @settings(max_examples=100)
    def test___init___should_expose_its_size_across_the_argument_domain(self, size):
        """Test field exposure and message content over the argument.

        Given:
            Any optional payload size.
        When:
            A DiscoveryBlockExhausted is constructed from it.
        Then:
            It should expose the size unchanged and name it in the
            message, omitting the clause entirely where it is None.
        """
        # Act
        error = DiscoveryBlockExhausted(size)

        # Assert
        assert error.size == size

        message = str(error)
        assert "Worker metadata exceeds its registered block" in message
        if size is None:
            assert "bytes" not in message
        else:
            assert f"{size} bytes" in message

    @given(size=st.one_of(st.none(), st.integers()))
    @settings(max_examples=50)
    def test___reduce___should_preserve_its_size_across_a_boundary(self, size):
        """Test reconstruction survives pickling.

        Given:
            Any optional payload size.
        When:
            A DiscoveryBlockExhausted is pickled and unpickled.
        Then:
            It should restore the size and its args.
        """
        # Arrange
        error = DiscoveryBlockExhausted(size)

        # Act
        restored = pickle.loads(pickle.dumps(error))

        # Assert
        assert restored.size == size
        assert restored.args == error.args

    @given(size=st.one_of(st.none(), st.integers()))
    @settings(max_examples=25)
    def test___init___should_survive_the_worker_exception_serializer(self, size):
        """Test the exception survives the worker exception serializer.

        Given:
            Any optional payload size.
        When:
            A DiscoveryBlockExhausted is encoded in an exception
            response frame and decoded.
        Then:
            It should arrive as the same class with its size and the
            same message.
        """
        # Arrange
        error = DiscoveryBlockExhausted(size)

        # Act
        restored = _marshalled(error)

        # Assert
        assert isinstance(restored, DiscoveryBlockExhausted)
        assert restored.size == size
        assert str(restored) == str(error)


class TestDiscoveryWorkerNotFound:
    @given(uid=st.one_of(st.none(), st.uuids()))
    @example(uid=uuid.UUID(int=7))
    @example(uid=None)
    @settings(max_examples=100)
    def test___init___should_expose_its_uid_across_the_argument_domain(self, uid):
        """Test field exposure and message content over the argument.

        Given:
            Any optional worker UID.
        When:
            A DiscoveryWorkerNotFound is constructed from it.
        Then:
            It should expose the UID unchanged and name it in the
            message, omitting the clause entirely where it is None.
        """
        # Act
        error = DiscoveryWorkerNotFound(uid)

        # Assert
        assert error.uid == uid

        message = str(error)
        assert "not found in discovery registry" in message
        if uid is None:
            assert message == "Worker not found in discovery registry"
        else:
            assert str(uid) in message

    @given(uid=st.one_of(st.none(), st.uuids()))
    @settings(max_examples=50)
    def test___reduce___should_preserve_its_uid_across_a_boundary(self, uid):
        """Test reconstruction survives pickling.

        Given:
            Any optional worker UID.
        When:
            A DiscoveryWorkerNotFound is pickled and unpickled.
        Then:
            It should restore the UID as a UUID and its args.
        """
        # Arrange
        error = DiscoveryWorkerNotFound(uid)

        # Act
        restored = pickle.loads(pickle.dumps(error))

        # Assert
        assert restored.uid == uid
        assert restored.args == error.args

    @given(uid=st.one_of(st.none(), st.uuids()))
    @settings(max_examples=25)
    def test___init___should_survive_the_worker_exception_serializer(self, uid):
        """Test the exception survives the worker exception serializer.

        Given:
            Any optional worker UID.
        When:
            A DiscoveryWorkerNotFound is encoded in an exception
            response frame and decoded.
        Then:
            It should arrive as the same class with its UID and the same
            message.
        """
        # Arrange
        error = DiscoveryWorkerNotFound(uid)

        # Act
        restored = _marshalled(error)

        # Assert
        assert isinstance(restored, DiscoveryWorkerNotFound)
        assert restored.uid == uid
        assert str(restored) == str(error)


class TestDiscoveryNamespaceInUse:
    @given(
        namespace=st.one_of(st.none(), st.text()),
        segment=st.one_of(st.none(), st.text()),
    )
    @settings(max_examples=100)
    def test___init___should_expose_its_fields_across_the_argument_domain(
        self, namespace, segment
    ):
        """Test field exposure and message content over both arguments.

        Given:
            Any optional namespace and any optional segment.
        When:
            A DiscoveryNamespaceInUse is constructed from them.
        Then:
            It should expose both unchanged, quote each supplied field
            in its message, and omit the corresponding clause entirely
            where a field is None.
        """
        # Act
        error = DiscoveryNamespaceInUse(namespace, segment=segment)

        # Assert
        assert error.namespace == namespace
        assert error.segment == segment

        message = str(error)
        if namespace is None:
            assert "already in use" in message
        else:
            assert repr(namespace) in message
        if segment is None:
            assert "remove shared memory" not in message
        else:
            assert repr(segment) in message

    @given(
        namespace=st.one_of(st.none(), st.text()),
        segment=st.one_of(st.none(), st.text()),
    )
    @settings(max_examples=50)
    def test___reduce___should_preserve_its_fields_across_a_process_boundary(
        self, namespace, segment
    ):
        """Test reconstruction survives pickling.

        Given:
            Any optional namespace and any optional segment.
        When:
            A DiscoveryNamespaceInUse is pickled and unpickled.
        Then:
            It should restore both fields and its args.
        """
        # Arrange
        error = DiscoveryNamespaceInUse(namespace, segment=segment)

        # Act
        restored = pickle.loads(pickle.dumps(error))

        # Assert
        assert restored.namespace == namespace
        assert restored.segment == segment
        assert restored.args == error.args

    @given(
        namespace=st.one_of(st.none(), st.text()),
        segment=st.one_of(st.none(), st.text()),
    )
    @settings(max_examples=25)
    def test___init___should_survive_the_worker_exception_serializer(
        self, namespace, segment
    ):
        """Test the exception survives the worker exception serializer.

        Given:
            Any optional namespace and any optional segment.
        When:
            A DiscoveryNamespaceInUse is encoded in an exception
            response frame and decoded.
        Then:
            It should arrive as the same class with both fields and the
            same message.
        """
        # Arrange
        error = DiscoveryNamespaceInUse(namespace, segment=segment)

        # Act
        restored = _marshalled(error)

        # Assert
        assert isinstance(restored, DiscoveryNamespaceInUse)
        assert restored.namespace == namespace
        assert restored.segment == segment
        assert str(restored) == str(error)


class TestDiscoveryNamespaceNotFound:
    @given(namespace=st.one_of(st.none(), st.text()))
    @settings(max_examples=100)
    def test___init___should_expose_its_namespace_across_the_argument_domain(
        self, namespace
    ):
        """Test field exposure and message content over the argument.

        Given:
            Any optional namespace.
        When:
            A DiscoveryNamespaceNotFound is constructed from it.
        Then:
            It should expose the namespace unchanged and quote it in
            the message, omitting the clause entirely where it is None.
        """
        # Act
        error = DiscoveryNamespaceNotFound(namespace)

        # Assert
        assert error.namespace == namespace

        message = str(error)
        assert "No discovery registry for namespace" in message
        if namespace is None:
            assert message == "No discovery registry for namespace"
        else:
            assert repr(namespace) in message

    @given(namespace=st.one_of(st.none(), st.text()))
    @settings(max_examples=50)
    def test___reduce___should_preserve_its_namespace_across_a_process_boundary(
        self, namespace
    ):
        """Test reconstruction survives pickling.

        Given:
            Any optional namespace.
        When:
            A DiscoveryNamespaceNotFound is pickled and unpickled.
        Then:
            It should restore the namespace and its args.
        """
        # Arrange
        error = DiscoveryNamespaceNotFound(namespace)

        # Act
        restored = pickle.loads(pickle.dumps(error))

        # Assert
        assert restored.namespace == namespace
        assert restored.args == error.args

    @given(namespace=st.one_of(st.none(), st.text()))
    @settings(max_examples=25)
    def test___init___should_survive_the_worker_exception_serializer(self, namespace):
        """Test the exception survives the worker exception serializer.

        Given:
            Any optional namespace.
        When:
            A DiscoveryNamespaceNotFound is encoded in an exception
            response frame and decoded.
        Then:
            It should arrive as the same class with its namespace and
            the same message.
        """
        # Arrange
        error = DiscoveryNamespaceNotFound(namespace)

        # Act
        restored = _marshalled(error)

        # Assert
        assert isinstance(restored, DiscoveryNamespaceNotFound)
        assert restored.namespace == namespace
        assert str(restored) == str(error)


class TestDiscoveryExceptionReconstruction:
    """Contracts shared by every discovery exception."""

    @pytest.mark.parametrize(
        "cls",
        [
            DiscoveryCapacityExhausted,
            DiscoveryBlockExhausted,
            DiscoveryWorkerNotFound,
            DiscoveryNamespaceInUse,
            DiscoveryNamespaceNotFound,
        ],
    )
    def test___init___should_subclass_wool_error_not_runtime_error(self, cls):
        """Test each discovery exception joins the Wool umbrella.

        Given:
            A discovery exception class.
        When:
            An instance is raised and caught as wool.WoolError.
        Then:
            It should be caught by that clause, be the very class
            re-exported from wool, and not subclass RuntimeError.
        """
        # Arrange
        raised = cls()

        # Act
        try:
            raise raised
        except wool.WoolError as error:
            caught = error

        # Assert
        assert caught is raised
        assert getattr(wool, cls.__name__) is cls
        assert not issubclass(cls, RuntimeError)

    @pytest.mark.parametrize(("cls", "args", "fields"), _RECONSTRUCTION_CASES)
    def test___init___should_reconstruct_from_args(self, cls, args, fields):
        """Test every discovery exception rebuilds intact from its args.

        Given:
            A discovery exception constructed with its fields either
            known or omitted.
        When:
            A second instance is built from the first's args, as
            ``cls(*error.args)``.
        Then:
            It should restore every structured field with its own type
            and render the same message.
        """
        # Arrange
        error = cls(*args)

        # Act
        rebuilt = type(error)(*error.args)

        # Assert
        for field in fields:
            assert getattr(rebuilt, field) == getattr(error, field)
        assert str(rebuilt) == str(error)

    @pytest.mark.parametrize(("cls", "args", "fields"), _RECONSTRUCTION_CASES)
    def test___init___should_survive_the_fallback_when_its_cause_cannot_pickle(
        self, cls, args, fields
    ):
        """Test the type-preserving fallback keeps the class and its fields.

        Given:
            A discovery exception whose __cause__ holds an unpicklable
            object, so the serializer's primary dump fails and its
            type-preserving fallback runs.
        When:
            It is encoded in an exception response frame and decoded.
        Then:
            It should warn that fidelity was lost and still arrive as
            its own class with every structured field and the same
            message.
        """

        # Arrange
        class _Unpicklable(Exception):
            def __init__(self):
                super().__init__()
                self._lock = threading.Lock()

        error = cls(*args)
        error.__cause__ = _Unpicklable()

        # Act
        with pytest.warns(SerializationWarning):
            restored = _marshalled(error)

        # Assert
        assert type(restored) is cls
        for field in fields:
            assert getattr(restored, field) == getattr(error, field)
        assert str(restored) == str(error)
