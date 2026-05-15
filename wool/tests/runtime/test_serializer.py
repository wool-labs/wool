import pickle

import cloudpickle
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.serializer import CloudpickleSerializer
from wool.runtime.serializer import Serializer

_GUARD_MESSAGE = "instances of this type must be pickled via wool.runtime.serializer"


class _GuardedType:
    """Test double for a type that adopts the Wool pickling protocol."""

    def __init__(self, payload):
        self.payload = payload

    def __eq__(self, other):
        return isinstance(other, _GuardedType) and self.payload == other.payload

    def __hash__(self):
        return hash(("_GuardedType", self.payload))

    def __wool_reduce__(self):
        return (_GuardedType, (self.payload,))

    def __reduce_ex__(self, *_):
        raise TypeError(_GUARD_MESSAGE)


class _Unguarded:
    """Test double for a plain type that does not adopt the Wool pickling protocol."""

    def __init__(self, payload):
        self.payload = payload

    def __eq__(self, other):
        return isinstance(other, _Unguarded) and self.payload == other.payload

    def __hash__(self):
        return hash(("_Unguarded", self.payload))


def _arbitrary_payloads():
    """Recursive Hypothesis strategy covering nested cloudpickle-picklable values."""
    primitives = st.one_of(
        st.none(),
        st.booleans(),
        st.text(),
        st.integers(),
        st.binary(),
        st.floats(allow_nan=False),
    )
    return st.recursive(
        primitives,
        lambda children: st.one_of(
            st.lists(children),
            st.tuples(children, children),
            st.dictionaries(st.text(), children),
        ),
        max_leaves=20,
    )


class TestCloudpickleSerializer:
    def test_dumps_with_guarded_type(self):
        """Test CloudpickleSerializer.dumps round-trips a guarded type.

        Given:
            A CloudpickleSerializer instance and a _GuardedType payload.
        When:
            The serializer dumps and loads the payload.
        Then:
            It should round-trip to an equal instance.
        """
        # Arrange
        serializer = CloudpickleSerializer()
        instance = _GuardedType("hello")

        # Act
        restored = serializer.loads(serializer.dumps(instance))

        # Assert
        assert restored == instance

    def test_dumps_with_lambda(self):
        """Test the serializer preserves cloudpickle's lambda-pickling behavior.

        Given:
            A lambda — picklable by cloudpickle but not by stdlib pickle.
        When:
            The serializer dumps and loads the lambda.
        Then:
            It should round-trip to a callable that returns the same value
            (regression guard against accidentally bypassing cloudpickle).
        """
        # Arrange
        serializer = CloudpickleSerializer()
        lam = lambda x: x * 2  # noqa: E731

        # Act
        restored = serializer.loads(serializer.dumps(lam))

        # Assert
        assert restored(21) == 42

    def test_dumps_with_guarded_type_via_vanilla_pickle(self):
        """Test vanilla pickle and cloudpickle paths fail for guarded types.

        Given:
            A _GuardedType instance.
        When:
            pickle.dumps and cloudpickle.dumps are called on the instance.
        Then:
            Both should raise TypeError from the __reduce_ex__ guard.
        """
        # Arrange
        instance = _GuardedType("hello")

        # Act & assert
        with pytest.raises(TypeError, match=_GUARD_MESSAGE):
            pickle.dumps(instance)
        with pytest.raises(TypeError, match=_GUARD_MESSAGE):
            cloudpickle.dumps(instance)

    @given(payload=_arbitrary_payloads())
    @settings(
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow],
    )
    def test_dumps_round_trips_arbitrary_payloads(self, payload):
        """Test the serializer round-trips arbitrary cloudpickle-picklable payloads.

        Given:
            Any nested combination of None, bool, text, int, float, bytes,
            list, tuple, or dict.
        When:
            The payload is serialized and deserialized.
        Then:
            It should equal the original.
        """
        # Arrange
        serializer = CloudpickleSerializer()

        # Act
        restored = serializer.loads(serializer.dumps(payload))

        # Assert
        assert restored == payload

    def test_hash_and_equality_contract(self):
        """Test all CloudpickleSerializer instances are interchangeable.

        Given:
            Two distinct CloudpickleSerializer instances and an arbitrary
            object of a different type.
        When:
            Their hashes and equality are evaluated.
        Then:
            The two CloudpickleSerializer instances should compare equal,
            share a hash (so the LRU cache deduplicates them), and not
            compare equal to the other-type object.
        """
        # Arrange
        first = CloudpickleSerializer()
        second = CloudpickleSerializer()

        # Act & assert
        assert first == second
        assert hash(first) == hash(second)
        assert first != object()

    def test___instancecheck___with_serializer_protocol(self):
        """Test CloudpickleSerializer satisfies the Serializer protocol.

        Given:
            A CloudpickleSerializer instance.
        When:
            isinstance is evaluated against the runtime-checkable Serializer
            protocol.
        Then:
            It should return True.
        """
        # Arrange, act, & assert
        assert isinstance(CloudpickleSerializer(), Serializer)

    def test_dumps_with_unguarded_type(self):
        """Test the serializer delegates to cloudpickle for unguarded types.

        Given:
            A CloudpickleSerializer and an instance of a class defined
            inside the test method — a payload that requires cloudpickle's
            non-default reduction (stdlib pickle alone cannot resolve a
            local class by qualified name).
        When:
            The serializer dumps the instance.
        Then:
            The bytes should round-trip via cloudpickle.loads to an equal
            value, which only succeeds if the pickler delegated to
            cloudpickle's special-case handling rather than returning
            NotImplemented directly.
        """

        # Arrange
        class _LocalUnguarded:
            def __init__(self, payload):
                self.payload = payload

            def __eq__(self, other):
                return (
                    isinstance(other, _LocalUnguarded) and self.payload == other.payload
                )

            def __hash__(self):
                return hash(("_LocalUnguarded", self.payload))

        serializer = CloudpickleSerializer()
        instance = _LocalUnguarded("hello")

        # Act
        restored = cloudpickle.loads(serializer.dumps(instance))

        # Assert
        assert restored == instance

    def test_dumps_with_instance_attribute_named___wool_reduce__(self):
        """Test an instance attribute named __wool_reduce__ does not opt in.

        Given:
            An unguarded instance with an instance attribute named
            __wool_reduce__ that raises if invoked.
        When:
            The serializer dumps the instance.
        Then:
            It should not invoke the instance attribute (protocol opt-in is
            class-level, not instance-level), so dump must succeed without
            raising.
        """

        # Arrange
        def _raise():
            raise RuntimeError("instance attribute must not be invoked")

        serializer = CloudpickleSerializer()
        instance = _Unguarded("hello")
        setattr(instance, "__wool_reduce__", _raise)

        # Act — must not raise
        serializer.dumps(instance)

    def test_dumps_with_raising___wool_reduce__(self):
        """Test an exception inside __wool_reduce__ propagates from dumps.

        Given:
            A guarded type whose __wool_reduce__ raises ValueError.
        When:
            The serializer dumps an instance.
        Then:
            It should propagate the original ValueError.
        """

        # Arrange
        class _Failing:
            def __wool_reduce__(self):
                raise ValueError("boom")

        serializer = CloudpickleSerializer()

        # Act & assert
        with pytest.raises(ValueError, match="boom"):
            serializer.dumps(_Failing())
