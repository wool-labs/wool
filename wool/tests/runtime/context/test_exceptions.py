import pickle
import warnings
from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from wool.runtime.context.exceptions import ChainContention
from wool.runtime.context.exceptions import ChainSerializationError
from wool.runtime.context.exceptions import ContextVarCollision
from wool.runtime.context.exceptions import SerializationError
from wool.runtime.context.exceptions import SerializationWarning
from wool.runtime.context.exceptions import TaskFactoryDisplaced


class TestSerializationError:
    def test___init___should_default_structured_fields_to_none_when_message_only(self):
        """Test SerializationError construction with only a message.

        Given:
            The SerializationError class.
        When:
            An instance is constructed with only a message argument.
        Then:
            It should expose None for cause and value_repr, and carry
            the message on args.
        """
        # Act
        error = SerializationError("encode failed")

        # Assert
        assert error.cause is None
        assert error.value_repr is None
        assert error.args == ("encode failed",)

    def test___init___should_expose_structured_fields_when_provided(self):
        """Test SerializationError construction with structured fields.

        Given:
            An underlying cause exception and a value repr preview.
        When:
            An instance is constructed with the cause and value_repr
            keyword fields alongside a message.
        Then:
            It should expose both structured fields unchanged and
            carry the message on args.
        """
        # Arrange
        cause = TypeError("cannot pickle lock")

        # Act
        error = SerializationError(
            "encode failed",
            cause=cause,
            value_repr="<unlocked _thread.lock object>",
        )

        # Assert
        assert error.cause is cause
        assert error.value_repr == "<unlocked _thread.lock object>"
        assert "encode failed" in error.args

    def test_catchability_should_catch_as_wool_error(self):
        """Test SerializationError is catchable as a WoolError.

        Given:
            A SerializationError instance.
        When:
            It is raised inside a try block with an except clause for
            wool.WoolError.
        Then:
            It should be caught by that clause and be the raised
            instance.
        """
        # Arrange
        error = SerializationError("boom")

        # Act
        try:
            raise error
        except wool.WoolError as caught_error:
            caught = caught_error

        # Assert
        assert caught is error


class TestChainSerializationError:
    def test_warnings_should_keep_warnings_and_summarize_message(self):
        """Test ChainSerializationError keeps warnings and summarizes them.

        Given:
            Two SerializationWarning instances interleaved with a
            plain string argument.
        When:
            A ChainSerializationError is constructed from them.
        Then:
            It should keep exactly the two warnings on the warnings
            attribute and carry a synthesized summary message on args
            instead of the raw arguments tuple.
        """
        # Arrange
        warning_a = SerializationWarning("first failure")
        warning_b = SerializationWarning("second failure")

        # Act
        error = ChainSerializationError(warning_a, "plain", warning_b)

        # Assert
        assert error.warnings == (warning_a, warning_b)
        assert error.args == (str(error),)
        assert "plain" not in str(error)

    def test_warnings_should_return_empty_tuple_when_no_warning_args(self):
        """Test ChainSerializationError.warnings with no warning args.

        Given:
            A ChainSerializationError constructed without any
            SerializationWarning arguments.
        When:
            Its warnings property is read.
        Then:
            It should return an empty tuple.
        """
        # Act
        error = ChainSerializationError("no warnings here")

        # Assert
        assert error.warnings == ()

    def test_catchability_should_catch_as_serialization_error(self):
        """Test ChainSerializationError is catchable as SerializationError.

        Given:
            A ChainSerializationError instance.
        When:
            It is raised inside a try block with an except clause for
            wool.SerializationError.
        Then:
            It should be caught by that clause and be the raised
            instance.
        """
        # Arrange
        error = ChainSerializationError(SerializationWarning("promoted"))

        # Act
        try:
            raise error
        except wool.SerializationError as caught_error:
            caught = caught_error

        # Assert
        assert caught is error

    def test_roots_should_subclass_wool_error_not_runtime_error(self):
        """Test ChainSerializationError subclasses WoolError, not RuntimeError.

        Given:
            The ChainSerializationError aggregator class.
        When:
            Its position in the exception hierarchy is checked.
        Then:
            It should subclass wool.WoolError (catchable via
            wool.SerializationError) and must not subclass RuntimeError,
            so a documented ``except RuntimeError`` would not catch it.
        """
        # Arrange, act, & assert
        assert issubclass(ChainSerializationError, wool.WoolError)
        assert issubclass(ChainSerializationError, wool.SerializationError)
        assert not issubclass(ChainSerializationError, RuntimeError)


class TestSerializationWarning:
    def test___init___should_default_structured_fields_to_none_when_message_only(self):
        """Test SerializationWarning construction with only a message.

        Given:
            The SerializationWarning class.
        When:
            An instance is constructed with only a message argument.
        Then:
            It should expose None for every structured field — cause,
            var_key, direction, original_type — and carry the
            message on args.
        """
        # Act
        warning = SerializationWarning("something failed to serialize")

        # Assert
        assert warning.cause is None
        assert warning.var_key is None
        assert warning.direction is None
        assert warning.original_type is None
        assert warning.args == ("something failed to serialize",)

    def test___init___should_expose_structured_fields_when_provided(self):
        """Test SerializationWarning construction with structured fields.

        Given:
            A cause exception, a (namespace, name) variable key, and a
            direction literal.
        When:
            An instance is constructed with all keyword fields —
            cause, var_key, direction, and original_type.
        Then:
            It should expose each structured field unchanged as a
            public attribute.
        """
        # Arrange
        cause = TypeError("cannot pickle lock")
        var_key = ("test_ns", "test_var")

        # Act
        warning = SerializationWarning(
            "value failed to encode",
            cause=cause,
            var_key=var_key,
            direction="encode",
            original_type=ValueError,
        )

        # Assert
        assert warning.cause is cause
        assert warning.var_key == var_key
        assert warning.direction == "encode"
        assert warning.original_type is ValueError

    def test_serialization_warning_should_subclass_wool_warning(self):
        """Test SerializationWarning is a WoolWarning subclass.

        Given:
            The SerializationWarning class.
        When:
            Its subclass relationship to WoolWarning is checked.
        Then:
            It should be a subclass of WoolWarning — so callers can
            promote every Wool warning category to an error with a
            single filter on the umbrella.
        """
        # Arrange, act, & assert
        assert issubclass(SerializationWarning, wool.WoolWarning)

    def test_serialization_warning_should_be_re_exported_from_wool(self):
        """Test SerializationWarning is re-exported on the wool package.

        Given:
            The wool package and the SerializationWarning class.
        When:
            wool.SerializationWarning is accessed.
        Then:
            It should be the same class as the one defined in
            wool.runtime.context.exceptions.
        """
        # Arrange, act, & assert
        assert wool.SerializationWarning is SerializationWarning

    def test_promotion_should_raise_warning_via_umbrella_filter(self):
        """Test the WoolWarning umbrella filter promotes the warning.

        Given:
            A warnings filter promoting wool.WoolWarning to an error.
        When:
            A SerializationWarning is emitted via warnings.warn.
        Then:
            It should raise the emitted SerializationWarning instance
            as an exception — the umbrella's single-filter strict-mode
            recipe works through the base class, not just the leaf.
        """
        # Arrange
        emitted = SerializationWarning("bad var", direction="encode")

        # Act & assert
        with warnings.catch_warnings():
            warnings.filterwarnings("error", category=wool.WoolWarning)
            with pytest.raises(SerializationWarning) as exc_info:
                warnings.warn(emitted, stacklevel=2)
        assert exc_info.value is emitted

    @given(
        message=st.text(),
        var_key=st.one_of(st.none(), st.tuples(st.text(), st.text())),
        direction=st.sampled_from(["encode", "decode", None]),
    )
    @settings(max_examples=100, deadline=None)
    def test_serializer_roundtrip_should_preserve_structured_fields(
        self, message, var_key, direction
    ):
        """Test the serializer round-trips a warning's structured fields.

        Given:
            Any combination of message text, optional (namespace,
            name) variable key, and optional direction literal.
        When:
            The warning is serialized and deserialized through
            wool.__serializer__.
        Then:
            The restored instance should preserve args, var_key, and
            direction.
        """
        # Arrange
        warning = SerializationWarning(
            message,
            var_key=var_key,
            direction=direction,
        )

        # Act
        restored = wool.__serializer__.loads(wool.__serializer__.dumps(warning))

        # Assert
        assert restored.args == warning.args
        assert restored.var_key == var_key
        assert restored.direction == direction


class TestChainContention:
    def test_chain_contention_should_subclass_wool_error(self):
        """Test ChainContention is a WoolError subclass.

        Given:
            The wool.ChainContention exception class.
        When:
            Its subclass relationship to WoolError is checked.
        Then:
            It should be a subclass of WoolError — catchable under the
            single Wool-domain umbrella.
        """
        # Arrange, act, & assert
        assert issubclass(ChainContention, wool.WoolError)

    def test_unknown_kind_should_raise_value_error(self):
        """Test ChainContention rejects unknown ``kind`` values.

        Given:
            A ``kind`` value that is not one of the recognised
            literals (``"thread"``, ``"task"``, ``"create_task"``).
        When:
            ChainContention is constructed with that kind.
        Then:
            It should raise ValueError naming the unknown kind and
            listing the recognised set — this guards dynamic call
            sites (notably ``__reduce__``-driven cross-process
            reconstruction where a forward-compat receiver might
            decode a ``kind`` value it does not know) so the
            exception fails loud at construction rather than later
            with a KeyError during message formatting.
        """
        # Arrange, act, & assert
        with pytest.raises(ValueError, match="unknown ChainContention kind"):
            ChainContention(chain_id=uuid4(), kind="bogus")  # type: ignore[arg-type]

    def test_chain_contention_should_round_trip_through_pickle(self):
        """Test ChainContention survives a pickle round-trip.

        Given:
            A ChainContention raised with structured kwargs.
        When:
            The exception is pickled and unpickled.
        Then:
            The restored instance should carry the same chain id, kind,
            and identity fields, and reconstruct an equivalent message.
        """
        # Arrange
        chain_id = uuid4()
        exc = ChainContention(
            chain_id=chain_id,
            kind="thread",
            owning_thread=12345,
            current_thread=67890,
        )

        # Act
        restored = pickle.loads(pickle.dumps(exc))

        # Assert
        assert isinstance(restored, ChainContention)
        assert restored.chain_id == chain_id
        assert restored.kind == "thread"
        assert restored.owning_thread == 12345
        assert restored.current_thread == 67890
        assert str(restored) == str(exc)

    def test_chain_contention_should_interpolate_chain_id_when_create_task_kind(self):
        """Test the create_task kind interpolates the chain id.

        Given:
            A ChainContention with kind="create_task" raised by the task
            factory when an armed context is re-passed to create_task.
        When:
            The exception's string form is inspected.
        Then:
            The message should mention create_task and interpolate the
            chain id, and the exception's structured fields should
            reflect the create_task kind.
        """
        # Arrange
        chain_id = uuid4()

        # Act
        exc = ChainContention(chain_id=chain_id, kind="create_task")

        # Assert
        assert exc.kind == "create_task"
        assert exc.chain_id == chain_id
        assert "create_task" in str(exc)
        assert str(chain_id) in str(exc)


class TestContextVarCollision:
    def test_context_var_collision_should_subclass_wool_error(self):
        """Test ContextVarCollision is a WoolError subclass.

        Given:
            The wool.ContextVarCollision exception class.
        When:
            Its subclass relationship to WoolError is checked.
        Then:
            It should be a subclass of WoolError — catchable under the
            single Wool-domain umbrella.
        """
        # Arrange, act, & assert
        assert issubclass(ContextVarCollision, wool.WoolError)


class TestTaskFactoryDisplaced:
    def test_task_factory_displaced_should_subclass_wool_error(self):
        """Test TaskFactoryDisplaced is a WoolError subclass.

        Given:
            The TaskFactoryDisplaced exception class.
        When:
            Its subclass relationship to WoolError is checked.
        Then:
            It should be a subclass of WoolError — displacement is an
            unconditional structural failure, not a tunable warning;
            this lets callers catch it under the single Wool-domain
            umbrella.
        """
        # Arrange, act, & assert
        assert issubclass(TaskFactoryDisplaced, wool.WoolError)

    def test_task_factory_displaced_should_be_re_exported_from_wool(self):
        """Test TaskFactoryDisplaced is re-exported on the wool package.

        Given:
            The wool package and the TaskFactoryDisplaced class.
        When:
            wool.TaskFactoryDisplaced is accessed.
        Then:
            It should be the same class as the one defined in
            wool.runtime.context.exceptions.
        """
        # Arrange, act, & assert
        assert wool.TaskFactoryDisplaced is TaskFactoryDisplaced
