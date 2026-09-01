import pickle

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from wool.runtime.discovery.exceptions import DiscoveryNamespaceInUse
from wool.runtime.discovery.exceptions import DiscoveryNamespaceNotFound


class TestDiscoveryNamespaceInUse:
    def test___init___should_subclass_wool_error_not_runtime_error(self):
        """Test DiscoveryNamespaceInUse joins the Wool umbrella.

        Given:
            The DiscoveryNamespaceInUse class.
        When:
            An instance is raised and caught as wool.WoolError.
        Then:
            It should be caught by that clause, be the very class
            re-exported from wool, and not subclass RuntimeError —
            a single except clause must match every Wool signal.
        """
        # Arrange
        raised = DiscoveryNamespaceInUse("ns")

        # Act
        try:
            raise raised
        except wool.WoolError as error:
            caught = error

        # Assert
        assert caught is raised
        assert wool.DiscoveryNamespaceInUse is DiscoveryNamespaceInUse
        assert not issubclass(DiscoveryNamespaceInUse, RuntimeError)

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
            assert "already has an owner" in message
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
            It should restore both fields and its args — a worker
            subprocess can raise this and have it marshalled back, and
            default exception unpickling reconstructs from args alone.
        """
        # Arrange
        error = DiscoveryNamespaceInUse(namespace, segment=segment)

        # Act
        restored = pickle.loads(pickle.dumps(error))

        # Assert
        assert restored.namespace == namespace
        assert restored.segment == segment
        assert restored.args == error.args


class TestDiscoveryNamespaceNotFound:
    def test___init___should_subclass_wool_error_not_runtime_error(self):
        """Test DiscoveryNamespaceNotFound joins the Wool umbrella.

        Given:
            The DiscoveryNamespaceNotFound class.
        When:
            An instance is raised and caught as wool.WoolError.
        Then:
            It should be caught by that clause, be the very class
            re-exported from wool, and not subclass RuntimeError —
            a single except clause must match every Wool signal.
        """
        # Arrange
        raised = DiscoveryNamespaceNotFound("ns")

        # Act
        try:
            raise raised
        except wool.WoolError as error:
            caught = error

        # Assert
        assert caught is raised
        assert wool.DiscoveryNamespaceNotFound is DiscoveryNamespaceNotFound
        assert not issubclass(DiscoveryNamespaceNotFound, RuntimeError)

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
        if namespace is not None:
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
            It should restore the namespace and its args — a borrower
            in a worker subprocess can raise this and have it
            marshalled back to its caller.
        """
        # Arrange
        error = DiscoveryNamespaceNotFound(namespace)

        # Act
        restored = pickle.loads(pickle.dumps(error))

        # Assert
        assert restored.namespace == namespace
        assert restored.args == error.args


@pytest.mark.parametrize("cls", [DiscoveryNamespaceInUse, DiscoveryNamespaceNotFound])
def test_discovery_exceptions_should_be_constructible_from_a_message_alone(cls):
    """Test each class reconstructs from its message alone.

    Given:
        A discovery namespace exception class.
    When:
        It is constructed with a single positional argument, the form
        default exception unpickling uses.
    Then:
        It should construct without error — a required field would
        silently break reconstruction at a process boundary.
    """
    # Act
    error = cls("some-namespace")

    # Assert
    assert isinstance(error, wool.WoolError)
