from wool import protocol


class TestProtocolPackage:
    """Tests for :py:mod:`wool.protocol` package-level API."""

    def test_wire_submodule_accessible(self):
        """Test wire submodule is importable from protocol.

        Given:
            The protocol package.
        When:
            The wire attribute is accessed.
        Then:
            It should be a module with expected names.
        """
        assert hasattr(protocol, "Request")
        assert hasattr(protocol, "Response")
        assert hasattr(protocol, "WorkerServicer")

    def test_add_to_server_mapping(self):
        """Test add_to_server maps WorkerServicer to its registrar.

        Given:
            The protocol add_to_server dict.
        When:
            WorkerServicer is used as a key.
        Then:
            It should map to add_WorkerServicer_to_server.
        """
        assert protocol.WorkerServicer in protocol.add_to_server
        assert (
            protocol.add_to_server[protocol.WorkerServicer]
            is protocol.add_WorkerServicer_to_server
        )

    def test___version___exists(self):
        """Test __version__ is available on protocol package.

        Given:
            The protocol package.
        When:
            __version__ is accessed.
        Then:
            It should be a string.
        """
        assert isinstance(protocol.__version__, str)
