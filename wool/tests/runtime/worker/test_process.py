import signal
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import grpc
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.worker.process import WorkerProcess
from wool.runtime.worker.process import _proxy_factory
from wool.runtime.worker.process import _proxy_finalizer
from wool.runtime.worker.process import _sigint_handler
from wool.runtime.worker.process import _signal_handlers
from wool.runtime.worker.process import _sigterm_handler


class TestSignalHandlers:
    """Test suite for signal handler functions."""

    def test__sigterm_handler_calls_service_stop_when_loop_is_running(self, mocker):
        """Test _sigterm_handler calls service.stop when loop is running.

        Given:
            A running event loop and WorkerService
        When:
            _sigterm_handler is called with SIGTERM
        Then:
            It should schedule service.stop with timeout=0 via call_soon_threadsafe
        """
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = True
        mock_service = MagicMock()

        # Mock pb.worker.StopRequest
        mock_stop_request = MagicMock()
        mocker.patch(
            "wool.runtime.worker.process.pb.worker.StopRequest",
            return_value=mock_stop_request,
        )

        _sigterm_handler(mock_loop, mock_service, signal.SIGTERM, None)

        mock_loop.is_running.assert_called_once()
        mock_loop.call_soon_threadsafe.assert_called_once()

    def test__sigterm_handler_does_nothing_when_loop_is_not_running(self):
        """Test _sigterm_handler does nothing when loop is not running.

        Given:
            An event loop that is not running
        When:
            _sigterm_handler is called
        Then:
            It should not call call_soon_threadsafe
        """
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = False
        mock_service = MagicMock()

        _sigterm_handler(mock_loop, mock_service, signal.SIGTERM, None)

        mock_loop.is_running.assert_called_once()
        mock_loop.call_soon_threadsafe.assert_not_called()

    def test__sigint_handler_calls_service_stop_when_loop_is_running(self, mocker):
        """Test _sigint_handler calls service.stop when loop is running.

        Given:
            A running event loop and WorkerService
        When:
            _sigint_handler is called with SIGINT
        Then:
            It should schedule service.stop with timeout=None via call_soon_threadsafe
        """
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = True
        mock_service = MagicMock()

        # Mock pb.worker.StopRequest
        mock_stop_request = MagicMock()
        mocker.patch(
            "wool.runtime.worker.process.pb.worker.StopRequest",
            return_value=mock_stop_request,
        )

        _sigint_handler(mock_loop, mock_service, signal.SIGINT, None)

        mock_loop.is_running.assert_called_once()
        mock_loop.call_soon_threadsafe.assert_called_once()

    def test__sigint_handler_does_nothing_when_loop_is_not_running(self):
        """Test _sigint_handler does nothing when loop is not running.

        Given:
            An event loop that is not running
        When:
            _sigint_handler is called
        Then:
            It should not call call_soon_threadsafe
        """
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = False
        mock_service = MagicMock()

        _sigint_handler(mock_loop, mock_service, signal.SIGINT, None)

        mock_loop.is_running.assert_called_once()
        mock_loop.call_soon_threadsafe.assert_not_called()

    @pytest.mark.asyncio
    async def test__signal_handlers_installs_and_restores_handlers(self, mocker):
        """Test _signal_handlers installs handlers and restores on exit.

        Given:
            A WorkerService and existing signal handlers
        When:
            Entering and exiting the _signal_handlers context manager
        Then:
            It should install new handlers and restore old handlers on exit
        """
        mock_service = MagicMock()

        # Mock existing signal handlers
        old_sigterm = MagicMock()
        old_sigint = MagicMock()

        # Track signal.signal calls
        signal_calls = []
        original_signal = signal.signal

        def mock_signal(sig, handler):
            signal_calls.append((sig, handler))
            if sig == signal.SIGTERM:
                return old_sigterm
            elif sig == signal.SIGINT:
                return old_sigint
            return original_signal(sig, handler)

        mocker.patch("signal.signal", side_effect=mock_signal)

        # Use the context manager
        with _signal_handlers(mock_service):
            # Verify handlers were installed
            assert len(signal_calls) == 2
            assert signal_calls[0][0] == signal.SIGTERM
            assert signal_calls[1][0] == signal.SIGINT
            # Handlers should be partial functions
            assert callable(signal_calls[0][1])
            assert callable(signal_calls[1][1])

        # Verify handlers were restored
        assert len(signal_calls) == 4
        assert signal_calls[2] == (signal.SIGTERM, old_sigterm)
        assert signal_calls[3] == (signal.SIGINT, old_sigint)

    @pytest.mark.asyncio
    async def test__signal_handlers_restores_handlers_even_on_exception(self, mocker):
        """Test _signal_handlers restores handlers even if exception occurs.

        Given:
            A WorkerService and existing signal handlers
        When:
            An exception is raised within the _signal_handlers context
        Then:
            It should still restore old handlers before propagating exception
        """
        mock_service = MagicMock()

        # Mock existing signal handlers
        old_sigterm = MagicMock()
        old_sigint = MagicMock()

        # Track signal.signal calls
        signal_calls = []
        original_signal = signal.signal

        def mock_signal(sig, handler):
            signal_calls.append((sig, handler))
            if sig == signal.SIGTERM:
                return old_sigterm
            elif sig == signal.SIGINT:
                return old_sigint
            return original_signal(sig, handler)

        mocker.patch("signal.signal", side_effect=mock_signal)

        # Use the context manager with exception
        with pytest.raises(RuntimeError, match="Test error"):
            with _signal_handlers(mock_service):
                raise RuntimeError("Test error")

        # Verify handlers were still restored
        assert len(signal_calls) == 4
        assert signal_calls[2] == (signal.SIGTERM, old_sigterm)
        assert signal_calls[3] == (signal.SIGINT, old_sigint)


class TestWorkerProcess:
    """Test suite for WorkerProcess."""

    def test_init_with_default_parameters(self):
        """Test WorkerProcess initialization with default parameters.

        Given:
            No custom parameters
        When:
            WorkerProcess is instantiated
        Then:
            It should use default host 127.0.0.1, port None, and address None
        """
        process = WorkerProcess()
        assert process.host == "127.0.0.1"
        assert process.port is None  # port 0 means not started
        assert process.address is None

    def test_init_with_custom_host_and_port(self):
        """Test WorkerProcess initialization with custom host and port.

        Given:
            Custom host and port values
        When:
            WorkerProcess is instantiated
        Then:
            It should use those values for address
        """
        process = WorkerProcess(host="0.0.0.0", port=8080)
        assert process.host == "0.0.0.0"
        assert process.port == 8080
        assert process.address == "0.0.0.0:8080"

    @given(
        grace_period=st.floats(min_value=0.1, max_value=300.0),
        ttl=st.floats(min_value=0.1, max_value=300.0),
    )
    def test_init_with_custom_timeouts(self, grace_period, ttl):
        """Test WorkerProcess initialization with custom timeout parameters.

        Given:
            Custom shutdown grace period and proxy pool TTL
        When:
            WorkerProcess is instantiated
        Then:
            It should initialize without error
        """
        process = WorkerProcess(shutdown_grace_period=grace_period, proxy_pool_ttl=ttl)
        # Timeouts are internal config - just verify construction succeeds
        assert process.host == "127.0.0.1"
        assert process.address is None

    def test_init_raises_error_for_blank_host(self):
        """Test WorkerProcess initialization raises error for blank host.

        Given:
            An empty string for host
        When:
            WorkerProcess is instantiated
        Then:
            It should raise ValueError
        """
        with pytest.raises(ValueError, match="Host must be a non-blank string"):
            WorkerProcess(host="")

    def test_init_raises_error_for_negative_port(self):
        """Test WorkerProcess initialization raises error for negative port.

        Given:
            A negative port number
        When:
            WorkerProcess is instantiated
        Then:
            It should raise ValueError
        """
        with pytest.raises(ValueError, match="Port must be a positive integer"):
            WorkerProcess(port=-1)

    def test_init_raises_error_for_non_positive_grace_period(self):
        """Test WorkerProcess initialization raises error for non-positive grace period.

        Given:
            A non-positive shutdown grace period
        When:
            WorkerProcess is instantiated
        Then:
            It should raise ValueError
        """
        with pytest.raises(ValueError, match="Shutdown grace period must be positive"):
            WorkerProcess(shutdown_grace_period=0)

    def test_init_raises_error_for_non_positive_ttl(self):
        """Test WorkerProcess initialization raises error for non-positive TTL.

        Given:
            A non-positive proxy pool TTL
        When:
            WorkerProcess is instantiated
        Then:
            It should raise ValueError
        """
        with pytest.raises(ValueError, match="Proxy pool TTL must be positive"):
            WorkerProcess(proxy_pool_ttl=0)

    def test_address_returns_none_when_port_is_zero(self):
        """Test address property returns None when port is zero.

        Given:
            A WorkerProcess with port 0 (not started)
        When:
            The address property is accessed
        Then:
            It should return None
        """
        process = WorkerProcess(port=0)
        assert process.address is None

    def test_address_returns_formatted_string_when_port_is_set(self):
        """Test address property returns formatted string when port is set.

        Given:
            A WorkerProcess with a non-zero port
        When:
            The address property is accessed
        Then:
            It should return formatted "host:port" string
        """
        process = WorkerProcess(host="192.168.1.100", port=50051)
        assert process.address == "192.168.1.100:50051"

    def test_host_returns_configured_host(self):
        """Test host property returns the configured host.

        Given:
            A WorkerProcess with a specific host
        When:
            The host property is accessed
        Then:
            It should return the configured host
        """
        process = WorkerProcess(host="0.0.0.0")
        assert process.host == "0.0.0.0"

    def test_port_returns_none_when_port_is_zero(self):
        """Test port property returns None when port is zero.

        Given:
            A WorkerProcess with port 0
        When:
            The port property is accessed
        Then:
            It should return None
        """
        process = WorkerProcess(port=0)
        assert process.port is None

    def test_port_returns_configured_port(self):
        """Test port property returns configured port when non-zero.

        Given:
            A WorkerProcess with a non-zero port
        When:
            The port property is accessed
        Then:
            It should return the configured port
        """
        process = WorkerProcess(port=8080)
        assert process.port == 8080

    @given(port=st.integers(min_value=1, max_value=65535))
    def test_init_accepts_all_valid_ports(self, port):
        """Test WorkerProcess accepts all valid port numbers.

        Given:
            Any valid port number in range 1-65535
        When:
            WorkerProcess is instantiated with that port
        Then:
            It should initialize successfully and expose the port
        """
        process = WorkerProcess(port=port)
        assert process.port == port
        assert process.address and f":{port}" in process.address

    @given(
        host=st.one_of(
            st.ip_addresses(v=4).map(str),
            st.from_regex(
                r"^[a-z0-9]([a-z0-9\-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9\-]{0,61}[a-z0-9])?)*$",
                fullmatch=True,
            ),
        ),
        port=st.integers(min_value=1, max_value=65535),
    )
    def test_address_parsing_roundtrip(self, host, port):
        """Test address formatting and parsing is reversible.

        Given:
            Random valid host and port combinations
        When:
            Address is formatted via WorkerProcess
        Then:
            Parsing the address should recover original host and port
        """
        process = WorkerProcess(host=host, port=port)
        address = process.address
        assert address

        # Parse address back
        parsed_host, parsed_port_str = address.split(":")
        parsed_port = int(parsed_port_str)

        assert parsed_host == host
        assert parsed_port == port

    @given(
        host=st.one_of(
            st.ip_addresses(v=4).map(str),
            st.ip_addresses(v=6).map(str),
            st.from_regex(
                r"^[a-z0-9]([a-z0-9\-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9\-]{0,61}[a-z0-9])?)*$",
                fullmatch=True,
            ),
        )
    )
    def test_init_accepts_various_host_formats(self, host):
        """Test WorkerProcess accepts various valid host formats.

        Given:
            Valid IPv4, IPv6, or hostname formats
        When:
            WorkerProcess is instantiated with the host
        Then:
            It should initialize successfully
        """
        process = WorkerProcess(host=host, port=50051)
        assert process.host == host
        assert host in process.address

    @given(port=st.one_of(st.integers(max_value=-1), st.integers(min_value=65536)))
    def test_init_rejects_invalid_ports(self, port):
        """Test WorkerProcess rejects invalid port numbers.

        Given:
            Invalid port numbers (negative or > 65535)
        When:
            WorkerProcess is instantiated
        Then:
            It should raise ValueError
        """
        with pytest.raises(ValueError, match="Port must be a positive integer"):
            WorkerProcess(port=port)

    @given(
        grace_period=st.one_of(
            st.floats(max_value=0.0, allow_nan=False, allow_infinity=False),
            st.just(0),
        )
    )
    def test_init_rejects_non_positive_grace_periods(self, grace_period):
        """Test WorkerProcess rejects all non-positive grace periods.

        Given:
            Non-positive grace period values
        When:
            WorkerProcess is instantiated
        Then:
            It should raise ValueError
        """
        with pytest.raises(ValueError, match="Shutdown grace period must be positive"):
            WorkerProcess(shutdown_grace_period=grace_period)

    @given(
        ttl=st.one_of(
            st.floats(max_value=0.0, allow_nan=False, allow_infinity=False), st.just(0)
        )
    )
    def test_init_rejects_non_positive_ttls(self, ttl):
        """Test WorkerProcess rejects all non-positive TTL values.

        Given:
            Non-positive proxy pool TTL values
        When:
            WorkerProcess is instantiated
        Then:
            It should raise ValueError
        """
        with pytest.raises(ValueError, match="Proxy pool TTL must be positive"):
            WorkerProcess(proxy_pool_ttl=ttl)

    def test_start_raises_error_for_non_positive_timeout(self):
        """Test start raises ValueError for non-positive timeout.

        Given:
            A WorkerProcess instance
        When:
            start() is called with non-positive timeout
        Then:
            It should raise ValueError
        """
        process = WorkerProcess()
        with pytest.raises(ValueError, match="Timeout must be positive"):
            process.start(timeout=0)

    def test_start_calls_parent_start(self, mocker):
        """Test start method calls parent Process.start.

        Given:
            A WorkerProcess with mocked parent start and pipe
        When:
            start() is called
        Then:
            It should call multiprocessing.Process.start
        """
        # Mock pipe connections
        mock_get_port = MagicMock()
        mock_get_port.poll.return_value = True
        mock_get_port.recv.return_value = 50051
        mock_set_port = MagicMock()
        mocker.patch(
            "wool.runtime.worker.process.Pipe",
            return_value=(mock_get_port, mock_set_port),
        )

        # Mock parent start
        mock_parent_start = mocker.patch("multiprocessing.Process.start")

        process = WorkerProcess()
        process.start(timeout=60.0)

        mock_parent_start.assert_called_once()
        mock_get_port.poll.assert_called_once_with(timeout=60.0)
        mock_get_port.recv.assert_called_once()
        mock_get_port.close.assert_called_once()

    def test_start_receives_port_from_pipe(self, mocker):
        """Test start receives port from pipe and updates address.

        Given:
            A WorkerProcess with mocked pipe returning a port
        When:
            start() is called
        Then:
            It should receive the port and provide correct address
        """
        # Mock pipe connections
        mock_get_port = MagicMock()
        mock_get_port.poll.return_value = True
        mock_get_port.recv.return_value = 50051
        mock_set_port = MagicMock()
        mocker.patch(
            "wool.runtime.worker.process.Pipe",
            return_value=(mock_get_port, mock_set_port),
        )

        mocker.patch("multiprocessing.Process.start")

        process = WorkerProcess()
        process.start()

        assert process.port == 50051
        assert process.address == "127.0.0.1:50051"

    def test_start_raises_runtime_error_on_timeout(self, mocker):
        """Test start raises RuntimeError when process fails to start in time.

        Given:
            A WorkerProcess with mocked pipe that times out
        When:
            start() is called
        Then:
            It should raise RuntimeError and terminate the process
        """
        # Mock pipe connections - poll returns False to simulate timeout
        mock_get_port = MagicMock()
        mock_get_port.poll.return_value = False
        mock_set_port = MagicMock()
        mocker.patch(
            "wool.runtime.worker.process.Pipe",
            return_value=(mock_get_port, mock_set_port),
        )

        mocker.patch("multiprocessing.Process.start")

        process = WorkerProcess()
        mock_terminate = mocker.patch.object(process, "terminate")
        mock_join = mocker.patch.object(process, "join")

        with pytest.raises(
            RuntimeError, match="Worker process failed to start within .* seconds"
        ):
            process.start(timeout=1.0)

        mock_terminate.assert_called_once()
        mock_join.assert_called_once()

    def test_start_closes_pipe_after_receiving_port(self, mocker):
        """Test start closes the pipe connection after receiving port.

        Given:
            A WorkerProcess with mocked pipe
        When:
            start() successfully receives port
        Then:
            It should close the pipe connection
        """
        # Mock pipe connections
        mock_get_port = MagicMock()
        mock_get_port.poll.return_value = True
        mock_get_port.recv.return_value = 50051
        mock_set_port = MagicMock()
        mocker.patch(
            "wool.runtime.worker.process.Pipe",
            return_value=(mock_get_port, mock_set_port),
        )

        mocker.patch("multiprocessing.Process.start")

        process = WorkerProcess()
        process.start()

        mock_get_port.close.assert_called_once()

    @pytest.mark.asyncio
    async def test__proxy_factory_starts_proxy_when_not_started(self, mocker):
        """Test _proxy_factory starts proxy when not already started.

        Given:
            A WorkerProcess and a proxy with started=False
        When:
            _proxy_factory() is called
        Then:
            It should call proxy.start() and return the proxy
        """
        mock_proxy = mocker.MagicMock()
        mock_proxy.started = False
        mock_proxy.start = AsyncMock()

        result = await _proxy_factory(mock_proxy)

        mock_proxy.start.assert_called_once()
        assert result is mock_proxy

    @pytest.mark.asyncio
    async def test__proxy_factory_does_not_start_proxy_when_already_started(
        self, mocker
    ):
        """Test _proxy_factory does not start proxy when already started.

        Given:
            A WorkerProcess and a proxy with started=True
        When:
            _proxy_factory() is called
        Then:
            It should not call proxy.start() but still return the proxy
        """
        mock_proxy = mocker.MagicMock()
        mock_proxy.started = True
        mock_proxy.start = AsyncMock()

        result = await _proxy_factory(mock_proxy)

        mock_proxy.start.assert_not_called()
        assert result is mock_proxy

    @pytest.mark.asyncio
    async def test__proxy_finalizer_successfully_stops_proxy(self, mocker):
        """Test _proxy_finalizer successfully stops proxy.

        Given:
            A WorkerProcess and a proxy that stops successfully
        When:
            _proxy_finalizer() is called
        Then:
            It should call proxy.stop() and complete without error
        """
        mock_proxy = mocker.MagicMock()
        mock_proxy.stop = AsyncMock()

        await _proxy_finalizer(mock_proxy)

        mock_proxy.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test__proxy_finalizer_handles_exception_gracefully(self, mocker):
        """Test _proxy_finalizer handles exception from proxy.stop() gracefully.

        Given:
            A WorkerProcess and a proxy that raises exception on stop
        When:
            _proxy_finalizer() is called
        Then:
            It should catch the exception and complete without propagating it
        """
        mock_proxy = mocker.MagicMock()
        mock_proxy.stop = AsyncMock(side_effect=Exception("Stop failed"))

        # Should not raise exception
        await _proxy_finalizer(mock_proxy)

        mock_proxy.stop.assert_called_once()

    def test_run_sets_up_proxy_pool_and_starts_server(self, mocker):
        """Test run method sets up proxy pool and starts gRPC server.

        Given:
            A WorkerProcess with custom TTL
        When:
            run() is called
        Then:
            It should configure proxy pool and start the server
        """
        process = WorkerProcess(proxy_pool_ttl=120.0, shutdown_grace_period=30.0)

        # Mock the context variable and ResourcePool
        mock_proxy_pool = mocker.MagicMock()
        mocker.patch("wool.runtime.worker.process.wool.__proxy_pool__", mock_proxy_pool)

        mock_resource_pool = mocker.MagicMock()
        mock_resource_pool_class = mocker.patch(
            "wool.runtime.worker.process.ResourcePool", return_value=mock_resource_pool
        )

        # Mock gRPC server
        mock_server = MagicMock()
        mock_server.add_insecure_port = MagicMock(return_value=50051)
        mock_server.start = AsyncMock()
        mock_server.stop = AsyncMock()
        mocker.patch("grpc.aio.server", return_value=mock_server)

        # Mock WorkerService
        mock_service = MagicMock()
        mock_service.stopped.wait = AsyncMock()
        mock_service.configure_server = AsyncMock(return_value=50051)
        mocker.patch(
            "wool.runtime.worker.process.WorkerService", return_value=mock_service
        )

        # Mock signal handlers
        mocker.patch("wool.runtime.worker.process._signal_handlers")

        # Mock pipe
        mock_send = mocker.patch.object(process._set_port, "send")
        mock_close = mocker.patch.object(process._set_port, "close")

        # Let asyncio.run execute _serve
        process.run()

        # Verify ResourcePool was created with correct TTL
        mock_resource_pool_class.assert_called_once()
        call_kwargs = mock_resource_pool_class.call_args.kwargs
        assert call_kwargs["ttl"] == 120.0
        assert callable(call_kwargs["factory"])
        assert callable(call_kwargs["finalizer"])

        # Verify proxy pool context variable was set
        mock_proxy_pool.set.assert_called_once_with(mock_resource_pool)

        # Verify server lifecycle
        mock_server.start.assert_called_once()
        mock_send.assert_called_once_with(50051)
        mock_close.assert_called_once()
        mock_service.stopped.wait.assert_called_once()
        mock_server.stop.assert_called_once_with(grace=30.0)

    def test_run_sends_port_through_pipe(self, mocker):
        """Test run sends assigned port through multiprocessing pipe.

        Given:
            A WorkerProcess
        When:
            run() executes and server starts
        Then:
            It should send the port through the pipe
        """
        process = WorkerProcess(host="0.0.0.0", port=8080)

        # Mock infrastructure
        mocker.patch("wool.runtime.worker.process.wool.__proxy_pool__")
        mocker.patch("wool.runtime.worker.process.ResourcePool")

        # Mock gRPC server with specific port
        mock_server = MagicMock()
        mock_server.add_insecure_port = MagicMock(return_value=8080)
        mock_server.start = AsyncMock()
        mock_server.stop = AsyncMock()
        mocker.patch("grpc.aio.server", return_value=mock_server)

        # Mock WorkerService
        mock_service = MagicMock()
        mock_service.stopped.wait = AsyncMock()
        mock_service.configure_server = AsyncMock(return_value=8080)
        mocker.patch(
            "wool.runtime.worker.process.WorkerService", return_value=mock_service
        )

        # Mock signal handlers
        mocker.patch("wool.runtime.worker.process._signal_handlers")

        # Mock pipe
        mock_send = mocker.patch.object(process._set_port, "send")
        mocker.patch.object(process._set_port, "close")

        process.run()

        # Verify port was sent
        mock_send.assert_called_once_with(8080)

    def test_run_closes_pipe_even_on_error(self, mocker):
        """Test run closes pipe even if send fails.

        Given:
            A WorkerProcess where pipe.send raises error
        When:
            run() is called
        Then:
            It should still close the pipe and stop server
        """
        process = WorkerProcess()

        # Mock infrastructure
        mocker.patch("wool.runtime.worker.process.wool.__proxy_pool__")
        mocker.patch("wool.runtime.worker.process.ResourcePool")

        # Mock gRPC server
        mock_server = MagicMock()
        mock_server.add_insecure_port = MagicMock(return_value=50051)
        mock_server.start = AsyncMock()
        mock_server.stop = AsyncMock()
        mocker.patch("grpc.aio.server", return_value=mock_server)

        # Mock WorkerService
        mock_service = MagicMock()
        mock_service.stopped.wait = AsyncMock()
        mock_service.configure_server = AsyncMock(return_value=50051)
        mocker.patch(
            "wool.runtime.worker.process.WorkerService", return_value=mock_service
        )

        # Mock signal handlers
        mocker.patch("wool.runtime.worker.process._signal_handlers")

        # Mock pipe with send error
        mocker.patch.object(
            process._set_port, "send", side_effect=Exception("Pipe error")
        )
        mock_close = mocker.patch.object(process._set_port, "close")

        # Should raise the error
        with pytest.raises(Exception, match="Pipe error"):
            process.run()

        # But pipe should still be closed and server stopped
        mock_close.assert_called_once()
        mock_server.stop.assert_called_once()

    def test_run_stops_server_even_on_service_error(self, mocker):
        """Test run stops server even if service.wait raises error.

        Given:
            A WorkerProcess where service.stopped.wait raises error
        When:
            run() is called
        Then:
            It should still stop the server gracefully
        """
        process = WorkerProcess(shutdown_grace_period=45.0)

        # Mock infrastructure
        mocker.patch("wool.runtime.worker.process.wool.__proxy_pool__")
        mocker.patch("wool.runtime.worker.process.ResourcePool")

        # Mock gRPC server
        mock_server = MagicMock()
        mock_server.add_insecure_port = MagicMock(return_value=50051)
        mock_server.start = AsyncMock()
        mock_stop = AsyncMock()
        mock_server.stop = mock_stop
        mocker.patch("grpc.aio.server", return_value=mock_server)

        # Mock WorkerService with error
        mock_service = MagicMock()
        mock_service.stopped.wait = AsyncMock(side_effect=Exception("Service error"))
        mock_service.configure_server = AsyncMock(return_value=50051)
        mocker.patch(
            "wool.runtime.worker.process.WorkerService", return_value=mock_service
        )

        # Mock signal handlers
        mocker.patch("wool.runtime.worker.process._signal_handlers")

        # Mock pipe
        mocker.patch.object(process._set_port, "send")
        mocker.patch.object(process._set_port, "close")

        # Should raise the error
        with pytest.raises(Exception, match="Service error"):
            process.run()

        # But server.stop should still be called with correct grace period
        mock_stop.assert_called_once_with(grace=45.0)

    # === SINGLE-PORT ARCHITECTURE TESTS ===

    def test_serve_insecure_worker_single_port(self, mocker):
        """Test single insecure port for insecure workers.

        Given:
            WorkerProcess with server_credentials=None
        When:
            Process is started and server is configured
        Then:
            Only add_insecure_port is called, add_secure_port is not called
        """
        # Mock server
        mock_server = MagicMock()
        mock_server.add_insecure_port = MagicMock(return_value=50051)
        mock_server.add_secure_port = MagicMock()
        mock_server.start = AsyncMock()
        mock_server.stop = AsyncMock()
        mocker.patch("grpc.aio.server", return_value=mock_server)

        # Mock WorkerService
        mock_service = MagicMock()
        mock_service.configure_server = AsyncMock(return_value=50051)
        mock_service.stopped.wait = AsyncMock()
        mocker.patch(
            "wool.runtime.worker.process.WorkerService", return_value=mock_service
        )

        # Mock signal handlers
        mocker.patch("wool.runtime.worker.process._signal_handlers")

        # Create insecure worker
        process = WorkerProcess(host="127.0.0.1", port=0, server_credentials=None)

        # Mock pipe
        mocker.patch.object(process._set_port, "send")
        mocker.patch.object(process._set_port, "close")

        # Run process (will block, so we need to mock stopped.wait)
        process.run()

        # Verify only insecure port was added
        mock_server.add_insecure_port.assert_called_once()
        mock_server.add_secure_port.assert_not_called()

    def test_serve_secure_worker_single_port(self, mocker):
        """Test single secure port for secure workers.

        Given:
            WorkerProcess with valid ServerCredentials
        When:
            Process is started and server is configured
        Then:
            Only add_secure_port is called with credentials, add_insecure_port is not called
        """
        # Create server credentials
        dummy_key = (
            b"-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----"
        )
        dummy_cert = b"-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        server_creds = grpc.ssl_server_credentials([(dummy_key, dummy_cert)])

        # Mock server
        mock_server = MagicMock()
        mock_server.add_secure_port = MagicMock(return_value=50051)
        mock_server.start = AsyncMock()
        mock_server.stop = AsyncMock()
        mock_server.add_insecure_port = MagicMock()
        mock_server.start = AsyncMock()
        mock_server.stop = AsyncMock()
        mocker.patch("grpc.aio.server", return_value=mock_server)

        # Mock WorkerService
        mock_service = MagicMock()
        mock_service.configure_server = AsyncMock(return_value=50051)
        mock_service.stopped.wait = AsyncMock()
        mocker.patch(
            "wool.runtime.worker.process.WorkerService", return_value=mock_service
        )

        # Mock signal handlers
        mocker.patch("wool.runtime.worker.process._signal_handlers")

        # Create secure worker
        process = WorkerProcess(
            host="127.0.0.1", port=0, server_credentials=server_creds
        )

        # Mock pipe
        mocker.patch.object(process._set_port, "send")
        mocker.patch.object(process._set_port, "close")

        # Run process
        process.run()

        # Verify only secure port was added
        mock_server.add_secure_port.assert_called_once()
        mock_server.add_insecure_port.assert_not_called()

    def test_serve_callable_credentials_resolved(self, mocker):
        """Test callable credential resolution.

        Given:
            WorkerProcess with callable ServerCredentials
        When:
            Process is started and server is configured
        Then:
            Credentials are resolved and add_secure_port is called with resolved credentials
        """
        # Create callable credentials
        dummy_key = (
            b"-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----"
        )
        dummy_cert = b"-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"

        def server_creds_factory():
            return grpc.ssl_server_credentials([(dummy_key, dummy_cert)])

        # Mock server
        mock_server = MagicMock()
        mock_server.add_secure_port = MagicMock(return_value=50051)
        mock_server.start = AsyncMock()
        mock_server.stop = AsyncMock()
        mocker.patch("grpc.aio.server", return_value=mock_server)

        # Mock WorkerService
        mock_service = MagicMock()
        mock_service.configure_server = AsyncMock(return_value=50051)
        mock_service.stopped.wait = AsyncMock()
        mocker.patch(
            "wool.runtime.worker.process.WorkerService", return_value=mock_service
        )

        # Mock signal handlers
        mocker.patch("wool.runtime.worker.process._signal_handlers")

        # Create secure worker with callable
        process = WorkerProcess(
            host="127.0.0.1", port=0, server_credentials=server_creds_factory
        )

        # Mock pipe
        mocker.patch.object(process._set_port, "send")
        mocker.patch.object(process._set_port, "close")

        # Run process
        process.run()

        # Verify secure port was added (credentials were resolved)
        mock_server.add_secure_port.assert_called_once()

    def test_serve_secure_worker_random_port_assignment(self, mocker):
        """Test random port assignment for secure worker.

        Given:
            WorkerProcess with ServerCredentials and port=0
        When:
            Process is started
        Then:
            A single random port is assigned and returned
        """
        dummy_key = (
            b"-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----"
        )
        dummy_cert = b"-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        server_creds = grpc.ssl_server_credentials([(dummy_key, dummy_cert)])

        # Mock server to return random port
        mock_server = MagicMock()
        mock_server.add_secure_port = MagicMock(return_value=54321)
        mock_server.start = AsyncMock()
        mock_server.stop = AsyncMock()
        mocker.patch("grpc.aio.server", return_value=mock_server)

        # Mock WorkerService
        mock_service = MagicMock()
        mock_service.configure_server = AsyncMock(return_value=54321)
        mock_service.stopped.wait = AsyncMock()
        mocker.patch(
            "wool.runtime.worker.process.WorkerService", return_value=mock_service
        )

        # Mock signal handlers
        mocker.patch("wool.runtime.worker.process._signal_handlers")

        # Create secure worker with port=0
        process = WorkerProcess(
            host="127.0.0.1", port=0, server_credentials=server_creds
        )

        # Mock pipe - capture the port that was sent
        sent_ports = []
        mocker.patch.object(
            process._set_port, "send", side_effect=lambda x: sent_ports.append(x)
        )
        mocker.patch.object(process._set_port, "close")

        # Run process
        process.run()

        # Verify a port was assigned
        assert len(sent_ports) == 1
        assert sent_ports[0] == 54321

    def test_serve_insecure_worker_random_port_assignment(self, mocker):
        """Test random port assignment for insecure worker.

        Given:
            WorkerProcess with no credentials and port=0
        When:
            Process is started
        Then:
            A single random port is assigned and returned
        """
        # Mock server to return random port
        mock_server = MagicMock()
        mock_server.add_insecure_port = MagicMock(return_value=54322)
        mock_server.start = AsyncMock()
        mock_server.stop = AsyncMock()
        mocker.patch("grpc.aio.server", return_value=mock_server)

        # Mock WorkerService
        mock_service = MagicMock()
        mock_service.configure_server = AsyncMock(return_value=54322)
        mock_service.stopped.wait = AsyncMock()
        mocker.patch(
            "wool.runtime.worker.process.WorkerService", return_value=mock_service
        )

        # Mock signal handlers
        mocker.patch("wool.runtime.worker.process._signal_handlers")

        # Create insecure worker with port=0
        process = WorkerProcess(host="127.0.0.1", port=0, server_credentials=None)

        # Mock pipe - capture the port that was sent
        sent_ports = []
        mocker.patch.object(
            process._set_port, "send", side_effect=lambda x: sent_ports.append(x)
        )
        mocker.patch.object(process._set_port, "close")

        # Run process
        process.run()

        # Verify a port was assigned
        assert len(sent_ports) == 1
        assert sent_ports[0] == 54322

    def test_serve_no_dual_port_architecture(self, mocker):
        """Test no dual-port architecture.

        Given:
            WorkerProcess with ServerCredentials
        When:
            Process is started and port is retrieved
        Then:
            Port number matches the secure port, no additional localhost port exists
        """
        dummy_key = (
            b"-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----"
        )
        dummy_cert = b"-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        server_creds = grpc.ssl_server_credentials([(dummy_key, dummy_cert)])

        # Mock server
        mock_server = MagicMock()
        mock_server.add_secure_port = MagicMock(return_value=50051)
        mock_server.start = AsyncMock()
        mock_server.stop = AsyncMock()
        mock_server.add_insecure_port = MagicMock()
        mocker.patch("grpc.aio.server", return_value=mock_server)

        # Mock WorkerService
        mock_service = MagicMock()
        mock_service.configure_server = AsyncMock(return_value=50051)
        mock_service.stopped.wait = AsyncMock()
        mocker.patch(
            "wool.runtime.worker.process.WorkerService", return_value=mock_service
        )

        # Mock signal handlers
        mocker.patch("wool.runtime.worker.process._signal_handlers")

        # Create secure worker
        process = WorkerProcess(
            host="127.0.0.1", port=0, server_credentials=server_creds
        )

        # Mock pipe
        mocker.patch.object(process._set_port, "send")
        mocker.patch.object(process._set_port, "close")

        # Run process
        process.run()

        # Verify no insecure port was added (no dual-port architecture)
        mock_server.add_insecure_port.assert_not_called()
        # Verify only one port was added
        assert mock_server.add_secure_port.call_count == 1

    def test_serve_no_insecure_backdoor(self, mocker):
        """Test no insecure localhost backdoor.

        Given:
            Running WorkerProcess with ServerCredentials
        When:
            Attempt to connect via insecure channel to the port
        Then:
            Connection fails or is rejected (no insecure fallback port)
        """
        dummy_key = (
            b"-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----"
        )
        dummy_cert = b"-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        server_creds = grpc.ssl_server_credentials([(dummy_key, dummy_cert)])

        # Mock server
        mock_server = MagicMock()
        mock_server.add_secure_port = MagicMock(return_value=50051)
        mock_server.start = AsyncMock()
        mock_server.stop = AsyncMock()
        mock_server.add_insecure_port = MagicMock()
        mocker.patch("grpc.aio.server", return_value=mock_server)

        # Mock WorkerService
        mock_service = MagicMock()
        mock_service.configure_server = AsyncMock(return_value=50051)
        mock_service.stopped.wait = AsyncMock()
        mocker.patch(
            "wool.runtime.worker.process.WorkerService", return_value=mock_service
        )

        # Mock signal handlers
        mocker.patch("wool.runtime.worker.process._signal_handlers")

        # Create secure worker
        process = WorkerProcess(
            host="127.0.0.1", port=0, server_credentials=server_creds
        )

        # Mock pipe
        mocker.patch.object(process._set_port, "send")
        mocker.patch.object(process._set_port, "close")

        # Run process
        process.run()

        # Verify no insecure port exists - no backdoor
        # The key assertion: add_insecure_port was never called
        mock_server.add_insecure_port.assert_not_called()

    @given(has_credentials=st.booleans())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_single_port_invariant(self, has_credentials, mocker):
        """Test single-port invariant property.

        Given:
            Any WorkerProcess with valid or None credentials
        When:
            Process is started and configured
        Then:
            Exactly one port is bound (either secure or insecure, never both)
        """
        # Create credentials if needed
        if has_credentials:
            dummy_key = (
                b"-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----"
            )
            dummy_cert = b"-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
            server_creds = grpc.ssl_server_credentials([(dummy_key, dummy_cert)])
        else:
            server_creds = None

        # Mock server
        mock_server = MagicMock()
        mock_server.add_secure_port = MagicMock(return_value=50051)
        mock_server.start = AsyncMock()
        mock_server.stop = AsyncMock()
        mock_server.add_insecure_port = MagicMock(return_value=50051)
        mocker.patch("grpc.aio.server", return_value=mock_server)

        # Mock WorkerService
        mock_service = MagicMock()
        mock_service.configure_server = AsyncMock(return_value=50051)
        mock_service.stopped.wait = AsyncMock()
        mocker.patch(
            "wool.runtime.worker.process.WorkerService", return_value=mock_service
        )

        # Mock signal handlers
        mocker.patch("wool.runtime.worker.process._signal_handlers")

        # Create worker
        process = WorkerProcess(
            host="127.0.0.1", port=0, server_credentials=server_creds
        )

        # Mock pipe
        mocker.patch.object(process._set_port, "send")
        mocker.patch.object(process._set_port, "close")

        # Run process
        process.run()

        # Verify exactly one port binding method was called
        secure_calls = mock_server.add_secure_port.call_count
        insecure_calls = mock_server.add_insecure_port.call_count
        assert secure_calls + insecure_calls == 1, "Exactly one port must be bound"

        # Verify correct method was called based on credentials
        if has_credentials:
            assert secure_calls == 1, "Secure worker must use secure port"
            assert insecure_calls == 0, "Secure worker must not have insecure port"
        else:
            assert insecure_calls == 1, "Insecure worker must use insecure port"
            assert secure_calls == 0, "Insecure worker must not have secure port"
