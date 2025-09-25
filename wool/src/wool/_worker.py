from __future__ import annotations

import asyncio
import os
import signal
import uuid
from abc import ABC
from abc import abstractmethod
from contextlib import asynccontextmanager
from contextlib import contextmanager
from multiprocessing import Pipe
from multiprocessing import Process
from multiprocessing.connection import Connection
from typing import TYPE_CHECKING
from typing import Any
from typing import AsyncIterator
from typing import Final
from typing import Generic
from typing import Protocol
from typing import TypeAlias
from typing import TypeVar
from typing import final

import cloudpickle
import grpc.aio
from grpc import StatusCode
from grpc.aio import ServicerContext

import wool
from wool import _protobuf as pb
from wool._resource_pool import ResourcePool
from wool._work import WoolTask
from wool._work import WoolTaskEvent
from wool._worker_discovery import RegistryServiceLike
from wool._worker_discovery import WorkerInfo

if TYPE_CHECKING:
    from wool._worker_proxy import WorkerProxy

_ip_address: str | None = None
_EXTERNAL_DNS_SERVER: Final[str] = "8.8.8.8"  # Google DNS for IP detection


@contextmanager
def _signal_handlers(service: "WorkerService"):
    """Context manager for setting up signal handlers for graceful shutdown.

    Installs SIGTERM and SIGINT handlers that gracefully shut down the worker
    service when the process receives termination signals.

    :param service:
        The :py:class:`WorkerService` instance to shut down on signal receipt.
    :yields:
        Control to the calling context with signal handlers installed.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    def sigterm_handler(signum, frame):
        if loop.is_running():
            loop.call_soon_threadsafe(
                lambda: asyncio.create_task(service._stop(timeout=0))
            )

    def sigint_handler(signum, frame):
        if loop.is_running():
            loop.call_soon_threadsafe(
                lambda: asyncio.create_task(service._stop(timeout=None))
            )

    old_sigterm = signal.signal(signal.SIGTERM, sigterm_handler)
    old_sigint = signal.signal(signal.SIGINT, sigint_handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, old_sigterm)
        signal.signal(signal.SIGINT, old_sigint)


_T_RegistryService = TypeVar(
    "_T_RegistryService", bound=RegistryServiceLike, covariant=True
)


# public
class Worker(ABC, Generic[_T_RegistryService]):
    """Abstract base class for worker implementations in the wool framework.

    Workers are individual processes that execute distributed tasks within
    a worker pool. Each worker runs a gRPC server and registers itself with
    a discovery service to be found by client sessions.

    This class defines the core interface that all worker implementations
    must provide, including lifecycle management and registry service
    integration for peer-to-peer discovery.

    :param tags:
        Capability tags associated with this worker for filtering and
        selection by client sessions.
    :param registry_service:
        Service instance for worker registration and discovery within
        the distributed pool.
    :param extra:
        Additional arbitrary metadata as key-value pairs.
    """

    _info: WorkerInfo | None = None
    _started: bool = False
    _registry_service: RegistryServiceLike
    _uid: Final[str]
    _tags: Final[set[str]]
    _extra: Final[dict[str, Any]]

    def __init__(
        self,
        *tags: str,
        registry_service: _T_RegistryService,
        **extra: Any,
    ):
        self._uid = f"worker-{uuid.uuid4().hex}"
        self._tags = set(tags)
        self._extra = extra
        self._registry_service = registry_service

    @property
    def uid(self) -> str:
        """The worker's unique identifier."""
        return self._uid

    @property
    def info(self) -> WorkerInfo | None:
        """Worker information including network address and metadata.

        :returns:
            The worker's complete information or None if not started.
        """
        return self._info

    @property
    def tags(self) -> set[str]:
        """Capability tags for this worker."""
        return self._tags

    @property
    def extra(self) -> dict[str, Any]:
        """Additional arbitrary metadata for this worker."""
        return self._extra

    @property
    @abstractmethod
    def address(self) -> str | None: ...

    @property
    @abstractmethod
    def host(self) -> str | None: ...

    @property
    @abstractmethod
    def port(self) -> int | None: ...

    @final
    async def start(self):
        """Start the worker and register it with the pool.

        This method is a final implementation that calls the abstract
        `_start` method to initialize the worker process and register
        it with the registry service.
        """
        if self._started:
            raise RuntimeError("Worker has already been started")
        if self._registry_service:
            await self._registry_service.start()
        await self._start()
        self._started = True

    @final
    async def stop(self):
        """Stop the worker and unregister it from the pool.

        This method is a final implementation that calls the abstract
        `_stop` method to gracefully shut down the worker process and
        unregister it from the registry service.
        """
        if not self._started:
            raise RuntimeError("Worker has not been started")
        await self._stop()
        if self._registry_service:
            await self._registry_service.stop()

    @abstractmethod
    async def _start(self):
        """Implementation-specific worker startup logic.

        Subclasses must implement this method to handle the actual
        startup of their worker process and gRPC server.
        """
        ...

    @abstractmethod
    async def _stop(self):
        """Implementation-specific worker shutdown logic.

        Subclasses must implement this method to handle the graceful
        shutdown of their worker process and cleanup of resources.
        """
        ...


# public
class WorkerFactory(Generic[_T_RegistryService], Protocol):
    """Protocol for creating worker instances with registry integration.

    Defines the callable interface for worker factory implementations
    that can create :py:class:`Worker` instances configured with specific
    capability tags and metadata.

    Worker factories are used by :py:class:`WorkerPool` to spawn multiple
    worker processes with consistent configuration.
    """

    def __call__(self, *tags: str, **_) -> Worker[_T_RegistryService]:
        """Create a new worker instance.

        :param tags:
            Additional tags to associate with this worker for discovery
            and filtering purposes.
        :returns:
            A new :py:class:`Worker` instance configured with the
            specified tags and metadata.
        """
        ...


# public
class LocalWorker(Worker[_T_RegistryService]):
    """Local worker implementation that runs tasks in a separate process.

    :py:class:`LocalWorker` creates and manages a dedicated worker process
    that hosts a gRPC server for executing distributed wool tasks. Each
    worker automatically registers itself with the provided registry service
    for discovery by client sessions.

    The worker process runs independently and can handle multiple concurrent
    tasks within its own asyncio event loop, providing process-level
    isolation for task execution.

    :param tags:
        Capability tags to associate with this worker for filtering
        and selection by client sessions.
    :param registry_service:
        Service instance for worker registration and discovery.
    :param extra:
        Additional arbitrary metadata as key-value pairs.
    """

    _worker_process: WorkerProcess

    def __init__(
        self,
        *tags: str,
        host: str = "127.0.0.1",
        port: int = 0,
        registry_service: _T_RegistryService,
        **extra: Any,
    ):
        super().__init__(*tags, registry_service=registry_service, **extra)
        self._worker_process = WorkerProcess(host=host, port=port)

    @property
    def address(self) -> str | None:
        """The network address where the worker is listening.

        :returns:
            The address in "host:port" format, or None if not started.
        """
        return self._worker_process.address

    @property
    def host(self) -> str | None:
        """The host where the worker is listening.

        :returns:
            The host address, or None if not started.
        """
        return self._info.host if self._info else None

    @property
    def port(self) -> int | None:
        """The port where the worker is listening.

        :returns:
            The port number, or None if not started.
        """
        return self._info.port if self._info else None

    async def _start(self):
        """Start the worker process and register it with the pool.

        Initializes the registry service, starts the worker process
        with its gRPC server, and registers the worker's network
        address with the registry for discovery by client sessions.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._worker_process.start)
        if not self._worker_process.address:
            raise RuntimeError("Worker process failed to start - no address")
        if not self._worker_process.pid:
            raise RuntimeError("Worker process failed to start - no PID")

        # Parse host and port from address
        host, port_str = self._worker_process.address.split(":")
        port = int(port_str)

        # Create the WorkerInfo with the actual host, port, and pid
        self._info = WorkerInfo(
            uid=self._uid,
            host=host,
            port=port,
            pid=self._worker_process.pid,
            version=wool.__version__,
            tags=self._tags,
            extra=self._extra,
        )
        await self._registry_service.register(self._info)

    async def _stop(self):
        """Stop the worker process and unregister it from the pool.

        Unregisters the worker from the registry service, gracefully
        shuts down the worker process using SIGINT, and cleans up
        the registry service. If graceful shutdown fails, the process
        is forcefully terminated.
        """
        if not self._info:
            raise RuntimeError("Cannot unregister - worker has no info")

        await self._registry_service.unregister(self._info)

        if not self._worker_process.is_alive():
            return
        try:
            if self._worker_process.pid:
                os.kill(self._worker_process.pid, signal.SIGINT)
                self._worker_process.join()
        except OSError:
            if self._worker_process.is_alive():
                self._worker_process.kill()


class WorkerProcess(Process):
    """A :py:class:`multiprocessing.Process` that runs a gRPC worker
    server.

    :py:class:`WorkerProcess` creates an isolated Python process that hosts a
    gRPC server for executing distributed tasks. Each process maintains
    its own event loop and serves as an independent worker node in the
    wool distributed runtime.

    :param port:
        Optional port number where the gRPC server will listen.
        If None, a random available port will be selected.
    :param args:
        Additional positional arguments passed to the parent
        :py:class:`multiprocessing.Process` class.
    :param kwargs:
        Additional keyword arguments passed to the parent
        :py:class:`multiprocessing.Process` class.

    .. attribute:: address
        The network address where the gRPC server is listening.
    """

    _port: int | None
    _get_port: Connection
    _set_port: Connection

    def __init__(self, *args, host: str = "127.0.0.1", port: int = 0, **kwargs):
        super().__init__(*args, **kwargs)
        if not host:
            raise ValueError("Host must be a non-blank string")
        self._host = host
        if port < 0:
            raise ValueError("Port must be a positive integer")
        self._port = port
        self._get_port, self._set_port = Pipe(duplex=False)

    @property
    def address(self) -> str | None:
        """The network address where the gRPC server is listening.

        :returns:
            The address in "host:port" format, or None if not started.
        """
        return self._address(self._host, self._port) if self._port else None

    @property
    def host(self) -> str | None:
        """The host where the gRPC server is listening.

        :returns:
            The host address, or None if not started.
        """
        return self._host

    @property
    def port(self) -> int | None:
        """The port where the gRPC server is listening.

        :returns:
            The port number, or None if not started.
        """
        return self._port or None

    def start(self):
        """Start the worker process.

        Launches the worker process and waits until it has started
        listening on a port. After starting, the :attr:`address`
        property will contain the actual network address.

        :raises RuntimeError:
            If the worker process fails to start within 10 seconds.
        :raises ValueError:
            If the port is negative.
        """
        super().start()
        # Add timeout to prevent hanging if child process fails to start
        if self._get_port.poll(timeout=10):  # 10 second timeout
            self._port = self._get_port.recv()
        else:
            self.terminate()
            self.join()
            raise RuntimeError("Worker process failed to start within 10 seconds")
        self._get_port.close()

    def run(self) -> None:
        """Run the worker process.

        Sets the event loop for this process and starts the gRPC server,
        blocking until the server is stopped.
        """

        async def proxy_factory(proxy: WorkerProxy):
            """Factory function for WorkerProxy instances in ResourcePool.

            Starts the proxy if not already started and returns it.
            The proxy object itself is used as the cache key.

            :param proxy:
                The WorkerProxy instance to start (passed as key from ResourcePool).
            :returns:
                The started WorkerProxy instance.
            """
            if not proxy.started:
                await proxy.start()
            return proxy

        async def proxy_finalizer(proxy: WorkerProxy):
            """Finalizer function for WorkerProxy instances in ResourcePool.

            Stops the proxy when it's being cleaned up from the resource pool.
            Based on the cleanup logic from WorkerProxyCache._delayed_cleanup.

            :param proxy:
                The WorkerProxy instance to clean up.
            """
            try:
                await proxy.stop()
            except Exception:
                pass

        wool.__proxy_pool__.set(
            ResourcePool(factory=proxy_factory, finalizer=proxy_finalizer, ttl=60)
        )
        asyncio.run(self._serve())

    async def _serve(self):
        """Start the gRPC server in this worker process.

        This method is called by the event loop to start serving
        requests. It creates a gRPC server, adds the worker service, and
        starts listening for incoming connections.
        """
        server = grpc.aio.server()
        port = server.add_insecure_port(self._address(self._host, self._port))
        service = WorkerService()
        pb.add_to_server[pb.worker.WorkerServicer](service, server)

        with _signal_handlers(service):
            try:
                await server.start()
                try:
                    self._set_port.send(port)
                finally:
                    self._set_port.close()
                await service.stopped.wait()
            finally:
                await server.stop(grace=60)

    def _address(self, host, port) -> str:
        """Format network address for the given port.

        :param port:
            Port number to include in the address.
        :returns:
            Address string in "host:port" format.
        """
        return f"{host}:{port}"


class WorkerService(pb.worker.WorkerServicer):
    """gRPC service implementation for executing distributed wool tasks.

    :py:class:`WorkerService` implements the gRPC WorkerServicer
    interface, providing remote procedure calls for task scheduling
    and worker lifecycle management. Tasks are executed in the same
    asyncio event loop as the gRPC server.

    .. note::
        Tasks are executed asynchronously in the current event loop
        and results are serialized for transport back to the client.
        The service maintains a set of running tasks for proper
        lifecycle management during shutdown.

        During shutdown, the service stops accepting new requests
        immediately when the :meth:`stop` RPC is called, returning
        UNAVAILABLE errors to new :meth:`dispatch` requests while
        allowing existing tasks to complete gracefully.

        The service provides :attr:`stopping` and
        :attr:`stopped` properties to access the internal shutdown
        state events.
    """

    _tasks: set[asyncio.Task]
    _stopped: asyncio.Event
    _stopping: asyncio.Event
    _task_completed: asyncio.Event

    def __init__(self):
        self._stopped = asyncio.Event()
        self._stopping = asyncio.Event()
        self._task_completed = asyncio.Event()
        self._tasks = set()

    @property
    def stopping(self) -> asyncio.Event:
        """Event signaling that the service is stopping.

        :returns:
            An :py:class:`asyncio.Event` that is set when the service
            begins shutdown.
        """
        return self._stopping

    @property
    def stopped(self) -> asyncio.Event:
        """Event signaling that the service has stopped.

        :returns:
            An :py:class:`asyncio.Event` that is set when the service
            has completed shutdown.
        """
        return self._stopped

    @contextmanager
    def _running(self, wool_task: WoolTask):
        """Context manager for tracking running tasks.

        Manages the lifecycle of a task execution, adding it to the
        active tasks set and emitting appropriate events. Ensures
        proper cleanup when the task completes or fails.

        :param wool_task:
            The :py:class:`WoolTask` instance to execute and track.
        :yields:
            The :py:class:`asyncio.Task` created for the wool task.

        .. note::
            Emits a :py:class:`WoolTaskEvent` with type "task-scheduled"
            when the task begins execution.
        """
        WoolTaskEvent("task-scheduled", task=wool_task).emit()
        task = asyncio.create_task(wool_task.run())
        self._tasks.add(task)
        try:
            yield task
        finally:
            self._tasks.remove(task)

    async def dispatch(
        self, request: pb.task.Task, context: ServicerContext
    ) -> AsyncIterator[pb.worker.Response]:
        """Execute a task in the current event loop.

        Deserializes the incoming task into a :py:class:`WoolTask`
        instance, schedules it for execution in the current asyncio
        event loop, and yields responses for acknowledgment and result.

        :param request:
            The protobuf task message containing the serialized task
            data.
        :param context:
            The :py:class:`grpc.aio.ServicerContext` for this request.
        :yields:
            First yields an Ack Response when task processing begins,
            then yields a Response containing the task result.

        .. note::
            Emits a :py:class:`WoolTaskEvent` when the task is
            scheduled for execution.
        """
        if self._stopping.is_set():
            await context.abort(
                StatusCode.UNAVAILABLE, "Worker service is shutting down"
            )

        with self._running(WoolTask.from_protobuf(request)) as task:
            # Yield acknowledgment that task was received and processing is starting
            yield pb.worker.Response(ack=pb.worker.Ack())

            try:
                result = pb.task.Result(dump=cloudpickle.dumps(await task))
                yield pb.worker.Response(result=result)
            except Exception as e:
                exception = pb.task.Exception(dump=cloudpickle.dumps(e))
                yield pb.worker.Response(exception=exception)

    async def stop(
        self, request: pb.worker.StopRequest, context: ServicerContext
    ) -> pb.worker.Void:
        """Stop the worker service and its thread.

        Gracefully shuts down the worker thread and signals the server
        to stop accepting new requests. This method is idempotent and
        can be called multiple times safely.

        :param request:
            The protobuf stop request containing the wait timeout.
        :param context:
            The :py:class:`grpc.aio.ServicerContext` for this request.
        :returns:
            An empty protobuf response indicating completion.
        """
        if self._stopping.is_set():
            return pb.worker.Void()
        await self._stop(timeout=request.wait)
        return pb.worker.Void()

    async def _stop(self, *, timeout: float | None = 0) -> None:
        self._stopping.set()
        await self._await_or_cancel_tasks(timeout=timeout)

        # Clean up the session cache to prevent issues during shutdown
        try:
            proxy_pool = wool.__proxy_pool__.get()
            assert proxy_pool
            await proxy_pool.clear()
        finally:
            self._stopped.set()

    async def _await_or_cancel_tasks(self, *, timeout: float | None = 0) -> None:
        """Stop the worker service gracefully.

        Gracefully shuts down the worker service by canceling or waiting
        for running tasks. This method is idempotent and can be called
        multiple times safely.

        :param timeout:
            Maximum time to wait for tasks to complete. If 0 (default),
            tasks are canceled immediately. If None, waits indefinitely.
            If a positive number, waits for that many seconds before
            canceling tasks.

        .. note::
            If a timeout occurs while waiting for tasks to complete,
            the method recursively calls itself with a timeout of 0
            to cancel all remaining tasks immediately.
        """
        if self._tasks and timeout == 0:
            await self._cancel(*self._tasks)
        elif self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                return await self._await_or_cancel_tasks(timeout=0)

    async def _cancel(self, *tasks: asyncio.Task):
        """Cancel multiple tasks safely.

        Cancels the provided tasks while performing safety checks to
        avoid canceling the current task or already completed tasks.
        Waits for all cancelled tasks to complete in parallel and handles
        cancellation exceptions.

        :param tasks:
            The :py:class:`asyncio.Task` instances to cancel.

        .. note::
            This method performs the following safety checks:
            - Avoids canceling the current task (would cause deadlock)
            - Only cancels tasks that are not already done
            - Properly handles :py:exc:`asyncio.CancelledError`
              exceptions.
        """
        current = asyncio.current_task()
        to_cancel = [task for task in tasks if not task.done() and task != current]

        # Cancel all tasks first
        for task in to_cancel:
            task.cancel()

        # Wait for all cancelled tasks in parallel
        if to_cancel:
            await asyncio.gather(*to_cancel, return_exceptions=True)


DispatchCall: TypeAlias = grpc.aio.UnaryStreamCall[pb.task.Task, pb.worker.Response]


@asynccontextmanager
async def with_timeout(context, timeout):
    """Async context manager wrapper that adds timeout to context entry.

    :param context:
        The async context manager to wrap.
    :param timeout:
        Timeout in seconds for context entry.
    :yields:
        Control to the calling context.
    :raises asyncio.TimeoutError:
        If context entry exceeds the timeout.
    """
    await asyncio.wait_for(context.__aenter__(), timeout=timeout)
    exception_type = exception_value = exception_traceback = None
    try:
        yield
    except BaseException as exception:
        exception_type = type(exception)
        exception_value = exception
        exception_traceback = exception.__traceback__
        raise
    finally:
        await context.__aexit__(exception_type, exception_value, exception_traceback)


T = TypeVar("T")


class DispatchStream(Generic[T]):
    """Async iterator wrapper for streaming dispatch results.

    Simplified wrapper that focuses solely on stream iteration and response handling.
    Channel management is now handled by the WorkerClient.
    """

    def __init__(self, stream: DispatchCall):
        """Initialize the streaming dispatch result wrapper.

        :param stream:
            The underlying gRPC response stream.
        """
        self._stream = stream
        self._iter = aiter(stream)

    def __aiter__(self) -> AsyncIterator[T]:
        """Return self as the async iterator."""
        return self

    async def __anext__(self) -> T:
        """Get the next response from the stream.

        :returns:
            The next task result from the worker.
        :raises StopAsyncIteration:
            When the stream is exhausted.
        """
        try:
            response = await anext(self._iter)
            if response.HasField("result"):
                return cloudpickle.loads(response.result.dump)
            elif response.HasField("exception"):
                raise cloudpickle.loads(response.exception.dump)
            else:
                raise RuntimeError(f"Received unexpected response: {response}")
        except Exception as exception:
            await self._handle_exception(exception)

    async def _handle_exception(self, exception):
        try:
            self._stream.cancel()
        except Exception as cancel_exception:
            raise cancel_exception from exception
        else:
            raise exception


class WorkerClient:
    """Client for dispatching tasks to a specific worker.

    Simplified client that maintains a persistent gRPC channel to a single
    worker. The client manages the channel lifecycle and provides task
    dispatch functionality with proper error handling.

    :param address:
        The network address of the target worker in "host:port" format.
    """

    def __init__(self, address: str):
        self._channel = grpc.aio.insecure_channel(
            address,
            # options=[
            #     ("grpc.keepalive_time_ms", 10000),
            #     ("grpc.keepalive_timeout_ms", 5000),
            #     ("grpc.http2.max_pings_without_data", 0),
            #     ("grpc.http2.min_time_between_pings_ms", 10000),
            #     ("grpc.max_receive_message_length", 100 * 1024 * 1024),
            #     ("grpc.max_send_message_length", 100 * 1024 * 1024),
            # ],
        )
        self._stub = pb.worker.WorkerStub(self._channel)
        self._semaphore = asyncio.Semaphore(100)

    async def dispatch(self, task: WoolTask) -> AsyncIterator[pb.task.Result]:
        """Dispatch task to worker with on-demand channel acquisition.

        Acquires a channel from the global channel pool, creates a WorkerStub,
        dispatches the task, and verifies the first response is an Ack.
        The channel is automatically managed by the underlying infrastructure.

        :param task:
            The WoolTask to dispatch to the worker.
        :returns:
            A DispatchStream for reading task results.
        :raises RuntimeError:
            If the worker doesn't acknowledge the task.
        """
        async with with_timeout(self._semaphore, timeout=60):
            call: DispatchCall = self._stub.dispatch(task.to_protobuf())

            try:
                first_response = await asyncio.wait_for(anext(aiter(call)), timeout=60)
                if not first_response.HasField("ack"):
                    raise UnexpectedResponse("Expected Ack response")
            except (
                asyncio.CancelledError,
                asyncio.TimeoutError,
                grpc.aio.AioRpcError,
                UnexpectedResponse,
            ):
                try:
                    call.cancel()
                except Exception:
                    pass
                raise

            async for result in DispatchStream(call):
                yield result

    async def stop(self):
        """Stop the client and close the gRPC channel.

        Gracefully closes the underlying gRPC channel and cleans up
        any resources associated with this client.
        """
        await self._channel.close()


class UnexpectedResponse(Exception): ...
