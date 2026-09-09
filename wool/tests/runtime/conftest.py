import asyncio
import threading

import pytest


async def _drain():
    """Wind the running loop down the way `asyncio.Runner.close` does.

    Cancels every other task and awaits it, then shuts down the loop's
    asynchronous generators and default executor, so the loop can close
    without destroying pending work.
    """
    current = asyncio.current_task()
    pending = [task for task in asyncio.all_tasks() if task is not current]
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    loop = asyncio.get_running_loop()
    await loop.shutdown_asyncgens()
    await loop.shutdown_default_executor()


@pytest.fixture
def background_loops():
    """Run event loops on daemon threads for cross-loop tests.

    Yields a ``spawn(*, teardown=None)`` returning a handle onto a
    freshly started loop: ``handle.loop`` is the loop itself,
    ``handle.submit(coro)`` schedules a coroutine on it and returns the
    concurrent future, ``handle.run(coro)`` submits and waits for the
    result, and ``handle.close()`` drains, stops, joins, and closes it. A
    ``teardown`` coroutine function is run on the loop before the drain,
    so a suite can clear what the loop cached rather than strand it for
    the next test to report; the drain, the stop, and the join run
    whether or not the teardown raised, and a loop whose thread does not
    stop within the timeout is reported rather than closed under a
    running thread. Every spawned loop is closed at teardown, each one
    even if an earlier one failed to, so a test only calls ``close``
    when it needs the loop to stop mid-test.
    """

    class BackgroundLoop:
        def __init__(self, teardown):
            self._teardown = teardown
            self.loop = asyncio.new_event_loop()
            self._thread = threading.Thread(target=self.loop.run_forever, daemon=True)
            self._thread.start()

        def submit(self, coro):
            return asyncio.run_coroutine_threadsafe(coro, self.loop)

        def run(self, coro, timeout=5):
            return self.submit(coro).result(timeout=timeout)

        def close(self, timeout=5):
            if self.loop.is_closed():
                return
            try:
                if self._teardown is not None:
                    self.run(self._teardown(), timeout=timeout)
            finally:
                try:
                    self.run(_drain(), timeout=timeout)
                finally:
                    self.loop.call_soon_threadsafe(self.loop.stop)
                    self._thread.join(timeout=timeout)
                    if self._thread.is_alive():
                        raise RuntimeError(
                            f"{self.loop!r} did not stop within {timeout}s"
                        )
                    self.loop.close()

    handles = []

    def spawn(*, teardown=None):
        handle = BackgroundLoop(teardown)
        handles.append(handle)
        return handle

    yield spawn

    failures = []
    for handle in handles:
        try:
            handle.close()
        except Exception as error:
            failures.append(error)
    if failures:
        raise failures[0]


@pytest.fixture
def stranded_loop():
    """Leave pool entries behind on a loop that has stopped running.

    Yields a ``strand(coro, *, close=True)`` that drives the coroutine
    to completion on a fresh event loop and returns
    ``(loop, result)``. The loop is closed on the way out by default,
    or merely stopped when ``close=False``, so a test can distinguish
    a closed loop from one that stopped without closing, or resume it
    to let a stale timer fire. Every loop is closed at teardown.
    """
    loops = []

    def strand(coro, *, close=True):
        loop = asyncio.new_event_loop()
        loops.append(loop)
        try:
            return loop, loop.run_until_complete(coro)
        finally:
            if close:
                loop.close()

    yield strand

    for loop in loops:
        loop.close()
