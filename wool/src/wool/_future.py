from __future__ import annotations

import asyncio
import concurrent.futures
import logging
from typing import Any
from typing import Generator
from typing import Generic
from typing import TypeVar
from typing import cast

from wool._utils import Undefined
from wool._utils import UndefinedType

T = TypeVar("T")


# PUBLIC
class WoolFuture(Generic[T]):
    """
    A future object representing the result of an asynchronous operation.

    WoolFuture provides methods to retrieve the result or exception of an
    asynchronous operation, set the result or exception, and await its
    completion.

    :param T: The type of the result.
    """

    def __init__(self) -> None:
        """
        Initialize a WoolFuture instance.
        """
        self._result: T | UndefinedType = Undefined
        self._exception: (
            BaseException | type[BaseException] | UndefinedType
        ) = Undefined
        self._done: bool = False
        self._cancelled: bool = False

    def __await__(self) -> Generator[Any, None, T]:
        """
        Await the completion of the future.

        :return: The result of the future.
        """

        async def _():
            while not self.done():
                await asyncio.sleep(0)
            else:
                return self.result()

        return _().__await__()

    def result(self) -> T:
        """
        Retrieve the result of the future.

        :return: The result of the future.
        :raises BaseException: If the future completed with an exception.
        :raises asyncio.InvalidStateError: If the future is not yet completed.
        """
        if self._exception is not Undefined:
            assert (
                isinstance(self._exception, BaseException)
                or isinstance(self._exception, type)
                and issubclass(self._exception, BaseException)
            )
            raise self._exception
        elif self._result is not Undefined:
            return cast(T, self._result)
        else:
            raise asyncio.InvalidStateError

    def set_result(self, result: T) -> None:
        """
        Set the result of the future.

        :param result: The result to set.
        :raises asyncio.InvalidStateError: If the future is already completed.
        """
        if self.done():
            raise asyncio.InvalidStateError
        else:
            self._result = result
            self._done = True

    def exception(self) -> BaseException | type[BaseException]:
        """
        Retrieve the exception of the future, if any.

        :return: The exception of the future.
        :raises asyncio.InvalidStateError: If the future is not yet completed
            or has no exception.
        """
        if self.done() and self._exception is not Undefined:
            return cast(BaseException | type[BaseException], self._exception)
        else:
            raise asyncio.InvalidStateError

    def set_exception(
        self, exception: BaseException | type[BaseException]
    ) -> None:
        """
        Set the exception of the future.

        :param exception: The exception to set.
        :raises asyncio.InvalidStateError: If the future is already completed.
        """
        if self.done():
            raise asyncio.InvalidStateError
        else:
            self._exception = exception
            self._done = True

    def done(self) -> bool:
        """
        Check if the future is completed.

        :return: True if the future is completed, False otherwise.
        """
        return self._done

    def cancel(self) -> None:
        """
        Cancel the future.

        :raises asyncio.InvalidStateError: If the future is already completed.
        """
        if self.done():
            raise asyncio.InvalidStateError
        else:
            self._cancelled = True
            self._done = True

    def cancelled(self) -> bool:
        """
        Check if the future was cancelled.

        :return: True if the future was cancelled, False otherwise.
        """
        return self._cancelled


async def poll(future: WoolFuture, task: concurrent.futures.Future) -> None:
    while True:
        if future.cancelled():
            task.cancel()
            return
        elif future.done():
            return
        else:
            await asyncio.sleep(0)


def fulfill(future: WoolFuture):
    def callback(task: concurrent.futures.Future):
        try:
            result = task.result()
        except concurrent.futures.CancelledError:
            if not future.done():
                future.cancel()
        except BaseException as e:
            logging.exception(e)
            future.set_exception(e)
        else:
            if not future.done():
                future.set_result(result)

    return callback
