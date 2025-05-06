from __future__ import annotations

import asyncio
import concurrent.futures
import logging
from typing import Any, Generator, Generic, TypeVar, cast

from wool._utils import Undefined, UndefinedType

T = TypeVar("T")


# PUBLIC
class WoolFuture(Generic[T]):
    def __init__(self) -> None:
        self._result: T | UndefinedType = Undefined
        self._exception: (
            BaseException | type[BaseException] | UndefinedType
        ) = Undefined
        self._done: bool = False
        self._cancelled: bool = False

    def __await__(self) -> Generator[Any, None, T]:
        async def _():
            while not self.done():
                await asyncio.sleep(0)
            else:
                return self.result()

        return _().__await__()

    def result(self) -> T:
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
        if self.done():
            raise asyncio.InvalidStateError
        else:
            self._result = result
            self._done = True

    def exception(self) -> BaseException | type[BaseException]:
        if self._exception is not Undefined:
            return cast(BaseException | type[BaseException], self._exception)
        else:
            raise asyncio.InvalidStateError

    def set_exception(
        self, exception: BaseException | type[BaseException]
    ) -> None:
        if self.done():
            raise asyncio.InvalidStateError
        else:
            self._exception = exception
            self._done = True

    def done(self) -> bool:
        return self._done or self._cancelled

    def cancel(self) -> None:
        self.set_exception(asyncio.CancelledError)
        self._cancelled = True

    def cancelled(self) -> bool:
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
            future.set_result(task.result())
        except concurrent.futures.CancelledError:
            if not future.cancelled():
                future.cancel()
        except BaseException as e:
            logging.exception(e)
            future.set_exception(e)

    return callback
