from __future__ import annotations

import logging
import os
import signal
from functools import partial
from multiprocessing.managers import (
    BaseManager,
    DictProxy,
)
from queue import Empty
from threading import Lock
from typing import TYPE_CHECKING, Any, Callable, TypeVar, overload
from uuid import UUID
from weakref import WeakValueDictionary

from wool._future import WoolFuture
from wool._queue import TaskQueue
from wool._typing import PassthroughDecorator

if TYPE_CHECKING:
    from wool._task import WoolTask


C = TypeVar("C", bound=Callable[..., Any])


_manager_registry = {}


def _register(fn, /, **kwargs):
    assert fn.__name__ not in _manager_registry, (
        f"Function '{fn.__name__}' already registered"
    )
    _manager_registry[fn.__name__] = (fn, kwargs)
    return fn


@overload
def register(
    fn: C,
    /,
    *,
    proxytype: None = None,
    method_to_typeid: None = None,
) -> C: ...


@overload
def register(
    fn: None = None,
    /,
    *,
    proxytype: type | None = None,
    method_to_typeid: dict[str, str] | None = None,
) -> PassthroughDecorator[C]: ...


def register(
    fn: C | None = None,
    /,
    *,
    proxytype: type | None = None,
    method_to_typeid: dict[str, str] | None = None,
) -> PassthroughDecorator[C] | C:
    kwargs = {}
    if proxytype is not None:
        kwargs["proxytype"] = proxytype
    if method_to_typeid is not None:
        kwargs["method_to_typeid"] = method_to_typeid
    if fn:
        return _register(fn, **kwargs)
    else:
        return partial(_register, **kwargs)


class FuturesProxy(DictProxy):
    assert (method_to_typeid := getattr(DictProxy, "_method_to_typeid_"))
    _method_to_typeid_ = {
        **method_to_typeid,
        "__getitem__": "proxify",
        "setdefault": "proxify",
    }


_task_queue: TaskQueue[WoolTask] = TaskQueue(1000, None)

_task_queue_lock = Lock()

_task_futures: WeakValueDictionary[UUID, WoolFuture] = WeakValueDictionary()


@register
def put(task: WoolTask) -> WoolFuture:
    try:
        with queue_lock():
            queue().put(task, block=False)
            future = futures()[task.id] = WoolFuture()
            logging.debug(f"Pushed task {task.id} to queue: {task.tag}")
            return future
    except Exception as e:
        logging.exception(e)
        raise


@register
def get() -> WoolTask | Empty | None:
    try:
        return queue().get(block=False)
    except Empty as e:
        return e


@register
def proxify(value):
    return value


@register(proxytype=FuturesProxy)
def futures():
    global _task_futures
    if not _task_futures:
        _task_futures = WeakValueDictionary()
    return _task_futures


@register
def queue() -> TaskQueue:
    global _task_queue
    if not _task_queue:
        _task_queue = TaskQueue(1000, None)
    return _task_queue


@register
def queue_lock() -> Lock:
    global _task_queue_lock
    if not _task_queue_lock:
        _task_queue_lock = Lock()
    return _task_queue_lock


@register
def stop(wait: bool = True) -> None:
    os.kill(os.getpid(), signal.SIGINT if wait else signal.SIGTERM)


class ManagerMeta(type):
    def __new__(mcs, name, bases, attrs):
        cls = super().__new__(mcs, name, bases, attrs)
        assert (register := getattr(cls, "register"))
        for name, (fn, kwargs) in _manager_registry.items():
            register(name, callable=fn, **kwargs)
        return cls


class Manager(BaseManager, metaclass=ManagerMeta):
    if TYPE_CHECKING:
        put = staticmethod(put)
        get = staticmethod(get)
        proxify = staticmethod(proxify)
        futures = staticmethod(futures)
        queue = staticmethod(queue)
        queue_lock = staticmethod(queue_lock)
        stop = staticmethod(stop)
