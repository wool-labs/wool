from __future__ import annotations

import logging
from functools import partial
from multiprocessing.managers import BaseManager
from multiprocessing.managers import DictProxy
from queue import Empty
from threading import Event
from threading import Lock
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import TypeVar
from typing import overload
from uuid import UUID
from weakref import WeakValueDictionary

from wool._event import WoolTaskEvent
from wool._future import WoolFuture
from wool._queue import TaskQueue
from wool._typing import PassthroughDecorator

if TYPE_CHECKING:
    from wool._task import WoolTask


C = TypeVar("C", bound=Callable[..., Any])

_manager_registry = {}


def _register(fn, /, **kwargs):
    """
    Register a function in the manager registry.

    :param fn: The function to register.
    :type fn: Callable
    :param kwargs: Additional keyword arguments for registration.
    :type kwargs: dict
    :raises AssertionError: If the function is already registered.
    :return: The registered function.
    """
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
    exposed: tuple[str, ...] | None = None,
    proxytype: None = None,
    method_to_typeid: None = None,
) -> C: ...


@overload
def register(
    fn: None = None,
    /,
    *,
    exposed: tuple[str, ...] | None = None,
    proxytype: type | None = None,
    method_to_typeid: dict[str, str] | None = None,
) -> PassthroughDecorator[C]: ...


def register(
    fn: C | None = None,
    /,
    *,
    exposed: tuple[str, ...] | None = None,
    proxytype: type | None = None,
    method_to_typeid: dict[str, str] | None = None,
) -> PassthroughDecorator[C] | C:
    """
    Decorator to register a function or method with the manager.

    :param fn: The function to register.
    :type fn: Callable | None
    :param exposed: Tuple of method names to expose.
    :type exposed: tuple[str, ...] | None
    :param proxytype: Proxy type for the registered function.
    :type proxytype: type | None
    :param method_to_typeid: Mapping of method names to type IDs.
    :type method_to_typeid: dict[str, str] | None
    :return: A decorator or the registered function.
    """
    kwargs = {}
    if exposed is not None:
        kwargs["exposed"] = exposed
    if proxytype is not None:
        kwargs["proxytype"] = proxytype
    if method_to_typeid is not None:
        kwargs["method_to_typeid"] = method_to_typeid
    if fn:
        return _register(fn, **kwargs)
    else:
        return partial(_register, **kwargs)


class FuturesProxy(DictProxy):
    """
    A proxy for managing futures in the task queue.

    This proxy extends the default `DictProxy` to include additional
    methods for proxifying items.
    """

    assert (method_to_typeid := getattr(DictProxy, "_method_to_typeid_"))
    _method_to_typeid_ = {
        **method_to_typeid,
        "__getitem__": "proxify",
        "setdefault": "proxify",
    }


_task_queue: TaskQueue[WoolTask] = TaskQueue(100000, None)

_task_queue_lock = Lock()

_task_futures: WeakValueDictionary[UUID, WoolFuture] = WeakValueDictionary()


@register
def put(task: WoolTask) -> WoolFuture:
    """
    Add a task to the task queue and return its associated future.

    :param task: The task to add to the queue.
    :type task: WoolTask
    :return: The future associated with the task.
    :raises Exception: If an error occurs while adding the task.
    """
    try:
        with queue_lock():
            queue().put(task, block=False)
            future = futures()[task.id] = WoolFuture()
            WoolTaskEvent("task-queued", task=task).emit()
            return future
    except Exception as e:
        logging.exception(e)
        raise


@register
def get() -> WoolTask | Empty | None:
    """
    Retrieve a task from the task queue.

    :return: The next task in the queue, or an Empty exception if the queue is 
        empty.
    """
    try:
        return queue().get(block=False)
    except Empty as e:
        return e


@register
def proxify(value):
    """
    Proxify a value for use in the manager.

    :param value: The value to proxify.
    :return: The proxified value.
    """
    return value


@register(proxytype=FuturesProxy)
def futures():
    """
    Retrieve the dictionary of task futures.

    :return: A dictionary of task futures.
    """
    global _task_futures
    if not _task_futures:
        _task_futures = WeakValueDictionary()
    return _task_futures


@register
def queue() -> TaskQueue:
    """
    Retrieve the task queue.

    :return: The task queue.
    """
    global _task_queue
    if not _task_queue:
        _task_queue = TaskQueue(1000, None)
    return _task_queue


@register
def queue_lock() -> Lock:
    """
    Retrieve the lock for the task queue.

    :return: The task queue lock.
    """
    global _task_queue_lock
    if not _task_queue_lock:
        _task_queue_lock = Lock()
    return _task_queue_lock


_stop_event = Event()
_wait_event = Event()


@register(exposed=("is_set", "set", "clear", "wait"))
def stopping() -> Event:
    """
    Retrieve the event indicating whether the manager is stopping.

    :return: The stopping event.
    """
    return _stop_event


@register(exposed=("is_set", "set", "clear", "wait"))
def waiting() -> Event:
    """
    Retrieve the event indicating whether the manager is waiting.

    :return: The waiting event.
    """
    return _wait_event


class ManagerMeta(type):
    """
    Metaclass for the Manager class.

    Automatically registers functions and methods with the manager
    based on the `_manager_registry`.
    """

    def __new__(mcs, name, bases, attrs):
        cls = super().__new__(mcs, name, bases, attrs)
        assert (register := getattr(cls, "register"))
        for name, (fn, kwargs) in _manager_registry.items():
            register(name, callable=fn, **kwargs)
        return cls


class Manager(BaseManager, metaclass=ManagerMeta):
    """
    A multiprocessing-based manager for coordinating tasks and workers.

    The Manager class provides methods for managing task queues,
    futures, and synchronization events.
    """

    if TYPE_CHECKING:
        put = staticmethod(put)
        get = staticmethod(get)
        proxify = staticmethod(proxify)
        futures = staticmethod(futures)
        queue = staticmethod(queue)
        queue_lock = staticmethod(queue_lock)
        stopping = staticmethod(stopping)
        waiting = staticmethod(waiting)
