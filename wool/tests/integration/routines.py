"""Test routines for integration tests.

This module defines all ``@wool.routine`` decorated functions used by the
integration test suite. It is NOT a test file — no ``test_`` prefix — so
pytest will not collect it directly.

Routines are organized by dimension:
- D1 (RoutineShape): coroutine vs async generator vs nested variants
- D8 (RoutineBinding): module function vs instance/class/static method
"""

import wool


@wool.routine
async def add(a: int, b: int) -> int:
    """Simple coroutine that returns the sum of two integers."""
    return a + b


@wool.routine
async def gen_range(n: int):
    """Async generator that yields integers 0..n-1."""
    for i in range(n):
        yield i


@wool.routine
async def echo_send(n: int):
    """Async generator with asend support.

    Yields "ready" first, then echoes back any value sent via asend().
    """
    value = yield "ready"
    for _ in range(n):
        value = yield value


@wool.routine
async def resilient_counter(start: int):
    """Async generator with athrow support.

    Yields incrementing integers from *start*. When a ValueError is
    thrown, resets the counter to 0 and continues.
    """
    counter = start
    while True:
        try:
            yield counter
            counter += 1
        except ValueError:
            counter = 0


@wool.routine
async def closeable_gen():
    """Async generator for aclose testing.

    Yields "alive" in a loop until closed.
    """
    while True:
        yield "alive"


@wool.routine
async def nested_add(a: int, b: int) -> int:
    """Coroutine that dispatches to ``add``, triggering nested dispatch."""
    return await add(a, b)


@wool.routine
async def nested_gen(n: int):
    """Async generator that yields from ``gen_range``, nested streaming."""
    async for item in gen_range(n):
        yield item


class Routines:
    """Test class providing instance, class, and static method bindings."""

    @wool.routine
    async def instance_add(self, a: int, b: int) -> int:
        """Instance method coroutine."""
        return a + b

    @wool.routine
    async def instance_gen(self, n: int):
        """Instance method async generator."""
        for i in range(n):
            yield i

    @wool.routine
    async def instance_echo_send(self, n: int):
        """Instance method async generator with asend."""
        value = yield "ready"
        for _ in range(n):
            value = yield value

    @wool.routine
    async def instance_resilient_counter(self, start: int):
        """Instance method async generator with athrow."""
        counter = start
        while True:
            try:
                yield counter
                counter += 1
            except ValueError:
                counter = 0

    @wool.routine
    async def instance_closeable_gen(self):
        """Instance method async generator for aclose."""
        while True:
            yield "alive"

    @classmethod
    @wool.routine
    async def class_add(cls, a: int, b: int) -> int:
        """Class method coroutine."""
        return a + b

    @classmethod
    @wool.routine
    async def class_gen(cls, n: int):
        """Class method async generator."""
        for i in range(n):
            yield i

    @staticmethod
    @wool.routine
    async def static_add(a: int, b: int) -> int:
        """Static method coroutine."""
        return a + b

    @staticmethod
    @wool.routine
    async def static_gen(n: int):
        """Static method async generator."""
        for i in range(n):
            yield i
