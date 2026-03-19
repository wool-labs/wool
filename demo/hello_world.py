"""Distributed String Solver — a Wool bilateral-streaming demo.

Solves a target string character-by-character by dispatching one worker
per position. Each worker runs a binary-search guesser that yields
guesses and receives ``"higher"`` / ``"lower"`` / ``"match"`` hints
via ``asend()``. After too many misses the outer routine throws a
``Reset`` via ``athrow()`` to restart the search from a random offset.

The inner ``guess`` routine demonstrates bilateral streaming; the outer
``solve`` routine demonstrates nested routine dispatch — it is itself a
``@wool.routine`` that calls ``guess`` on a (potentially different)
worker, driving the conversation loop server-side.

To run: 
>>> python demo/hello_world.py
"""

from __future__ import annotations

import asyncio
import logging
import random
import string
import sys
import time

import wool

logging.disable(logging.CRITICAL)

TARGET = "Hello world!"
CHARSET = string.ascii_letters + string.digits + string.punctuation + " "
MAX_GUESSES_BEFORE_RESET = 20

# ANSI escapes
GREEN = "\033[32m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"
HIDE_CURSOR = "\033[?25l"
SHOW_CURSOR = "\033[?25h"


class Restart(Exception):
    """Thrown into the guesser to restart from a random offset."""


@wool.routine
async def guess():
    """Bilateral generator: binary-search over CHARSET for *target*.

    Yields guesses, receives ``"higher"`` / ``"lower"`` / ``"match"``
    hints via ``asend()``.  Catches :class:`Restart` via ``athrow()``
    to restart from a random position in the charset.
    """
    chars = sorted(CHARSET)
    lo, hi = 0, len(chars) - 1
    while True:
        guess = random.randint(lo, hi)
        try:
            # await asyncio.sleep(0.05)
            hint = yield chars[guess]
        except Restart:
            lo, hi = 0, len(chars) - 1
            lo = random.randint(0, hi)
            continue

        if hint == "match":
            return
        elif hint == "higher":
            lo = guess + 1
        elif hint == "lower":
            hi = guess - 1

        if lo > hi:
            lo, hi = 0, len(chars) - 1


@wool.routine
async def solve(target: str):
    """Solve a single character by driving a ``guess`` conversation.

    This is a nested routine — it runs on a worker and dispatches
    ``guess`` onto the pool, then drives the bilateral protocol
    until the character is found. Yields each guess attempt so the
    client can render progress.
    """
    stream = guess()
    attempt = await anext(stream)
    attempts = 0
    while True:
        time.sleep(1 / 20)
        attempts += 1
        yield attempt
        if attempt == target:
            await stream.aclose()
            return

        if attempts >= MAX_GUESSES_BEFORE_RESET:
            attempts = 0
            attempt = await stream.athrow(Restart)
        else:
            hint = "higher" if ord(attempt) < ord(target) else "lower"
            attempt = await stream.asend(hint)


async def main():
    n = len(TARGET)
    display = list(" " * n)
    solved = [False] * n

    sys.stdout.write(f"{HIDE_CURSOR}\n")

    try:
        async with wool.WorkerPool(size=12):

            async def consume(position: int, stream):
                async for char in stream:
                    display[position] = char
                    if char == TARGET[position]:
                        solved[position] = True
                        return

            async def render():
                t0 = time.perf_counter()
                while not all(solved):
                    rendered = ""
                    for i in range(n):
                        if solved[i]:
                            rendered += f"{GREEN}{BOLD}{display[i]}{RESET}"
                        else:
                            rendered += f"{DIM}{display[i]}{RESET}"
                    sys.stdout.write(f"\r  {rendered}")
                    sys.stdout.flush()
                    await asyncio.sleep(1 / 30)

                elapsed = time.perf_counter() - t0
                final = "".join(
                    f"{GREEN}{BOLD}{c}{RESET}" for c in display
                )
                sys.stdout.write(f"\r  {final}\n\n")
                sys.stdout.write(f"  Solved in {elapsed:.2f}s\n\n")
                sys.stdout.flush()

            async with asyncio.TaskGroup() as tg:
                for i, ch in enumerate(TARGET):
                    tg.create_task(consume(i, solve(ch)))
                tg.create_task(render())
    finally:
        sys.stdout.write(SHOW_CURSOR)
        sys.stdout.flush()


if __name__ == "__main__":
    asyncio.run(main())
