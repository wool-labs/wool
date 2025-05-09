import functools

import wool
import wool.locking


def lock(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if client := wool.locking.__locking_session__.get():
            return wool.task(fn)(*args, __wool_session__=client, **kwargs)
        else:
            raise RuntimeError("No lock session found in context.")

    return wrapper
