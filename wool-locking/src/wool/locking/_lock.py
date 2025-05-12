import functools
import logging

import wool
import wool.locking


# PUBLIC
def lock(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if isinstance(
            client := wool.locking.__locking_session__.get(), wool.LocalSession
        ):
            logging.warning(
                "No lock session found in context. Using local lock."
            )
        return wool.task(fn)(*args, __wool_session__=client, **kwargs)

    return wrapper
