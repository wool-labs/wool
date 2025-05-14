import functools
import logging

import wool
import wool.locking


# PUBLIC
def lock(fn):
    """
    Decorator to execute a function within a lock session.

    If no lock session is found in the current context, a local lock is used 
    as a fallback. The function is executed as a Wool task within the lock 
    session.

    :param fn: The function to decorate.
    :return: The decorated function.
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        """
        Wrapper function for the lock decorator.

        :param args: Positional arguments for the function.
        :param kwargs: Keyword arguments for the function.
        :return: The result of the function execution.
        """
        if isinstance(
            client := wool.locking.__locking_session__.get(), wool.LocalSession
        ):
            logging.warning(
                "No lock session found in context. Using local lock."
            )
        return wool.task(fn)(*args, __wool_session__=client, **kwargs)

    return wrapper
