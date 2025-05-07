import wool
import wool.locking


def lock(fn):
    def wrapper(*args, **kwargs):
        if client := wool.locking.__lock_client__.get():
            return wool.task(fn)(*args, __wool_client__ = client, **kwargs)
        else:
            raise RuntimeError("No lock client found in context.")
        
    return wrapper
