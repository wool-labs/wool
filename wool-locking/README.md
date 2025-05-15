# Wool Locking

Wool Locking is an extension of the Wool framework, providing distributed locking primitives for inter-worker and inter-pool synchronization. It allows for the execution of tasks within a locking session, ensuring safe and efficient coordination in distributed environments.

## Installation

### Using pip

To install the package using pip, run the following command:

```sh
[uv] pip install --pre wool-locking
```

### Cloning from GitHub

To install the package by cloning from GitHub, run the following commands:

```sh
git clone https://github.com/wool-labs/wool.git
cd wool/wool-locking
[uv] pip install .
```

## Usage

### Locking Decorator

The `@wool.locking.lock` decorator allows you to execute a function within a lock session. If no lock session is found in the current context, a local lock is used as a fallback.

```python
import wool.locking

@wool.locking.lock
async def critical_section():
    # Code that requires distributed locking
    ...
```

### Lock Pool

The `wool.locking.pool` function creates a specialized worker pool for managing distributed locking tasks. It uses a single worker and integrates with `LockPoolSession` and `LockScheduler`.

```python
import wool.locking

@wool.locking.pool(port=48900, authkey=b"lockkey")
async def locked_task():
    # Code executed within the lock pool
    ...
```

### Lock Pool Session

The `wool.locking.session` function declares a lock pool session context. This can be used to tightly couple tasks with a specific lock pool.

```python
import wool.locking

@wool.locking.session(port=48900, authkey=b"lockkey")
@wool.task
async def locked_task():
    # Code executed within the lock pool session
    ...
```

### Sample Application

Below is an example of how to use Wool Locking to execute tasks with distributed locking:

Module defining locked tasks:
`tasks.py`
```python
import asyncio, wool.locking

@wool.locking.lock
async def locked_task(x, y):
    await asyncio.sleep(1)
    return x + y
```

Module executing locked workflow:
`main.py`
```python
import asyncio, wool.locking
from tasks import locked_task

async def main():
    with wool.locking.session(port=48900, authkey=b"lockkey"):
        result = await locked_task(1, 2)
        print(f"Result: {result}")

asyncio.new_event_loop().run_until_complete(main())
```

To run the demo, first start a lock pool specifying the module defining the tasks to be executed:
```bash
wool lock-pool up --port 48900 --authkey deadbeef
```

Next, in a separate terminal, execute the application defined in `main.py` and, finally, stop the lock pool:
```bash
python main.py
wool lock-pool down --port 48900 --authkey deadbeef
```

## License

This project is licensed under the Apache License Version 2.0.
