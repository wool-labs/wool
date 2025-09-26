# Wool

Wool is a native Python package for transparently executing tasks in a horizontally scalable, distributed network of agnostic worker processes. Any picklable async function or method can be converted into a task with a simple decorator and a client connection.

## Installation

### Using pip

To install the package using pip, run the following command:

```sh
[uv] pip install --pre wool
```

### Cloning from GitHub

To install the package by cloning from GitHub, run the following commands:

```sh
git clone https://github.com/wool-labs/wool.git
cd wool
[uv] pip install ./wool
```

## Usage

### CLI Commands

Wool provides a command-line interface (CLI) for managing the worker pool.

To list the available commands:

```sh
wool --help
```

#### Start the Worker Pool

To start the worker pool, use the `up` command:

```sh
wool pool up --host <host> --port <port> --authkey <authkey> --breadth <breadth> --module <module>
```

- `--host`: The host address (default: `localhost`).
- `--port`: The port number (default: `0`).
- `--authkey`: The authentication key (default: `b""`).
- `--breadth`: The number of worker processes (default: number of CPU cores).
- `--module`: Python module containing Wool task definitions to be executed by this pool (optional, can be specified multiple times).

#### Stop the Worker Pool

To stop the worker pool, use the `down` command:

```sh
wool pool down --host <host> --port <port> --authkey <authkey> --wait
```

- `--host`: The host address (default: `localhost`).
- `--port`: The port number (required).
- `--authkey`: The authentication key (default: `b""`).
- `--wait`: Wait for in-flight tasks to complete before shutting down.

#### Ping the Worker Pool

To ping the worker pool, use the `ping` command:

```sh
wool ping --host <host> --port <port> --authkey <authkey>
```

- `--host`: The host address (default: `localhost`).
- `--port`: The port number (required).
- `--authkey`: The authentication key (default: `b""`).

### Sample Python Application

Below is an example of how to create a Wool client connection, decorate an async function using the `task` decorator, and execute the function remotely:

Module defining remote tasks:
`tasks.py`
```python
import asyncio, wool

# Decorate an async function using the `task` decorator
@wool.task
async def sample_task(x, y):
    await asyncio.sleep(1)
    return x + y
```

Module executing remote workflow:
`main.py`
```python
import asyncio, wool
from tasks import sample_task

# Execute the decorated function in an external worker pool
async def main():
    with wool.PoolSession(port=5050, authkey=b"deadbeef"):
        result = await sample_task(1, 2)
        print(f"Result: {result}")

asyncio.new_event_loop().run_until_complete(main())
```

To run the demo, first start a worker pool specifying the module defining the tasks to be executed:
```bash
wool pool up --port 5050 --authkey deadbeef --breadth 1 --module tasks
```

Next, in a separate terminal, execute the application defined in `main.py` and, finally, stop the worker pool:
```bash
python main.py
wool pool down --port 5050 --authkey deadbeef
```

## License

This project is licensed under the Apache License Version 2.0.
