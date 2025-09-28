![](https://raw.githubusercontent.com/wool-labs/wool/refs/heads/main/assets/woolly-transparent-bg-2048.png)

**Wool** is a Python framework for transparently scheduling and executing tasks in a horizontally scalable, distributed network of agnostic worker processes. Any picklable async function or method can be converted into a task with a simple decorator and a client connection.

## Installation

### Using pip

To install the package using pip, run the following command:

```sh
pip install --pre wool
```

### Cloning from GitHub

To install the package by cloning from GitHub, run the following commands:

```sh
git clone https://github.com/wool-labs/wool.git
cd wool
pip install ./wool
```

## Features

- **Simple decorator-based API** - Just decorate async functions with `@wool.work` to make them distributable
- **Automatic worker discovery** - Workers register themselves and are discovered automatically
- **Multiple discovery backends** - Local (shared memory), LAN (Zeroconf/mDNS), or custom implementations
- **Load balancing** - Built-in round-robin with support for custom strategies
- **Process isolation** - Each worker runs in its own process with separate memory
- **Fault tolerance** - Automatic failover when workers become unavailable
- **Zero configuration** - Works out of the box for common use cases

## Quick Start

### 1. Task Declaration

```python
import wool

@wool.work
async def fibonacci(n: int) -> int:
    if n <= 1:
        return n
    return await fibonacci(n - 1) + await fibonacci(n - 2)
```

### 2. Ephemeral Pool (Local Workers)

```python
import asyncio
import wool

async def main():
    # Spawn local workers that are automatically cleaned up
    async with wool.WorkerPool(size=4):
        result = await fibonacci(10)
        print(f"Result: {result}")

if __name__ == "__main__":
    asyncio.run(main())
```

### 3. Durable Pool (Remote Workers)

```python
import asyncio
import wool

async def main():
    # Connect to existing workers on the network
    discovery = wool.LanDiscoveryService(filter=lambda w: "production" in w.tags)
    
    async with wool.WorkerPool(discovery=discovery):
        result = await fibonacci(10)
        print(f"Result: {result}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Performance optimization

- Minimize the size of arguments and return values to reduce serialization overhead.
- For large datasets, consider using shared memory or passing references (e.g., file paths) instead of transferring the entire data.
- Profile tasks to identify and optimize performance bottlenecks.

### Task cancellation

- Handle task cancellations gracefully by cleaning up resources and rolling back partial changes.
- Use `asyncio.CancelledError` to detect and respond to cancellations.

### Error propagation

- Wool propagates exceptions raised within tasks to the caller. Use this feature to handle errors centrally in your application.

## License

This project is licensed under the Apache License Version 2.0.
