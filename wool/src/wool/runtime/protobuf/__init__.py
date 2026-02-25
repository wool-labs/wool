import os
import sys
from typing import Protocol

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from . import task as task
from . import worker as worker


class AddServicerToServerProtocol(Protocol):
    @staticmethod
    def __call__(servicer, server) -> None: ...


add_to_server: dict[type[worker.WorkerServicer], AddServicerToServerProtocol] = {
    worker.WorkerServicer: worker.add_WorkerServicer_to_server,
}
