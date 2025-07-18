import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


class ProtobufImportError(ImportError):
    def __init__(self, exception: ImportError):
        super().__init__(
            f"{str(exception)} - ensure protocol buffers are compiled."
        )
