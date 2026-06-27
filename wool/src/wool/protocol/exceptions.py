"""The protocol package's import-failure signal.

Provides `ProtobufImportError`: the exception that surfaces a failed
import of the generated protobuf wire modules.
"""


class ProtobufImportError(ImportError):
    """Raised when the generated protobuf wire modules fail to import.

    The generated modules are build artifacts — absent from a source
    checkout until the protobuf definitions are compiled — so a failed
    import usually means a missing build step, not a packaging defect.
    The wrapper preserves the underlying error's message and appends
    that remedy. Subclassing `ImportError` keeps the failure catchable
    as the import error it is.

    :param exception:
        The underlying `ImportError` from the failed import, whose
        message this exception's own message extends.
    """

    def __init__(self, exception: ImportError):
        super().__init__(f"{str(exception)} - ensure protocol buffers are compiled.")
