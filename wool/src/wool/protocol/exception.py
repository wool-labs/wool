class ProtobufImportError(ImportError):
    def __init__(self, exception: ImportError):
        super().__init__(f"{str(exception)} - ensure protocol buffers are compiled.")
