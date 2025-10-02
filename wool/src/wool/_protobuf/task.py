try:
    from wool._protobuf.task_pb2 import Exception
    from wool._protobuf.task_pb2 import Result
    from wool._protobuf.task_pb2 import Task
    from wool._protobuf.task_pb2 import Worker as Worker
except ImportError as e:
    from wool._protobuf.exception import ProtobufImportError

    raise ProtobufImportError(e) from e

__all__ = ["Exception", "Result", "Task", "Worker"]
