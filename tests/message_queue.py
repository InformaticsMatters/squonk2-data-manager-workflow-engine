from contextlib import suppress
from multiprocessing import Event, Process, Queue
from queue import Empty
from typing import Callable, Optional

from google.protobuf.message import Message
from informaticsmatters.protobuf.datamanager.pod_message_pb2 import PodMessage
from informaticsmatters.protobuf.datamanager.workflow_message_pb2 import WorkflowMessage


class UnitTestMessageQueue(Process):
    """A simple asynchronous message passer, used by the Validator
    (and UnitTestInstanceLauncher) to send ProtocolBuffer messages to the Engine."""

    def __init__(self, receiver: Optional[Callable[[Message], None]] = None):
        super().__init__()
        self._stop = Event()
        self._queue = Queue()
        self._receiver = receiver

    def set_receiver(self, receiver: Callable[[Message], None]):
        """Set or replace the receiver function, used unit-testing the WorkflowEngine."""
        assert receiver
        self._receiver = receiver

    def run(self):
        while not self._stop.is_set():
            with suppress(Empty):
                if item := self._queue.get(True, 0.25):
                    msg = None
                    # Convert the message (bytes) back to a ProtocolBuffer object.
                    # We only support Workflow and Pod messages for testing...
                    if item["class"] == "WorkflowMessage":
                        msg = WorkflowMessage()
                        msg.ParseFromString(item["bytes"])
                    elif item["class"] == "PodMessage":
                        msg = PodMessage()
                        msg.ParseFromString(item["bytes"])
                    assert msg
                    if self._receiver:
                        self._receiver(msg)

    def put(self, msg: Message):
        """Puts a protocol buffer message onto the queue."""
        self._queue.put({"class": type(msg).__name__, "bytes": msg.SerializeToString()})

    def stop(self):
        """A request to stop the process."""
        self._stop.set()
