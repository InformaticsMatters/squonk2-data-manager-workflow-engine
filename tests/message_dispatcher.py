"""The UnitTest Message Dispatcher.

A very simple object that relies on an underlying message queue and is designed
to emulate the behaviour of the message queue used in the Data Manager.
Here we offer a minimal implementation that simply sends a (protocol buffer) message
to the queue.
"""

from google.protobuf.message import Message

from tests.message_queue import UnitTestMessageQueue


class UnitTestMessageDispatcher:
    """A minimal Message dispatcher to support testing."""

    def __init__(self, msg_queue: UnitTestMessageQueue):
        super().__init__()
        self._msg_queue: UnitTestMessageQueue = msg_queue

    def send(self, message: Message) -> None:
        print(f"UnitTestMessageDispatcher.send:\n{message}")
        self._msg_queue.put(message)
