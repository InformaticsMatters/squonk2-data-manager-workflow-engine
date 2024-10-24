from google.protobuf.message import Message

from workflow.workflow_abc import MessageDispatcher


class UnitTestMessageDispatcher(MessageDispatcher):
    """A minimal Message dispatcher to support testing."""

    def send(self, message: Message) -> None:
        assert message
