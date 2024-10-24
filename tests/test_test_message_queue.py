import time
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.unit

from google.protobuf.message import Message
from informaticsmatters.protobuf.datamanager.workflow_message_pb2 import WorkflowMessage

from tests.message_queue import UnitTestMessageQueue


class Receiver:
    """A dummy message receiver."""

    def handle_msg(self, msg: Message) -> None:
        assert msg
        print(f"Received message ({type(msg).__name__})")


def test_start_and_stop():
    # Arrange
    receiver = Receiver()
    utmq = UnitTestMessageQueue(receiver=receiver.handle_msg)
    utmq.start()

    # Act
    utmq.stop()
    utmq.join()

    # Assert


def test_send_messages():
    # Arrange
    receiver = Receiver()
    utmq = UnitTestMessageQueue(receiver=receiver.handle_msg)
    utmq.start()

    # Act
    msg = WorkflowMessage()
    msg.timestamp = f"{datetime.now(timezone.utc).isoformat()}Z"
    msg.action = "START"
    msg.running_workflow = "r-workflow-00000000-0000-0000-0000-000000000000"
    utmq.put(msg)

    msg = WorkflowMessage()
    msg.timestamp = f"{datetime.now(timezone.utc).isoformat()}Z"
    msg.action = "STOP"
    msg.running_workflow = "r-workflow-00000000-0000-0000-0000-000000000000"
    utmq.put(msg)

    time.sleep(0.5)

    utmq.stop()
    utmq.join()

    # Assert
