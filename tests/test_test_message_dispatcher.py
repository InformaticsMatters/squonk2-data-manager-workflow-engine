from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.unit

from informaticsmatters.protobuf.datamanager.workflow_message_pb2 import WorkflowMessage

from tests.message_dispatcher import UnitTestMessageDispatcher
from tests.message_queue import UnitTestMessageQueue


@pytest.fixture
def basic_dispatcher():
    utmq = UnitTestMessageQueue()
    utmd = UnitTestMessageDispatcher(msg_queue=utmq)
    utmq.start()

    yield utmd

    utmq.stop()
    utmq.join()


def test_send_start(basic_dispatcher):
    # Arrange
    msg = WorkflowMessage()
    msg.timestamp = f"{datetime.now(timezone.utc).isoformat()}Z"
    msg.action = "START"
    msg.running_workflow = "r-workflow-00000000-0000-0000-0000-000000000000"

    # Act
    basic_dispatcher.send(msg)

    # Assert
