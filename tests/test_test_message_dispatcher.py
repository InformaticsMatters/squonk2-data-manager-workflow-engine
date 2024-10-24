# Tests for the decoder package.

from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.unit

from informaticsmatters.protobuf.datamanager.workflow_message_pb2 import WorkflowMessage

from tests.message_dispatcher import UnitTestMessageDispatcher
from tests.message_queue import UnitTestMessageQueue


def test_get_nop_job():
    # Arrange
    utmq = UnitTestMessageQueue()
    utmd = UnitTestMessageDispatcher(msg_queue=utmq)
    msg = WorkflowMessage()
    msg.timestamp = f"{datetime.now(timezone.utc).isoformat()}Z"
    msg.action = "START"
    msg.running_workflow = "r-workflow-00000000-0000-0000-0000-000000000000"

    # Act
    utmd.send(msg)

    # Assert
