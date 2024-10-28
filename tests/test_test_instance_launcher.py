# Tests for the decoder package.

import pytest

pytestmark = pytest.mark.unit

from tests.api_adapter import UnitTestAPIAdapter
from tests.instance_launcher import UnitTestInstanceLauncher
from tests.message_dispatcher import UnitTestMessageDispatcher
from tests.message_queue import UnitTestMessageQueue


@pytest.fixture
def basic_launcher():
    api_adapter = UnitTestAPIAdapter()
    message_queue = UnitTestMessageQueue()
    message_dispatcher = UnitTestMessageDispatcher(msg_queue=message_queue)
    return UnitTestInstanceLauncher(
        api_adapter=api_adapter, msg_dispatcher=message_dispatcher
    )


def test_get_nop_job(basic_launcher):
    # Arrange

    # Act
    result = basic_launcher.launch(
        project_id="project-00000000-0000-0000-0000-000000000000",
        workflow_id="workflow-00000000-0000-0000-0000-000000000000",
        workflow_definition={},
        step="step-1",
        step_specification={"job": "nop"},
        completion_callback=None,
    )

    # Assert
    assert result.error == 0
    assert result.command.startswith("python ")
    assert result.command.endswith("tests/jobs/nop.py")
