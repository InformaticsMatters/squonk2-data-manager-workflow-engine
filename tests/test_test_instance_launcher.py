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
    instance_launcher = UnitTestInstanceLauncher(
        api_adapter=api_adapter, msg_dispatcher=message_dispatcher
    )
    return [api_adapter, instance_launcher]


def test_get_nop_job(basic_launcher):
    # Arrange
    utaa = basic_launcher[0]
    launcher = basic_launcher[1]
    response = utaa.create_workflow(workflow_definition={"name": "blah"})
    response = utaa.create_running_workflow(workflow_id=response["id"])
    response = utaa.create_running_workflow_step(
        running_workflow_id=response["id"], step="step-1"
    )
    rwfsid = response["id"]

    # Act
    result = launcher.launch(
        project_id="project-00000000-0000-0000-0000-000000000000",
        workflow_id="workflow-00000000-0000-0000-0000-000000000000",
        running_workflow_step_id=rwfsid,
        workflow_definition={},
        step_specification={"job": "nop", "variables": {"x": 1}},
    )

    # Assert
    assert result.error == 0
    assert result.command.startswith("python ")
    assert result.command.endswith("tests/jobs/nop.py")
