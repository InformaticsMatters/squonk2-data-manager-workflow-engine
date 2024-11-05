# Tests for the decoder package.
import json

import pytest

pytestmark = pytest.mark.unit

from tests.api_adapter import UnitTestAPIAdapter
from tests.config import TEST_PROJECT_ID
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


def test_launch_nop(basic_launcher):
    # Arrange
    utaa = basic_launcher[0]
    launcher = basic_launcher[1]
    response = utaa.create_workflow(workflow_definition={"name": "blah"})
    rwfid = response["id"]
    response = utaa.create_running_workflow(
        user_id="dlister",
        workflow_id=response["id"],
        project_id=TEST_PROJECT_ID,
        variables={},
    )
    response = utaa.create_running_workflow_step(
        running_workflow_id=response["id"], step="step-1"
    )
    rwfsid = response["id"]

    # Act
    result = launcher.launch(
        project_id=TEST_PROJECT_ID,
        running_workflow_id=rwfid,
        running_workflow_step_id=rwfsid,
        step_specification=json.dumps(
            {"collection": "workflow-engine-unit-test-jobs", "job": "nop"}
        ),
        variables={},
    )

    # Assert
    assert result.error == 0
    assert result.command.startswith("python ")
    assert result.command.endswith("tests/jobs/nop.py")


def test_launch_nop_fail(basic_launcher):
    # Arrange
    utaa = basic_launcher[0]
    launcher = basic_launcher[1]
    response = utaa.create_workflow(workflow_definition={"name": "blah"})
    response = utaa.create_running_workflow(
        user_id="dlister",
        workflow_id=response["id"],
        project_id=TEST_PROJECT_ID,
        variables={},
    )
    rwfid = response["id"]
    response = utaa.create_running_workflow_step(
        running_workflow_id=response["id"], step="step-1"
    )
    rwfsid = response["id"]

    # Act
    result = launcher.launch(
        project_id=TEST_PROJECT_ID,
        running_workflow_id=rwfid,
        running_workflow_step_id=rwfsid,
        step_specification=json.dumps(
            {"collection": "workflow-engine-unit-test-jobs", "job": "nop-fail"}
        ),
        variables={},
    )

    # Assert
    assert result.error == 0
    assert result.command.startswith("python ")
    assert result.command.endswith("tests/jobs/nop-fail.py")


def test_launch_smiles_to_file(basic_launcher):
    # Arrange
    utaa = basic_launcher[0]
    launcher = basic_launcher[1]
    response = utaa.create_workflow(workflow_definition={"name": "blah"})
    response = utaa.create_running_workflow(
        user_id="dlister",
        workflow_id=response["id"],
        project_id=TEST_PROJECT_ID,
        variables={},
    )
    rwfid = response["id"]
    response = utaa.create_running_workflow_step(
        running_workflow_id=response["id"], step="step-1"
    )
    rwfsid = response["id"]

    # Act
    result = launcher.launch(
        project_id=TEST_PROJECT_ID,
        running_workflow_id=rwfid,
        running_workflow_step_id=rwfsid,
        step_specification=json.dumps(
            {
                "collection": "workflow-engine-unit-test-jobs",
                "job": "smiles-to-file",
            }
        ),
        variables={"smiles": "C1=CC=CC=C1", "outputFile": "output.smi"},
    )

    # Assert
    assert result.error == 0
    assert result.command.startswith("python ")
    assert result.command.endswith(
        "tests/jobs/smiles-to-file.py --smiles C1=CC=CC=C1 --output output.smi"
    )
