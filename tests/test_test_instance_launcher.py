import json

import pytest

pytestmark = pytest.mark.unit

from tests.config import TEST_PROJECT_ID
from tests.instance_launcher import UnitTestInstanceLauncher
from tests.message_dispatcher import UnitTestMessageDispatcher
from tests.message_queue import UnitTestMessageQueue
from tests.wapi_adapter import UnitTestWorkflowAPIAdapter
from workflow.workflow_abc import LaunchParameters
from workflow.workflow_engine import DM_JOB_APPLICATION_ID


@pytest.fixture
def basic_launcher():
    wapi_adapter = UnitTestWorkflowAPIAdapter()
    message_queue = UnitTestMessageQueue()
    message_dispatcher = UnitTestMessageDispatcher(msg_queue=message_queue)
    instance_launcher = UnitTestInstanceLauncher(
        wapi_adapter=wapi_adapter, msg_dispatcher=message_dispatcher
    )
    return [wapi_adapter, instance_launcher]


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
    lp: LaunchParameters = LaunchParameters(
        project_id=TEST_PROJECT_ID,
        application_id=DM_JOB_APPLICATION_ID,
        name="Test Instance",
        launching_user_name="dlister",
        launching_user_api_token="1234567890",
        running_workflow_id=rwfid,
        running_workflow_step_id=rwfsid,
        specification={"collection": "workflow-engine-unit-test-jobs", "job": "nop"},
        specification_variables={},
    )

    # Act
    result = launcher.launch(lp)

    # Assert
    assert result.error_num == 0
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
    lp: LaunchParameters = LaunchParameters(
        project_id=TEST_PROJECT_ID,
        application_id=DM_JOB_APPLICATION_ID,
        name="Test Instance",
        launching_user_name="dlister",
        launching_user_api_token="1234567890",
        running_workflow_id=rwfid,
        running_workflow_step_id=rwfsid,
        specification={
            "collection": "workflow-engine-unit-test-jobs",
            "job": "nop-fail",
        },
        specification_variables={},
    )

    # Act
    result = launcher.launch(lp)

    # Assert
    assert result.error_num == 0
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
    lp: LaunchParameters = LaunchParameters(
        project_id=TEST_PROJECT_ID,
        application_id=DM_JOB_APPLICATION_ID,
        name="Test Instance",
        launching_user_name="dlister",
        launching_user_api_token="1234567890",
        running_workflow_id=rwfid,
        running_workflow_step_id=rwfsid,
        specification={
            "collection": "workflow-engine-unit-test-jobs",
            "job": "smiles-to-file",
        },
        specification_variables={"smiles": "C1=CC=CC=C1", "outputFile": "output.smi"},
    )

    # Act
    result = launcher.launch(lp)

    # Assert
    assert result.error_num == 0
    assert result.command.startswith("python ")
    assert result.command.endswith(
        "tests/jobs/smiles-to-file.py --smiles C1=CC=CC=C1 --output output.smi"
    )
