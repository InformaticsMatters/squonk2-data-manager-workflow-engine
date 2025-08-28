import pytest

pytestmark = pytest.mark.unit

from tests.config import TEST_PROJECT_ID
from tests.instance_launcher import UnitTestInstanceLauncher
from tests.message_dispatcher import UnitTestMessageDispatcher
from tests.message_queue import UnitTestMessageQueue
from tests.wapi_adapter import UnitTestWorkflowAPIAdapter
from workflow.workflow_abc import LaunchParameters


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
    lp: LaunchParameters = LaunchParameters(
        project_id=TEST_PROJECT_ID,
        name="Test Instance",
        launching_user_name="dlister",
        launching_user_api_token="1234567890",
        running_workflow_id=rwfid,
        step_name="step-1",
        specification={"collection": "workflow-engine-unit-test-jobs", "job": "nop"},
    )

    # Act
    result = launcher.launch(launch_parameters=lp)

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
    lp: LaunchParameters = LaunchParameters(
        project_id=TEST_PROJECT_ID,
        name="Test Instance",
        launching_user_name="dlister",
        launching_user_api_token="1234567890",
        running_workflow_id=rwfid,
        step_name="step-1",
        specification={
            "collection": "workflow-engine-unit-test-jobs",
            "job": "nop-fail",
        },
    )

    # Act
    result = launcher.launch(launch_parameters=lp)

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
    lp: LaunchParameters = LaunchParameters(
        project_id=TEST_PROJECT_ID,
        name="Test Instance",
        launching_user_name="dlister",
        launching_user_api_token="1234567890",
        running_workflow_id=rwfid,
        step_name="step-1",
        specification={
            "collection": "workflow-engine-unit-test-jobs",
            "job": "smiles-to-file",
        },
        variables={"smiles": "C1=CC=CC=C1", "outputFile": "output.smi"},
    )

    # Act
    result = launcher.launch(launch_parameters=lp)

    # Assert
    assert result.error_num == 0
    assert result.command.startswith("python ")
    assert result.command.endswith(
        "tests/jobs/smiles-to-file.py --smiles C1=CC=CC=C1 --output output.smi"
    )
