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


def test_launch_is_idempotent_for_the_same_step_replica(basic_launcher):
    """A launch is uniquely identified by its running workflow, step name and
    replica number. Launching the same triple twice must not create a second
    Instance or RunningWorkflowStep - the engine re-assesses every step on every
    event, so it must be safe for it to ask twice."""
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
        specification={"collection": "workflow-engine-unit-test-jobs", "job": "nop"},
    )

    # Act
    first_result = launcher.launch(launch_parameters=lp)
    second_result = launcher.launch(launch_parameters=lp)

    # Assert
    assert first_result.error_num == 0
    assert not first_result.already_launched
    # The second launch must be a no-op that reports the original step...
    assert second_result.error_num == 0
    assert second_result.already_launched
    assert (
        second_result.running_workflow_step_id == first_result.running_workflow_step_id
    )
    # ...and only one RunningWorkflowStep must exist.
    response = utaa.get_running_workflow_steps(running_workflow_id=rwfid)
    assert response["count"] == 1


def test_launch_creates_a_step_for_each_replica(basic_launcher):
    """Replicas of the same step are distinct launches and must each create
    their own RunningWorkflowStep."""
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

    # Act
    for replica in range(2):
        result = launcher.launch(
            launch_parameters=LaunchParameters(
                project_id=TEST_PROJECT_ID,
                name="Test Instance",
                launching_user_name="dlister",
                launching_user_api_token="1234567890",
                running_workflow_id=rwfid,
                step_name="step-1",
                step_replication_number=replica,
                total_number_of_replicas=2,
                specification={
                    "collection": "workflow-engine-unit-test-jobs",
                    "job": "nop",
                },
            )
        )
        assert not result.already_launched

    # Assert
    response = utaa.get_running_workflow_steps(running_workflow_id=rwfid)
    assert response["count"] == 2
