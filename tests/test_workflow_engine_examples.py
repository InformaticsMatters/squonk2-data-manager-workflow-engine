import os
import time
from datetime import datetime, timezone
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

from informaticsmatters.protobuf.datamanager.workflow_message_pb2 import WorkflowMessage

from tests.config import TEST_PROJECT_ID
from tests.instance_launcher import UnitTestInstanceLauncher, project_file_exists
from tests.message_dispatcher import UnitTestMessageDispatcher
from tests.message_queue import UnitTestMessageQueue
from tests.wapi_adapter import UnitTestWorkflowAPIAdapter
from workflow.workflow_engine import WorkflowEngine


@pytest.fixture
def basic_engine():
    wapi_adapter = UnitTestWorkflowAPIAdapter()
    message_queue = UnitTestMessageQueue()
    message_dispatcher = UnitTestMessageDispatcher(msg_queue=message_queue)
    instance_launcher = UnitTestInstanceLauncher(
        wapi_adapter=wapi_adapter, msg_dispatcher=message_dispatcher
    )
    workflow_engine = WorkflowEngine(
        wapi_adapter=wapi_adapter, instance_launcher=instance_launcher
    )
    message_queue.set_receiver(workflow_engine.handle_message)
    print("Starting message queue...")
    message_queue.start()

    yield [wapi_adapter, message_dispatcher]

    print("Stopping message queue...")
    message_queue.stop()
    message_queue.join()
    print("Stopped")


def start_workflow(
    md, da, workflow_file_name: str, variables: dict[str, Any] | None = None
) -> str:
    """A convenience function to handle all the 'START' logic for a workflow.
    It is given the message dispatcher, data adapter, and the base-name of the
    workflow definition - i.e. the filename without the '.yaml' extension
    (expected to be in the workflow-definitions directory).

    It loads the workflow definition into the API adapter, creates a running workflow
    from it and then sends a 'START' message which should cause the workflow engine to
    start the workflow."""

    # To start a workflow we need to:
    # 1. Load and create a Workflow Definition
    # 2. Create a Running Workflow record
    # 3. Send a Workflow START message
    #
    # 1.
    workflow_path = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", f"{workflow_file_name}.yaml"
    )
    with open(workflow_path, "r", encoding="utf8") as wf_file:
        wf_definition = yaml.load(wf_file, Loader=yaml.FullLoader)
    assert wf_definition
    wf_response = da.create_workflow(workflow_definition=wf_definition)
    print(f"Created workflow definition {wf_response}")
    # 2.
    response = da.create_running_workflow(
        user_id="dlister",
        workflow_id=wf_response["id"],
        project_id=TEST_PROJECT_ID,
        variables=variables,
    )
    r_wfid = response["id"]
    assert r_wfid
    print(f"Created running workflow {r_wfid}")
    # 3.
    msg = WorkflowMessage()
    msg.timestamp = f"{datetime.now(timezone.utc).isoformat()}Z"
    msg.action = "START"
    msg.running_workflow = r_wfid
    md.send(msg)
    print("Sent START message")

    return r_wfid


def wait_for_workflow(
    da,
    r_wfid,
    *,
    expect_success=True,
    completion_attempts=20,
    completion_poll_period_s=0.25,
) -> None:
    """A convenience function to wait for and check a workflow execution
    (by inspecting the anticipated DB/API records). The workflow is expected
    to start (because start_workflow() has been called), this function
    waits for the running workflow to complete (by polling the API)
    while also checking the expected success/failure status.
    """
    assert isinstance(da, UnitTestWorkflowAPIAdapter)
    assert isinstance(r_wfid, str)

    # We wait for the workflow to complete by polling the API and checking
    # the running workflow's 'done' status. The user can specify whether
    # the workflow is expected to succeed or fail. Any further checks
    # are the responsibility of the caller.
    attempts = 0
    done = False
    while not done:
        response, _ = da.get_running_workflow(running_workflow_id=r_wfid)
        if response["done"]:
            done = True
        else:
            attempts += 1
            if attempts > completion_attempts:
                break
            time.sleep(completion_poll_period_s)
    # When we get here the workflow must have finished (not timed-out),
    # and it must have passed (or failed) according the the caller's expectation.
    assert response["done"]
    assert response["success"] == expect_success


def test_workflow_engine_example_two_step_nop(basic_engine):
    # Arrange
    da, md = basic_engine

    # Act
    r_wfid = start_workflow(md, da, "example-two-step-nop", {})

    # Assert
    wait_for_workflow(da, r_wfid)
    # Additional, detailed checks...
    # Check there are the right number of RunningWorkflowStep Records
    # (and they're all set to success/done)
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert response["count"] == 2
    for step in response["running_workflow_steps"]:
        assert step["done"]
        assert step["success"]


def test_workflow_engine_example_nop_fail(basic_engine):
    # Arrange
    da, md = basic_engine

    # Act
    r_wfid = start_workflow(md, da, "example-nop-fail", {})

    # Assert
    wait_for_workflow(da, r_wfid, expect_success=False)
    # Additional, detailed checks...
    # Check we only have one RunningWorkflowStep, and it failed
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert response["count"] == 1
    assert response["running_workflow_steps"][0]["done"]
    assert not response["running_workflow_steps"][0]["success"]


def test_workflow_engine_example_smiles_to_file(basic_engine):
    # Arrange
    da, md = basic_engine
    # Make sure a file that should be generated by the test
    # does not exist before we run the test.
    output_file = "ethanol.smi"
    assert not project_file_exists(output_file)

    # Act
    r_wfid = start_workflow(
        md, da, "example-smiles-to-file", {"smiles": "CCO", "outputFile": output_file}
    )

    # Assert
    wait_for_workflow(da, r_wfid)
    # Additional, detailed checks...
    # Check we only have one RunningWorkflowStep, and it succeeded
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert response["count"] == 1
    assert response["running_workflow_steps"][0]["done"]
    assert response["running_workflow_steps"][0]["success"]
    # This test should generate a file in the simulated project directory
    assert project_file_exists(output_file)


@pytest.mark.skip(reason="The engine does not currently create the required variables")
def test_workflow_engine_shortcut_example_1(basic_engine):
    # Arrange
    da, md = basic_engine
    # Make sure files that should be generated by the test
    # do not exist before we run the test.
    output_file_a = "a.sdf"
    assert not project_file_exists(output_file_a)
    output_file_b = "b.sdf"
    assert not project_file_exists(output_file_b)

    # Act
    r_wfid = start_workflow(md, da, "shortcut-example-1", {})

    # Assert
    wait_for_workflow(da, r_wfid)
    # Additional, detailed checks...
    # Check we only have one RunningWorkflowStep, and it succeeded
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert response["count"] == 2
    assert response["running_workflow_steps"][0]["done"]
    assert response["running_workflow_steps"][0]["success"]
    assert response["running_workflow_steps"][1]["done"]
    assert response["running_workflow_steps"][1]["success"]
    # This test should generate a file in the simulated project directory
    assert project_file_exists(output_file_a)
    assert project_file_exists(output_file_b)


@pytest.mark.skip(reason="The engine does not currently create the required variables")
def test_workflow_engine_simple_python_molprops(basic_engine):
    # Arrange
    da, md = basic_engine
    # Make sure files that should be generated by the test
    # do not exist before we run the test.
    output_file_a = "a.sdf"
    assert not project_file_exists(output_file_a)
    output_file_b = "b.sdf"
    assert not project_file_exists(output_file_b)

    # Act
    r_wfid = start_workflow(
        md, da, "simple-python-molprops", {"candidateMolecules": "C"}
    )

    # Assert
    wait_for_workflow(da, r_wfid)
    # Additional, detailed checks...
    # Check we only have one RunningWorkflowStep, and it succeeded
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert response["count"] == 2
    assert response["running_workflow_steps"][0]["done"]
    assert response["running_workflow_steps"][0]["success"]
    assert response["running_workflow_steps"][1]["done"]
    assert response["running_workflow_steps"][1]["success"]
    # This test should generate a file in the simulated project directory
    assert project_file_exists(output_file_a)
    assert project_file_exists(output_file_b)
