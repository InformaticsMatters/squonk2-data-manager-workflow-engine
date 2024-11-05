# A unit (functional) test for the WorkflowEngine's handling of 'Example 1'.
import os
import time
from datetime import datetime, timezone

import pytest
import yaml

pytestmark = pytest.mark.unit

from informaticsmatters.protobuf.datamanager.workflow_message_pb2 import WorkflowMessage

from tests.api_adapter import UnitTestAPIAdapter
from tests.config import TEST_PROJECT_ID
from tests.instance_launcher import UnitTestInstanceLauncher, project_file_exists
from tests.message_dispatcher import UnitTestMessageDispatcher
from tests.message_queue import UnitTestMessageQueue
from workflow.workflow_engine import WorkflowEngine


@pytest.fixture
def basic_engine():
    api_adapter = UnitTestAPIAdapter()
    message_queue = UnitTestMessageQueue()
    message_dispatcher = UnitTestMessageDispatcher(msg_queue=message_queue)
    instance_launcher = UnitTestInstanceLauncher(
        api_adapter=api_adapter, msg_dispatcher=message_dispatcher
    )
    workflow_engine = WorkflowEngine(
        api_adapter=api_adapter, instance_launcher=instance_launcher
    )
    message_queue.set_receiver(workflow_engine.handle_message)
    print("Starting message queue...")
    message_queue.start()

    yield [api_adapter, message_dispatcher]

    print("Stopping message queue...")
    message_queue.stop()
    message_queue.join()
    print("Stopped")


def start_workflow(md, da, workflow_file_name, variables) -> str:
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
    wfid = da.create_workflow(workflow_definition=wf_definition)
    assert wfid
    print(f"Created workflow definition {wfid}")
    # 2.
    response = da.create_running_workflow(
        workflow_id=wfid, project_id=TEST_PROJECT_ID, variables=variables
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


def wait_for_workflow(da, r_wfid, expect_success=True):
    """A convenience function to wait for and check a workflow execution
    (by inspecting the anticipated DB/API records). The workflow is expected
    to start (because start_workflow() has been called), this function
    waits for the running workflow to complete (by polling the API).
    """

    # We wait for the workflow to complete by polling the API and checking
    # the running workflow's 'done' status. The user can specify whether
    # the workflow is expected to succeed or fail. Any further checks
    # are the responsibility of the caller.
    attempts = 0
    done = False
    r_wf = None
    while not done:
        response = da.get_running_workflow(running_workflow_id=r_wfid)
        assert "running_workflow" in response
        r_wf = response["running_workflow"]
        if r_wf["done"]:
            done = True
        else:
            attempts += 1
            if attempts > 10:
                break
            time.sleep(0.5)
    assert r_wf
    assert r_wf["done"]
    assert r_wf["success"] == expect_success


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
        assert step["running_workflow_step"]["done"]
        assert step["running_workflow_step"]["success"]


def test_workflow_engine_example_nop_fail(basic_engine):
    # Arrange
    da, md = basic_engine

    # Act
    r_wfid = start_workflow(md, da, "example-nop-fail", {})

    # Assert
    wait_for_workflow(da, r_wfid, expect_success=False)
    # Additional, detailed checks...
    # Check we only haver one step, and it failed
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert response["count"] == 1
    assert response["running_workflow_steps"][0]["running_workflow_step"]["done"]
    assert not response["running_workflow_steps"][0]["running_workflow_step"]["success"]


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
    # Check we only haver one step, and it failed
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert response["count"] == 1
    assert response["running_workflow_steps"][0]["running_workflow_step"]["done"]
    assert response["running_workflow_steps"][0]["running_workflow_step"]["success"]
    # This test should generate some files now existing the project directory
    assert project_file_exists(output_file)
