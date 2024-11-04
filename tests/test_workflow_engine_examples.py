# A unit (functional) test for the WorkflowEngine's handling of 'Example 1'.
import os
import time
from datetime import datetime, timezone

import pytest
import yaml

pytestmark = pytest.mark.unit

from informaticsmatters.protobuf.datamanager.workflow_message_pb2 import WorkflowMessage

from tests.api_adapter import UnitTestAPIAdapter
from tests.instance_launcher import UnitTestInstanceLauncher
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
    return [
        api_adapter,
        message_queue,
        message_dispatcher,
        workflow_engine,
    ]


def test_workflow_engine_with_two_step_nop(basic_engine):
    # Arrange
    da, mq, md, _ = basic_engine

    # Act
    # To test the WorkflowEngine we need to:
    # 1. Start the message queue
    # 2. Load and create a Workflow Definition
    # 3. Create a Running Workflow record
    # 4. Send a Workflow START message
    #
    # 1. (Start the message queue)
    mq.start()
    # 2. (Load/create the workflow definition to be tested)
    workflow_file_name = "example-two-step-nop"
    workflow_path = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", f"{workflow_file_name}.yaml"
    )
    with open(workflow_path, "r", encoding="utf8") as wf_file:
        wf_definition = yaml.load(wf_file, Loader=yaml.FullLoader)
    assert wf_definition
    wfid = da.create_workflow(workflow_definition=wf_definition)
    assert wfid
    print(f"Created workflow definition {wfid}")
    # 3. (Create a running workflow record)
    response = da.create_running_workflow(workflow_definition_id=wfid)
    r_wfid = response["id"]
    assert r_wfid
    print(f"Created running workflow {r_wfid}")
    # 4. (Send the Workflow START message)
    msg = WorkflowMessage()
    msg.timestamp = f"{datetime.now(timezone.utc).isoformat()}Z"
    msg.action = "START"
    msg.running_workflow = r_wfid
    md.send(msg)
    print("Sent START message")

    # Assert
    # Wait until the workflow is done (successfully)
    # But don't wait for ever!
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
    # Stop the message queue
    print("Stopping message queue...")
    mq.stop()
    mq.join()
    print("Stopped")
    assert r_wf
    assert r_wf["done"]
    assert r_wf["success"]
    # Now check there are the right number of RunningWorkflowStep Records
    # (and they're all set to success/done)
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert response["count"] == 2
    for step in response["running_workflow_steps"]:
        assert step["running_workflow_step"]["done"]
        assert step["running_workflow_step"]["success"]
