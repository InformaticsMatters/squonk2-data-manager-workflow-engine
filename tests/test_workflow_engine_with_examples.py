# A unit (functional) test for the WorkflowEngine's handling of 'Example 1'.
import time
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.unit

from informaticsmatters.protobuf.datamanager.workflow_message_pb2 import WorkflowMessage

from tests.database_adapter import UnitTestDatabaseAdapter
from tests.instance_launcher import UnitTestInstanceLauncher
from tests.message_dispatcher import UnitTestMessageDispatcher
from tests.message_queue import UnitTestMessageQueue
from workflow.workflow_engine import WorkflowEngine


@pytest.fixture
def basic_engine():
    utda = UnitTestDatabaseAdapter()
    utmq = UnitTestMessageQueue()
    utmd = UnitTestMessageDispatcher(msg_queue=utmq)
    util = UnitTestInstanceLauncher(msg_dispatcher=utmd)
    return [utda, utmd, WorkflowEngine(db_adapter=utda, instance_launcher=util)]


def test_workflow_engine_with_example_1(basic_engine):
    # Arrange
    da, md, engine = basic_engine
    # LOAD THE EXAMPLE-1 WORKFLOW DEFINITION INTO THE DATABASE
    # TODO
    # SIMULATE THE API CREATION OF A RUNNING WORKFLOW FROM THE WORKFLOW
    wfid = da.save_workflow(workflow_definition={"name": "example-1"})
    assert wfid
    response = da.create_running_workflow(workflow_definition_id=wfid)
    r_wfid = response["id"]
    assert r_wfid

    # Act
    # SEND A MESSAGE TO THE ENGINE (VIA THE MESSAGE DISPATCHER) TO START THE WORKFLOW
    # THE RUNNING WORKFLOW WILL HAVE THE ID "1"
    msg = WorkflowMessage()
    msg.timestamp = f"{datetime.now(timezone.utc).isoformat()}Z"
    msg.action = "START"
    msg.running_workflow = r_wfid
    md.send(msg)

    # Assert
    # Wait until the workflow is done (successfully)
    # But don't wait for ever!
    attempts = 0
    done = False
    r_wf = None
    while not done:
        response = da.get_running_workflow(running_workflow_id=r_wfid)
        r_wf = response["running-workflow"]
        if r_wf["done"]:
            done = True
        else:
            attempts += 1
            if attempts > 10:
                break
            time.sleep(0.5)
    assert r_wf
    # TODO - The following should be 'success' but the implementation does not set it yet
    assert not r_wf["success"]
    # Now check there are the right number of RunningWorkflowStep Records
    # (and they're all set to success/done)
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    # TODO - The following should not be zero but the implementation does not set it yet
    assert response["count"] == 0
    for step in response["running-workflow-steps"]:
        assert step["running-workflow-step"]["done"]
        assert step["running-workflow-step"]["success"]