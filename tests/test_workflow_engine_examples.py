import os
import time
from datetime import datetime, timezone
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

from informaticsmatters.protobuf.datamanager.workflow_message_pb2 import WorkflowMessage

from tests.config import TEST_PROJECT_ID
from tests.instance_launcher import (
    EXECUTION_DIRECTORY,
    UnitTestInstanceLauncher,
    project_file_exists,
)
from tests.message_dispatcher import UnitTestMessageDispatcher
from tests.message_queue import UnitTestMessageQueue
from tests.wapi_adapter import UnitTestWorkflowAPIAdapter
from workflow.workflow_engine import WorkflowEngine
from workflow.workflow_validator import (
    ValidationLevel,
    ValidationResult,
    WorkflowValidator,
)


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
    # 2. Validate the workflow for running
    # 3. Create a Running Workflow record
    # 4. Send a Workflow START message
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
    vr_result: ValidationResult = WorkflowValidator.validate(
        workflow_definition=wf_definition,
        variables=variables,
        level=ValidationLevel.RUN,
    )
    assert vr_result.error_num == 0
    # 3.
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


def test_workflow_engine_simple_python_molprops(basic_engine):
    # Arrange
    da, md = basic_engine
    # Make sure files that should be generated by the test
    # do not exist before we run the test.
    output_file_1 = "step1.out.smi"
    assert not project_file_exists(output_file_1)
    output_file_2 = "step2.out.smi"
    assert not project_file_exists(output_file_2)
    # And create the test's input file.
    input_file_1 = "input1.smi"
    input_file_1_content = """O=C(CSCc1ccc(Cl)s1)N1CCC(O)CC1
        RDKit          3D

    18 19  0  0  0  0  0  0  0  0999 V2000
        8.7102   -1.3539   24.2760 O   0  0  0  0  0  0  0  0  0  0  0  0
        9.4334   -2.1203   23.6716 C   0  0  0  0  0  0  0  0  0  0  0  0
    10.3260   -1.7920   22.4941 C   0  0  0  0  0  0  0  0  0  0  0  0
        9.5607   -0.5667   21.3699 S   0  0  0  0  0  0  0  0  0  0  0  0
        7.9641   -1.3976   21.0216 C   0  0  0  0  0  0  0  0  0  0  0  0
        7.1007   -0.5241   20.1671 C   0  0  0  0  0  0  0  0  0  0  0  0
        5.7930   -0.1276   20.3932 C   0  0  0  0  0  0  0  0  0  0  0  0
        5.2841    0.6934   19.3422 C   0  0  0  0  0  0  0  0  0  0  0  0
        6.2234    0.8796   18.3624 C   0  0  0  0  0  0  0  0  0  0  0  0
        6.0491    1.8209   16.9402 Cl  0  0  0  0  0  0  0  0  0  0  0  0
        7.6812    0.0795   18.6678 S   0  0  0  0  0  0  0  0  0  0  0  0
        9.5928   -3.4405   24.2306 N   0  0  0  0  0  0  0  0  0  0  0  0
    10.8197   -3.4856   25.0609 C   0  0  0  0  0  0  0  0  0  0  0  0
    11.0016   -4.9279   25.4571 C   0  0  0  0  0  0  0  0  0  0  0  0
        9.9315   -5.2800   26.4615 C   0  0  0  0  0  0  0  0  0  0  0  0
    10.3887   -4.7677   27.7090 O   0  0  0  0  0  0  0  0  0  0  0  0
        8.5793   -4.6419   26.1747 C   0  0  0  0  0  0  0  0  0  0  0  0
        8.3826   -4.0949   24.7695 C   0  0  0  0  0  0  0  0  0  0  0  0
    1  2  2  0
    2  3  1  0
    2 12  1  0
    3  4  1  0
    4  5  1  0
    5  6  1  0
    6  7  2  0
    7  8  1  0
    8  9  2  0
    9 10  1  0
    9 11  1  0
    11  6  1  0
    12 13  1  0
    13 14  1  0
    14 15  1  0
    15 16  1  0
    15 17  1  0
    17 18  1  0
    18 12  1  0
    M  END

    $$$$
    """
    with open(
        f"{EXECUTION_DIRECTORY}/{input_file_1}", mode="wt", encoding="utf8"
    ) as input_file:
        input_file.writelines(input_file_1_content)

    # Act
    r_wfid = start_workflow(
        md, da, "simple-python-molprops", {"candidateMolecules": input_file_1}
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
    assert project_file_exists(output_file_1)
    assert project_file_exists(output_file_2)


@pytest.mark.skip(reason="The engine does not currently create the required variables")
def test_workflow_engine_simple_python_molprops_with_option(basic_engine):
    # Arrange
    da, md = basic_engine
    # Make sure files that should be generated by the test
    # do not exist before we run the test.
    output_file_1 = "step1.out.smi"
    assert not project_file_exists(output_file_1)
    output_file_2 = "step2.out.smi"
    assert not project_file_exists(output_file_2)
    # And create the test's input file.
    input_file_1 = "input1.smi"
    input_file_1_content = """O=C(CSCc1ccc(Cl)s1)N1CCC(O)CC1
        RDKit          3D

    18 19  0  0  0  0  0  0  0  0999 V2000
        8.7102   -1.3539   24.2760 O   0  0  0  0  0  0  0  0  0  0  0  0
        9.4334   -2.1203   23.6716 C   0  0  0  0  0  0  0  0  0  0  0  0
    10.3260   -1.7920   22.4941 C   0  0  0  0  0  0  0  0  0  0  0  0
        9.5607   -0.5667   21.3699 S   0  0  0  0  0  0  0  0  0  0  0  0
        7.9641   -1.3976   21.0216 C   0  0  0  0  0  0  0  0  0  0  0  0
        7.1007   -0.5241   20.1671 C   0  0  0  0  0  0  0  0  0  0  0  0
        5.7930   -0.1276   20.3932 C   0  0  0  0  0  0  0  0  0  0  0  0
        5.2841    0.6934   19.3422 C   0  0  0  0  0  0  0  0  0  0  0  0
        6.2234    0.8796   18.3624 C   0  0  0  0  0  0  0  0  0  0  0  0
        6.0491    1.8209   16.9402 Cl  0  0  0  0  0  0  0  0  0  0  0  0
        7.6812    0.0795   18.6678 S   0  0  0  0  0  0  0  0  0  0  0  0
        9.5928   -3.4405   24.2306 N   0  0  0  0  0  0  0  0  0  0  0  0
    10.8197   -3.4856   25.0609 C   0  0  0  0  0  0  0  0  0  0  0  0
    11.0016   -4.9279   25.4571 C   0  0  0  0  0  0  0  0  0  0  0  0
        9.9315   -5.2800   26.4615 C   0  0  0  0  0  0  0  0  0  0  0  0
    10.3887   -4.7677   27.7090 O   0  0  0  0  0  0  0  0  0  0  0  0
        8.5793   -4.6419   26.1747 C   0  0  0  0  0  0  0  0  0  0  0  0
        8.3826   -4.0949   24.7695 C   0  0  0  0  0  0  0  0  0  0  0  0
    1  2  2  0
    2  3  1  0
    2 12  1  0
    3  4  1  0
    4  5  1  0
    5  6  1  0
    6  7  2  0
    7  8  1  0
    8  9  2  0
    9 10  1  0
    9 11  1  0
    11  6  1  0
    12 13  1  0
    13 14  1  0
    14 15  1  0
    15 16  1  0
    15 17  1  0
    17 18  1  0
    18 12  1  0
    M  END

    $$$$
    """
    with open(
        f"{EXECUTION_DIRECTORY}/{input_file_1}", mode="wt", encoding="utf8"
    ) as input_file:
        input_file.writelines(input_file_1_content)

    # Act
    r_wfid = start_workflow(
        md,
        da,
        "simple-python-molprops-with-options",
        {"candidateMolecules": input_file_1},
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
    assert project_file_exists(output_file_1)
    assert project_file_exists(output_file_2)
