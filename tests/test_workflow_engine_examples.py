import os
import time
from datetime import datetime, timezone
from pprint import pprint
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

from informaticsmatters.protobuf.datamanager.pod_message_pb2 import PodMessage
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

    yield [message_dispatcher, wapi_adapter]

    print("Stopping message queue...")
    message_queue.stop()
    message_queue.join()
    print("Stopped")


@pytest.fixture
def manual_engine():
    """An engine whose message queue is NOT running, so nothing consumes the
    PodMessages its launcher produces. Messages have to be handed to
    'handle_message()' by the test, which lets a test observe exactly what the
    engine did in response to one message."""
    wapi_adapter = UnitTestWorkflowAPIAdapter()
    message_queue = UnitTestMessageQueue()
    message_dispatcher = UnitTestMessageDispatcher(msg_queue=message_queue)
    instance_launcher = UnitTestInstanceLauncher(
        wapi_adapter=wapi_adapter, msg_dispatcher=message_dispatcher
    )
    workflow_engine = WorkflowEngine(
        wapi_adapter=wapi_adapter, instance_launcher=instance_launcher
    )
    return [workflow_engine, wapi_adapter]


def create_running_workflow(da, workflow_file_name: str, variables=None) -> str:
    """Loads a workflow definition and creates a RunningWorkflow record from it,
    returning the running workflow ID. Unlike 'start_workflow()' this sends no
    START message - the caller decides how the engine gets to hear about it."""
    workflow_path = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", f"{workflow_file_name}.yaml"
    )
    with open(workflow_path, "r", encoding="utf8") as wf_file:
        wf_definition = yaml.load(wf_file, Loader=yaml.FullLoader)
    assert wf_definition
    wf_response = da.create_workflow(workflow_definition=wf_definition)
    response = da.create_running_workflow(
        user_id="dlister",
        workflow_id=wf_response["id"],
        project_id=TEST_PROJECT_ID,
        variables=variables or {},
    )
    return str(response["id"])


def pod_message_for(instance_id: str, exit_code: int = 0) -> PodMessage:
    """Builds the PodMessage the DM would send when an Instance finishes."""
    msg = PodMessage()
    msg.timestamp = f"{datetime.now(timezone.utc).isoformat()}Z"
    msg.phase = "Completed"
    msg.instance = instance_id
    msg.has_exit_code = True
    msg.exit_code = exit_code
    return msg


def assert_each_step_launched_once(da, r_wfid) -> None:
    """The engine re-assesses every step each time it handles a message, so a
    step that has already run must never be launched a second time."""
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    launches = [
        (step["name"], step.get("replica", 0))
        for step in response["running_workflow_steps"]
    ]
    assert len(launches) == len(set(launches)), f"Duplicate step launches: {launches}"


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
        wapi_adapter=da,
        variables=variables,
        level=ValidationLevel.RUN,
    )
    print("vr_result", vr_result)
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
    response = None
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
    assert response
    assert response["done"]
    assert response["success"] == expect_success


def test_workflow_engine_start_launches_every_ready_step(manual_engine):
    """Two steps, neither depending on the other, must BOTH be launched by the
    single START message - i.e. without waiting for a PodMessage. The queue is
    not running here, so the two step records can only have come from START."""
    # Arrange
    we, da = manual_engine
    r_wfid = create_running_workflow(da, "example-two-independent-nops")
    msg = WorkflowMessage()
    msg.timestamp = f"{datetime.now(timezone.utc).isoformat()}Z"
    msg.action = "START"
    msg.running_workflow = r_wfid

    # Act
    we.handle_message(msg)

    # Assert
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert response["count"] == 2
    assert {step["name"] for step in response["running_workflow_steps"]} == {
        "step-1",
        "step-2",
    }


def test_workflow_engine_start_only_launches_ready_steps(manual_engine):
    """The counterpart to the test above - a step that depends on another must
    NOT be launched by START, even when it is the first in the definition."""
    # Arrange
    we, da = manual_engine
    r_wfid = create_running_workflow(da, "example-steps-out-of-order")
    msg = WorkflowMessage()
    msg.timestamp = f"{datetime.now(timezone.utc).isoformat()}Z"
    msg.action = "START"
    msg.running_workflow = r_wfid

    # Act
    we.handle_message(msg)

    # Assert
    # "consumer" is declared first but depends on "provider", so only
    # "provider" is READY.
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert response["count"] == 1
    assert response["running_workflow_steps"][0]["name"] == "provider"


def test_workflow_engine_example_two_independent_nops(basic_engine):
    # Arrange
    md, da = basic_engine

    # Act
    r_wfid = start_workflow(md, da, "example-two-independent-nops", {})

    # Assert
    wait_for_workflow(da, r_wfid)
    # Additional, detailed checks...
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert response["count"] == 2
    for step in response["running_workflow_steps"]:
        assert step["done"]
        assert step["success"]
    assert_each_step_launched_once(da, r_wfid)


def test_workflow_engine_example_steps_out_of_order(basic_engine):
    """The step that runs first is the one that is READY, not the one that
    happens to be first in the definition."""
    # Arrange
    md, da = basic_engine

    # Act
    r_wfid = start_workflow(md, da, "example-steps-out-of-order", {})

    # Assert
    wait_for_workflow(da, r_wfid)
    # Additional, detailed checks...
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert response["count"] == 2
    for step in response["running_workflow_steps"]:
        assert step["done"]
        assert step["success"]
    # The provider is second in the definition but must have been launched first.
    launch_order = [step["name"] for step in response["running_workflow_steps"]]
    assert launch_order == ["provider", "consumer"]
    assert_each_step_launched_once(da, r_wfid)


def test_workflow_engine_example_diamond(basic_engine):
    """One step fans out to two, which fan back in to a fourth."""
    # Arrange
    md, da = basic_engine
    assert not project_file_exists("merged.out")

    # Act
    r_wfid = start_workflow(md, da, "example-diamond", {})

    # Assert
    wait_for_workflow(da, r_wfid)
    # Additional, detailed checks...
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert response["count"] == 4
    for step in response["running_workflow_steps"]:
        assert step["done"]
        assert step["success"]
    steps = {step["name"]: step for step in response["running_workflow_steps"]}
    assert set(steps) == {"split", "branch-a", "branch-b", "merge"}
    # The fan-in step must have taken an input from each branch...
    assert steps["merge"]["variables"]["inputFileA"] == "branch-a.out"
    assert steps["merge"]["variables"]["inputFileB"] == "branch-b.out"
    # ...and it must have been launched last, after both branches.
    launch_order = [step["name"] for step in response["running_workflow_steps"]]
    assert launch_order[0] == "split"
    assert launch_order[3] == "merge"
    assert_each_step_launched_once(da, r_wfid)
    # The merged file must contain the output of both branches
    assert project_file_exists("merged.out")


def test_workflow_engine_example_fan_in_waits_for_every_branch(manual_engine):
    """The fan-in step must not launch when only one of its two branches is
    done. Here only 'branch-a' is complete, so 'merge' must stay unlaunched."""
    # Arrange
    we, da = manual_engine
    r_wfid = create_running_workflow(da, "example-diamond")
    msg = WorkflowMessage()
    msg.timestamp = f"{datetime.now(timezone.utc).isoformat()}Z"
    msg.action = "START"
    msg.running_workflow = r_wfid
    we.handle_message(msg)
    # START launches 'split' only. Completing it makes both branches READY.
    steps = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert steps["count"] == 1
    split = steps["running_workflow_steps"][0]
    we.handle_message(pod_message_for(split["instance_id"]))
    # Both branches must now exist. Complete only one of them.
    steps = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert {s["name"] for s in steps["running_workflow_steps"]} == {
        "split",
        "branch-a",
        "branch-b",
    }
    branch_a = next(
        s for s in steps["running_workflow_steps"] if s["name"] == "branch-a"
    )

    # Act
    we.handle_message(pod_message_for(branch_a["instance_id"]))

    # Assert
    steps = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert "merge" not in {s["name"] for s in steps["running_workflow_steps"]}
    # And the workflow must not have been declared finished.
    running_workflow, _ = da.get_running_workflow(running_workflow_id=r_wfid)
    assert not running_workflow["done"]


def test_workflow_engine_example_unsatisfiable_step(basic_engine):
    """A step that can never become READY must fail the running workflow,
    not leave it hanging and not be mistaken for a successful finish."""
    # Arrange
    md, da = basic_engine

    # Act
    r_wfid = start_workflow(md, da, "example-unsatisfiable-step", {})

    # Assert
    wait_for_workflow(da, r_wfid, expect_success=False)
    # Additional, detailed checks...
    # The runnable step must still have run...
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    assert response["count"] == 1
    assert response["running_workflow_steps"][0]["name"] == "provider"
    # ...and the workflow must say which step it could not run.
    running_workflow, _ = da.get_running_workflow(running_workflow_id=r_wfid)
    assert "consumer" in running_workflow["error_msg"]


def test_workflow_engine_example_two_step_nop(basic_engine):
    # Arrange
    md, da = basic_engine

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
    md, da = basic_engine

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
    md, da = basic_engine
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


def test_workflow_engine_simple_python_molprops(basic_engine):
    # Arrange
    md, da = basic_engine

    da.mock_get_running_workflow_step_output_values_for_output(
        step_name="step2",
        output_variable="outputFile",
        output="step1.out.smi",
    )

    # Create the test's input file.
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

    # Make sure files that should be generated by the test
    # do not exist before we run the test.
    output_file_1 = "results.smi"
    assert not project_file_exists(output_file_1)
    output_file_2 = "clustered-results.smi"
    assert not project_file_exists(output_file_2)

    # Act
    r_wfid = start_workflow(
        md,
        da,
        "simple-python-molprops",
        {
            "candidateMolecules": input_file_1,
            "clusteredMolecules": "clustered-results.smi",
        },
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
    # This test should generate the expected file in the simulated project directory
    assert project_file_exists(output_file_2)


def test_workflow_engine_simple_python_molprops_with_options(basic_engine):
    # Arrange
    md, da = basic_engine

    da.mock_get_running_workflow_step_output_values_for_output(
        step_name="step1",
        output_variable="outputFile",
        output="step1.out.smi",
    )

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
        {
            "candidateMolecules": input_file_1,
            "clusteredMolecules": output_file_2,
            "rdkitPropertyName": "prop",
            "rdkitPropertyValue": 1.2,
        },
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


@pytest.mark.skip(reason="Relies on files in instance directories")
def test_workflow_engine_simple_python_split_combine(basic_engine):
    # Arrange
    md, da = basic_engine

    da.mock_get_running_workflow_step_output_values_for_output(
        step_name="split",
        output_variable="outputBase",
        output=["chunk_1.smi", "chunk_2.smi"],
    )

    # Make sure files that should be generated by the test
    # do not exist before we run the test.
    output_file_first = "chunk_1.smi"
    output_file_second = "chunk_2.smi"
    assert not project_file_exists(output_file_first)
    assert not project_file_exists(output_file_second)
    # And create the test's input file.
    input_file_1 = "input1.smi"
    input_file_1_content = """O=C(CSCc1ccc(Cl)s1)N1CCC(O)CC1
    COCN1C(=O)NC(C)(C)C1=O"""
    with open(
        f"{EXECUTION_DIRECTORY}/{input_file_1}", mode="wt", encoding="utf8"
    ) as input_file:
        input_file.writelines(input_file_1_content)

    # Act
    r_wfid = start_workflow(
        md,
        da,
        "simple-python-split-combine",
        {"candidateMolecules": input_file_1, "combination": "combination.smi"},
    )

    # Assert
    wait_for_workflow(da, r_wfid)
    # Additional, detailed checks...
    # Check we only have one RunningWorkflowStep, and it succeeded
    response = da.get_running_workflow_steps(running_workflow_id=r_wfid)
    print("response")
    pprint(response)

    assert response["count"] == 4
    rwf_steps = response["running_workflow_steps"]
    for rwf_step in rwf_steps:
        assert rwf_step["done"]
        assert rwf_step["success"]
