import pytest

pytestmark = pytest.mark.unit

from tests.config import TEST_PROJECT_ID
from tests.wapi_adapter import UnitTestWorkflowAPIAdapter


def test_get_nop_job():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()

    # Act
    jd = utaa.get_job(
        collection="workflow-engine-unit-test-jobs", job="nop", version="1.0.0"
    )

    # Assert
    assert jd["command"] == "nop.py"


def test_get_unknown_workflow():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()

    # Act
    wfd = utaa.get_workflow(workflow_id="workflow-00000000-0000-0000-0000-000000000001")

    # Assert
    assert wfd == {}


def test_create_workflow():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()

    # Act
    response = utaa.create_workflow(workflow_definition={"name": "blah"})

    # Assert
    assert response["id"] == "workflow-00000000-0000-0000-0000-000000000001"


def test_get_workflow():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_workflow(workflow_definition={"name": "blah"})
    wfid = response["id"]

    # Act
    wf = utaa.get_workflow(workflow_id=wfid)

    # Assert
    assert wf["workflow"]["name"] == "blah"


def test_get_workflow_by_name():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    _ = utaa.create_workflow(workflow_definition={"name": "blah"})

    # Act
    response = utaa.get_workflow_by_name(name="blah", version="1.0.0")

    # Assert
    assert response["workflow"]["name"] == "blah"
    assert "id" in response


def test_create_running_workflow():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_workflow(workflow_definition={"name": "blah"})

    # Act
    response = utaa.create_running_workflow(
        user_id="dlister",
        workflow_id=response["id"],
        project_id=TEST_PROJECT_ID,
        variables={"x": 1},
    )

    # Assert
    assert response["id"] == "r-workflow-00000000-0000-0000-0000-000000000001"


def test_get_running_workflow():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_workflow(workflow_definition={"name": "blah"})
    wfid = response["id"]
    response = utaa.create_running_workflow(
        user_id="dlister",
        workflow_id=wfid,
        project_id=TEST_PROJECT_ID,
        variables={"x": 1},
    )
    rwfid = response["id"]

    # Act
    response = utaa.get_running_workflow(running_workflow_id=rwfid)

    # Assert
    rwf = response["running_workflow"]
    assert not rwf["done"]
    assert rwf["workflow"] == wfid
    assert rwf["variables"] == {"x": 1}


def test_set_running_workflow_done_when_success():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_workflow(workflow_definition={"name": "blah"})
    response = utaa.create_running_workflow(
        user_id="dlister",
        workflow_id=response["id"],
        project_id=TEST_PROJECT_ID,
        variables={},
    )
    rwfid = response["id"]

    # Act
    utaa.set_running_workflow_done(running_workflow_id=rwfid, success=True)

    # Assert
    response = utaa.get_running_workflow(running_workflow_id=rwfid)
    assert response["running_workflow"]["done"]
    assert response["running_workflow"]["success"]
    assert response["running_workflow"]["error"] is None
    assert response["running_workflow"]["error_msg"] is None


def test_set_running_workflow_done_when_failed():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_workflow(workflow_definition={"name": "blah"})
    response = utaa.create_running_workflow(
        user_id="dlister",
        workflow_id=response["id"],
        project_id=TEST_PROJECT_ID,
        variables={},
    )
    rwfid = response["id"]

    # Act
    utaa.set_running_workflow_done(
        running_workflow_id=rwfid, success=False, error=1, error_msg="Bang!"
    )

    # Assert
    response = utaa.get_running_workflow(running_workflow_id=rwfid)
    assert response["running_workflow"]["done"]
    assert not response["running_workflow"]["success"]
    assert response["running_workflow"]["error"] == 1
    assert response["running_workflow"]["error_msg"] == "Bang!"


def test_create_running_workflow_step():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_workflow(workflow_definition={"name": "blah"})
    response = utaa.create_running_workflow(
        user_id="dlister",
        workflow_id=response["id"],
        project_id=TEST_PROJECT_ID,
        variables={},
    )

    # Act
    response = utaa.create_running_workflow_step(
        running_workflow_id=response["id"], step="step-1"
    )

    # Assert
    assert response["id"] == "r-workflow-step-00000000-0000-0000-0000-000000000001"


def test_set_running_workflow_step_done_when_success():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_workflow(workflow_definition={"name": "blah"})
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
    utaa.set_running_workflow_step_done(running_workflow_step_id=rwfsid, success=True)

    # Assert
    response = utaa.get_running_workflow_step(running_workflow_step_id=rwfsid)
    assert response["running_workflow_step"]["done"]
    assert response["running_workflow_step"]["success"]
    assert response["running_workflow_step"]["error"] is None
    assert response["running_workflow_step"]["error_msg"] is None


def test_set_running_workflow_step_done_when_failed():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_workflow(workflow_definition={"name": "blah"})
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
    utaa.set_running_workflow_step_done(
        running_workflow_step_id=rwfsid, success=False, error=1, error_msg="Bang!"
    )

    # Assert
    response = utaa.get_running_workflow_step(running_workflow_step_id=rwfsid)
    assert response["running_workflow_step"]["done"]
    assert not response["running_workflow_step"]["success"]
    assert response["running_workflow_step"]["error"] == 1
    assert response["running_workflow_step"]["error_msg"] == "Bang!"


def test_get_running_workflow_step():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_workflow(workflow_definition={"name": "blah"})
    wfid = response["id"]
    response = utaa.create_running_workflow(
        user_id="dlister",
        workflow_id=wfid,
        project_id=TEST_PROJECT_ID,
        variables={},
    )
    rwfid = response["id"]
    response = utaa.create_running_workflow_step(
        running_workflow_id=rwfid, step="step-1"
    )
    rwfsid = response["id"]

    # Act
    response = utaa.get_running_workflow_step(running_workflow_step_id=rwfsid)

    # Assert
    rwfs = response["running_workflow_step"]
    assert rwfs["step"] == "step-1"
    assert not rwfs["done"]
    assert rwfs["running_workflow"] == rwfid


def test_get_running_workflow_steps():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_workflow(workflow_definition={"name": "blah"})
    wfid = response["id"]
    response = utaa.create_running_workflow(
        user_id="dlister",
        workflow_id=wfid,
        project_id=TEST_PROJECT_ID,
        variables={},
    )
    rwfid = response["id"]
    response = utaa.create_running_workflow_step(
        running_workflow_id=rwfid, step="step-1"
    )
    rwfsid = response["id"]

    # Act
    response = utaa.get_running_workflow_steps(running_workflow_id=rwfid)

    # Assert
    assert response["count"] == 1
    rwfs = response["running_workflow_steps"][0]
    assert rwfs["id"] == rwfsid
    assert rwfs["running_workflow_step"]["step"] == "step-1"
    assert not rwfs["running_workflow_step"]["done"]
    assert rwfs["running_workflow_step"]["running_workflow"] == rwfid


def test_create_instance():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()

    # Act
    response = utaa.create_instance(running_workflow_step_id="r-workflow-step-000")

    # Assert
    assert "id" in response


def test_create_and_get_instance():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_instance(running_workflow_step_id="r-workflow-step-000")
    instance_id = response["id"]

    # Act
    response = utaa.get_instance(instance_id=instance_id)

    # Assert
    assert response["running_workflow_step"] == "r-workflow-step-000"


def test_create_task():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()

    # Act
    response = utaa.create_task(instance_id="instance-000")

    # Assert
    assert "id" in response


def test_create_and_get_task():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_task(instance_id="instance-000")
    task_id = response["id"]

    # Act
    response = utaa.get_task(task_id=task_id)

    # Assert
    assert not response["done"]
    assert response["exit_code"] == 0
