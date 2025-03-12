import pytest

pytestmark = pytest.mark.unit

from tests.config import TEST_PROJECT_ID
from tests.wapi_adapter import UnitTestWorkflowAPIAdapter


def test_get_nop_job():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()

    # Act
    jd, _ = utaa.get_job(
        collection="workflow-engine-unit-test-jobs", job="nop", version="1.0.0"
    )

    # Assert
    assert jd["command"] == "nop.py"


def test_get_unknown_workflow():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()

    # Act
    wfd, _ = utaa.get_workflow(
        workflow_id="workflow-00000000-0000-0000-0000-000000000001"
    )

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
    wf, _ = utaa.get_workflow(workflow_id=wfid)

    # Assert
    assert wf["name"] == "blah"


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
    response, _ = utaa.get_running_workflow(running_workflow_id=rwfid)

    # Assert
    assert not response["done"]
    assert response["workflow"]["id"] == wfid
    assert response["variables"] == {"x": 1}


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
    response, _ = utaa.get_running_workflow(running_workflow_id=rwfid)
    assert response["done"]
    assert response["success"]
    assert response["error"] is None
    assert response["error_msg"] is None


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
    response, _ = utaa.get_running_workflow(running_workflow_id=rwfid)
    assert response["done"]
    assert not response["success"]
    assert response["error"] == 1
    assert response["error_msg"] == "Bang!"


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
    response, _ = utaa.create_running_workflow_step(
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
    response, _ = utaa.create_running_workflow_step(
        running_workflow_id=response["id"], step="step-1"
    )
    rwfsid = response["id"]

    # Act
    utaa.set_running_workflow_step_done(running_workflow_step_id=rwfsid, success=True)

    # Assert
    response, _ = utaa.get_running_workflow_step(running_workflow_step_id=rwfsid)
    assert response["done"]
    assert response["success"]
    assert response["error"] is None
    assert response["error_msg"] is None


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
    response, _ = utaa.create_running_workflow_step(
        running_workflow_id=response["id"], step="step-1"
    )
    rwfsid = response["id"]

    # Act
    utaa.set_running_workflow_step_done(
        running_workflow_step_id=rwfsid, success=False, error=1, error_msg="Bang!"
    )

    # Assert
    response, _ = utaa.get_running_workflow_step(running_workflow_step_id=rwfsid)
    assert response["done"]
    assert not response["success"]
    assert response["error"] == 1
    assert response["error_msg"] == "Bang!"


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
    response, _ = utaa.create_running_workflow_step(
        running_workflow_id=rwfid, step="step-1"
    )
    rwfsid = response["id"]

    # Act
    response, _ = utaa.get_running_workflow_step(running_workflow_step_id=rwfsid)

    # Assert
    assert response["name"] == "step-1"
    assert not response["done"]
    assert response["running_workflow"] == rwfid


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
    response, _ = utaa.get_instance(instance_id=instance_id)

    # Assert
    assert response["running_workflow_step"] == "r-workflow-step-000"
