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


def test_get_running_steps_when_none_running():
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
    response, _ = utaa.get_running_steps(running_workflow_id=rwfid)

    # Assert
    assert response["count"] == 0
    assert response["steps"] == []


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
    assert response["error_num"] is None
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
        running_workflow_id=rwfid, success=False, error_num=1, error_msg="Bang!"
    )

    # Assert
    response, _ = utaa.get_running_workflow(running_workflow_id=rwfid)
    assert response["done"]
    assert not response["success"]
    assert response["error_num"] == 1
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


def test_set_running_workflow_step_variables():
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
    utaa.set_running_workflow_step_variables(
        running_workflow_step_id=rwfsid, variables={"z": 42}
    )

    # Assert
    response, _ = utaa.get_running_workflow_step(running_workflow_step_id=rwfsid)
    assert response["variables"] == {"z": 42}


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
    assert response["error_num"] is None
    assert response["error_msg"] is None
    assert response["variables"] == {}


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
        running_workflow_step_id=rwfsid, success=False, error_num=1, error_msg="Bang!"
    )

    # Assert
    response, _ = utaa.get_running_workflow_step(running_workflow_step_id=rwfsid)
    assert response["done"]
    assert not response["success"]
    assert response["error_num"] == 1
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
    assert response["running_workflow"]["id"] == rwfid
    assert "prior_running_workflow_step" not in response


def test_get_running_workflow_step_with_prior_step():
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
        running_workflow_id=rwfid,
        step="step-1",
        prior_running_workflow_step_id="r-workflow-step-111",
    )
    rwfsid = response["id"]

    # Act
    response, _ = utaa.get_running_workflow_step(running_workflow_step_id=rwfsid)

    # Assert
    assert response["name"] == "step-1"
    assert not response["done"]
    assert response["running_workflow"]["id"] == rwfid
    assert "prior_running_workflow_step" in response
    assert response["prior_running_workflow_step"]["id"] == "r-workflow-step-111"


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
    assert response["running_workflow_step_id"] == "r-workflow-step-000"


def test_get_workflow_steps_driving_this_step_when_1st_step():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_workflow(
        workflow_definition={
            "name": "blah",
            "steps": [{"name": "step-1"}, {"name": "step-2"}, {"name": "step-3"}],
        }
    )
    response = utaa.create_running_workflow(
        user_id="dlister",
        workflow_id=response["id"],
        project_id=TEST_PROJECT_ID,
        variables={},
    )
    response, _ = utaa.create_running_workflow_step(
        running_workflow_id=response["id"], step="step-1"
    )
    rwfs_id = response["id"]

    # Act
    response, _ = utaa.get_workflow_steps_driving_this_step(
        running_workflow_step_id=rwfs_id
    )

    # Assert
    assert response["caller_step_index"] == 0
    assert len(response["steps"]) == 3
    assert response["steps"][0]["name"] == "step-1"
    assert response["steps"][1]["name"] == "step-2"
    assert response["steps"][2]["name"] == "step-3"


def test_get_workflow_steps_driving_this_step_when_2nd_step():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_workflow(
        workflow_definition={
            "name": "blah",
            "steps": [{"name": "step-1"}, {"name": "step-2"}, {"name": "step-3"}],
        }
    )
    response = utaa.create_running_workflow(
        user_id="dlister",
        workflow_id=response["id"],
        project_id=TEST_PROJECT_ID,
        variables={},
    )
    response, _ = utaa.create_running_workflow_step(
        running_workflow_id=response["id"], step="step-2"
    )
    rwfs_id = response["id"]

    # Act
    response, _ = utaa.get_workflow_steps_driving_this_step(
        running_workflow_step_id=rwfs_id
    )

    # Assert
    assert response["caller_step_index"] == 1
    assert len(response["steps"]) == 3
    assert response["steps"][0]["name"] == "step-1"
    assert response["steps"][1]["name"] == "step-2"
    assert response["steps"][2]["name"] == "step-3"


def test_get_running_workflow_step_by_name():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()
    response = utaa.create_workflow(
        workflow_definition={
            "name": "blah",
            "steps": [{"name": "step-1"}, {"name": "step-2"}, {"name": "step-3"}],
        }
    )
    response = utaa.create_running_workflow(
        user_id="dlister",
        workflow_id=response["id"],
        project_id=TEST_PROJECT_ID,
        variables={},
    )
    rwf_id = response["id"]
    response, _ = utaa.create_running_workflow_step(
        running_workflow_id=rwf_id, step="step-2"
    )
    rwfs_id = response["id"]

    # Act
    response, _ = utaa.get_running_workflow_step_by_name(
        name="step-2", running_workflow_id=rwf_id
    )

    # Assert
    assert response["running_workflow"]["id"] == rwf_id
    assert response["name"] == "step-2"
    assert response["id"] == rwfs_id


def test_mock_get_running_workflow_step_output_values_for_output():
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

    # Act
    utaa.mock_get_running_workflow_step_output_values_for_output(
        step_name="step-1", output_variable="results", output=["a", "b"]
    )

    # Assert
    response, _ = utaa.get_running_workflow_step_output_values_for_output(
        running_workflow_step_id="r-workflow-step-00000000-0000-0000-0000-000000000001",
        output_variable="results",
    )
    assert "output" in response
    assert len(response["output"]) == 2
    assert "a" in response["output"]
    assert "b" in response["output"]


def test_basic_get_running_workflow_step_output_values_for_output_when_step_variable_name_unknown():
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

    # Act
    utaa.mock_get_running_workflow_step_output_values_for_output(
        step_name="step-1", output_variable="results", output=["a", "b"]
    )

    # Assert
    with pytest.raises(AssertionError):
        _, _ = utaa.get_running_workflow_step_output_values_for_output(
            running_workflow_step_id="r-workflow-step-00000000-0000-0000-0000-000000000001",
            output_variable="unknownVariable",
        )


def test_basic_get_running_workflow_step_output_values_for_output_when_step_unknown():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()

    # Act
    with pytest.raises(AssertionError):
        _, _ = utaa.get_running_workflow_step_output_values_for_output(
            running_workflow_step_id="r-workflow-step-00000000-0000-0000-0000-000000000001",
            output_variable="outputFile",
        )

    # Assert


def test_basic_realise():
    # Arrange
    utaa = UnitTestWorkflowAPIAdapter()

    # Act
    response, _ = utaa.realise_outputs(
        running_workflow_step_id="r-workflow-step-00000000-0000-0000-0000-000000000001",
    )

    # Assert
    assert not response
