# Tests for the decoder package.

import pytest

pytestmark = pytest.mark.unit

from tests.api_adapter import UnitTestAPIAdapter


def test_get_nop_job():
    # Arrange
    utaa = UnitTestAPIAdapter()

    # Act
    jd = utaa.get_job(
        collection="workflow-engine-unit-test-jobs", job="nop", version="1.0.0"
    )

    # Assert
    assert jd["command"] == "python --version"


def test_get_unknown_workflow():
    # Arrange
    utaa = UnitTestAPIAdapter()

    # Act
    wfd = utaa.get_workflow(
        workflow_definition_id="workflow-00000000-0000-0000-0000-000000000001"
    )

    # Assert
    assert wfd == {}


def test_save_workflow():
    # Arrange
    utaa = UnitTestAPIAdapter()

    # Act
    wfid = utaa.save_workflow(workflow_definition={"name": "blah"})

    # Assert
    assert wfid == {"id": "workflow-00000000-0000-0000-0000-000000000001"}


def test_get_workflow():
    # Arrange
    utaa = UnitTestAPIAdapter()
    response = utaa.save_workflow(workflow_definition={"name": "blah"})
    wfid = response["id"]

    # Act
    wf = utaa.get_workflow(workflow_definition_id=wfid)

    # Assert
    assert wf["workflow"]["name"] == "blah"


def test_get_workflow_by_name():
    # Arrange
    utaa = UnitTestAPIAdapter()
    _ = utaa.save_workflow(workflow_definition={"name": "blah"})

    # Act
    response = utaa.get_workflow_by_name(name="blah", version="1.0.0")

    # Assert
    assert response["workflow"]["name"] == "blah"
    assert "id" in response
