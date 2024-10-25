# Tests for the decoder package.

import pytest

pytestmark = pytest.mark.unit

from tests.database_adapter import UnitTestDatabaseAdapter


def test_get_nop_job():
    # Arrange
    utda = UnitTestDatabaseAdapter()

    # Act
    jd = utda.get_job(
        collection="workflow-engine-unit-test-jobs", job="nop", version="1.0.0"
    )

    # Assert
    assert jd["command"] == "python --version"


def test_get_unknown_workflow():
    # Arrange
    utda = UnitTestDatabaseAdapter()

    # Act
    wfid = utda.get_workflow(
        workflow_definition_id="workflow-00000000-0000-0000-0000-000000000001"
    )

    # Assert
    assert wfid is None


def test_save_workflow():
    # Arrange
    utda = UnitTestDatabaseAdapter()

    # Act
    wfid = utda.save_workflow(workflow_definition={"name": "blah"})

    # Assert
    assert wfid == "workflow-00000000-0000-0000-0000-000000000001"


def test_get_workflow():
    # Arrange
    utda = UnitTestDatabaseAdapter()
    wfid = utda.save_workflow(workflow_definition={"name": "blah"})

    # Act
    wf = utda.get_workflow(workflow_definition_id=wfid)

    # Assert
    assert wf == {"name": "blah"}


def test_get_workflow_by_name():
    # Arrange
    utda = UnitTestDatabaseAdapter()
    wfid = utda.save_workflow(workflow_definition={"name": "blah"})

    # Act
    wf = utda.get_workflow_by_name(name="blah", version="1.0.0")

    # Assert
    assert wf == {"name": "blah"}
