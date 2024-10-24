# Tests for the decoder package.

import pytest

pytestmark = pytest.mark.unit

from tests.instance_launcher import UnitTestInstanceLauncher


def test_get_nop_job():
    # Arrange
    util = UnitTestInstanceLauncher()

    # Act
    result = util.launch(
        project_id="project-00000000-0000-0000-0000-000000000000",
        workflow_id="workflow-00000000-0000-0000-0000-000000000000",
        workflow_definition={},
        step="step-1",
        step_specification={"job": "nop"},
        completion_callback=None,
    )

    # Assert
    assert result.error == 0
