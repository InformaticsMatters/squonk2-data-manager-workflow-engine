import pytest

pytestmark = pytest.mark.unit

from tests.test_decoder_minimal import _MINIMAL_WORKFLOW
from workflow.worklfow_validator import ValidationLevel, WorkflowValidator


def test_validate_minimal_for_create():
    # Arrange

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.CREATE,
        workflow_definition=_MINIMAL_WORKFLOW,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None
