# Tests for the decoder package.
from typing import Any, Dict

import pytest

pytestmark = pytest.mark.unit

from tests.test_decoder_minimal import _MINIMAL_WORKFLOW
from workflow.worklfow_validator import (
    DatabaseAdapter,
    ValidationLevel,
    WorkflowValidator,
)


class DummyDatabaseAdapter(DatabaseAdapter):
    """A minimal Database adapter simply to satisfy the tests in this file."""

    def get_workflow(self, *, workflow_definition_id: str) -> Dict[str, Any]:
        return _MINIMAL_WORKFLOW

    def get_workflow_by_name(self, *, name: str, version: str) -> Dict[str, Any]:
        return _MINIMAL_WORKFLOW

    def get_job(self, *, collection: str, job: str, version: str) -> Dict[str, Any]:
        return {}


def test_validate_minimal_for_create():
    # Arrange
    db_adapter = DummyDatabaseAdapter()
    validator = WorkflowValidator(db_adapter=db_adapter)

    # Act
    error = validator.validate(
        level=ValidationLevel.CREATE,
        workflow_definition=_MINIMAL_WORKFLOW,
    )

    # Assert
    assert error.error == 0
    assert error.error_msg is None
