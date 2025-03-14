import os
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

from tests.test_decoder import _MINIMAL_WORKFLOW
from workflow.workflow_validator import ValidationLevel, WorkflowValidator


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


def test_validate_example_smiles_to_file_for_create():
    # Arrange
    workflow_file: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "example-smiles-to-file.yaml"
    )
    with open(workflow_file, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.CREATE,
        workflow_definition=workflow,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None
