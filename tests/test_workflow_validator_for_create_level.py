import os
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

from tests.test_decoder import _MINIMAL_WORKFLOW
from workflow.workflow_validator import ValidationLevel, WorkflowValidator


def test_validate_minimal():
    # Arrange

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.CREATE,
        workflow_definition=_MINIMAL_WORKFLOW,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_example_nop_file():
    # Arrange
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "example-nop-fail.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
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


def test_validate_example_smiles_to_file():
    # Arrange
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "example-smiles-to-file.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
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


def test_validate_example_two_step_nop():
    # Arrange
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "example-two-step-nop.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
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


def test_validate_shortcut_example_1():
    # Arrange
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "shortcut-example-1.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
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


def test_validate_simple_python_molprops():
    # Arrange
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "simple-python-molprops.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
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


def test_validate_simple_python_molprops_with_options():
    # Arrange
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__),
        "workflow-definitions",
        "simple-python-molprops-with-options.yaml",
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
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
