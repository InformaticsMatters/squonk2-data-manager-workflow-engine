import os
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

from workflow.workflow_validator import ValidationLevel, WorkflowValidator


def test_validate_example_nop_file():
    # Arrange
    workflow_file: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "example-nop-fail.yaml"
    )
    with open(workflow_file, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_duplicate_step_names():
    # Arrange
    workflow_file: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "duplicate-step-names.yaml"
    )
    with open(workflow_file, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
    )

    # Assert
    assert error.error_num == 2
    assert error.error_msg == ["Duplicate step names found: step-1"]


def test_validate_example_smiles_to_file():
    # Arrange
    workflow_file: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "example-smiles-to-file.yaml"
    )
    with open(workflow_file, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_example_two_step_nop():
    # Arrange
    workflow_file: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "example-two-step-nop.yaml"
    )
    with open(workflow_file, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_shortcut_example_1():
    # Arrange
    workflow_file: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "shortcut-example-1.yaml"
    )
    with open(workflow_file, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_simple_python_molprops():
    # Arrange
    workflow_file: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "simple-python-molprops.yaml"
    )
    with open(workflow_file, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow
    variables = {"candidateMolecules": "input.sdf", "clusteredMolecules": "output.sdf"}

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
        variables=variables,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_simple_python_molprops_with_options_when_missing_required():
    # Arrange
    workflow_file: str = os.path.join(
        os.path.dirname(__file__),
        "workflow-definitions",
        "simple-python-molprops-with-options.yaml",
    )
    with open(workflow_file, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow
    variables = {
        "candidateMolecules": "input.sdf",
        "clusteredMolecules": "output.sdf",
    }

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
        variables=variables,
    )

    # Assert
    assert error.error_num == 7
    assert error.error_msg == [
        "Missing workflow variable values for: rdkitPropertyValue"
    ]


def test_validate_simple_python_molprops_with_options():
    # Arrange
    workflow_file: str = os.path.join(
        os.path.dirname(__file__),
        "workflow-definitions",
        "simple-python-molprops-with-options.yaml",
    )
    with open(workflow_file, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow
    variables = {
        "candidateMolecules": "input.sdf",
        "clusteredMolecules": "output.sdf",
        "rdkitPropertyName": "col1",
        "rdkitPropertyValue": 123,
    }

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
        variables=variables,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_simple_python_molprops_with_missing_input():
    # Arrange
    workflow_file: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "simple-python-molprops.yaml"
    )
    with open(workflow_file, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow
    variables = {"clusteredMolecules": "output.sdf"}

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
        variables=variables,
    )

    # Assert
    assert error.error_num == 7
    assert error.error_msg == [
        "Missing workflow variable values for: candidateMolecules"
    ]


def test_validate_duplicate_workflow_variable_names():
    # Arrange
    workflow_file: str = os.path.join(
        os.path.dirname(__file__),
        "workflow-definitions",
        "duplicate-workflow-variable-names.yaml",
    )
    with open(workflow_file, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.TAG,
        workflow_definition=workflow,
    )

    # Assert
    assert error.error_num == 6
    assert error.error_msg == ["Duplicate workflow variable names found: x"]
