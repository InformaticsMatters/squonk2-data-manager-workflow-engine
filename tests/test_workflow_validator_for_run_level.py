import os
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

from tests.wapi_adapter import UnitTestWorkflowAPIAdapter
from workflow.workflow_validator import ValidationLevel, WorkflowValidator


@pytest.fixture
def wapi():
    wapi_adapter = UnitTestWorkflowAPIAdapter()
    yield wapi_adapter


def test_validate_example_nop_file(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "example-nop-fail.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_duplicate_step_names(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "duplicate-step-names.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 2
    assert error.error_msg == ["Duplicate step names found: step-1"]


def test_validate_example_smiles_to_file(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "example-smiles-to-file.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
        variables={"smiles": "C", "outputFile": "blob.smi"},
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_example_two_step_nop(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "example-two-step-nop.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_shortcut_example_1(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "shortcut-example-1.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_simple_python_molprops(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "simple-python-molprops.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow
    variables = {"candidateMolecules": "input.sdf", "clusteredMolecules": "output.sdf"}

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
        variables=variables,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_simple_python_molprops_with_options_when_missing_required(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__),
        "workflow-definitions",
        "simple-python-molprops-with-options.yaml",
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow
    variables = {
        "candidateMolecules": "input.sdf",
        "clusteredMolecules": "output.sdf",
        "outputFile": "results.sdf",
        "rdkitPropertyName": "name",
    }

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
        variables=variables,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 8
    assert error.error_msg == [
        "Missing workflow variable values for: rdkitPropertyValue"
    ]


def test_validate_simple_python_molprops_with_options(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__),
        "workflow-definitions",
        "simple-python-molprops-with-options.yaml",
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow
    variables = {
        "candidateMolecules": "input.sdf",
        "clusteredMolecules": "output.sdf",
        "rdkitPropertyName": "col1",
        "rdkitPropertyValue": 123,
        "outputFile": "results.sdf",
    }

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
        variables=variables,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_simple_python_molprops_with_missing_input(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "simple-python-molprops.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow
    variables = {"clusteredMolecules": "output.sdf"}

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.RUN,
        workflow_definition=workflow,
        variables=variables,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 8
    assert error.error_msg == [
        "Missing workflow variable values for: candidateMolecules"
    ]
