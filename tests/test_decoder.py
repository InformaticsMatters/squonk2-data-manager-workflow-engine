import os
from typing import Any, Dict

import pytest
import yaml

pytestmark = pytest.mark.unit

from workflow import decoder

# Pre-load some example workflows...
_MINIMAL_WORKFLOW_FILE: str = os.path.join(
    os.path.dirname(__file__), "workflow-definitions", "minimal.yaml"
)
with open(_MINIMAL_WORKFLOW_FILE, "r", encoding="utf8") as workflow_file:
    _MINIMAL_WORKFLOW: Dict[str, Any] = yaml.safe_load(workflow_file)
assert _MINIMAL_WORKFLOW

_SHORTCUT_EXAMPLE_1_WORKFLOW_FILE: str = os.path.join(
    os.path.dirname(__file__), "workflow-definitions", "shortcut-example-1.yaml"
)
with open(_SHORTCUT_EXAMPLE_1_WORKFLOW_FILE, "r", encoding="utf8") as workflow_file:
    _SHORTCUT_EXAMPLE_1_WORKFLOW: Dict[str, Any] = yaml.safe_load(workflow_file)
assert _SHORTCUT_EXAMPLE_1_WORKFLOW

_SIMPLE_PYTHON_MOLPROPS_WORKFLOW_FILE: str = os.path.join(
    os.path.dirname(__file__), "workflow-definitions", "simple-python-molprops.yaml"
)
with open(_SIMPLE_PYTHON_MOLPROPS_WORKFLOW_FILE, "r", encoding="utf8") as workflow_file:
    _SIMPLE_PYTHON_MOLPROPS_WORKFLOW: Dict[str, Any] = yaml.safe_load(workflow_file)
assert _SIMPLE_PYTHON_MOLPROPS_WORKFLOW

_DUPLICATE_WORKFLOW_VARIABLE_NAMES_WORKFLOW_FILE: str = os.path.join(
    os.path.dirname(__file__),
    "workflow-definitions",
    "duplicate-workflow-variable-names.yaml",
)
with open(
    _DUPLICATE_WORKFLOW_VARIABLE_NAMES_WORKFLOW_FILE, "r", encoding="utf8"
) as workflow_file:
    _DUPLICATE_WORKFLOW_VARIABLE_NAMES_WORKFLOW: Dict[str, Any] = yaml.safe_load(
        workflow_file
    )
assert _DUPLICATE_WORKFLOW_VARIABLE_NAMES_WORKFLOW

_STEP_SPECIFICATION_VARIABLE_NAMES_WORKFLOW_FILE: str = os.path.join(
    os.path.dirname(__file__),
    "workflow-definitions",
    "step-specification-variable-names.yaml",
)
with open(
    _STEP_SPECIFICATION_VARIABLE_NAMES_WORKFLOW_FILE, "r", encoding="utf8"
) as workflow_file:
    _STEP_SPECIFICATION_VARIABLE_NAMES_WORKFLOW: Dict[str, Any] = yaml.safe_load(
        workflow_file
    )
assert _STEP_SPECIFICATION_VARIABLE_NAMES_WORKFLOW

_WORKFLOW_OPTIONS_WORKFLOW_FILE: str = os.path.join(
    os.path.dirname(__file__),
    "workflow-definitions",
    "workflow-options.yaml",
)
with open(_WORKFLOW_OPTIONS_WORKFLOW_FILE, "r", encoding="utf8") as workflow_file:
    _WORKFLOW_OPTIONS: Dict[str, Any] = yaml.safe_load(workflow_file)
assert _WORKFLOW_OPTIONS


def test_validate_schema_for_minimal():
    # Arrange

    # Act
    error = decoder.validate_schema(_MINIMAL_WORKFLOW)

    # Assert
    assert error is None


def test_minimal_get_step_names():
    # Arrange

    # Act
    names = decoder.get_step_names(_MINIMAL_WORKFLOW)

    # Assert
    assert names == ["step-1"]


def test_workflow_without_name():
    # Arrange
    workflow = _MINIMAL_WORKFLOW.copy()
    _ = workflow.pop("name", None)

    # Act
    error = decoder.validate_schema(workflow)

    # Assert
    assert error == "'name' is a required property"


def test_workflow_name_with_spaces():
    # Arrange
    workflow = _MINIMAL_WORKFLOW.copy()
    workflow["name"] = "workflow with spaces"

    # Act
    error = decoder.validate_schema(workflow)

    # Assert
    assert (
        error == "'workflow with spaces' does not match '^[a-z][a-z0-9-]{,63}(?<!-)$'"
    )


def test_validate_schema_for_shortcut_example_1():
    # Arrange

    # Act
    error = decoder.validate_schema(_SHORTCUT_EXAMPLE_1_WORKFLOW)

    # Assert
    assert error is None


def test_validate_schema_for_python_simple_molprops():
    # Arrange

    # Act
    error = decoder.validate_schema(_SIMPLE_PYTHON_MOLPROPS_WORKFLOW)

    # Assert
    assert error is None


def test_validate_schema_for_step_specification_variable_names():
    # Arrange

    # Act
    error = decoder.validate_schema(_STEP_SPECIFICATION_VARIABLE_NAMES_WORKFLOW)

    # Assert
    assert error is None


def test_validate_schema_for_workflow_options():
    # Arrange

    # Act
    error = decoder.validate_schema(_WORKFLOW_OPTIONS)

    # Assert
    assert error is None


def test_get_workflow_variables_for_smiple_python_molprops():
    # Arrange

    # Act
    wf_variables = decoder.get_variable_names(_SIMPLE_PYTHON_MOLPROPS_WORKFLOW)

    # Assert
    assert len(wf_variables) == 2
    assert "candidateMolecules" in wf_variables
    assert "clusteredMolecules" in wf_variables


def test_get_workflow_description():
    # Arrange

    # Act
    description = decoder.get_description(_SIMPLE_PYTHON_MOLPROPS_WORKFLOW)

    # Assert
    assert description == "A simple python experimental workflow"


def test_get_workflow_name():
    # Arrange

    # Act
    name = decoder.get_name(_SIMPLE_PYTHON_MOLPROPS_WORKFLOW)

    # Assert
    assert name == "python-workflow"


def test_get_workflow_steps():
    # Arrange

    # Act
    steps = decoder.get_steps(_SIMPLE_PYTHON_MOLPROPS_WORKFLOW)

    # Assert
    assert len(steps) == 2
    assert steps[0]["name"] == "step1"
    assert steps[1]["name"] == "step2"


def test_get_workflow_variables_for_duplicate_variables():
    # Arrange

    # Act
    names = decoder.get_variable_names(_DUPLICATE_WORKFLOW_VARIABLE_NAMES_WORKFLOW)

    # Assert
    assert len(names) == 2
    assert names[0] == "x"
    assert names[1] == "x"
