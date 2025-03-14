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


def test_validate_minimal():
    # Arrange

    # Act
    error = decoder.validate_schema(_MINIMAL_WORKFLOW)

    # Assert
    assert error is None


def test_validate_minimal_get_step_names():
    # Arrange

    # Act
    names = decoder.get_step_names(_MINIMAL_WORKFLOW)

    # Assert
    assert names == ["step-1"]


def test_validate_without_name():
    # Arrange
    workflow = _MINIMAL_WORKFLOW.copy()
    _ = workflow.pop("name", None)

    # Act
    error = decoder.validate_schema(workflow)

    # Assert
    assert error == "'name' is a required property"


def test_validate_name_with_spaces():
    # Arrange
    workflow = _MINIMAL_WORKFLOW.copy()
    workflow["name"] = "workflow with spaces"

    # Act
    error = decoder.validate_schema(workflow)

    # Assert
    assert (
        error == "'workflow with spaces' does not match '^[a-z][a-z0-9-]{,63}(?<!-)$'"
    )


def test_validate_shortcut_example_1():
    # Arrange

    # Act
    error = decoder.validate_schema(_SHORTCUT_EXAMPLE_1_WORKFLOW)

    # Assert
    assert error is None
