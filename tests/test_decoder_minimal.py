# Tests for the decoder package.
from typing import Any, Dict

import pytest

pytestmark = pytest.mark.unit

from workflow import decoder

# A minimal Workflow Definition.
# Tests can use this and adjust accordingly.
_MINIMAL: Dict[str, Any] = {
    "kind": "DataManagerWorkflow",
    "kind-version": "2024.1",
    "name": "workflow-minimal",
    "steps": [
        {
            "name": "step-1",
            "specification": "{}",
        }
    ],
}


def test_validate_minimal():
    # Arrange

    # Act
    error = decoder.validate_schema(_MINIMAL)

    # Assert
    assert error is None


def test_validate_without_name():
    # Arrange
    workflow = _MINIMAL.copy()
    _ = workflow.pop("name", None)

    # Act
    error = decoder.validate_schema(workflow)

    # Assert
    assert error == "'name' is a required property"


def test_validate_name_with_spaces():
    # Arrange
    workflow = _MINIMAL.copy()
    workflow["name"] = "workflow with spaces"

    # Act
    error = decoder.validate_schema(workflow)

    # Assert
    assert (
        error == "'workflow with spaces' does not match '^[a-z][a-z0-9-]{,63}(?<!-)$'"
    )
