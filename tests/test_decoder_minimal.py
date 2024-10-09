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
    "name": "test-minimal",
}


def test_validate_minimal():
    # Arrange

    # Act
    error = decoder.validate_schema(_MINIMAL)

    # Assert
    assert error is None
