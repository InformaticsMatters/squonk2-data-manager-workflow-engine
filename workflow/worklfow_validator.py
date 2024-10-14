"""The WorkflowEngine validation logic.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from .decoder import validate_schema
from .workflow_abc import DatabaseAdapter


class ValidationLevel(Enum):
    """Workflow validation levels."""

    CREATE = 1
    RUN = 2
    TAG = 3


@dataclass
class ValidationResult:
    """Workflow validation results."""

    error: int
    error_msg: Optional[List[str]]


# A successful validation result.
_VALIDATION_SUCCESS = ValidationResult(error=0, error_msg=None)


class WorkflowValidator:
    """The workflow validator. Typically used from teh context of the API
    to check workflow content prior to creation and execution.
    """

    def __init__(
        self,
        *,
        db_adapter: DatabaseAdapter,
    ):
        assert db_adapter

        self.db_adapter = db_adapter

    def validate(
        self,
        *,
        level: ValidationLevel,
        workflow_definition: Dict[str, Any],
        workflow_inputs: Optional[Dict[str, Any]],
    ) -> ValidationResult:
        """Validates the workflow definition (and inputs) based
        on the provided 'level'."""
        assert level in ValidationLevel
        assert isinstance(workflow_definition, dict)
        if workflow_inputs:
            assert isinstance(workflow_inputs, dict)

        if error := validate_schema(workflow_definition):
            return ValidationResult(error=1, error_msg=[error])

        return _VALIDATION_SUCCESS
