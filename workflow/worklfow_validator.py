"""The WorkflowEngine validation logic."""

from dataclasses import dataclass
from enum import Enum
from typing import Any

from .decoder import validate_schema


class ValidationLevel(Enum):
    """Workflow validation levels."""

    CREATE = 1
    RUN = 2
    TAG = 3


@dataclass
class ValidationResult:
    """Workflow validation results."""

    error_num: int
    error_msg: list[str] | None


# Handy successful results
_VALIDATION_SUCCESS = ValidationResult(error_num=0, error_msg=None)


class WorkflowValidator:
    """The workflow validator. Typically used from the context of the API
    to check workflow content prior to creation and execution.
    """

    @classmethod
    def validate(
        cls,
        *,
        level: ValidationLevel,
        workflow_definition: dict[str, Any],
        workflow_inputs: dict[str, Any] | None = None,
    ) -> ValidationResult:
        """Validates the workflow definition (and inputs)
        based on the provided 'level'."""
        assert level in ValidationLevel
        assert isinstance(workflow_definition, dict)
        if workflow_inputs:
            assert isinstance(workflow_inputs, dict)

        if error := validate_schema(workflow_definition):
            return ValidationResult(error_num=1, error_msg=[error])

        return _VALIDATION_SUCCESS
