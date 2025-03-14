"""The WorkflowEngine validation logic."""

import json
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

        # ALl levels require a schema validation
        if error := validate_schema(workflow_definition):
            return ValidationResult(error_num=1, error_msg=[error])

        if level == ValidationLevel.RUN:
            run_level_result: ValidationResult = WorkflowValidator._validate_run_level(
                workflow_definition=workflow_definition,
                workflow_inputs=workflow_inputs,
            )
            if run_level_result.error_num:
                return run_level_result

        return _VALIDATION_SUCCESS

    @classmethod
    def _validate_run_level(
        cls,
        *,
        workflow_definition: dict[str, Any],
        workflow_inputs: dict[str, Any] | None = None,
    ) -> ValidationResult:
        assert workflow_definition
        del workflow_inputs

        # RUN level requires that the specification is a valid JSON string.
        # and contains properties for 'collection', 'job', and 'version'.
        try:
            specification = json.loads(workflow_definition["specification"])
        except json.decoder.JSONDecodeError as e:
            return ValidationResult(
                error_num=1,
                error_msg=[
                    f"Error decoding specification, which is not valid JSON: {e}"
                ],
            )
        except TypeError as e:
            return ValidationResult(
                error_num=2,
                error_msg=[
                    f"Error decoding specification, which is not valid JSON: {e}"
                ],
            )
        expected_keys: set[str] = {"collection", "job", "version"}
        missing_keys: list[str] = []
        missing_keys.extend(
            expected_key
            for expected_key in expected_keys
            if expected_key not in specification
        )
        if missing_keys:
            return ValidationResult(
                error_num=2,
                error_msg=[f"Specification is missing: {', '.join(missing_keys)}"],
            )

        return _VALIDATION_SUCCESS
