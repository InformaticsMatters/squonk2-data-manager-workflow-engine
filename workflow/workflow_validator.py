"""The WorkflowEngine validation logic."""

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any

from .decoder import (
    get_required_variable_names,
    get_steps,
    get_variable_names,
    validate_schema,
)


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
        variables: dict[str, Any] | None = None,
    ) -> ValidationResult:
        """Validates the workflow definition (and inputs)
        based on the provided 'level'."""
        assert level in ValidationLevel
        assert isinstance(workflow_definition, dict)
        if variables:
            assert isinstance(variables, dict)

        # ALl levels need to pass schema validation
        if error := validate_schema(workflow_definition):
            return ValidationResult(error_num=1, error_msg=[error])

        # Now level-specific validation...
        if level in (ValidationLevel.TAG, ValidationLevel.RUN):
            level_result: ValidationResult = WorkflowValidator._validate_tag_level(
                workflow_definition=workflow_definition,
            )
            if level_result.error_num:
                return level_result
        if level == ValidationLevel.RUN:
            level_result = WorkflowValidator._validate_run_level(
                workflow_definition=workflow_definition,
                variables=variables,
            )
            if level_result.error_num:
                return level_result

        # OK if we get here
        return _VALIDATION_SUCCESS

    @classmethod
    def _validate_tag_level(
        cls,
        *,
        workflow_definition: dict[str, Any],
    ) -> ValidationResult:
        assert workflow_definition

        # TAG level requires that each step name is unique.
        duplicate_names: set[str] = set()
        step_names: set[str] = set()
        for step in get_steps(workflow_definition):
            step_name = step["name"]
            if step_name not in duplicate_names and step_name in step_names:
                duplicate_names.add(step_name)
            step_names.add(step_name)
        if duplicate_names:
            return ValidationResult(
                error_num=2,
                error_msg=[f"Duplicate step names found: {', '.join(duplicate_names)}"],
            )
        # Workflow variables must be unique.
        duplicate_names = set()
        variable_names: set[str] = set()
        wf_variable_names: list[str] = get_variable_names(workflow_definition)
        for wf_variable_name in wf_variable_names:
            if (
                wf_variable_name not in duplicate_names
                and wf_variable_name in variable_names
            ):
                duplicate_names.add(wf_variable_name)
            variable_names.add(wf_variable_name)
        if duplicate_names:
            return ValidationResult(
                error_num=3,
                error_msg=[
                    f"Duplicate workflow variable names found: {', '.join(duplicate_names)}"
                ],
            )

        return _VALIDATION_SUCCESS

    @classmethod
    def _validate_run_level(
        cls,
        *,
        workflow_definition: dict[str, Any],
        variables: dict[str, Any] | None = None,
    ) -> ValidationResult:
        assert workflow_definition

        # RUN level requires that each step specification is a valid JSON string.
        # and contains properties for 'collection', 'job', and 'version'.
        for step in workflow_definition["steps"]:
            try:
                specification = json.loads(step["specification"])
            except json.decoder.JSONDecodeError as e:
                return ValidationResult(
                    error_num=2,
                    error_msg=[
                        f"Error decoding specification, which is not valid JSON: {e}"
                    ],
                )
            except TypeError as e:
                return ValidationResult(
                    error_num=3,
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

        # We must have values for all the variables defined in the workflow.
        wf_variables: list[str] = get_required_variable_names(workflow_definition)
        missing_values: list[str] = []
        missing_values.extend(
            wf_variable
            for wf_variable in wf_variables
            if not variables or wf_variable not in variables
        )
        if missing_values:
            return ValidationResult(
                error_num=3,
                error_msg=[
                    f"Missing workflow variable values for: {', '.join(missing_values)}"
                ],
            )

        return _VALIDATION_SUCCESS
