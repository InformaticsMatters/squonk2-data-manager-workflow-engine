"""The WorkflowEngine validation logic.

A module that provides workflow validation at levels beyond the schema. A workflow
definition is a complex structure, and not all of its content can be checked using
a JSON/YAML schema alone. This module provides a number of validation levels
with increasing levels of 'inspection'. The levels are called CREATE, RUN, and
TAG.

    CREATE level validation simply checks that the workflow complies with the schema.
    Workflows are permitted in the DM that do not comply with the schema. This is
    becuase the DM is also used as a persistent store for Wwrfklows while editing - this
    allows a user to 'save' a workflow that is incomplete with the intention of
    adjusting it at a later date prior to execution.

    TAG level validation takes things a little further. In 'production' mode
    tagging is required prior to exeution. TAG level validatioin ensures that a workflow
    _should_ run if it is run - for examplke variable names are all correctly defined
    and there are no duplicates.

    RUN level extends TAG level validation by ensuring, for example, all the
    workflow variables are defined.

Validation is designed to allow a more relaxed engine implementation, negating the
need for the engine to 'check', for example, that variables exist - the validator
ensures they do so that the engine can concentrate on laucnhing steps rather than
implementing swatchs of lines of logic to protect against mal-use.

It is the Data Manager that is responsible for invoking the validator. It does this
prior to allowing a user to run a workflow. When the engine receives a 'Workflow Start'
message it can be sure that the chosen workflow can run.

Module philosophy
-----------------
Here we define the 'ValidationLevel' enumeration and 'ValidationResult' dataclass
used as a return object by the validation function. The module defines the
'WorkflowValidator' class with one (class-level) function ... 'validate()'.
The 'validate()' function ensures that the checks based on the validation level
are executed and any breach is returned via a 'ValidationResult' instance.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any

from .decoder import (
    get_steps,
    get_workflow_variable_names,
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

        # TAG level requires that each step name is unique,
        # and all the output variable names in the step are unique.
        duplicate_names: set[str] = set()
        all_step_names: set[str] = set()
        for step in get_steps(workflow_definition):
            step_name: str = step["name"]
            if step_name not in duplicate_names and step_name in all_step_names:
                duplicate_names.add(step_name)
            all_step_names.add(step_name)
        if duplicate_names:
            return ValidationResult(
                error_num=2,
                error_msg=[f"Duplicate step names found: {', '.join(duplicate_names)}"],
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

        # We must have values for all the variables defined in the workflow.
        wf_variables: set[str] = get_workflow_variable_names(workflow_definition)
        missing_values: list[str] = []
        missing_values.extend(
            wf_variable
            for wf_variable in wf_variables
            if not variables or wf_variable not in variables
        )
        if missing_values:
            return ValidationResult(
                error_num=8,
                error_msg=[
                    f"Missing workflow variable values for: {', '.join(missing_values)}"
                ],
            )

        return _VALIDATION_SUCCESS
