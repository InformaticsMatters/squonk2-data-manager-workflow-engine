"""The WorkflowEngine validation logic.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from .decoder import validate_schema
from .workflow_abc import DatabaseAdapter, MessageDispatcher


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


@dataclass
class StartResult:
    """WorkflowEngine start workflow result."""

    error: int
    error_msg: Optional[str]
    running_workflow_id: Optional[str]


@dataclass
class StopResult:
    """WorkflowEngine stop workflow result."""

    error: int
    error_msg: Optional[str]


# Handy successful results
_VALIDATION_SUCCESS = ValidationResult(error=0, error_msg=None)
_SUCCESS_STOP_RESULT: StopResult = StopResult(error=0, error_msg=None)


class WorkflowValidator:
    """The workflow validator. Typically used from teh context of the API
    to check workflow content prior to creation and execution.
    """

    def __init__(
        self, *, db_adapter: DatabaseAdapter, msg_dispatcher: MessageDispatcher
    ):
        assert db_adapter

        self._db_adapter = db_adapter
        self._msg_dispatcher = msg_dispatcher

    def validate(
        self,
        *,
        level: ValidationLevel,
        workflow_definition: Dict[str, Any],
        workflow_inputs: Optional[Dict[str, Any]] = None,
    ) -> ValidationResult:
        """Validates the workflow definition (and inputs)
        based on the provided 'level'."""
        assert level in ValidationLevel
        assert isinstance(workflow_definition, dict)
        if workflow_inputs:
            assert isinstance(workflow_inputs, dict)

        if error := validate_schema(workflow_definition):
            return ValidationResult(error=1, error_msg=[error])

        return _VALIDATION_SUCCESS

    def start(
        self,
        *,
        project_id: str,
        workflow_id: str,
        workflow_definition: Dict[str, Any],
        workflow_parameters: Dict[str, Any],
    ) -> StartResult:
        """Called to initiate workflow by finding the first Instance (or instances)
        to run and then launching them. It is used from the API Pod, and apart from
        validating the workflow definition for suitability it sends a Start message
        to the internal message bus.
        """
        assert project_id
        assert workflow_id
        assert workflow_definition
        assert workflow_parameters

        return StartResult(
            error=0,
            error_msg=None,
            running_workflow_id="r-workflow-6aacd971-ca87-4098-bb70-c1c5f19f4dbf",
        )

    def stop(
        self,
        *,
        running_workflow_id: str,
    ) -> StopResult:
        """Stop a running workflow. It is used from the API Pod, and apart from
        validating the workflow definition for suitability it sends a Stop message
        to the internal message bus."""
        assert running_workflow_id

        return _SUCCESS_STOP_RESULT
