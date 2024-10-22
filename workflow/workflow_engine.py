"""The WorkflowEngine execution logic.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

from informaticsmatters.protobuf.datamanager.pod_message_pb2 import PodMessage

from .workflow_abc import DatabaseAdapter, InstanceLauncher


@dataclass
class HandlePodMessageResult:
    """WorkflowEngine handle message result."""

    error: int
    error_msg: Optional[str]


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


_SUCCESS_MESSAGE_RESULT: HandlePodMessageResult = HandlePodMessageResult(
    error=0, error_msg=None
)
_SUCCESS_STOP_RESULT: StopResult = StopResult(error=0, error_msg=None)


class WorkflowValidator:
    """The workflow validator. Typically used from teh context of the API
    to check workflow content prior to creation and execution.
    """

    def __init__(
        self,
        *,
        instance_launcher: InstanceLauncher,
        db_adapter: DatabaseAdapter,
    ):
        # Keep the instance launcher and database adapter
        self._instance_launcher = instance_launcher
        self._db_adapter = db_adapter

    def handle_pod_message(
        self,
        *,
        pod_msg: PodMessage,
    ) -> HandlePodMessageResult:
        """Given a PodMessage, we use it to identify the Pod (Instance) exit code,
        workflow and step and decide what to do next.
        """
        assert pod_msg

        return _SUCCESS_MESSAGE_RESULT

    def start(
        self,
        *,
        project_id: str,
        workflow_id: str,
        workflow_definition: Dict[str, Any],
        workflow_parameters: Dict[str, Any],
    ) -> StartResult:
        """Called to initiate workflow by finding the first Instance (or instances)
        to run and then launching them.
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
        """Stop a running workflow."""
        assert running_workflow_id

        return _SUCCESS_STOP_RESULT
