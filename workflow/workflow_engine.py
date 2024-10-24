"""The WorkflowEngine execution logic.
"""

from dataclasses import dataclass
from typing import Optional

from informaticsmatters.protobuf.datamanager.pod_message_pb2 import PodMessage

from .workflow_abc import DatabaseAdapter, InstanceLauncher


@dataclass
class HandlePodMessageResult:
    """WorkflowEngine handle message result."""

    error: int
    error_msg: Optional[str]


_SUCCESS_MESSAGE_RESULT: HandlePodMessageResult = HandlePodMessageResult(
    error=0, error_msg=None
)


class WorkflowEngine:
    """The workflow engine. An event-driven engine that manages the execution
    of workflow instances. The engine is responsible for launching instances or
    reporting failures and conclusions.
    """

    def __init__(
        self,
        *,
        instance_launcher: InstanceLauncher,
        db_adapter: DatabaseAdapter,
    ):
        # Keep the dependent objects
        self._instance_launcher = instance_launcher
        self._db_adapter = db_adapter

    def handle_pod_message(
        self,
        pod_msg: PodMessage,
    ) -> HandlePodMessageResult:
        """Given a PodMessage, we use it to identify the Pod (Instance) exit code,
        workflow and step and decide what to do next.

        Only pod messages relating to workflow instances will be delivered to this method.
        The Pod message has an 'instance' property that provides the UUID of
        the instance that was run. This can be used to correlate the instance with the
        running workflow step.
        """
        assert pod_msg

        return _SUCCESS_MESSAGE_RESULT
