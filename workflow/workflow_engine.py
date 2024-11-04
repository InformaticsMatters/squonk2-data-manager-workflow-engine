"""The WorkflowEngine execution logic.
"""

import logging
import sys

from google.protobuf.message import Message
from informaticsmatters.protobuf.datamanager.pod_message_pb2 import PodMessage
from informaticsmatters.protobuf.datamanager.workflow_message_pb2 import WorkflowMessage

from .workflow_abc import APIAdapter, InstanceLauncher

_LOGGER: logging.Logger = logging.getLogger(__name__)
_LOGGER.setLevel(logging.INFO)
_LOGGER.addHandler(logging.StreamHandler(sys.stdout))


class WorkflowEngine:
    """The workflow engine. An event-driven engine that manages the execution
    of workflow instances. The engine is responsible for launching instances or
    reporting failures and conclusions.
    """

    def __init__(
        self,
        *,
        api_adapter: APIAdapter,
        instance_launcher: InstanceLauncher,
    ):
        # Keep the dependent objects
        self._api_adapter = api_adapter
        self._instance_launcher = instance_launcher

    def handle_message(self, msg: Message) -> None:
        """Given a Pod Message, we use it to identify the Pod (Instance) exit code,
        workflow and step and decide what to do next.

        Only pod messages relating to workflow instances will be delivered to this method.
        The Pod message has an 'instance' property that provides the UUID of
        the instance that was run. This can be used to correlate the instance with the
        running workflow step.

        Additionally we will encounter WorkflowMessages that signal the need to
        start and stop workflows.
        """
        assert msg

        _LOGGER.info("> WE.handle_message() : GOT WorkflowMessage:\n%s", str(msg))

        # Is this a WorkflowMessage or a PodMessage?
        if isinstance(msg, PodMessage):
            self._handle_pod_message(msg)
        else:
            self._handle_workflow_message(msg)

    def _handle_workflow_message(self, msg: WorkflowMessage) -> None:
        assert msg

        _LOGGER.info("WE> WorkflowMessage:\n%s", str(msg))
        if msg.action == "START":
            _LOGGER.info("action=%s", msg.action)
            # Load the workflow definition and run the first step...
            rf = self._api_adapter.get_running_workflow(
                running_workflow_id=msg.running_workflow
            )
            assert rf
            _LOGGER.info("RunningWorkflow: %s", rf)

        else:
            _LOGGER.info("action=%s", msg.action)

    def _handle_pod_message(self, msg: PodMessage) -> None:
        assert msg

        _LOGGER.info("WE> PodMessage:\n%s", str(msg))
