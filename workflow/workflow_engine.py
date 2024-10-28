"""The WorkflowEngine execution logic.
"""

from google.protobuf.message import Message

from .workflow_abc import DatabaseAdapter, InstanceLauncher


class WorkflowEngine:
    """The workflow engine. An event-driven engine that manages the execution
    of workflow instances. The engine is responsible for launching instances or
    reporting failures and conclusions.
    """

    def __init__(
        self,
        *,
        db_adapter: DatabaseAdapter,
        instance_launcher: InstanceLauncher,
    ):
        # Keep the dependent objects
        self._db_adapter = db_adapter
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
