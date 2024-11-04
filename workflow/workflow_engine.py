"""The WorkflowEngine execution logic.
"""

import ast
import logging
import sys
from typing import Any, Dict

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
        """Handles a WorkflowMessage. This is a message that signals a START or STOP
        of a workflow. On START we will load the workflow definition and run (launch)
        the first step."""
        assert msg

        # ALL THIS CODE ADDED SIMPLY TO DEMONSTRATE THE USE OF THE API ADAPTER
        # AND THE INSTANCE LAUNCHER FOR THE SIMPLEST OF WORKFLOWS: -
        # THE "TWO-STEP NOP".
        # THERE IS NWO SPECIFICATION MANIPULATION NEEDED FOR THIS EXAMPLE
        # THE STEPS HAVE NO INPUTS OR OUTPUTS.
        # THIS FUNCTION PROBABLY NEEDS TO BE A LOT MORE SOPHISTICATED!

        _LOGGER.info("WE> WorkflowMessage:\n%s", str(msg))
        if msg.action == "START":
            # Using the running workflow get the workflow definition
            response = self._api_adapter.get_running_workflow(
                running_workflow_id=msg.running_workflow
            )
            assert "running_workflow" in response
            running_workflow = response["running_workflow"]
            _LOGGER.info("RunningWorkflow: %s", running_workflow)
            workflow_id = running_workflow["workflow"]["id"]
            response = self._api_adapter.get_workflow(workflow_id=workflow_id)
            assert "workflow" in response
            workflow = response["workflow"]
            # Now find the first step
            # and create a RunningWorkflowStep record prior to launching the instance
            response = self._api_adapter.create_running_workflow_step(
                running_workflow_id=msg.running_workflow,
                step=workflow["steps"][0]["name"],
            )
            assert "id" in response
            running_workflow_step_id = response["id"]
            # The specification is a string here.
            # It needs to be a dictionary for the launch() method.
            step = workflow["steps"][0]
            step_specification: Dict[str, Any] = ast.literal_eval(step["specification"])
            self._instance_launcher.launch(
                project_id="project-000",
                workflow_id=workflow_id,
                running_workflow_step_id=running_workflow_step_id,
                workflow_definition=workflow,
                step_specification=step_specification,
            )

        else:
            _LOGGER.info("action=%s", msg.action)

    def _handle_pod_message(self, msg: PodMessage) -> None:
        """Handles a PodMessage. This is a message that signals the completion of a
        step within a workflow. Steps run as "instances" and the Pod message
        identifies the Instance. Using the Instance record we can get the
        "running workflow step" and then identify the "running workflow" and the
        "workflow".

        First thing is to adjust the workflow step with the step's success state and
        optional error code. If the step was successful we can find the next step
        and launch that, or consider the last step to have run and modify the
        running workflow record and set's it's success status."""
        assert msg

        # The PodMessage has a 'instance', 'has_exit_code', and 'exit_code' values.
        _LOGGER.info("WE> PodMessage:\n%s", str(msg))

        # ALL THIS CODE ADDED SIMPLY TO DEMONSTRATE THE USE OF THE API ADAPTER
        # AND THE INSTANCE LAUNCHER FOR THE SIMPLEST OF WORKFLOWS: -
        # THE "TWO-STEP NOP".
        # THERE IS NWO SPECIFICATION MANIPULATION NEEDED FOR THIS EXAMPLE
        # THE STEPS HAVE NO INPUTS OR OUTPUTS.
        # THIS FUNCTION PROBABLY NEEDS TO BE A LOT MORE SOPHISTICATED!

        # Ignore anything without an exit code.
        if not msg.has_exit_code:
            _LOGGER.warning("WE> PodMessage: No exit code")
            return

        instance_id: str = msg.instance
        exit_code: int = msg.exit_code
        _LOGGER.info(
            "WE> PodMessage: instance=%s, exit_code=%d", instance_id, exit_code
        )

        # Ignore instances without a running workflow step
        response = self._api_adapter.get_instance(instance_id=instance_id)
        if "running_workflow_step" not in response:
            _LOGGER.warning("WE> PodMessage: Without running_workflow_step")
            return
        running_workflow_step_id: str = response["running_workflow_step"]
        response = self._api_adapter.get_running_workflow_step(
            running_workflow_step_id=running_workflow_step_id
        )
        step_name: str = response["running_workflow_step"]["step"]

        # Set the step as completed (successful or otherwise)
        success: bool = exit_code == 0
        self._api_adapter.set_running_workflow_step_done(
            running_workflow_step_id=running_workflow_step_id,
            success=success,
        )

        # Get the step's running workflow and workflow IDs and records.
        running_workflow_id = response["running_workflow_step"]["running_workflow"]
        assert running_workflow_id
        response = self._api_adapter.get_running_workflow(
            running_workflow_id=running_workflow_id
        )
        workflow_id = response["running_workflow"]["workflow"]["id"]
        assert workflow_id
        response = self._api_adapter.get_workflow(workflow_id=workflow_id)
        workflow = response["workflow"]

        end_of_workflow: bool = True
        if success:
            # Given the step for the instance just finished,
            # find the next step in the workflow and launch it.
            # If there are no more steps then the workflow is done
            # so we need to set the running workflow as done
            # and set it's success status too.
            for step in workflow["steps"]:
                if step["name"] == step_name:
                    step_index = workflow["steps"].index(step)
                    if step_index + 1 < len(workflow["steps"]):
                        next_step = workflow["steps"][step_index + 1]
                        response = self._api_adapter.create_running_workflow_step(
                            running_workflow_id=running_workflow_id,
                            step=next_step["name"],
                        )
                        assert "id" in response
                        running_workflow_step_id = response["id"]
                        step_specification: Dict[str, Any] = ast.literal_eval(
                            next_step["specification"]
                        )
                        self._instance_launcher.launch(
                            project_id="project-000",
                            workflow_id=workflow_id,
                            running_workflow_step_id=running_workflow_step_id,
                            workflow_definition=workflow,
                            step_specification=step_specification,
                        )
                        end_of_workflow = False
                        break

        if end_of_workflow:
            self._api_adapter.set_running_workflow_done(
                running_workflow_id=running_workflow_id,
                success=success,
            )
