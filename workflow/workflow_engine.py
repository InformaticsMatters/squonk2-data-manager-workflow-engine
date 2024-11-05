"""The WorkflowEngine execution logic.

It responds to Pod and Workflow protocol buffer messages received by its
'handle_message()' function, messages delivered by the message handler in the PBC Pod.
There are no other methods in this class.

Its role is to translate a pre-validated workflow definition into the ordered execution
of step "Jobs" that manifest as Pod "Instances" that run in a project directory in the
DM.

Workflow messages initiate (START) and terminate (STOP) workflows. Pod messages signal
the end of individual workflow steps and carry the exit code of the executed Job.
The engine used START messages to launch the first "step" in a workflow and the Pod
messages to signal the success (or failure) of a prior step. A step's success is used,
along with it's original workflow definition to determine the next action
(run the next step or signal the end of the workflow).

Before a START message is transmitted the author (typically the Workflow Validator)
will have created a RunningWorkflow record in the DM. The ID of this record is passed
in the START message that is sent. The engine uses this ID to find the running workflow
and the workflow. The engine creates RunningWorkflowStep records for each step that
is executed, and it uses thew InstanceLauncher to launch the Job (a Pod) for each step.
"""

import logging
import sys

from google.protobuf.message import Message
from informaticsmatters.protobuf.datamanager.pod_message_pb2 import PodMessage
from informaticsmatters.protobuf.datamanager.workflow_message_pb2 import WorkflowMessage

from .workflow_abc import APIAdapter, InstanceLauncher, LaunchResult

_LOGGER: logging.Logger = logging.getLogger(__name__)
_LOGGER.setLevel(logging.INFO)
_LOGGER.addHandler(logging.StreamHandler(sys.stdout))


class WorkflowEngine:
    """The workflow engine."""

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
        """Expect Workflow and Pod messages.

        Only pod messages relating to workflow instances will be delivered to this method.
        The Pod message has an 'instance' property that contains the UUID of
        the instance that was run. This is used to correlate the instance with the
        running workflow step, and (ultimately the running workflow and workflow).
        """
        assert msg

        _LOGGER.debug("Message:\n%s", str(msg))

        if isinstance(msg, PodMessage):
            self._handle_pod_message(msg)
        else:
            self._handle_workflow_message(msg)

    def _handle_workflow_message(self, msg: WorkflowMessage) -> None:
        """WorkflowMessages signal the need to start (or stop) a workflow using its
        'action' string field (one of 'START' or 'START').
        The message contains a 'running_workflow' field that contains the UUID
        of an existing RunningWorkflow record in the DM. Using this
        we can locate the Workflow record and interrogate that to identify which
        step (or steps) to launch (run) first."""
        assert msg

        # ALL THIS CODE ADDED SIMPLY TO DEMONSTRATE THE USE OF THE API ADAPTER
        # AND THE INSTANCE LAUNCHER FOR THE SIMPLEST OF WORKFLOWS: -
        # THE "TWO-STEP NOP".
        # THERE IS NWO SPECIFICATION MANIPULATION NEEDED FOR THIS EXAMPLE
        # THE STEPS HAVE NO INPUTS OR OUTPUTS.
        # THIS FUNCTION PROBABLY NEEDS TO BE A LOT MORE SOPHISTICATED!

        _LOGGER.debug("WE> WorkflowMessage:\n%s", str(msg))

        action = msg.action
        r_wfid = msg.running_workflow

        assert action in ["START", "STOP"]
        if action == "START":
            # Using the running workflow...
            response = self._api_adapter.get_running_workflow(
                running_workflow_id=r_wfid
            )
            assert "running_workflow" in response
            running_workflow = response["running_workflow"]
            _LOGGER.debug("RunningWorkflow: %s", running_workflow)
            # ...get the workflow definition...
            workflow_id = running_workflow["workflow"]["id"]
            response = self._api_adapter.get_workflow(workflow_id=workflow_id)
            assert "workflow" in response
            workflow = response["workflow"]

            # Now find the first step and create a RunningWorkflowStep record...
            first_step: str = workflow["steps"][0]["name"]
            response = self._api_adapter.create_running_workflow_step(
                running_workflow_id=r_wfid,
                step=first_step,
            )
            assert "id" in response
            running_workflow_step_id = response["id"]

            # The step's 'specification' is a string here - pass it directly to the
            # launcher along with any appropriate 'variables'. The launcher
            # will get the step's Job command and apply the variables to it to
            # form the command that will be executed for the step.
            step = workflow["steps"][0]
            project_id = running_workflow["project_id"]
            variables = running_workflow["variables"]
            lr: LaunchResult = self._instance_launcher.launch(
                project_id=project_id,
                running_workflow_id=msg.running_workflow,
                running_workflow_step_id=running_workflow_step_id,
                step_specification=step["specification"],
                variables=variables,
            )
            assert lr.error == 0
            _LOGGER.info(
                "Launched initial step: %s (command=%s)", first_step, lr.command
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
        _LOGGER.debug("WE> PodMessage:\n%s", str(msg))

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
        _LOGGER.debug(
            "WE> PodMessage: instance=%s, exit_code=%d", instance_id, exit_code
        )
        response = self._api_adapter.get_instance(instance_id=instance_id)
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
        running_workflow = response["running_workflow"]
        workflow_id = running_workflow["workflow"]["id"]
        assert workflow_id
        response = self._api_adapter.get_workflow(workflow_id=workflow_id)

        end_of_workflow: bool = True
        if success:
            # Given the step for the instance just finished,
            # find the next step in the workflow and launch it.
            workflow = response["workflow"]
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
                        project_id = running_workflow["project_id"]
                        variables = running_workflow["variables"]
                        lr: LaunchResult = self._instance_launcher.launch(
                            project_id=project_id,
                            running_workflow_id=running_workflow_id,
                            running_workflow_step_id=running_workflow_step_id,
                            step_specification=next_step["specification"],
                            variables=variables,
                        )
                        end_of_workflow = False
                        assert lr.error == 0
                        _LOGGER.info(
                            "Launched step: %s (command=%s)",
                            next_step["name"],
                            lr.command,
                        )
                        break

        # If there are no more steps then the workflow is done
        # so we need to set the running workflow as done
        # and set its success status too.
        if end_of_workflow:
            self._api_adapter.set_running_workflow_done(
                running_workflow_id=running_workflow_id,
                success=success,
            )
