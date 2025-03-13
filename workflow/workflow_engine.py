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

import json
import logging
import sys
from typing import Any, Dict, Optional

from google.protobuf.message import Message
from informaticsmatters.protobuf.datamanager.pod_message_pb2 import PodMessage
from informaticsmatters.protobuf.datamanager.workflow_message_pb2 import WorkflowMessage

from workflow.workflow_abc import (
    InstanceLauncher,
    LaunchParameters,
    LaunchResult,
    WorkflowAPIAdapter,
)

_LOGGER: logging.Logger = logging.getLogger(__name__)
_LOGGER.setLevel(logging.INFO)
_LOGGER.addHandler(logging.StreamHandler(sys.stdout))


class WorkflowEngine:
    """The workflow engine."""

    def __init__(
        self,
        *,
        wapi_adapter: WorkflowAPIAdapter,
        instance_launcher: InstanceLauncher,
    ):
        # Keep the dependent objects
        self._wapi_adapter = wapi_adapter
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

        _LOGGER.info("WorkflowMessage:\n%s", str(msg))
        assert msg.action in ["START", "STOP"]

        r_wfid = msg.running_workflow
        if msg.action == "START":
            self._handle_workflow_start_message(r_wfid)
        else:
            # STOP is not implemented yet and probably not for some time.
            # So just log and ignore for now!
            _LOGGER.warning(
                "Got STOP action for %s - but it's not implemented yet!", r_wfid
            )

    def _handle_workflow_start_message(self, r_wfid: str) -> None:
        """Logic to handle a START message. Here we use the running workflow
        (and workflow) to find the first step in the workflow and launch it, passing
        the running workflow variables to the launcher."""

        rwf_response, _ = self._wapi_adapter.get_running_workflow(
            running_workflow_id=r_wfid
        )
        _LOGGER.debug(
            "API.get_running_workflow(%s) returned: -\n%s", r_wfid, str(rwf_response)
        )
        assert "running_user" in rwf_response
        launching_user_name: str = rwf_response["running_user"]
        # Now get the workflow definition (to get all the steps)
        wfid = rwf_response["workflow"]["id"]
        wf_response, _ = self._wapi_adapter.get_workflow(workflow_id=wfid)
        _LOGGER.debug("API.get_workflow(%s) returned: -\n%s", wfid, str(wf_response))

        # Now find the first step,
        # and create a corresponding RunningWorkflowStep record...
        first_step: Dict[str, Any] = wf_response["steps"][0]
        first_step_name: str = first_step["name"]
        response, _ = self._wapi_adapter.create_running_workflow_step(
            running_workflow_id=r_wfid,
            step=first_step_name,
        )
        _LOGGER.debug(
            "API.create_running_workflow_step(%s, %s) returned: -\n%s",
            r_wfid,
            first_step_name,
            str(response),
        )
        assert "id" in response
        r_wfsid = response["id"]

        # The step's 'specification' is a string - pass it directly to the
        # launcher along with any (optional) 'variables'. The launcher
        # will apply the variables to step's Job command but we need to handle
        # any launch problems. The validator should have checked to ensure that
        # variable expansion will work, but we must prepare for the unexpected.

        project_id = rwf_response["project"]["id"]
        variables: dict[str, Any] | None = rwf_response.get("variables")

        _LOGGER.info(
            "Launching first step: RunningWorkflow=%s RunningWorkflowStep=%s step=%s"
            " (name=%s project=%s, variables=%s)",
            r_wfid,
            r_wfsid,
            first_step_name,
            rwf_response["name"],
            project_id,
            variables,
        )

        lp: LaunchParameters = LaunchParameters(
            project_id=project_id,
            name=first_step_name,
            debug=rwf_response.get("debug"),
            launching_user_name=launching_user_name,
            launching_user_api_token=rwf_response["running_user_api_token"],
            specification=json.loads(first_step["specification"]),
            specification_variables=variables,
            running_workflow_id=r_wfid,
            running_workflow_step_id=r_wfsid,
        )
        lr: LaunchResult = self._instance_launcher.launch(launch_parameters=lp)
        if lr.error_num:
            self._set_step_error(
                first_step_name, r_wfid, r_wfsid, lr.error_num, lr.error_msg
            )
        else:
            _LOGGER.info(
                "Launched first step '%s' (command=%s)", first_step_name, lr.command
            )

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
        _LOGGER.info("PodMessage:\n%s", str(msg))

        # ALL THIS CODE ADDED SIMPLY TO DEMONSTRATE THE USE OF THE API ADAPTER
        # AND THE INSTANCE LAUNCHER FOR THE SIMPLEST OF WORKFLOWS: -
        # THE "TWO-STEP NOP".
        # THERE IS NO SPECIFICATION MANIPULATION NEEDED FOR THIS EXAMPLE
        # THE STEPS HAVE NO INPUTS OR OUTPUTS.
        # THIS FUNCTION PROBABLY NEEDS TO BE A LOT MORE SOPHISTICATED!

        # Ignore anything without an exit code.
        if not msg.has_exit_code:
            _LOGGER.error("PodMessage has no exit code")
            return

        instance_id: str = msg.instance
        exit_code: int = msg.exit_code
        response, _ = self._wapi_adapter.get_instance(instance_id=instance_id)
        _LOGGER.debug(
            "API.get_instance(%s) returned: -\n%s", instance_id, str(response)
        )
        r_wfsid: str | None = response.get("running_workflow_step_id")
        assert r_wfsid
        rwfs_response, _ = self._wapi_adapter.get_running_workflow_step(
            running_workflow_step_id=r_wfsid
        )
        _LOGGER.debug(
            "API.get_running_workflow_step(%s) returned: -\n%s",
            r_wfsid,
            str(rwfs_response),
        )
        step_name: str = rwfs_response["name"]

        # Get the step's running workflow record.
        r_wfid: str = rwfs_response["running_workflow"]["id"]
        assert r_wfid
        rwf_response, _ = self._wapi_adapter.get_running_workflow(
            running_workflow_id=r_wfid
        )
        _LOGGER.debug(
            "API.get_running_workflow(%s) returned: -\n%s", r_wfid, str(rwf_response)
        )

        if exit_code:
            # The job was launched but it failed.
            # Set a step error,
            # This will also set a workflow error so we can leave.
            self._set_step_error(step_name, r_wfid, r_wfsid, exit_code, "Job failed")
            return

        # The prior step completed successfully if we get here.

        self._wapi_adapter.set_running_workflow_step_done(
            running_workflow_step_id=r_wfsid,
            success=True,
        )
        wfid = rwf_response["workflow"]["id"]
        assert wfid
        wf_response, _ = self._wapi_adapter.get_workflow(workflow_id=wfid)
        _LOGGER.debug("API.get_workflow(%s) returned: -\n%s", wfid, str(wf_response))

        # Given the step for the instance just finished (successfully),
        # find the next step n the workflow
        # (using the name of the prior step as an index)
        # and launch it.
        #
        # If there are no more steps then the workflow is done.

        lr: Optional[LaunchResult] = None
        for step in wf_response["steps"]:
            if step["name"] == step_name:
                step_index = wf_response["steps"].index(step)
                if step_index + 1 < len(wf_response["steps"]):

                    # There's another step - for this simple logic it is the next step.

                    next_step = wf_response["steps"][step_index + 1]
                    next_step_name = next_step["name"]
                    rwfs_response, _ = self._wapi_adapter.create_running_workflow_step(
                        running_workflow_id=r_wfid,
                        step=next_step_name,
                    )
                    _LOGGER.debug(
                        "API.create_running_workflow_step(%s, %s) returned: -\n%s",
                        r_wfid,
                        next_step_name,
                        str(response),
                    )
                    assert "id" in rwfs_response
                    new_r_wfsid: str = rwfs_response["id"]
                    project_id: str = rwf_response["project"]["id"]
                    variables: dict[str, Any] | None = rwf_response.get("variables")
                    lp: LaunchParameters = LaunchParameters(
                        project_id=project_id,
                        name=next_step_name,
                        debug=rwf_response.get("debug"),
                        launching_user_name=rwf_response["running_user"],
                        launching_user_api_token=rwf_response["running_user_api_token"],
                        specification=json.loads(next_step["specification"]),
                        specification_variables=variables,
                        running_workflow_id=r_wfid,
                        running_workflow_step_id=new_r_wfsid,
                    )
                    lr = self._instance_launcher.launch(launch_parameters=lp)
                    # Handle a launch error?
                    if lr.error_num:
                        self._set_step_error(
                            next_step_name,
                            r_wfid,
                            new_r_wfsid,
                            lr.error_num,
                            lr.error_msg,
                        )
                    else:
                        _LOGGER.info(
                            "Launched step: %s (command=%s)",
                            next_step["name"],
                            lr.command,
                        )

                    # Something was started (or there was a launch error).
                    break

        # If there's no launch result this must be the (successful) end of the workflow.
        # If there is a launch result it was either successful
        # (and not the end of the workflow) or unsuccessful
        # (and the workflow will have been marked as done anyway).
        if lr is None:
            self._wapi_adapter.set_running_workflow_done(
                running_workflow_id=r_wfid,
                success=True,
            )

    def _set_step_error(
        self,
        step_name: str,
        r_wfid: str,
        r_wfsid: str,
        error: Optional[int],
        error_msg: Optional[str],
    ) -> None:
        """Set the error state for a running workflow step (and the running workflow).
        Calling this method essentially 'ends' the running workflow."""
        _LOGGER.warning(
            "Failed to launch step '%s' (error=%d error_msg=%s)",
            step_name,
            error,
            error_msg,
        )
        self._wapi_adapter.set_running_workflow_step_done(
            running_workflow_step_id=r_wfsid,
            success=False,
            error=error,
            error_msg=error_msg,
        )
        # We must also set the running workflow as done (failed)
        self._wapi_adapter.set_running_workflow_done(
            running_workflow_id=r_wfid,
            success=False,
            error=error,
            error_msg=error_msg,
        )
