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

from decoder.decoder import TextEncoding, decode
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
        """Logic to handle a START message. This is the beginning of a new
        running workflow. We use the running workflow (and workflow) to find the
        first step in the Workflow and launch it, passing the running workflow variables
        to the launcher.

        The first step is relatively easy (?) - all the variables
        (for the first step's 'command') will (must) be defined
        in the RunningWorkflow's variables."""

        rwf_response, _ = self._wapi_adapter.get_running_workflow(
            running_workflow_id=r_wfid
        )
        _LOGGER.debug(
            "API.get_running_workflow(%s) returned: -\n%s", r_wfid, str(rwf_response)
        )
        assert "running_user" in rwf_response
        # Now get the workflow definition (to get all the steps)
        wfid = rwf_response["workflow"]["id"]
        wf_response, _ = self._wapi_adapter.get_workflow(workflow_id=wfid)
        _LOGGER.debug("API.get_workflow(%s) returned: -\n%s", wfid, str(wf_response))

        # Now find the first step,
        # and create a corresponding RunningWorkflowStep record...
        first_step: Dict[str, Any] = wf_response["steps"][0]
        first_step_name: str = first_step["name"]
        # We need this even if the following goes wrong.
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
        r_wfsid: str = response["id"]

        # Launch the first step.
        # If there's a launch problem the step (and running workflow) will have
        # and error, stopping it. There will be no Pod event as the launch has failed.
        self._launch(rwf=rwf_response, rwfs_id=r_wfsid, step=first_step)

    def _handle_pod_message(self, msg: PodMessage) -> None:
        """Handles a PodMessage. This is a message that signals the completion of a
        prior step Job within an existing running workflow.

        Steps run as "instances" and the Pod message identifies the Instance.
        Using the Instance record we can get the "running workflow step",
        and then identify the "running workflow" and the "workflow".

        First thing is to adjust the workflow step with the step's success state and
        optional error code. If the step was successful, armed with the step's
        Workflow we can determine what needs to be done next -
        is this the end or is there another step to run?

        If there's another step to run we must determine what variables are
        available and present them to the next step. It doesn't matter if we
        provide variables the next step's command does not need, but we MUST
        provide all the variables that the next step's command does need.

        We also have a 'housekeeping' responsibility - i.e. to keep the
        RunningWorkflowStep and RunningWorkflow status up to date."""
        assert msg

        # The PodMessage has an 'instance', 'has_exit_code', and 'exit_code' values.
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

        # The Instance tells us whether the Step (Job) was successful
        # (i.e. we can simply check the 'exit_code').
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

        # If the Step failed there's no need for us to inspect the Workflow
        # (for the next step) as we simply stop here, reporting the appropriate status).
        if exit_code:
            # The job was launched but it failed.
            # Set a step error,
            # This will also set a workflow error so we can leave.
            self._set_step_error(step_name, r_wfid, r_wfsid, exit_code, "Job failed")
            return

        # If we get here the prior step completed successfully
        # and so we can mark the Step as DOne (successfully),
        # and then inspect the Workflow to determine the next step.

        self._wapi_adapter.set_running_workflow_step_done(
            running_workflow_step_id=r_wfsid,
            success=True,
        )
        wfid = rwf_response["workflow"]["id"]
        assert wfid
        wf_response, _ = self._wapi_adapter.get_workflow(workflow_id=wfid)
        _LOGGER.debug("API.get_workflow(%s) returned: -\n%s", wfid, str(wf_response))

        # We have the step from the Instance that's just finished,
        # so we can use that to find the next step in the Workflow definition.
        # (using the name of the completed step step as an index).
        # Once found, we can launch it (with any variables we think we need).
        #
        # If there are no more steps then the RunningWorkflow is set to
        # finished (done).

        launch_attempted: bool = False
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
                    assert "id" in rwfs_response
                    r_wfsid = rwfs_response["id"]
                    assert r_wfsid
                    _LOGGER.debug(
                        "API.create_running_workflow_step(%s, %s) returned: -\n%s",
                        r_wfid,
                        next_step_name,
                        str(response),
                    )

                    self._launch(
                        rwf=rwf_response,
                        rwfs_id=r_wfsid,
                        step=next_step,
                    )

                    # Something was started (or there was a launch error and the step
                    # and running workflow error will have been set).
                    # Regardless we can stop now.
                    launch_attempted = True
                    break

        # If no launch was attempted we can assume this is the end of the running workflow.
        if not launch_attempted:
            self._wapi_adapter.set_running_workflow_done(
                running_workflow_id=r_wfid,
                success=True,
            )

    def _validate_step_command(
        self,
        *,
        step: dict[str, Any],
        running_workflow_variables: dict[str, Any] | None = None,
    ) -> str | dict[str, Any]:
        """Returns an error message if the command isn't valid.
        Without a message we return all the variables that were (successfully)
        applied to the command."""
        # We get the Job from the step specification, which must contain
        # the keys "collection", "job", and "version". Here we assume that
        # the workflow definition has passed the RUN-level validation
        # which means we can get these values.
        step_spec: dict[str, Any] = json.loads(step["specification"])
        job_collection: str = step_spec["collection"]
        job_job: str = step_spec["job"]
        job_version: str = step_spec["version"]
        job, _ = self._wapi_adapter.get_job(
            collection=job_collection, job=job_job, version=job_version
        )
        _LOGGER.debug(
            "API.get_job(%s, %s, %s) returned: -\n%s",
            job_collection,
            job_job,
            job_version,
            str(job),
        )

        # The step's 'specification' is a string - pass it directly to the
        # launcher along with any (optional) 'workflow variables'. The launcher
        # will apply the variables to the step's Job command but we need to handle
        # any launch problems. The validator should have checked to ensure that
        # variable expansion will work, but we must prepare for the unexpected.
        #
        # What the engine has to do here is make sure that the Job
        # that's about to be launched has all its configuration requirements
        # satisfied (inputs, outputs and options). Basically we must ensure
        # that the Job definition's 'command' can be compiled by applying
        # the available variables.
        #
        # To prevent launcher errors relating to decoding we get the command ourselves
        # and then apply the current set of variables. And we use the JobDecoder's
        # 'decode()' method to do this. It returns a tuple (str and boolean).
        # If the boolean is True then the command can be compiled
        # (i.e. it has no missing variables) and the launcher should not complain
        # about the command (as we'll pass the same variables to it.
        # If the returned boolean is False then we can expect the returned str
        # to contain an error message.
        #
        # The full set of step variables can be obtained
        # (in ascending order of priority) from...
        #
        # 1. The Job Step Specification
        # 2. The RunningWorkflow
        #
        # If variable 'x' is defined in all three then the RunningWorkflow's
        # value must be used.

        # 1. Get any variables from the step specification.
        all_variables = step_spec.pop("variables") if "variables" in step_spec else {}
        # 2. Merge running workflow variables on top of these
        if running_workflow_variables:
            all_variables |= running_workflow_variables

        # This gives all the running workflow and step-specific variables.
        # Now we have to inspect the workflow step 'inputs' (and 'options')
        # and see if there are further variables that need constructing
        # and then adding (merging) into the 'all_variables' dictionary.
        #
        # TBD

        message, success = decode(
            job["command"], all_variables, "command", TextEncoding.JINJA2_3_0
        )
        return all_variables if success else message

    def _launch(
        self,
        *,
        rwf: dict[str, Any],
        rwfs_id: str,
        step: dict[str, Any],
    ) -> None:
        step_name: str = step["name"]
        rwf_id: str = rwf["id"]

        _LOGGER.info("Validating step command: %s (step=%s)...", rwf_id, step_name)

        # Now check the step command can be executed
        # (by trying to decoding the Job command).
        #
        # We pass in the workflow variables (these are provided by the user
        # when the workflow is run. All workflow variables will be present in the
        # running workflow record)
        running_workflow_variables: dict[str, Any] | None = rwf.get("variables")
        error_or_variables: str | dict[str, Any] = self._validate_step_command(
            step=step,
            running_workflow_variables=running_workflow_variables,
        )
        if isinstance(error_or_variables, str):
            error_msg = error_or_variables
            _LOGGER.warning(
                "First step '%s' failed command validation (%s)", step_name, error_msg
            )
            self._set_step_error(step_name, rwf_id, rwfs_id, 1, error_msg)
            return

        project_id = rwf["project"]["id"]
        variables: dict[str, Any] = error_or_variables

        _LOGGER.info(
            "Launching first step: RunningWorkflow=%s RunningWorkflowStep=%s step=%s"
            " (name=%s project=%s, variables=%s)",
            rwf_id,
            rwfs_id,
            step_name,
            rwf["name"],
            project_id,
            variables,
        )

        lp: LaunchParameters = LaunchParameters(
            project_id=project_id,
            name=step_name,
            debug=rwf.get("debug"),
            launching_user_name=rwf["running_user"],
            launching_user_api_token=rwf["running_user_api_token"],
            specification=json.loads(step["specification"]),
            specification_variables=variables,
            running_workflow_id=rwf_id,
            running_workflow_step_id=rwfs_id,
        )
        lr: LaunchResult = self._instance_launcher.launch(launch_parameters=lp)
        if lr.error_num:
            self._set_step_error(step_name, rwf_id, rwfs_id, lr.error_num, lr.error_msg)
        else:
            _LOGGER.info("Launched step '%s' (command=%s)", step_name, lr.command)

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
            error_num=error,
            error_msg=error_msg,
        )
        # We must also set the running workflow as done (failed)
        self._wapi_adapter.set_running_workflow_done(
            running_workflow_id=r_wfid,
            success=False,
            error_num=error,
            error_msg=error_msg,
        )
