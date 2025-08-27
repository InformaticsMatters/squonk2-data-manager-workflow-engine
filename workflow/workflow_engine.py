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

from .decoder import (
    get_step_input_variable_names,
    get_step_prior_step_variable_mapping,
    get_step_replicator,
    get_step_workflow_variable_mapping,
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
        'action' string field (one of 'START' or 'STOP').
        The message contains a 'running_workflow' field that contains the UUID
        of an existing RunningWorkflow record in the DM. Using this
        we can locate the Workflow record and interrogate that to identify which
        step (or steps) to launch (run) first."""
        assert msg

        _LOGGER.info("WorkflowMessage:\n%s", str(msg))
        if msg.action not in ["START", "STOP"]:
            _LOGGER.error("Ignoring unsupported action (%s)", msg.action)
            return

        r_wfid = msg.running_workflow
        if msg.action == "START":
            self._handle_workflow_start_message(r_wfid)
        else:
            self._handle_workflow_stop_message(r_wfid)

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
        self._launch(wf=wf_response, rwf=rwf_response, rwfs_id=r_wfsid, step=first_step)

    def _handle_workflow_stop_message(self, r_wfid: str) -> None:
        """Logic to handle a STOP message."""
        # Do nothing if the running workflow has already stopped.
        rwf_response, _ = self._wapi_adapter.get_running_workflow(
            running_workflow_id=r_wfid
        )
        _LOGGER.debug(
            "API.get_running_workflow(%s) returned: -\n%s", r_wfid, str(rwf_response)
        )
        if not rwf_response:
            _LOGGER.debug("Running workflow does not exist (%s)", r_wfid)
            return
        elif rwf_response["done"] is True:
            _LOGGER.debug("Running workflow already stopped (%s)", r_wfid)
            return

        # For this version all we can do is check that no steps are running.
        # If no steps are running we can safely mark the running workflow as stopped.
        response, _ = self._wapi_adapter.get_running_steps(running_workflow_id=r_wfid)
        _LOGGER.debug(
            "API.get_running_steps(%s) returned: -\n%s", r_wfid, str(response)
        )
        if response:
            if count := response["count"]:
                msg: str = "1 step is" if count == 1 else f"{count} steps are"
                _LOGGER.debug("Ignoring STOP for %s. %s still running", r_wfid, msg)
            else:
                self._wapi_adapter.set_running_workflow_done(
                    running_workflow_id=r_wfid,
                    success=False,
                    error_num=1,
                    error_msg="User stopped",
                )

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

        # If we get here the prior step completed successfullyso we
        # mark the Step as DONE (successfully).
        wfid = rwf_response["workflow"]["id"]
        assert wfid
        wf_response, _ = self._wapi_adapter.get_workflow(workflow_id=wfid)
        _LOGGER.debug("API.get_workflow(%s) returned: -\n%s", wfid, str(wf_response))

        # We then inspect the Workflow to determine the next step.
        self._wapi_adapter.set_running_workflow_step_done(
            running_workflow_step_id=r_wfsid,
            success=True,
        )

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
                        wf=wf_response,
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
        running_workflow_id: str,
        running_workflow_step_id: str,
        step: dict[str, Any],
        our_step_index: int,
        running_workflow_variables: dict[str, Any],
    ) -> str | dict[str, Any]:
        """Returns an error message if the command isn't valid.
        Without a message we return all the variables that were (successfully)
        applied to the command.

        We are also given a list of steps in workflow_steps and out position in
        the list with our_step_index."""
        assert our_step_index >= 0

        # We get the Job from the step specification, which must contain
        # the keys "collection", "job", and "version". Here we assume that
        # the workflow definition has passed the RUN-level validation
        # which means we can get these values.
        step_spec: dict[str, Any] = step["specification"]
        job_collection: str = step_spec["collection"]
        job_job: str = step_spec["job"]
        job_version: str = step_spec["version"]
        job, _ = self._wapi_adapter.get_job(
            collection=job_collection, job=job_job, version=job_version
        )
        _LOGGER.debug(
            "API.get_job(%s, %s, %s) for %s returned: -\n%s",
            job_collection,
            job_job,
            job_version,
            running_workflow_step_id,
            str(job),
        )

        # Start with any variables provided in the step's specification.
        # This will be ou t"all variables" map for this step,
        # whcih we will add to (and maybe even over-write)...
        all_variables: dict[str, Any] = step["specification"].get("variables", {})

        # Next, we iterate through the step's "variable mapping" block.
        # This tells us all the variables that are set from either the
        # 'workflow' or 'a prior step'.

        # Start with any workflow variables in the step.
        # This will be a list of tuples of "in" and "out" variable names.
        # "in" variables are worklfow variables, and "out" variables
        # are expected Job variables. We use this to add variables
        # to the "all variables" map.
        for from_to in get_step_workflow_variable_mapping(step=step):
            all_variables[from_to[1]] = running_workflow_variables[from_to[0]]

        # Now we apply variables from the "variable mapping" block
        # related to values used in prior steps. The decoder gives
        # us a map indexed by prior step name that's a list of "in" "out"
        # tuples as above.
        step_prior_v_map: dict[str, list[tuple[str, str]]] = (
            get_step_prior_step_variable_mapping(step=step)
        )
        for prior_step_name, v_map in step_prior_v_map.items():
            # Retrieve the prior "running" step
            # in order to get the variables that were set there...
            prior_step, _ = self._wapi_adapter.get_running_workflow_step_by_name(
                name=prior_step_name, running_workflow_id=running_workflow_id
            )
            # Copy "in" value to "out"...
            for from_to in v_map:
                all_variables[from_to[1]] = prior_step["variables"][from_to[0]]

        _LOGGER.debug(
            "Index %s (%s) workflow_variables=%s",
            our_step_index,
            running_workflow_step_id,
            all_variables,
        )

        # Set the variables for this step (so they can be inspected on error)
        self._wapi_adapter.set_running_workflow_step_variables(
            running_workflow_step_id=running_workflow_step_id,
            variables=all_variables,
        )

        # Now ... can the command be compiled!?
        message, success = decode(
            job["command"], all_variables, "command", TextEncoding.JINJA2_3_0
        )
        return all_variables if success else message

    def _launch(
        self,
        *,
        wf: dict[str, Any],
        rwf: dict[str, Any],
        rwfs_id: str,
        step: dict[str, Any],
    ) -> None:
        step_name: str = step["name"]
        rwf_id: str = rwf["id"]

        _LOGGER.info("Validating step command: %s (step=%s)...", rwf_id, step_name)

        # Get step data - importantly, giving us the sequence of steps in the response.
        # Steps will be in wf_step_data["steps"] and our position in the list
        # is wf_step_data["caller_step_index"]
        wf_step_data, _ = self._wapi_adapter.get_workflow_steps_driving_this_step(
            running_workflow_step_id=rwfs_id,
        )
        assert wf_step_data["caller_step_index"] >= 0
        our_step_index: int = wf_step_data["caller_step_index"]

        # A mojor pievce of work is to get ourselves into a position
        # that allows us to check the step command can be executed.
        # We do this by compiling a map of varibales we belive the step needs.

        # Get all the workflow variables that were provided
        # by the user when they "ran" the workflow...
        rwf_variables: dict[str, Any] = rwf.get("variables", {})
        error_or_variables: str | dict[str, Any] = self._validate_step_command(
            running_workflow_id=rwf_id,
            running_workflow_step_id=rwfs_id,
            step=step,
            our_step_index=our_step_index,
            running_workflow_variables=rwf_variables,
        )
        if isinstance(error_or_variables, str):
            error_msg = error_or_variables
            msg = f"Failed command validation error_msg={error_msg}"
            _LOGGER.warning(msg)
            self._set_step_error(step_name, rwf_id, rwfs_id, 1, msg)
            return

        project_id = rwf["project"]["id"]
        variables: dict[str, Any] = error_or_variables

        _LOGGER.info(
            "Launching step: RunningWorkflow=%s RunningWorkflowStep=%s step=%s"
            " variables=%s name=%s project=%s, (variables=%s)",
            rwf_id,
            rwfs_id,
            step_name,
            variables,
            rwf["name"],
            project_id,
            variables,
        )

        # Is this a replicating step?
        replicator = get_step_replicator(step=step)

        # When we launch a step we need to identify all the prior steps in the workflow,
        # those we depend on. The DataManager will then link their outputs to
        # out instance directory. For simple workflows there is only one prior step,
        # and it's the one immediately prior to this one.
        #
        # We put all the prior step IDs in: -
        #   'running_workflow_step_prior_steps'
        #       A list of step UUID strings.
        #
        # In this 'simple' linear implementation that is simply the immediately
        # preceding step.
        prior_steps: list[str] = []
        if our_step_index > 0:
            # We need the step ID of the prior step.
            prior_step_name = wf_step_data["steps"][our_step_index - 1]["name"]
            step_response, _ = self._wapi_adapter.get_running_workflow_step_by_name(
                name=prior_step_name,
                running_workflow_id=rwf_id,
            )
            assert "id" in step_response
            prior_steps.append(step_response["id"])

        # We must also identify workflow inputs that are required by the step we are
        # about to launch and pass those using a launch parameter. The launcher
        # will ensure these are copied into out instance directory before we are run.
        # We cannot provide the variable values (even though we have them) because
        # the DM passes input through 'InputHandlers', which may translate the value.
        # So we have to pass the name and let the DM move the files after
        # the InputHandler has run.
        #
        #   'running_workflow_step_inputs'
        #       A list of Job input variable names

        inputs: list[str] = []
        inputs.extend(iter(get_step_input_variable_names(wf, step_name)))
        if replicator:
            single_step_variables = []
            for replicating_param in variables[replicator]:
                ssv = {**variables}
                ssv[replicator] = replicating_param
                single_step_variables.append(ssv)
        else:
            single_step_variables = [variables]

        for params in single_step_variables:
            lp: LaunchParameters = LaunchParameters(
                project_id=project_id,
                name=step_name,
                debug=rwf.get("debug"),
                launching_user_name=rwf["running_user"],
                launching_user_api_token=rwf["running_user_api_token"],
                specification=step["specification"],
                specification_variables=params,
                running_workflow_id=rwf_id,
                running_workflow_step_id=rwfs_id,
                running_workflow_step_prior_steps=prior_steps,
                running_workflow_step_inputs=inputs,
            )
            lr: LaunchResult = self._instance_launcher.launch(launch_parameters=lp)
            if lr.error_num:
                self._set_step_error(
                    step_name, rwf_id, rwfs_id, lr.error_num, lr.error_msg
                )
            else:
                _LOGGER.info("Launched step '%s' (command=%s)", step_name, lr.command)

    def _set_step_error(
        self,
        step_name: str,
        r_wfid: str,
        r_wfsid: str,
        error_num: Optional[int],
        error_msg: Optional[str],
    ) -> None:
        """Set the error state for a running workflow step (and the running workflow).
        Calling this method essentially 'ends' the running workflow."""
        _LOGGER.warning(
            "Failed to launch step '%s' (error_num=%d error_msg=%s)",
            step_name,
            error_num,
            error_msg,
        )
        r_wf_error: str = f"Step '{step_name}' ERROR({error_num}): {error_msg}"
        self._wapi_adapter.set_running_workflow_step_done(
            running_workflow_step_id=r_wfsid,
            success=False,
            error_num=error_num,
            error_msg=r_wf_error,
        )
        # We must also set the running workflow as done (failed)
        self._wapi_adapter.set_running_workflow_done(
            running_workflow_id=r_wfid,
            success=False,
            error_num=error_num,
            error_msg=r_wf_error,
        )
