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
from dataclasses import dataclass
from typing import Any, Optional

import decoder.decoder as job_defintion_decoder
from decoder.decoder import TextEncoding
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
    Connector,
    get_step,
    get_step_predefined_variable_connections,
    get_step_prior_step_connections,
    get_step_workflow_variable_connections,
    is_workflow_output_variable,
)

_LOGGER: logging.Logger = logging.getLogger(__name__)
_LOGGER.setLevel(logging.INFO)
_LOGGER.addHandler(logging.StreamHandler(sys.stdout))


@dataclass
class StepPreparationResponse:
    """Step preparation response object. 'replicas' is +ve (non-zero) if a step
    can be launched - its value indicates how many times. If a step can be launched
    'variables' will not be None. If a parallel set of steps can take place
    (even just one) 'replica_variable' will be set and 'replica_values'
    will be a list containing a value for each step instance. If the step
    depends on a prior step the instance UUIDs of the steps will be listed
    in the 'dependent_instances' string list. If a step's outputs (files) are expected
    in the project directory they will be listed in 'outputs'.

    If preparation fails 'error_num' wil be set, and 'error_msg'
    should contain something useful."""

    replicas: int
    variables: dict[str, Any] | None = None
    replica_variable: str | None = None
    replica_values: list[str] | None = None
    dependent_instances: set[str] | None = None
    outputs: set[str] | None = None
    error_num: int = 0
    error_msg: str | None = None


class WorkflowEngine:
    """The workflow engine."""

    def __init__(
        self,
        *,
        wapi_adapter: WorkflowAPIAdapter,
        instance_launcher: InstanceLauncher,
        instance_link_glob: str = ".instance-*",
    ):
        """Initialiser, given a Workflow API adapter, Instance laucnher,
        and a step (directory) link 'glob' (a convenient directory glob to
        locate the DM hard-link directories of prior instances inserted into a
        step's instance directory, typically '.instance-*')"""
        # Keep the dependent objects
        self._wapi_adapter: WorkflowAPIAdapter = wapi_adapter
        self._instance_launcher: InstanceLauncher = instance_launcher
        self._instance_link_glob: str = instance_link_glob

        self._predefined_variables: dict[str, Any] = {
            "instance-link-glob": instance_link_glob
        }

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

        # Now find the first step (index 0)...
        first_step: dict[str, Any] = wf_response["steps"][0]

        sp_resp = self._prepare_step(
            wf=wf_response, step_definition=first_step, rwf=rwf_response
        )
        assert sp_resp.variables is not None
        assert sp_resp.error_msg is None
        # Launch it.
        # If there's a launch problem the step (and running workflow) will have
        # and error, stopping it. There will be no Pod event as the launch has failed.
        self._launch(
            rwf=rwf_response,
            step_definition=first_step,
            step_preparation_response=sp_resp,
        )

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

                    # There's another step!
                    # For this simple logic it is the next step.
                    next_step = wf_response["steps"][step_index + 1]

                    # A mojor piece of work to accomplish is to get ourselves into a position
                    # that allows us to check the step command can be executed.
                    # We do this by compiling a map of variables we belive the step needs.

                    # If the step about to be launched is based on a prior step
                    # that generates multiple outputs (files) then we have to
                    # exit unless all of the step instances have completed.
                    #
                    # Do we need a 'prepare variables' function?
                    # One that returns a map of variables or nothing
                    # (e.g. 'nothing' when a step launch cannot be attempted)
                    sp_resp = self._prepare_step(
                        wf=wf_response, step_definition=next_step, rwf=rwf_response
                    )
                    if sp_resp.replicas == 0:
                        # Cannot prepare variables for this step,
                        # it might be a step dependent on more than one prior step
                        # (like a 'combiner') and some prior steps may still
                        # be running ... or something's gone wrong.
                        if sp_resp.error_num:
                            self._wapi_adapter.set_running_workflow_done(
                                running_workflow_id=r_wfid,
                                success=False,
                                error_num=sp_resp.error_num,
                                error_msg=sp_resp.error_msg,
                            )
                        return

                    assert sp_resp.variables is not None
                    self._launch(
                        rwf=rwf_response,
                        step_definition=next_step,
                        step_preparation_response=sp_resp,
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

    def _get_step_job(self, *, step: dict[str, Any]) -> dict[str, Any]:
        """Gets the Job definition for a given Step."""
        # We get the Job from the step specification, which must contain
        # the keys "collection", "job", and "version". Here we assume that
        # the workflow definition has passed the RUN-level validation
        # which means we can get these values.
        assert "specification" in step
        step_spec: dict[str, Any] = step["specification"]
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

        return job

    def _prepare_step(
        self,
        *,
        step_definition: dict[str, Any],
        wf: dict[str, Any],
        rwf: dict[str, Any],
    ) -> StepPreparationResponse:
        """Attempts to prepare a map of step variables. If variables cannot be
        presented to the step we return an object with 'iterations' set to zero.
        If there's a problem that means we should be able to proceed but cannot,
        we set 'error_num' and 'error_msg'."""

        step_name: str = step_definition["name"]
        rwf_id: str = rwf["id"]

        # Before we move on, are we a combiner?
        #
        # We are if a variable in our step's plumbing refers to an input of ours
        # that is of type 'files'. If we are a combiner then we use the name of the
        # step we are combining (there can only be one) so that we can ensure
        # all its step instances have finished (successfully). We cannot
        # move on until all the files we depend on are ready.

        our_job_definition: dict[str, Any] = self._get_step_job(step=step_definition)
        our_inputs: dict[str, Any] = job_defintion_decoder.get_inputs(
            our_job_definition
        )
        plumbing_of_prior_steps: dict[str, list[Connector]] = (
            get_step_prior_step_connections(step_definition=step_definition)
        )
        step_is_combiner: bool = False
        step_name_being_combined: str | None = None
        combiner_input_variable: str | None = None
        num_step_recplicas_being_combined: int = 0
        for p_step_name, connections in plumbing_of_prior_steps.items():
            for connector in connections:
                if our_inputs.get(connector.out, {}).get("type") == "files":
                    step_name_being_combined = p_step_name
                    combiner_input_variable = connector.out
                    step_is_combiner = True
                    break
            if step_name_being_combined:
                break

        if step_is_combiner:
            assert step_name_being_combined
            assert combiner_input_variable

            # Are all the step instances we're combining done?

            response, _ = self._wapi_adapter.get_status_of_all_step_instances_by_name(
                name=step_name_being_combined,
                running_workflow_id=rwf_id,
            )
            assert "count" in response
            num_step_recplicas_being_combined = response["count"]
            assert num_step_recplicas_being_combined > 0
            assert "status" in response

            # Assume they're all done
            # and undo our assumption if not...
            all_step_instances_done: bool = True
            all_step_instances_successful: bool = True
            for status in response["status"]:
                if not status["done"]:
                    all_step_instances_done = False
                    break
                if not status["success"]:
                    all_step_instances_successful = False
                    break
            if not all_step_instances_done:
                # Can't move on - other steps need to finish.
                _LOGGER.debug(
                    "Assessing start of combiner step (%s)"
                    " but not all steps (%s) to be combined are done",
                    step_name,
                    step_name_being_combined,
                )
                return StepPreparationResponse(replicas=0)
            elif not all_step_instances_successful:
                # Can't move on - all prior steps are done,
                # but at least one was not successful.
                _LOGGER.warning(
                    "Assessing start of combiner step (%s)"
                    " but at least one step (%s) to be combined failed",
                    step_name,
                    step_name_being_combined,
                )
                return StepPreparationResponse(
                    replicas=0,
                    error_num=1,
                    error_msg=f"Prior instance of step '{step_name_being_combined}' has failed",
                )

        # I think we can start this step,
        # so compile a set of variables for it.

        # Outputs - a list of step files that are outputs,
        # and also designated as workflow outputs.
        # Any step can write files to the Projetc directory
        # but only job outputs that are also workflow outputs
        # are put in this list.
        outputs: set[str] = set()

        # Start with any variables provided in the step's specification.
        # A map that we will add to (and maybe even over-write)...
        variables: dict[str, Any] = step_definition["specification"].get(
            "variables", {}
        )
        # ...and the running workflow variables
        rwf_variables: dict[str, Any] = rwf.get("variables", {})

        # Process the step's "plumbing" relating to workflow variables.
        #
        # This will be a list of Connectors of "in" and "out" variable names.
        # "in" variables are worklfow variables, and "out" variables
        # are expected Job variables. We use this to add variables
        # to the variables map.
        for connector in get_step_workflow_variable_connections(
            step_definition=step_definition
        ):
            assert connector.in_ in rwf_variables
            variables[connector.out] = rwf_variables[connector.in_]
            if is_workflow_output_variable(wf, connector.in_):
                outputs.add(rwf_variables[connector.in_])

        # Process the step's "plumbing" relating to pre-defined variables.
        for connector in get_step_predefined_variable_connections(
            step_definition=step_definition
        ):
            assert connector.in_ in self._predefined_variables
            variables[connector.out] = self._predefined_variables[connector.in_]

        # Now process variables (in the "plumbing" block)
        # that relate to values used in prior steps.
        #
        # The decoder gives us a map indexed by prior step name that's a list of
        # "in"/"out" connectors as above. If this is a combiner step remember
        # that the combiner_input_variable is a used as a list.
        prior_step_plumbing: dict[str, list[Connector]] = (
            get_step_prior_step_connections(step_definition=step_definition)
        )
        for prior_step_name, connections in prior_step_plumbing.items():
            # Retrieve the first prior "running" step in order to get the variables
            # that were used for it.
            #
            # For a combiner step we only need to inspect the first instance of
            # the prior step (the default replica value is '0').
            # We assume all the combiner's prior (parallel) instances
            # have the same variables and values.
            prior_step, _ = self._wapi_adapter.get_running_workflow_step_by_name(
                name=prior_step_name,
                running_workflow_id=rwf_id,
            )
            # Copy "in" value to "out"...
            for connector in connections:
                assert connector.in_ in prior_step["variables"]
                variables[connector.out] = prior_step["variables"][connector.in_]

        # All variables are set ...
        # is this enough to satisfy the setp's Job command?

        job: dict[str, Any] = self._get_step_job(step=step_definition)
        message, success = job_defintion_decoder.decode(
            job["command"], variables, "command", TextEncoding.JINJA2_3_0
        )
        if not success:
            msg = f"Failed command validation for step {step_name} error_msg={message}"
            _LOGGER.warning(msg)
            return StepPreparationResponse(replicas=0, error_num=2, error_msg=msg)

        # Do we replicate this step (run it more than once)?
        #
        # We do if this is not a combiner step and a variable in this step's plumbing
        # refers to an output of a prior step whose type is 'files'.
        # If the prior step is a 'splitter' we populate the 'replication_values' array
        # with the list of files the prior step genrated for its output.
        #
        # In this engine we onlhy act on the _first_ match, i.e. there CANNOT
        # be more than one prior step variable that is 'files'!
        iter_values: list[str] = []
        iter_variable: str | None = None
        if not step_is_combiner:
            for p_step_name, connections in plumbing_of_prior_steps.items():
                # We need to get the Job definition for each step
                # and then check whether the (output) variable is of type 'files'...
                wf_step: dict[str, Any] = get_step(wf, p_step_name)
                assert wf_step
                job_definition: dict[str, Any] = self._get_step_job(step=wf_step)
                jd_outputs: dict[str, Any] = job_defintion_decoder.get_outputs(
                    job_definition
                )
                for connector in connections:
                    if jd_outputs.get(connector.in_, {}).get("type") == "files":
                        iter_variable = connector.out
                        # Get the prior running step's output values
                        response, _ = (
                            self._wapi_adapter.get_running_workflow_step_by_name(
                                name=p_step_name,
                                running_workflow_id=rwf_id,
                            )
                        )
                        rwfs_id = response["id"]
                        assert rwfs_id
                        result, _ = (
                            self._wapi_adapter.get_running_workflow_step_output_values_for_output(
                                running_workflow_step_id=rwfs_id,
                                output_variable=connector.in_,
                            )
                        )
                        iter_values = result["output"].copy()
                        break
                # Stop if we've got an iteration variable
                if iter_variable:
                    break

        # Get the list of instances we depend upon.
        dependent_instances: set[str] = set()
        for p_step_name in plumbing_of_prior_steps:
            # Assume any step can have multiple instances
            response, _ = self._wapi_adapter.get_status_of_all_step_instances_by_name(
                name=p_step_name,
                running_workflow_id=rwf_id,
            )
            for step in response["status"]:
                dependent_instances.add(step["instance_id"])

        num_step_instances: int = max(1, len(iter_values))
        return StepPreparationResponse(
            variables=variables,
            replicas=num_step_instances,
            replica_variable=iter_variable,
            replica_values=iter_values,
            dependent_instances=dependent_instances,
            outputs=outputs,
        )

    def _launch(
        self,
        *,
        rwf: dict[str, Any],
        step_definition: dict[str, Any],
        step_preparation_response: StepPreparationResponse,
    ) -> None:
        """Given a runningWorkflow record, a step defitnion (from the Workflow),
        and the step's variables (in a preparation object) this method launches
        one or more instances of the given step."""
        step_name: str = step_definition["name"]
        rwf_id: str = rwf["id"]
        project_id = rwf["project"]["id"]

        _LOGGER.info("SPR.variable=%s", step_preparation_response.variables)
        _LOGGER.info(
            "SPR.replica_variable=%s", step_preparation_response.replica_variable
        )
        _LOGGER.info("SPR.replica_values=%s", step_preparation_response.replica_values)
        _LOGGER.info(
            "SPR.dependent_instances=%s", step_preparation_response.dependent_instances
        )
        _LOGGER.info("SPR.outputs=%s", step_preparation_response.outputs)

        # Total replicas must be 1 or more
        total_replicas: int = step_preparation_response.replicas
        assert total_replicas >= 1

        variables = step_preparation_response.variables
        assert variables is not None
        for replica in range(step_preparation_response.replicas):

            # If we are replicating this step more than once
            # the 'replica_variable' will be set.
            # We must replace the step's variable
            # with a value expected for this iteration.
            if step_preparation_response.replica_variable:
                assert step_preparation_response.replica_values
                iter_value: str = step_preparation_response.replica_values[replica]
                _LOGGER.info(
                    "Replicating step: %s replica=%s variable=%s value=%s",
                    step_name,
                    replica,
                    step_preparation_response.replica_variable,
                    iter_value,
                )
                # Over-write the replicating variable
                # and set the replication number to a unique +ve non-zero value...
                variables[step_preparation_response.replica_variable] = iter_value

            _LOGGER.info(
                "Launching step: %s RunningWorkflow=%s (name=%s)"
                " step_variables=%s project=%s",
                step_name,
                rwf_id,
                rwf["name"],
                variables,
                project_id,
            )

            lp: LaunchParameters = LaunchParameters(
                project_id=project_id,
                name=step_name,
                debug=rwf.get("debug"),
                launching_user_name=rwf["running_user"],
                launching_user_api_token=rwf["running_user_api_token"],
                specification=step_definition["specification"],
                variables=variables,
                running_workflow_id=rwf_id,
                step_name=step_name,
                step_replication_number=replica,
                total_number_of_replicas=total_replicas,
                step_dependent_instances=step_preparation_response.dependent_instances,
            )
            lr: LaunchResult = self._instance_launcher.launch(launch_parameters=lp)
            rwfs_id = lr.running_workflow_step_id
            assert rwfs_id

            if lr.error_num:
                self._set_step_error(
                    step_name, rwf_id, rwfs_id, lr.error_num, lr.error_msg
                )
            else:
                _LOGGER.info(
                    "Launched step '%s' step_id=%s (command=%s)",
                    step_name,
                    rwfs_id,
                    lr.command,
                )

    def _set_step_error(
        self,
        step_name: str,
        r_wfid: str,
        r_wfsid: str | None,
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
        # There may be a pre-step error (so assume the ID can also be None)
        if r_wfsid:
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
