"""The WorkflowEngine - workflow execution logic.

This module realises workflow definitions, turning a definition into a controlled sequence
of Job executions. The Data Manager is responsible for storing and validating Workflows,
and this module is responsible for running them and reporting their state back to the
DM.

The engine is event-driven, responding to two types of message
(in the form of Protocol Buffers) - Workflow messages and a Pod messages.
These messages, sent from the DM Protocol Buffer Consumer (PBC), are delivered to the
engine via its 'handle_message()' method. The engine must react to these messages
appropriately by: -

-   Starting the execution of a new Workflow
    (when it receives a Workflow 'START' message)
-   Stopping the execution of an exiting Workflow
    (when it receives a Workflow 'STOP' message)
-   Progressing an exiting running workflow by launching any Step that its
    prior Steps have unblocked (when it receives a Pod message)

Both message types are handled the same way - the engine works out which Steps
are READY and launches all of them. This logic lives in '_launch_ready_steps()',
supported by '_get_step_states()' and '_get_ready_steps()'.

When running a workflow, once the engine determines the action (the Steps to run)
its most complex logic lies in the preparation of a set variables for the Step (Job).
This logic is confined to '_prepare_step()', which returns a 'StepPreparationResponse'
dataclass object. This object is used by the second key method in this module,
'_launch()'. The launch methods used the prepared variables and launches (using
a DM-provided 'InstanceLauncher' implementation) one or more Instances of a Step Job,
providing each with an appropriate set of command variables.

Module philosophy
-----------------
The module's role is to translate a pre-validated workflow definition into the
execution of Step "Jobs" that manifest as Pod "Instances" running in a project directory
under the control of the DM.

Workflow messages are used to initiate (START) and terminate (STOP) workflows.
Pod messages signal the end of a previously launched step and carry the exit code
of the executed Job.

The engine does not follow the order the steps happen to be written in. Instead,
each time it handles a message it examines every step in the workflow and launches
those that are READY. A step is READY when it has not already been launched and
every step it depends on has finished successfully. Dependencies come from the
step's "plumbing" - a step that takes no values from another step depends on
nothing and so is READY the moment the workflow starts. This means a workflow can
begin with several steps at once, that independent branches run concurrently, and
that a step drawing on two prior steps waits for both.

That design makes it essential that a step is launched only once. The engine
re-assesses steps that have already run, and excludes them by asking the DM
whether they were launched. This check cannot be atomic with the launch itself,
so the guarantee ultimately rests on 'InstanceLauncher.launch()' being idempotent
for a given (running workflow, step name, replica) - see 'workflow_abc.py'.

A running workflow is finished when nothing is running and nothing new could be
launched. If steps remain that were never launched, the workflow has stalled -
which a validated definition should make impossible - and it is failed rather
than being reported as a success.

The engine does has no persistence and not create database records. Instead it relies
on an API 'wrapper' to retrieve records and alter them.

Objects that provide API and InstanceLauncher implementations are made available
to the engine when the DM creates it. passing them through the class initialiser.

The engine is designed not to retain any state persistence, it reacts to messages,
reconstructing its state based on Workflow, RunningWorkflow, and RunningWorkflowStep
records maintained by the DM. There's no real 'pattern' here - it's simply complex
custom logic that is executed from the context of 'handle_message()'
that has to translate a workflow definition into running Job Instances.

If there is a pattern its closest approximation is probably a State pattern, closely
related to a Finite State Machine with the function 'handle_message()' used to alter
the engine's 'state'. The engine is in fact a complex running workflow 'state machine',
hence the term 'Engine' (another term for machine) used in its class name.

Only one instance of the engine is created by the DM so it also essentially exists as a
Singleton.

There are no sub-classes or other modules. Today all the state logic is captured
in this single module. There is no need to introduce level of redirection that simply
reduce the size of the file. There is a level of complexity that cannot be avoided -
the need to understand how to move a workflow forward and how to prepare a set of
variables for the next 'Step'.
"""

import logging
import sys
from dataclasses import dataclass, field
from typing import Any, Optional

import decoder.decoder as job_definition_decoder
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
    get_step_dependencies,
    get_step_names,
    get_step_predefined_variable_connections,
    get_step_prior_step_connections,
    get_step_specification,
    get_step_workflow_variable_connections,
    get_steps,
    is_workflow_input_variable,
    is_workflow_output_variable,
)

_LOGGER: logging.Logger = logging.getLogger(__name__)
_LOGGER.setLevel(logging.INFO)
_LOGGER.addHandler(logging.StreamHandler(sys.stdout))

# The variable expected to bu used by "combiner" steps,
# those that take inputs from multiple prior steps.
# This variable gets set to the engine's 'instance-link-glob'
# pre-defined variable
_INSTANCE_LINK_GLOB_VARIABLE: str = "dirsGlob"


@dataclass
class StepState:
    """The execution state of one Step, across all of its replicas.
    A Step is only 'done' once every replica it was launched with exists
    and has finished - while a Step is being fanned out the replicas that
    do exist may all be 'done' while others are yet to be created."""

    launched: bool
    done: bool
    success: bool


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
    replica_variable: str | None = None
    replica_instance_id: str | None = None
    variables: dict[str, Any] = field(default_factory=dict)
    replica_values: list[str] = field(default_factory=list)
    dependent_instances: set[str] = field(default_factory=set)
    outputs: set[str] = field(default_factory=set)
    inputs: set[str] = field(default_factory=set)
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
        instance_id_dir_prefix: str = ".",
    ):
        """Initialiser, given a Workflow API adapter, Instance launcher,
        and a step (directory) link 'glob' (a convenient directory glob to
        locate the DM hard-link directories of prior instances inserted into a
        step's instance directory, typically '.instance-*')"""
        # Keep the dependent objects
        self._wapi_adapter: WorkflowAPIAdapter = wapi_adapter
        self._instance_launcher: InstanceLauncher = instance_launcher
        self._instance_link_glob: str = instance_link_glob
        self._instance_id_dir_prefix: str = instance_id_dir_prefix

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
        running workflow. We use the running workflow (and workflow) to find every
        step that is READY and launch it, passing the running workflow variables
        to the launcher.

        At this point nothing has run, so the READY steps are those that do not
        depend on any other step - all the variables for their commands will
        (must) be defined in the RunningWorkflow's variables. There is usually
        one such step, but there can be several and all of them are launched.

        A step is not launched if there's an error preparing it."""

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

        # Launch whatever's READY.
        # If there's a launch problem the step (and running workflow) will have
        # an error, stopping it. There will be no Pod event as the launch has failed.
        if not self._launch_ready_steps(wf=wf_response, rwf=rwf_response):
            # Nothing could be started, so nothing will ever send us a Pod message
            # to move this workflow on. Unless a step preparation error has
            # already stopped the workflow, this workflow cannot run.
            self._set_running_workflow_done_if_stalled(wf=wf_response, rwf=rwf_response)

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

        # If we get here the prior step completed successfully
        # so we mark the Step as DONE (successfully).
        wfid = rwf_response["workflow"]["id"]
        assert wfid
        wf_response, _ = self._wapi_adapter.get_workflow(workflow_id=wfid)
        _LOGGER.debug("API.get_workflow(%s) returned: -\n%s", wfid, str(wf_response))

        # We then inspect the Workflow to determine the next step.
        _LOGGER.debug("End of RunningWorkflowStep %s (%s)", r_wfsid, r_wfid)
        self._wapi_adapter.set_running_workflow_step_done(
            running_workflow_step_id=r_wfsid,
            success=True,
        )

        # A step has just finished, so steps that were waiting on it may now be
        # READY. We re-assess the whole workflow and launch everything we can -
        # this step may have been the last thing several steps were waiting for.
        #
        # A major piece of work to accomplish is to get ourselves into a position
        # that allows us to check the step command can be executed.
        # We do this by compiling a map of variables we believe each step needs.
        if self._launch_ready_steps(wf=wf_response, rwf=rwf_response):
            # Something was started (or there was a launch error and the step
            # and running workflow error will have been set).
            # Regardless we can stop now - a Pod message will bring us back.
            return

        # Nothing was launched. Either the workflow still has steps running
        # (in which case their Pod messages will bring us back) or it has
        # reached its end.
        self._set_running_workflow_done_if_stalled(wf=wf_response, rwf=rwf_response)

    def _set_running_workflow_done_if_stalled(
        self, *, wf: dict[str, Any], rwf: dict[str, Any]
    ) -> None:
        """Called when nothing could be launched. If any step is still running
        we do nothing - its Pod message will bring us back. Otherwise the running
        workflow has stopped moving and we record why.

        Every step launched and finished is a successful workflow. Anything else
        means steps remain that will never become READY - which a validated
        workflow should make impossible, so it is an error rather than success."""
        r_wfid: str = rwf["id"]

        # Do nothing if the running workflow has already been stopped
        # (a step or preparation error will have done this).
        rwf_response, _ = self._wapi_adapter.get_running_workflow(
            running_workflow_id=r_wfid
        )
        if rwf_response.get("done"):
            _LOGGER.debug("Running workflow already stopped (%s)", r_wfid)
            return

        step_states: dict[str, StepState] = self._get_step_states(wf=wf, rwf_id=r_wfid)
        if any(state.launched and not state.done for state in step_states.values()):
            _LOGGER.debug("Steps are still running for %s", r_wfid)
            return

        if unrunnable := [
            step_name for step_name, state in step_states.items() if not state.launched
        ]:
            msg: str = (
                "The following steps could not be run:"
                f" {', '.join(sorted(unrunnable))}"
            )
            _LOGGER.warning("%s (%s)", msg, r_wfid)
            self._wapi_adapter.set_running_workflow_done(
                running_workflow_id=r_wfid,
                success=False,
                error_num=6,
                error_msg=msg,
            )
            return

        _LOGGER.debug("End of RunningWorkflow %s", r_wfid)
        self._wapi_adapter.set_running_workflow_done(
            running_workflow_id=r_wfid,
            success=True,
        )

    def _get_step_states(
        self, *, wf: dict[str, Any], rwf_id: str
    ) -> dict[str, StepState]:
        """Returns the execution state of every Step in the given Workflow,
        indexed by step name. State is reconstructed from the DM's records -
        the engine caches nothing between messages."""
        states: dict[str, StepState] = {}
        for step_name in get_step_names(wf):
            response, _ = self._wapi_adapter.get_status_of_all_step_instances_by_name(
                name=step_name,
                running_workflow_id=rwf_id,
            )
            assert "count" in response
            count: int = response["count"]
            if not count:
                states[step_name] = StepState(launched=False, done=False, success=False)
                continue
            # Every replica that was launched must exist and have finished
            # before we can call the step 'done'. The 'replicas' value tells us
            # how many to expect - a step that is still being fanned out will
            # have fewer records than that.
            statuses: list[dict[str, Any]] = response["status"]
            expected_replicas: int = statuses[0].get("replicas", count)
            done: bool = count == expected_replicas and all(
                status["done"] for status in statuses
            )
            states[step_name] = StepState(
                launched=True,
                done=done,
                success=done and all(status["success"] for status in statuses),
            )
        return states

    def _get_ready_steps(
        self, *, wf: dict[str, Any], step_states: dict[str, StepState]
    ) -> list[dict[str, Any]]:
        """Returns the definitions of every Step that can be launched right now.

        A Step is READY when it has not already been launched and every step it
        depends on has finished successfully. A Step that depends on nothing is
        therefore READY the moment its workflow starts. Steps are returned in
        definition order so that launches are deterministic."""
        ready: list[dict[str, Any]] = []
        for step in get_steps(wf):
            step_name: str = step["name"]
            # Never launch a step twice. The engine re-assesses every step on
            # every message, so steps that have run are still sitting here.
            if step_states[step_name].launched:
                continue
            # A dependency on a step that isn't in the workflow can never be
            # satisfied, so the step is never READY. Validation should stop a
            # definition like that reaching us, but if one does we leave the
            # step unlaunched and let the caller report a stalled workflow -
            # which is far kinder than launching it and failing an assertion
            # while preparing its variables.
            dependencies: set[str] = get_step_dependencies(step_definition=step)
            if all(
                dependency in step_states and step_states[dependency].success
                for dependency in dependencies
            ):
                ready.append(step)
        return ready

    def _launch_ready_steps(self, *, wf: dict[str, Any], rwf: dict[str, Any]) -> int:
        """Finds every READY Step and launches it, returning the number of steps
        that were launched. Zero is not an error - it usually just means the
        workflow is waiting on steps that are still running.

        If a step cannot be prepared the running workflow is failed and we stop."""
        rwf_id: str = rwf["id"]
        step_states: dict[str, StepState] = self._get_step_states(wf=wf, rwf_id=rwf_id)
        ready_steps: list[dict[str, Any]] = self._get_ready_steps(
            wf=wf, step_states=step_states
        )
        _LOGGER.info(
            "Ready steps for %s: %s",
            rwf_id,
            [step["name"] for step in ready_steps],
        )

        launched: int = 0
        for step in ready_steps:
            sp_resp: StepPreparationResponse = self._prepare_step(
                wf=wf, step_definition=step, rwf=rwf
            )
            if sp_resp.error_num:
                self._wapi_adapter.set_running_workflow_done(
                    running_workflow_id=rwf_id,
                    success=False,
                    error_num=sp_resp.error_num,
                    error_msg=sp_resp.error_msg,
                )
                return launched
            if sp_resp.replicas == 0:
                # Not an error - the step cannot be prepared yet,
                # so we'll re-assess it when a later message arrives.
                _LOGGER.info(
                    "Step '%s' is not yet preparable - deferring", step["name"]
                )
                continue
            if self._launch(
                rwf=rwf,
                step_definition=step,
                step_preparation_response=sp_resp,
            ):
                launched += 1

        return launched

    def _get_step_job(self, *, step: dict[str, Any]) -> dict[str, Any]:
        """Gets the Job definition for a given Step."""
        # We get the Job from the step specification, which must contain
        # the keys "collection", "job", and "version". Here we assume that
        # the workflow definition has passed the RUN-level validation
        # which means we can get these values.
        #
        # The validator should have verified the Job exists, but it might not
        # when we need it - so this method might return '{}'.
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
        # Why?
        #
        # A combiner's execution is based on the possible concurrent execution
        # of one (or more) prior steps. If we are a combiner then we use the name of the
        # step we are combining (there can only be one) so that we can ensure
        # all its step instances have finished (successfully) before continuing.
        #
        # We are a combiner if a variable in our step's plumbing refers to an input
        # whose origin is of type 'files'.

        _LOGGER.info("Preparing step '%s'...", step_name)

        our_job_definition: dict[str, Any] = self._get_step_job(step=step_definition)
        if not our_job_definition:
            return StepPreparationResponse(
                replicas=0,
                error_num=1,
                error_msg=f"The Job for step '{step_name}' is not present",
            )
        our_inputs: dict[str, Any] = job_definition_decoder.get_inputs(
            our_job_definition
        )
        # get all our step connections that relate to prior steps.
        # If we're a combiner we will have variables based on prior steps.
        plumbing_of_prior_steps: dict[str, list[Connector]] = (
            get_step_prior_step_connections(step_definition=step_definition)
        )

        _LOGGER.debug("Step '%s' inputs=%s", step_name, our_inputs)
        _LOGGER.debug(
            "Step '%s' prior step plumbing=%s", step_name, plumbing_of_prior_steps
        )

        # We are a combiner if a variable in our step's plumbing refers to one of
        # our own inputs whose type is 'files'. Combiners handle their prior-step
        # inputs differently (a directory glob rather than named files) and are
        # never replicated, which is all this flag is used for.
        #
        # We do not need to check here that the steps we combine have finished.
        # A step is only prepared once it is READY, and READY already requires
        # every step it depends on to have completed successfully.
        we_are_a_combiner: bool = any(
            our_inputs.get(connector.out, {}).get("type") == "files"
            for connections in plumbing_of_prior_steps.values()
            for connector in connections
        )

        _LOGGER.debug("Step '%s' is combiner (%s)", step_name, we_are_a_combiner)

        # We can now compile a set of variables for the step.

        # Inputs - a list of step files that are workflow inputs.
        # These are project files that are copied into the step instance.
        inputs: set[str] = set()
        # Outputs - a list of step files that are workflow outputs.
        # Any step can write files to the Project directory
        # but this only consists of job outputs that are also workflow outputs.
        outputs: set[str] = set()

        # Our initial set of variables begins with the variables provided in the step's
        # specification. It is a map that we will add to and then (eventually)
        # pass to the instance launcher. Here we refer to them as 'prime_variables'.
        prime_variables: dict[str, Any] = step_definition["specification"].get(
            "variables", {}
        )
        # The variables provided by the user when running the workflow
        # (the running workflow variables)...
        rwf_variables: dict[str, Any] = rwf.get("variables", {})

        # Adjust our prime variables by adding any values
        # from workflow variables that are mentioned in the step's "plumbing".
        #
        # The decoder gives us a list of 'Connectors' that are a par of variable
        # names representing "in" (workflow) and "out" (step) variable names.
        # "in" variables are workflow variables, and "out" variables
        # are expected Step (Job) variables. We use these connections to
        # take workflow variables and put them in our variables map.
        for connector in get_step_workflow_variable_connections(
            step_definition=step_definition
        ):
            assert connector.in_ in rwf_variables
            prime_variables[connector.out] = rwf_variables[connector.in_]
            if is_workflow_output_variable(wf, connector.in_):
                outputs.add(rwf_variables[connector.in_])
            elif is_workflow_input_variable(wf, connector.in_):
                inputs.add(rwf_variables[connector.in_])

        # Add any pre-defined variables used in the step's "plumbing"
        for connector in get_step_predefined_variable_connections(
            step_definition=step_definition
        ):
            assert connector.in_ in self._predefined_variables
            prime_variables[connector.out] = self._predefined_variables[connector.in_]

        # Using the "plumbing" again so that we can add any variables
        # that relate to values used in prior steps.
        #
        # The decoder gives us a set of "in"/"out" connectors as above
        # indexed by the prior step name.
        #
        # 'inputs' here are not copied to our step's instance directory,
        # instead we need to prefix any 'input' with the instance directory for the
        # step the input belongs to. e.g. "file.txt" will become
        # ".instance-0000/file.txt".
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
            # have the same variables and values. Combiners handle inputs from
            # prior steps differently - i.e. they must use a directory 'glob'
            # due to the uncontrolled number of prior steps.
            prior_step, _ = self._wapi_adapter.get_running_workflow_step_by_name(
                name=prior_step_name,
                running_workflow_id=rwf_id,
            )
            assert prior_step
            _LOGGER.info(
                "API.get_running_workflow_step_by_name(%s) got %s\n",
                prior_step_name,
                str(prior_step),
            )
            assert "instance_id" in prior_step
            p_i_id: str = prior_step["instance_id"]
            p_i_dir: str = f"{self._instance_id_dir_prefix}{p_i_id}"
            # Get prior step Job (to look for its outputs that are our inputs)
            # (if we're not a combiner)
            p_job_outputs: dict[str, Any] = {}
            if not we_are_a_combiner:
                p_step_spec: dict[str, Any] = get_step_specification(
                    wf, prior_step_name
                )
                _LOGGER.info("get_step_specification() got %s\n", str(p_step_spec))
                p_job, _ = self._wapi_adapter.get_job(
                    collection=p_step_spec["collection"],
                    job=p_step_spec["job"],
                    version=p_step_spec["version"],
                )
                _LOGGER.info("API.get_job() got %s\n", str(p_job))
                assert p_job
                p_job_outputs = job_definition_decoder.get_outputs(p_job)
            # Copy "in" value to "out"...
            # (prefixing inputs with instance directory if required)
            assert "variables" in prior_step
            for connector in connections:
                assert connector.in_ in prior_step["variables"]
                value: str = prior_step["variables"][connector.in_]
                if not we_are_a_combiner and connector.in_ in p_job_outputs:
                    # Prefix with prior-step's instance directory
                    value = f"{p_i_dir}/{value}"
                prime_variables[connector.out] = value

        # Our step's prime variables are now set.

        # Before we return these to the caller do we have enough
        # to satisfy the step Job's command? It's a simple check -
        # we give the step's Job command and our prime variables
        # to the Job decoder - it wil tell us if an important
        # variable is missing....
        message, success = job_definition_decoder.decode(
            our_job_definition["command"],
            prime_variables,
            "command",
            TextEncoding.JINJA2_3_0,
        )
        if not success:
            msg = f"Failed command validation for step {step_name} error_msg={message}"
            _LOGGER.warning(msg)
            return StepPreparationResponse(replicas=0, error_num=3, error_msg=msg)

        # Do we replicate this step (run it more than once in parallel)?
        #
        # Why?
        #
        # We need to set the number of step replicas to run.
        #
        # If we're not a combiner and a variable in our "plumbing" refers to a variable
        # of type "files" in a prior step then we are expected to run multiple times
        # (even if just once). The number of times we're expected to run is dictated
        # by the number of values (files) in the "files" variable.
        #
        # In this engine we only act on the _first_ variable match, i.e. we do not
        # expect and wil not act on more than one prior step variable that is of type
        # "files".
        #
        # If we do run more than once we'll set 'iter_variable' to the name of our
        # variable (that is to be given multiple values) and 'iter_values' will
        # be the list of files produced by the dependent step forming out inputs.
        # If the dependent step produces file1, file2, and file3 we'll run out step
        # 3 times, with each being given a different file as its input.
        iter_values: list[str] = []
        iter_variable: str | None = None
        iter_instance_id: str | None = None
        if not we_are_a_combiner:
            for p_step_name, connections in plumbing_of_prior_steps.items():
                # We need to get the Job definition for each step
                # and then check whether the (output) variable is of type "files"...
                wf_step: dict[str, Any] = get_step(wf, p_step_name)
                assert wf_step
                job_definition: dict[str, Any] = self._get_step_job(step=wf_step)
                if not job_definition:
                    return StepPreparationResponse(
                        replicas=0,
                        error_num=4,
                        error_msg=f"The Job for step '{p_step_name}' is not present",
                    )
                jd_outputs: dict[str, Any] = job_definition_decoder.get_outputs(
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
                        iter_instance_id = response["instance_id"]
                        assert iter_instance_id
                        result, _ = (
                            self._wapi_adapter.get_running_workflow_step_output_values_for_output(
                                running_workflow_step_id=rwfs_id,
                                output_variable=connector.in_,
                            )
                        )
                        _LOGGER.info(
                            "API.get_running_workflow_step_output_values_for_output() got %s\n",
                            str(result),
                        )
                        iter_values = result["output"].copy()
                        break
                # Stop if we've got an iteration variable
                if iter_variable:
                    break

        # If we've set an iteration variable we should have at least one value.
        # If not we cannot continue.
        if iter_variable and len(iter_values) == 0:
            msg = f"The step prior to step '{step_name}' had no outputs. At least one is needed"
            _LOGGER.warning(msg)
            return StepPreparationResponse(replicas=0, error_num=5, error_msg=msg)

        # Get the list of instances we depend upon.
        #
        # We need to do this so that the launcher can hard-link
        # their instance directories into ours.
        dependent_instances: set[str] = set()
        for p_step_name in plumbing_of_prior_steps:
            # Any step can depend on multiple instances
            response, _ = self._wapi_adapter.get_status_of_all_step_instances_by_name(
                name=p_step_name,
                running_workflow_id=rwf_id,
            )
            for step in response["status"]:
                dependent_instances.add(step["instance_id"])

        # We're done.
        # We have a set of prime variables,
        # a list of dependent step instances,
        # and we know how many steps replicas to run.
        num_step_instances: int = max(1, len(iter_values))
        return StepPreparationResponse(
            variables=prime_variables,
            replicas=num_step_instances,
            replica_variable=iter_variable,
            replica_values=iter_values,
            replica_instance_id=iter_instance_id,
            dependent_instances=dependent_instances,
            outputs=outputs,
            inputs=inputs,
        )

    def _launch(
        self,
        *,
        rwf: dict[str, Any],
        step_definition: dict[str, Any],
        step_preparation_response: StepPreparationResponse,
    ) -> bool:
        """Given a runningWorkflow record, a step definition (from the Workflow),
        and the step's variables (in a preparation object) this method launches
        one or more instances of the given step. Returns True if at least one
        instance was launched."""
        step_name: str = step_definition["name"]
        rwf_id: str = rwf["id"]
        project_id = rwf["project"]["id"]

        _LOGGER.info("SPR.variables=%s", step_preparation_response.variables)
        _LOGGER.info(
            "SPR.replica_variable=%s", step_preparation_response.replica_variable
        )
        _LOGGER.info("SPR.replica_values=%s", step_preparation_response.replica_values)
        _LOGGER.info(
            "SPR.dependent_instances=%s", step_preparation_response.dependent_instances
        )
        _LOGGER.info("SPR.inputs=%s", step_preparation_response.inputs)
        _LOGGER.info("SPR.outputs=%s", step_preparation_response.outputs)

        # Total replicas must be 1 or more
        total_replicas: int = step_preparation_response.replicas
        assert total_replicas >= 1

        launched: bool = False
        variables = step_preparation_response.variables
        for replica in range(step_preparation_response.replicas):

            # If we are replicating this step more than once
            # the 'replica_variable' will be set.
            # We must replace the step's variable
            # with a value expected for this iteration.
            if step_preparation_response.replica_variable:
                assert step_preparation_response.replica_values
                iter_value: str = step_preparation_response.replica_values[replica]
                _LOGGER.info(
                    "Replicating step: %s replica=%s variable=%s value=%s origin=%s",
                    step_name,
                    replica,
                    step_preparation_response.replica_variable,
                    iter_value,
                    step_preparation_response.replica_instance_id,
                )
                # Over-write the replicating variable
                # and set the replication number to a unique +ve non-zero value...
                variables[step_preparation_response.replica_variable] = (
                    f"{self._instance_id_dir_prefix}"
                    f"{step_preparation_response.replica_instance_id}"
                    f"/{iter_value}"
                )

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
                step_dependent_instances=list(
                    step_preparation_response.dependent_instances
                ),
                step_project_inputs=list(step_preparation_response.inputs),
                step_project_outputs=list(step_preparation_response.outputs),
            )
            lr: LaunchResult = self._instance_launcher.launch(launch_parameters=lp)

            if lr.error_num:
                self._set_step_error(
                    step_name,
                    rwf_id,
                    lr.running_workflow_step_id,
                    lr.error_num,
                    lr.error_msg,
                )
            elif lr.already_launched:
                # Not an error. We asked for a step that had already been
                # launched, so nothing new is running and nothing new will
                # report back. We simply lost a race with another launch.
                _LOGGER.info(
                    "Step '%s' (replica %s) was already launched - ignoring",
                    step_name,
                    replica,
                )
            else:
                # No error - there must be a RunningWorkflowStep ID
                assert lr.running_workflow_step_id
                launched = True
                _LOGGER.info(
                    "Launched step '%s' step_id=%s (command=%s)",
                    step_name,
                    lr.running_workflow_step_id,
                    lr.command,
                )

        return launched

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
