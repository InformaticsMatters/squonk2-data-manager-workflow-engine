"""Workflow abstract base classes.
Interface definitions of class instances that must be made available to the Engine.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from google.protobuf.message import Message


@dataclass
class LaunchParameters:
    """Parameters to instantiate an Instance.
    The launching user API token is the second element when the request header's
    'Authorization' value is split on white-space."""

    # The Project UUID of the project to launch the instance in
    project_id: str
    # A symbolic name of the Instance
    name: str
    # The user name of the person launching the Instance (Workflow step)
    launching_user_name: str
    # The API Access token provided by the User
    launching_user_api_token: str
    # The specification, which can contain 'variables'
    specification: dict[str, Any]
    # An alternative way to pass variables to the specification.
    # If used it will replace any 'variables' already present in the specification.
    specification_variables: dict[str, Any] | None = None
    # A string. In DM v4 converted to a boolean and set in the
    # instance Pod as a label. Setting this means the Instances
    # that are created will not be automatically removed by the Job operator.
    debug: str | None = None
    # The RunningWorkflow UUID.
    # Required if the Instance is part of a Workflow step.
    running_workflow_id: str | None = None
    # The RunningWorkflowStep UUID.
    # Required if the Instance is part of a Workflow step.
    running_workflow_step_id: str | None = None
    # The application ID (a custom resource name)
    # used to identify the 'type' of Instance to create.
    # For DM Jobs this will be 'datamanagerjobs.squonk.it'
    application_id: str = "datamanagerjobs.squonk.it"


@dataclass
class LaunchResult:
    """Results returned from methods in the InstanceLauncher.
    Any error returned in this object is a launch error, not a Job error."""

    # A numeric non-zero error code if an error occurred
    # and an error message if the error number is not zero.
    error_num: int = 0
    error_msg: str | None = None
    # The following optional properties
    # may not be present if there's a launch error.
    #
    # The Instance UUID that was created for you.
    instance_id: str | None = None
    # The Task UUID that is handling the Instance launch
    task_id: str | None = None
    # The rendered command used in the instance
    command: str | None = None
    # A callback token (unused in Workflows)
    callback_token: str | None = None


class InstanceLauncher(ABC):
    """The class handling the launching of (Job) instances, used by the Engine
    to launch Workflow 'Step' Jobs."""

    @abstractmethod
    def launch(
        self,
        *,
        launch_parameters: LaunchParameters,
        **kwargs: str,
    ) -> LaunchResult:
        """Launch a (Job) Instance"""

        # launch() provides the instance launcher with sufficient information
        # to not only create an instance but also create any RunningWorkflow
        # and RunningWorkflowStep records. The WE must identify the step to run
        # and then render the specification (using the DM Job Decoder) using
        # workflow parameters and workflow input and output connections.
        #
        # A lot of logic will need to be 'refactored' and maybe the launcher()
        # needs to render the specification based on variables injected into the
        # step_specification by the WE? Remember that we have to deal with
        # "input Handlers" that manipulate the specification variables.
        # See _instance_preamble() in the DM's api_instance.py module.


class WorkflowAPIAdapter(ABC):
    """The APIAdapter providing read/write access to various Workflow tables and records
    in the Model that is owned by the DM. It provides the ability to create and retrieve
    Workflow, RunningWorkflow and RunningWorkflowStep records returning dictionary
    (API-like) responses."""

    @abstractmethod
    def get_workflow(
        self,
        *,
        workflow_id: str,
    ) -> tuple[dict[str, Any], int]:
        """Get a Workflow Record by ID."""
        # If present this should return:
        # {
        #    "name": "workflow-name",
        #    "steps": [
        #      {
        #        "name": "step-name"
        #        "specification": "{}",
        #       }
        #     ]
        # }
        # If not present an empty dictionary should be returned.
        #
        # The 'int' in the return tuple here (and elsewhere in this ABC)
        # is an HTTP status code to simplify the DM implementation,
        # and allow it to re-use any 'views.py' function that may be defined.
        # This value is ignored by the Engine.

    @abstractmethod
    def get_running_workflow(
        self, *, running_workflow_id: str
    ) -> tuple[dict[str, Any], int]:
        """Get a RunningWorkflow Record"""
        # Should return:
        # {
        #       "name": "workflow-name",
        #       "running_user": "alan",
        #       "running_user_api_token": "123456789",
        #       "done": False,
        #       "success": false,
        #       "error_num": 0,
        #       "error_msg": "",
        #       "workflow": {
        #          "id": "workflow-000",
        #       },
        #       "project": {
        #          "id": "project-000",
        #       },
        #       "variables": {
        #          "x": 1,
        #          "y": 2,
        #       },
        # }
        # If not present an empty dictionary should be returned.

    @abstractmethod
    def set_running_workflow_done(
        self,
        *,
        running_workflow_id: str,
        success: bool,
        error_num: int | None = None,
        error_msg: str | None = None,
    ) -> None:
        """Set the success value for a RunningWorkflow Record.
        If not successful an error code and message should be provided."""

    @abstractmethod
    def create_running_workflow_step(
        self,
        *,
        running_workflow_id: str,
        step: str,
        prior_running_workflow_step_id: str | None = None,
    ) -> tuple[dict[str, Any], int]:
        """Create a RunningWorkflowStep Record (from a RunningWorkflow)"""
        # Should return:
        # {
        #    "id": "r-workflow-step-00000000-0000-0000-0000-000000000001",
        # }

    @abstractmethod
    def get_running_workflow_step(
        self, *, running_workflow_step_id: str
    ) -> tuple[dict[str, Any], int]:
        """Get a RunningWorkflowStep Record"""
        # Should return:
        # {
        #       "name:": "step-1234",
        #       "done": False,
        #       "success": false,
        #       "error_num": 0,
        #       "error_msg": "",
        #       "variables": {
        #          "x": 1,
        #          "y": 2,
        #       },
        #       "running_workflow": {
        #          "id": "r-workflow-00000000-0000-0000-0000-000000000001",
        #       },
        # }
        # If not present an empty dictionary should be returned.
        #
        # For steps that are not the first in a workflow the following field
        # can be expected in the response: -
        #
        #       "prior_running_workflow_step": {
        #          "id": "r-worflkow-step-00000000-0000-0000-0000-000000000001",
        #       },

    @abstractmethod
    def set_running_workflow_step_variables(
        self,
        *,
        running_workflow_step_id: str,
        variables: dict[str, Any],
    ) -> None:
        """Set the variables used prior to decoding the step command for each step.
        This can be used to understand step failures but will also be vital
        when adding variables values to subsequent steps from prior step values."""

    @abstractmethod
    def set_running_workflow_step_done(
        self,
        *,
        running_workflow_step_id: str,
        success: bool,
        error_num: int | None = None,
        error_msg: str | None = None,
    ) -> None:
        """Set the success value for a RunningWorkflowStep Record,
        If not successful an error code and message should be provided."""

    @abstractmethod
    def get_workflow_steps_driving_this_step(
        self,
        *,
        running_workflow_step_id: str,
    ) -> tuple[dict[str, Any], int]:
        """Get all the step records that belong to the Workflow for the given
        RunningWorkflowStep record ID. You are also given the caller's position
        in the list, which will be -1 if the caller is not present."""
        # It should return:
        # {
        #    "caller_step_index": 0,
        #    "steps": "steps": [
        #      {
        #        "name": "step-name"
        #        "specification": "{}",
        #       }
        #     ]
        # }

    @abstractmethod
    def get_instance(self, *, instance_id: str) -> tuple[dict[str, Any], int]:
        """Get an Instance Record"""
        # For a RunningWorkflowStep Instance it should return:
        # {
        #    "running_workflow_step_id": "r-workflow-step-00000000-0000-0000-0000-000000000001",
        # }
        # If not present an empty dictionary should be returned.

    @abstractmethod
    def get_job(
        self,
        *,
        collection: str,
        job: str,
        version: str,
    ) -> tuple[dict[str, Any], int]:
        """Get a Job"""
        # Should return:
        # {
        #   "command": "<command string>",
        #   "definition": "<the definition as a Python dictionary>",
        # }
        # If not present an empty dictionary should be returned.


class MessageDispatcher(ABC):
    """The class handling the sending of messages (on the Data Manager message bus)."""

    @abstractmethod
    def send(self, message: Message) -> None:
        """Send a message"""
