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
    # The user name of the person launching the Instance
    launching_user_name: str
    # The API Access token provided by the User
    launching_user_api_token: str
    # The specification, which can contain 'variables'
    specification: dict[str, Any]
    # The 'preferred' way to provide variables for the Job's specification.
    # If used it will replace any 'variables' already present in the specification
    # (values are no merged).
    variables: dict[str, Any] | None = None
    # A string. In DM v4, if any value is set a corresponding boolean is set in the
    # instance Pod as a label. Setting this means the Instances
    # that are created will not be automatically removed by the Job operator.
    debug: str | None = None
    # The RunningWorkflow UUID.
    # Required if the Instance is part of a Workflow step.
    running_workflow_id: str | None = None
    # The RunningWorkflow's step name.
    # Required if the Instance is part of a Workflow step.
    step_name: str | None = None
    # The step replication number.
    # A numeric vale expected to be in the range 0 to total_number_of_replicas - 1.
    # If a step is laucnhed 5 times the values used when laucnhing each instance
    # must be 0, 1, 2, 3, and 4.
    step_replication_number: int = 0
    # The total number of replicas of this instance that are expected to be laucnhed.
    # This cannot be less than 1 and must be grater than any value of
    # 'step_replication_number' that will be used for the same step.
    total_number_of_replicas: int = 1
    # A set of dependent (prior step) instance directroies that are expected to be
    # hard-linked into the instance directory the launcher will create.
    # These are required so that the step can access the dependent step's files.
    # It is a set of instance UUIDs.
    step_dependent_instances: set[str] | None = None
    # A set of dependent project files that are expected to be hard-linked
    # into the instance directory the launcher will create.
    # These are required so that the step can access project files.
    # It is a set project-relative filenames (or directroies).
    step_project_inputs: set[str] | None = None
    # A set of step instance files that are expected to be hard-linked
    # into the surrounding Project directory.
    # It is a set instance-relative filenames (or directroies).
    step_project_outputs: set[str] | None = None
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
    # A running workflow step UUID
    # (if the step is part of a running workflow)
    running_workflow_step_id: str | None = None
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
        **kwargs: Any,
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
    (API-like) responses.

    This adapter also provides methods to copy outputs to the
    corresponding Project from Workflow steps that generate outputs."""

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
        #       "success": False,
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
    def get_running_steps(
        self, *, running_workflow_id: str
    ) -> tuple[dict[str, Any], int]:
        """Get a list of steps (their names) that are currently running for the
        given RunningWorkflow Record"""
        # Should return:
        # {
        #    "count": 1,
        #    "steps": [
        #       {
        #           "name": "step-1234"
        #       }
        #    ]
        # }

    @abstractmethod
    def get_status_of_all_step_instances_by_name(
        self, *, name: str, running_workflow_id: str
    ) -> tuple[dict[str, Any], int]:
        """Get a list of step execution statuses for the named step.
        This includes their step UUID (and instance UUID if available).
        """
        # Should return:
        # {
        #    "count": 2,
        #    "status": [
        #       {
        #           "done": True,
        #           "success": True,
        #           "running_workflow_step_id": "step-0001",
        #           "instance_id": "instance-0001"
        #       },
        #       {
        #           "done": False,
        #           "success": False,
        #           "running_workflow_step_id": "step-0002",
        #           "instance_id": "instance-0002"
        #       }
        #    ]
        # }

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
    def get_running_workflow_step(
        self, *, running_workflow_step_id: str
    ) -> tuple[dict[str, Any], int]:
        """Get a RunningWorkflowStep Record"""
        # Should return:
        # {
        #       "name": "step-1234",
        #       "done": False,
        #       "success": False,
        #       "error_num": 0,
        #       "error_msg": "",
        #       "replica": 0,
        #       "replicas": 0,
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
        # Additionally, if the step has started (an instance has been created)
        # the response will contain a "instance_directory" top-level property
        # that is the directory within the Project that's the step's working directory.
        #
        #       "instance_directory": ".instance-00000000-0000-0000-0000-00000000000a",
        #
        # For steps that are not the first in a workflow the following field
        # can be expected in the response: -
        #
        #       "prior_running_workflow_step": {
        #          "id": "r-workflow-step-00000000-0000-0000-0000-000000000001",
        #       },

    @abstractmethod
    def get_running_workflow_step_by_name(
        self, *, name: str, running_workflow_id: str, replica: int = 0
    ) -> tuple[dict[str, Any], int]:
        """Get a RunningWorkflowStep Record given a step name
        (and its RunningWorkflow ID). For steps that may be replicated
        the replica, a value of 1 or higher, is used to identify the specific replica.
        """
        # Should return:
        # {
        #       "id": "r-workflow-step-00000000-0000-0000-0000-000000000001",
        #       "name": "step-1234",
        #       "done": False,
        #       "success": False,
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
        # Additionally, if the step has started (an instance has been created)
        # the response will contain a "instance_directory" top-level property
        # that is the directory within the Project that's the step's working directory.
        #
        #       "instance_directory": ".instance-00000000-0000-0000-0000-00000000000a",
        #
        # For steps that are not the first in a workflow the following field
        # can be expected in the response: -
        #
        #       "prior_running_workflow_step": {
        #          "id": "r-workflow-step-00000000-0000-0000-0000-000000000001",
        #       },

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

    @abstractmethod
    def get_running_workflow_step_output_values_for_output(
        self, *, running_workflow_step_id: str, output_variable: str
    ) -> tuple[dict[str, Any], int]:
        """Gets the set of outputs generated for the output variable of a given step.
        The step must have stopped for this to return any meaningful value.
        Returned files might also include paths that are relative to the
        Step's instance directory."""
        # Should return a (possibly empty) list of paths and filenames:
        # {
        #   "output": ["dir/file1.sdf", "dir/file2.sdf"]
        # }


class MessageDispatcher(ABC):
    """The class handling the sending of messages (on the Data Manager message bus)."""

    @abstractmethod
    def send(self, message: Message) -> None:
        """Send a message"""
