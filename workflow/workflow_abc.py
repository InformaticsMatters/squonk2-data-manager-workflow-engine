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

    application_id: str
    project_id: str
    name: str
    launching_user_name: str
    launching_user_api_token: str
    specification: dict[str, Any]
    specification_variables: dict[str, Any] | None = None
    debug: bool | None = None
    callback_url: str | None = None
    callback_token: str | None = None
    callback_context: str | None = None
    generate_callback_token: bool | None = None
    running_workflow_id: str | None = None
    running_workflow_step_id: str | None = None


@dataclass
class LaunchResult:
    """Results returned from methods in the InstanceLauncher.
    Any error returned in this object is a launch error, not a Job error."""

    error_num: int = 0
    error_msg: str | None = None
    instance_id: str | None = None
    task_id: str | None = None
    callback_token: str | None = None
    command: str | None = None


class InstanceLauncher(ABC):
    """The class handling the launching of (Job) instances, used by the Engine
    to launch Workflow 'Step' Jobs."""

    @abstractmethod
    def launch(
        self,
        launch_parameters: LaunchParameters,
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
        #    "workflow": {
        #       "steps": [
        #          {
        #            "name": "step-name"
        #          }
        #       ]
        #    }
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
        #    "running_workflow": {
        #       "user_id": "alan",
        #       "user_api_token": "123456789",
        #       "done": False,
        #       "success": false,
        #       "error": None,
        #       "error_msg": None,
        #       "workflow": {"id": "workflow-000"},
        #       "project_id": "project-000",
        #       "variables": {"x": 1, "y": 2},
        #    }
        # }
        # If not present an empty dictionary should be returned.

    @abstractmethod
    def set_running_workflow_done(
        self,
        *,
        running_workflow_id: str,
        success: bool,
        error: int | None = None,
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
        #    "running_workflow_step": {
        #       "step:": "step-1234",
        #       "done": False,
        #       "success": false,
        #       "error": None,
        #       "error_msg": None,
        #       "running_workflow": "r-workflow-00000000-0000-0000-0000-000000000001",
        #    },
        # }
        # If not present an empty dictionary should be returned.

    @abstractmethod
    def set_running_workflow_step_done(
        self,
        *,
        running_workflow_step_id: str,
        success: bool,
        error: int | None = None,
        error_msg: str | None = None,
    ) -> None:
        """Set the success value for a RunningWorkflowStep Record,
        If not successful an error code and message should be provided."""

    @abstractmethod
    def get_instance(self, *, instance_id: str) -> tuple[dict[str, Any], int]:
        """Get an Instance Record"""
        # Should return:
        # {
        #    "running_workflow_step": "r-workflow-step-00000000-0000-0000-0000-000000000001",
        #    [...],
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
        # If not present an empty dictionary should be returned.


class MessageDispatcher(ABC):
    """The class handling the sending of messages (on the Data Manager message bus)."""

    @abstractmethod
    def send(self, message: Message) -> None:
        """Send a message"""
