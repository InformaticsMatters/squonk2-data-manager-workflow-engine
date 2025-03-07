"""Workflow abstract base classes.
Interface definitions of class instances that must be made available to the Engine.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from google.protobuf.message import Message


@dataclass
class LaunchResult:
    """Results returned from methods in the InstanceLauncher."""

    error: int
    error_msg: str | None = None
    instance_id: str | None = None
    task_id: str | None = None
    command: str | None = None


class InstanceLauncher(ABC):
    """The class handling the launching of (Job) instances, used by the Engine
    to launch Workflow 'Step' Jobs."""

    @abstractmethod
    def launch(
        self,
        *,
        project_id: str,
        running_workflow_id: str,
        running_workflow_step_id: str,
        step_specification: str,
        variables: dict[str, Any],
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
    def create_workflow(
        self,
        *,
        workflow_definition: dict[str, Any],
    ) -> dict[str, Any]:
        """Create a Workflow, getting an ID in return"""
        # Should return:
        # {
        #    "id": "workflow-00000000-0000-0000-0000-000000000001",
        # }

    @abstractmethod
    def get_workflow(
        self,
        *,
        workflow_id: str,
    ) -> dict[str, Any]:
        """Get a Workflow Record by ID."""
        # If present this should return:
        # {
        #    "workflow": <workflow>,
        # }

    @abstractmethod
    def get_workflow_by_name(
        self,
        *,
        name: str,
        version: str,
    ) -> dict[str, Any]:
        """Get a Workflow Record by name"""
        # If present this should return:
        # {
        #    "id": "workflow-00000000-0000-0000-0000-000000000001",
        #    "workflow": <workflow>,
        # }

    @abstractmethod
    def create_running_workflow(
        self,
        *,
        workflow_id: str,
        project_id: str,
        variables: dict[str, Any],
        user_id: str,
    ) -> dict[str, Any]:
        """Create a RunningWorkflow Record (from a Workflow)"""
        # Should return:
        # {
        #    "id": "r-workflow-00000000-0000-0000-0000-000000000001",
        # }

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
    def get_running_workflow(self, *, running_workflow_id: str) -> dict[str, Any]:
        """Get a RunningWorkflow Record"""
        # Should return:
        # {
        #    "running_workflow": {
        #       "user_id": "alan",
        #       "done": False,
        #       "success": false,
        #       "error": None,
        #       "error_msg": None,
        #       "workflow": {"id": "workflow-000"},
        #       "project_id": "project-000",
        #       "variables": {"x": 1, "y": 2},
        #    }
        # }

    @abstractmethod
    def create_running_workflow_step(
        self,
        *,
        running_workflow_id: str,
        step: str,
    ) -> dict[str, Any]:
        """Create a RunningWorkflowStep Record (from a RunningWorkflow)"""
        # Should return:
        # {
        #    "id": "r-workflow-step-00000000-0000-0000-0000-000000000001",
        # }

    @abstractmethod
    def get_running_workflow_step(
        self, *, running_workflow_step_id: str
    ) -> dict[str, Any]:
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
    def get_running_workflow_steps(
        self, *, running_workflow_id: str
    ) -> list[dict[str, Any]]:
        """Gets all the RunningWorkflowStep Records (for a RunningWorkflow)"""
        # Should return:
        # {
        #    "count": 1,
        #    "running_workflow_steps": [
        #       {
        #           "id": "r-workflow-step-00000000-0000-0000-0000-000000000001",
        #           "running_workflow_step": {
        #               "step:": "step-1234",
        #               "done": False,
        #               "success": false,
        #               "error": None,
        #               "error_msg": None,
        #               "workflow": "workflow-00000000-0000-0000-0000-000000000001",
        #           }
        #       ...
        #    ]
        # }

    @abstractmethod
    def create_instance(self, running_workflow_step_id: str) -> dict[str, Any]:
        """Create an Instance Record (for a RunningWorkflowStep)"""
        # Should return:
        # {
        #    "instance_id": "instance-00000000-0000-0000-0000-000000000001",
        #    "task_id": "task-00000000-0000-0000-0000-000000000001",
        # }

    @abstractmethod
    def get_instance(self, *, instance_id: str) -> dict[str, Any]:
        """Get an Instance Record"""
        # Should return:
        # {
        #    "running_workflow_step": "r-workflow-step-00000000-0000-0000-0000-000000000001",
        #    [...],
        # }

    @abstractmethod
    def create_task(self, instance_id: str) -> dict[str, Any]:
        """Create a Task Record (for an Instance)"""
        # Should return:
        # {
        #    "id": "task-00000000-0000-0000-0000-000000000001",
        # }

    @abstractmethod
    def get_task(self, *, task_id: str) -> dict[str, Any]:
        """Get a Task Record"""
        # Should return:
        # {
        #    "done": True,
        #    "exit_code": 0,
        #    [...],
        # }

    @abstractmethod
    def get_job(
        self,
        *,
        collection: str,
        job: str,
        version: str,
    ) -> dict[str, Any] | None:
        """Get a Job"""


class MessageDispatcher(ABC):
    """The class handling the sending of messages (on the Data Manager message bus)."""

    @abstractmethod
    def send(self, message: Message) -> None:
        """Send a message"""
