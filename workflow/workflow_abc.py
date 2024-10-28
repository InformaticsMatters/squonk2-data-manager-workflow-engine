"""Workflow abstract base classes.
Interface definitions of class instances that must be provided to the Engine.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from google.protobuf.message import Message
from informaticsmatters.protobuf.datamanager.pod_message_pb2 import PodMessage


@dataclass
class LaunchResult:
    """Results returned from methods in the InstanceLauncher."""

    error: int
    error_msg: Optional[str]
    instance_id: Optional[str]
    task_id: Optional[str]
    command: Optional[str]


class InstanceLauncher(ABC):
    """The class handling the launching of (Job) instances."""

    @abstractmethod
    def launch(
        self,
        *,
        project_id: str,
        workflow_id: str,
        workflow_definition: Dict[str, Any],
        step: str,
        step_specification: Dict[str, Any],
        completion_callback: Optional[Callable[[PodMessage], None]],
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
        #
        # The completion_callback is only used in local testing and is a function
        # that should be able to process a PodMessage that indicates a workflow Job
        # has completed. When the WorkflowEngine is embedded in the data Manager
        # the Data Manager will not permit the use of this parameter.


class APIAdapter(ABC):
    """The APIAdapter providing read/write access to the Model. It provides
    the ability to create and retrieve Workflow, RunningWorkflow and RunningWorkflowStep
    records returning dictionary (API-like) responses."""

    @abstractmethod
    def save_workflow(
        self,
        *,
        workflow_definition: Dict[str, Any],
    ) -> str:
        """Save a Workflow, getting an ID in return"""
        # Should return:
        # {
        #    "id": "workflow-00000000-0000-0000-0000-000000000001",
        # }

    @abstractmethod
    def get_workflow(
        self,
        *,
        workflow_definition_id: str,
    ) -> Dict[str, Any]:
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
    ) -> Dict[str, Any]:
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
        workflow_definition_id: str,
    ) -> str:
        """Create a RunningWorkflow Record (from a Workflow)"""
        # Should return:
        # {
        #    "id": "r-workflow-00000000-0000-0000-0000-000000000001",
        # }

    @abstractmethod
    def get_running_workflow(self, *, running_workflow_id: str) -> Dict[str, Any]:
        """Get a RunningWorkflow Record"""
        # Should return:
        # {
        #    "running-workflow": {
        #       "done": False,
        #       "success": false,
        #       "workflow": "workflow-000",
        #    }
        # }

    @abstractmethod
    def create_running_workflow_step(
        self,
        *,
        running_workflow_id: str,
    ) -> str:
        """Create a RunningWorkflowStep Record (from a RunningWorkflow)"""
        # Should return:
        # {
        #    "id": "r-workflow-step-00000000-0000-0000-0000-000000000001",
        # }

    @abstractmethod
    def get_running_workflow_step(
        self, *, running_workflow_step_id: str
    ) -> Dict[str, Any]:
        """Get a RunningWorkflowStep Record"""
        # Should return:
        # {
        #    "running-workflow-step": {
        #       "done": False,
        #       "success": false,
        #       "running-workflow": "r-workflow-000",
        #    },
        # }

    @abstractmethod
    def get_running_workflow_steps(
        self, *, running_workflow_id: str
    ) -> List[Dict[str, Any]]:
        """Gets all the RunningWorkflowStep Records (for a RunningWorkflow)"""
        # Should return:
        # {
        #    "count": 1,
        #    "running-workflow-steps": [
        #       {
        #           "id": "r-workflow-step-00000000-0000-0000-0000-000000000001",
        #           "running-workflow-step": {
        #               "done": False,
        #               "success": false,
        #               "workflow": "workflow-000",
        #           }
        #       ...
        #    ]
        # }

    @abstractmethod
    def get_job(
        self,
        *,
        collection: str,
        job: str,
        version: str,
    ) -> Optional[Dict[str, Any]]:
        """Get a Job"""


class MessageDispatcher(ABC):
    """The class handling the sending of messages (on the Data Manager message bus)."""

    @abstractmethod
    def send(self, message: Message) -> None:
        """Send a message"""
