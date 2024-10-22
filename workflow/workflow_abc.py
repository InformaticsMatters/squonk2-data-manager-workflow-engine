"""Workflow abstract base classes.
Interface definitions of class instances that must be provided to the Engine.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class LaunchResult:
    """Results returned from methods in the InstanceLauncher."""

    error: int
    error_msg: Optional[str]
    instance_id: Optional[str]
    task_id: Optional[str]


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


class DatabaseAdapter(ABC):
    """The DatabaseAdapter will not need to provide a save/commit service.
    Instead it should be assumed that new database records are the
    responsibility of the governing application code. The WE simply has
    to implement engine logic and launch instance. The InstanceLauncher
    is responsible for creating RunningWorkflow and RunningWorkflowStep
    records for example."""

    @abstractmethod
    def get_workflow(
        self,
        *,
        workflow_definition_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get a Workflow by ID"""

    @abstractmethod
    def get_workflow_by_name(
        self,
        *,
        name: str,
        version: str,
    ) -> Optional[Dict[str, Any]]:
        """Get a Workflow by name"""

    @abstractmethod
    def get_job(
        self,
        *,
        collection: str,
        job: str,
        version: str,
    ) -> Optional[Dict[str, Any]]:
        """Get a Job"""
