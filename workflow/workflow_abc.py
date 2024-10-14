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
