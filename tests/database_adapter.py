import os
from typing import Any, Dict, List, Optional

import yaml

from workflow.workflow_abc import DatabaseAdapter

# Load the Unit test Job Definitions file now.
_JOB_DEFINITION_FILE: str = os.path.join(
    os.path.dirname(__file__), "job-definitions", "job-definitions.yaml"
)
with open(_JOB_DEFINITION_FILE, "r", encoding="utf8") as jd_file:
    _JOB_DEFINITIONS: Dict[str, Any] = yaml.load(jd_file, Loader=yaml.FullLoader)
assert _JOB_DEFINITIONS

_WORKFLOW_DEFINITION_ID_FORMAT: str = "workflow-00000000-0000-0000-0000-{id:012d}"
_RUNNING_WORKFLOW_ID_FORMAT: str = "r-workflow-00000000-0000-0000-0000-{id:012d}"
_RUNNING_WORKFLOW_STEP_ID_FORMAT: str = (
    "r-workflow-step-00000000-0000-0000-0000-{id:012d}"
)


class UnitTestDatabaseAdapter(DatabaseAdapter):
    """A minimal Database adapter. It serves-up Job Definitions
    from the job-definitions/job-definitions.yaml file and provides basic
    (in-memory) storage for Workflow Definitions and related tables."""

    def __init__(self):
        super().__init__()
        # A map of workflow definitions, keyed by workflow definition ID.
        self._workflow_definitions: Dict[str, Dict[str, Any]] = {}
        self._running_workflow: Dict[str, Dict[str, Any]] = {}
        self._running_workflow_steps: Dict[str, Dict[str, Any]] = {}

    def save_workflow(self, *, workflow_definition: Dict[str, Any]) -> str:
        next_id: int = len(self._workflow_definitions) + 1
        workflow_definition_id: str = _WORKFLOW_DEFINITION_ID_FORMAT.format(id=next_id)
        self._workflow_definitions[workflow_definition_id] = workflow_definition
        return {"id": workflow_definition_id}

    def get_workflow(self, *, workflow_definition_id: str) -> Dict[str, Any]:
        if workflow_definition_id not in self._workflow_definitions:
            return {}
        return {"workflow": self._workflow_definitions[workflow_definition_id]}

    def get_workflow_by_name(self, *, name: str, version: str) -> Dict[str, Any]:
        item = {}
        for wfid, value in self._workflow_definitions.items():
            if value["name"] == name:
                item = {"id": wfid, "workflow": value}
        return item

    def create_running_workflow(self, *, workflow_definition_id: str) -> str:
        next_id: int = len(self._running_workflow) + 1
        running_workflow_id: str = _RUNNING_WORKFLOW_ID_FORMAT.format(id=next_id)
        record = {"done": False, "success": False, "workflow": workflow_definition_id}
        self._running_workflow[running_workflow_id] = record
        return {"id": running_workflow_id}

    def get_running_workflow(self, *, running_workflow_id: str) -> Dict[str, Any]:
        if running_workflow_id not in self._running_workflow:
            return {}
        return {"running-workflow": self._running_workflow[running_workflow_id]}

    def create_running_workflow_step(self, *, running_workflow_id: str) -> str:
        next_id: int = len(self._running_workflow_steps) + 1
        running_workflow_step_id: str = _RUNNING_WORKFLOW_STEP_ID_FORMAT.format(
            id=next_id
        )
        record = {
            "done": False,
            "success": False,
            "running-workflow": running_workflow_id,
        }
        self._running_workflow_steps[running_workflow_step_id] = record
        return {"id": running_workflow_step_id}

    def get_running_workflow_step(
        self, *, running_workflow_step_id: str
    ) -> Dict[str, Any]:
        if running_workflow_step_id not in self._running_workflow_steps:
            return {}
        return {"id": self._running_workflow_steps[running_workflow_step_id]}

    def get_running_workflow_steps(
        self, *, running_workflow_id: str
    ) -> List[Dict[str, Any]]:
        steps = []
        for key, value in self._running_workflow_steps.items():
            if value["running-workflow"] == running_workflow_id:
                item = {"running-workflow-step": value, "id": key}
                steps.append(item)
        return {"count": len(steps), "running-workflow-steps": steps}

    def get_job(
        self, *, collection: str, job: str, version: str
    ) -> Optional[Dict[str, Any]]:
        assert collection == _JOB_DEFINITIONS["collection"]
        assert job in _JOB_DEFINITIONS["jobs"]

        jd = _JOB_DEFINITIONS["jobs"][job]
        response = {"command": jd["command"]}
        if "variables" in jd:
            response["variables"] = jd["variables"]
        return response
