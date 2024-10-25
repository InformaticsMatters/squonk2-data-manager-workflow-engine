import os
from typing import Any, Dict, Optional

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


class UnitTestDatabaseAdapter(DatabaseAdapter):
    """A minimal Database adapter. It serves-up Job Definitions
    from the job-definitions/job-definitions.yaml file and provides basic
    (in-memory) storage for Workflow Definitions and related tables."""

    def __init__(self):
        super().__init__()
        # A map of workflow definitions, keyed by workflow definition ID.
        self._workflow_definitions: Dict[str, Dict[str, Any]] = {}

    def save_workflow(self, *, workflow_definition: Dict[str, Any]) -> str:
        next_id: int = len(self._workflow_definitions) + 1
        workflow_definition_id: str = _WORKFLOW_DEFINITION_ID_FORMAT.format(id=next_id)
        self._workflow_definitions[workflow_definition_id] = workflow_definition
        return workflow_definition_id

    def get_workflow(self, *, workflow_definition_id: str) -> Optional[Dict[str, Any]]:
        return self._workflow_definitions.get(workflow_definition_id, None)

    def get_workflow_by_name(
        self, *, name: str, version: str
    ) -> Optional[Dict[str, Any]]:
        return next(
            (wd for wd in self._workflow_definitions.values() if wd["name"] == name),
            None,
        )

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
