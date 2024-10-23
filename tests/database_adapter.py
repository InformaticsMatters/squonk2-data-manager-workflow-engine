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


class UnitTestDatabaseAdapter(DatabaseAdapter):
    """A minimal Database adapter simply to serve-up Job Definitions."""

    def get_workflow(self, *, workflow_definition_id: str) -> Optional[Dict[str, Any]]:
        return {}

    def get_workflow_by_name(
        self, *, name: str, version: str
    ) -> Optional[Dict[str, Any]]:
        return {}

    def get_job(
        self, *, collection: str, job: str, version: str
    ) -> Optional[Dict[str, Any]]:
        if collection != _JOB_DEFINITIONS["collection"]:
            return 1
        if job not in _JOB_DEFINITIONS["jobs"]:
            return 2
        if version != _JOB_DEFINITIONS["jobs"][job]["version"]:
            return 3
        jd = _JOB_DEFINITIONS["jobs"][job]
        response = {"command": jd["command"]}
        if "variables" in jd:
            response["variables"] = jd["variables"]
        return response
