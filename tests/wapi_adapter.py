"""The UnitTest API Adapter.

This 'simulates' the sort of responses you can expect from the DM API/Model.
It stimulates a Database by using dictionaries that are pickled to (and unpickled from)
the file system after acquiring a lock object. Pickling is required to
(because it's a simple built-in mechanism in Python) to persist data
between processes in the multi-processing framework we run in because we ultimately need
to simulate the multi-pod messaging-based framework of the DM.

A separate pickle file is used for each 'simulated' model table and the object
initialiser resets all the pickle files (located in 'tests/pickle-files').

Job definitions are loaded (statically) from the content of the
'tests/job-definitions/job-definitions.yaml' file and yielded through the 'get_job()'
method.
"""

import os
from http import HTTPStatus
from multiprocessing import Lock
from pickle import Pickler, Unpickler
from typing import Any

import yaml

from workflow.workflow_abc import WorkflowAPIAdapter

# Load the Unit test Job Definitions file now.
_JOB_DEFINITION_FILE: str = os.path.join(
    os.path.dirname(__file__), "job-definitions", "job-definitions.yaml"
)
with open(_JOB_DEFINITION_FILE, "r", encoding="utf8") as jd_file:
    _JOB_DEFINITIONS: dict[str, Any] = yaml.load(jd_file, Loader=yaml.FullLoader)
assert _JOB_DEFINITIONS

# Table UUID formats
_INSTANCE_ID_FORMAT: str = "instance-00000000-0000-0000-0000-{id:012d}"
_WORKFLOW_DEFINITION_ID_FORMAT: str = "workflow-00000000-0000-0000-0000-{id:012d}"
_RUNNING_WORKFLOW_ID_FORMAT: str = "r-workflow-00000000-0000-0000-0000-{id:012d}"
_RUNNING_WORKFLOW_STEP_ID_FORMAT: str = (
    "r-workflow-step-00000000-0000-0000-0000-{id:012d}"
)

# Pickle files (for each 'Table')
_PICKLE_DIRECTORY: str = "tests/pickle-files"
_WORKFLOW_PICKLE_FILE: str = f"{_PICKLE_DIRECTORY}/workflow.pickle"
_RUNNING_WORKFLOW_PICKLE_FILE: str = f"{_PICKLE_DIRECTORY}/running-workflow.pickle"
_RUNNING_WORKFLOW_STEP_PICKLE_FILE: str = (
    f"{_PICKLE_DIRECTORY}/running-workflow-step.pickle"
)
_INSTANCE_PICKLE_FILE: str = f"{_PICKLE_DIRECTORY}/instance.pickle"


class UnitTestWorkflowAPIAdapter(WorkflowAPIAdapter):
    """A minimal API adapter. It serves-up Job Definitions
    from the job-definitions/job-definitions.yaml file and provides basic
    storage for Workflow Definitions and related tables.

    Because the adapter is used by the multi-processing test suite, it uses both a lock
    and pickle files to store data, so that data can be shared between processes.
    """

    lock = Lock()

    def __init__(self):
        super().__init__()
        # Safely initialise the pickle files
        UnitTestWorkflowAPIAdapter.lock.acquire()
        if not os.path.exists(_PICKLE_DIRECTORY):
            os.makedirs(_PICKLE_DIRECTORY)
        for file in [
            _WORKFLOW_PICKLE_FILE,
            _RUNNING_WORKFLOW_PICKLE_FILE,
            _RUNNING_WORKFLOW_STEP_PICKLE_FILE,
            _INSTANCE_PICKLE_FILE,
        ]:
            with open(file, "wb") as pickle_file:
                Pickler(pickle_file).dump({})
        UnitTestWorkflowAPIAdapter.lock.release()

    def get_workflow(self, *, workflow_id: str) -> dict[str, Any]:
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_WORKFLOW_PICKLE_FILE, "rb") as pickle_file:
            workflow = Unpickler(pickle_file).load()
        UnitTestWorkflowAPIAdapter.lock.release()

        response = workflow[workflow_id] if workflow_id in workflow else {}
        if response:
            response["id"] = workflow_id
        return response, 0

    def get_running_workflow(
        self, *, running_workflow_id: str
    ) -> tuple[dict[str, Any], int]:
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_PICKLE_FILE, "rb") as pickle_file:
            running_workflow = Unpickler(pickle_file).load()
        UnitTestWorkflowAPIAdapter.lock.release()

        if running_workflow_id not in running_workflow:
            return {}, 0
        response = running_workflow[running_workflow_id]
        response["id"] = running_workflow_id
        return response, 0

    def get_running_steps(
        self, *, running_workflow_id: str
    ) -> tuple[dict[str, Any], int]:
        # Does nothing at the moment - this is used for the STOP logic.
        return {"count": 0, "steps": []}, 0

    def set_running_workflow_done(
        self,
        *,
        running_workflow_id: str,
        success: bool,
        error_num: int | None = None,
        error_msg: str | None = None,
    ) -> None:
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_PICKLE_FILE, "rb") as pickle_file:
            running_workflow = Unpickler(pickle_file).load()

        assert running_workflow_id in running_workflow
        running_workflow[running_workflow_id]["done"] = True
        running_workflow[running_workflow_id]["success"] = success
        running_workflow[running_workflow_id]["error_num"] = error_num
        running_workflow[running_workflow_id]["error_msg"] = error_msg

        with open(_RUNNING_WORKFLOW_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(running_workflow)
        UnitTestWorkflowAPIAdapter.lock.release()

    def create_running_workflow_step(
        self,
        *,
        running_workflow_id: str,
        step: str,
        replica: int = 0,
        prior_running_workflow_step_id: str | None = None,
    ) -> dict[str, Any]:
        if replica:
            assert replica > 0

        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()

        next_id: int = len(running_workflow_step) + 1
        running_workflow_step_id: str = _RUNNING_WORKFLOW_STEP_ID_FORMAT.format(
            id=next_id
        )
        record = {
            "name": step,
            "done": False,
            "success": False,
            "replica": replica,
            "variables": {},
            "running_workflow": {"id": running_workflow_id},
        }
        if prior_running_workflow_step_id:
            record["prior_running_workflow_step"] = {
                "id": prior_running_workflow_step_id
            }
        running_workflow_step[running_workflow_step_id] = record

        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(running_workflow_step)
        UnitTestWorkflowAPIAdapter.lock.release()

        return {"id": running_workflow_step_id}, 0

    def get_running_workflow_step(
        self, *, running_workflow_step_id: str
    ) -> dict[str, Any]:
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()
        UnitTestWorkflowAPIAdapter.lock.release()

        if running_workflow_step_id not in running_workflow_step:
            return {}, 0
        response = running_workflow_step[running_workflow_step_id]
        response["id"] = running_workflow_step_id
        if response["replica"] == 0:
            _ = response.pop("replica")
        return response, 0

    def get_running_workflow_step_by_name(
        self, *, name: str, running_workflow_id: str, replica: int = 0
    ) -> dict[str, Any]:
        if replica:
            assert replica > 0
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()
        UnitTestWorkflowAPIAdapter.lock.release()

        for rwfs_id, record in running_workflow_step.items():
            if record["running_workflow"]["id"] != running_workflow_id:
                continue
            if record["name"] == name and record["replica"] == replica:
                response = record
                response["id"] = rwfs_id
                if record["replica"] == 0:
                    _ = response.pop("replica")
                return response, 0
        return {}, 0

    def set_running_workflow_step_variables(
        self,
        *,
        running_workflow_step_id: str,
        variables: dict[str, Any],
    ) -> None:
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()

        assert running_workflow_step_id in running_workflow_step
        running_workflow_step[running_workflow_step_id]["variables"] = variables

        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(running_workflow_step)
        UnitTestWorkflowAPIAdapter.lock.release()

    def set_running_workflow_step_done(
        self,
        *,
        running_workflow_step_id: str,
        success: bool,
        error_num: int | None = None,
        error_msg: str | None = None,
    ) -> None:
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()

        assert running_workflow_step_id in running_workflow_step
        running_workflow_step[running_workflow_step_id]["done"] = True
        running_workflow_step[running_workflow_step_id]["success"] = success
        running_workflow_step[running_workflow_step_id]["error_num"] = error_num
        running_workflow_step[running_workflow_step_id]["error_msg"] = error_msg

        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(running_workflow_step)
        UnitTestWorkflowAPIAdapter.lock.release()

    def get_workflow_steps_driving_this_step(
        self,
        *,
        running_workflow_step_id: str,
    ) -> tuple[dict[str, Any], int]:
        # To accomplish this we get the running workflow for the step,
        # then the workflow, then the steps from that workflow.
        # We return a dictionary and an HTTP response code.
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()
        UnitTestWorkflowAPIAdapter.lock.release()

        assert running_workflow_step_id in running_workflow_step

        running_workflow_id: str = running_workflow_step[running_workflow_step_id][
            "running_workflow"
        ]["id"]
        rwf_response, _ = self.get_running_workflow(
            running_workflow_id=running_workflow_id
        )
        assert rwf_response
        workflow_id: str = rwf_response["workflow"]["id"]
        wf_response, _ = self.get_workflow(workflow_id=workflow_id)
        assert wf_response
        # Find the caller's python in the step sequence (-1 if not found)
        caller_step_index: int = -1
        index: int = 0
        for step in wf_response["steps"]:
            if step["name"] == running_workflow_step[running_workflow_step_id]["name"]:
                caller_step_index = index
                break
            index += 1
        return {
            "caller_step_index": caller_step_index,
            "steps": wf_response["steps"].copy(),
        }, 0

    def get_instance(self, *, instance_id: str) -> dict[str, Any]:
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_INSTANCE_PICKLE_FILE, "rb") as pickle_file:
            instances = Unpickler(pickle_file).load()
        UnitTestWorkflowAPIAdapter.lock.release()

        response = {} if instance_id not in instances else instances[instance_id]
        return response, 0

    def get_job(self, *, collection: str, job: str, version: str) -> dict[str, Any]:
        assert collection == _JOB_DEFINITIONS["collection"]
        assert job in _JOB_DEFINITIONS["jobs"]
        assert version

        jd = _JOB_DEFINITIONS["jobs"][job]
        response = {"command": jd["command"], "definition": jd}
        if "variables" in jd:
            response["variables"] = jd["variables"]
        return response, 0

    # Methods required for the UnitTestInstanceLauncher and other (internal) logic
    # but not exposed to (or required by) the Workflow Engine...

    def create_workflow(self, *, workflow_definition: dict[str, Any]) -> dict[str, Any]:
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_WORKFLOW_PICKLE_FILE, "rb") as pickle_file:
            workflow = Unpickler(pickle_file).load()

        next_id: int = len(workflow) + 1
        workflow_definition_id: str = _WORKFLOW_DEFINITION_ID_FORMAT.format(id=next_id)
        workflow[workflow_definition_id] = workflow_definition
        workflow["name"] = "test-workflow"

        with open(_WORKFLOW_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(workflow)
        UnitTestWorkflowAPIAdapter.lock.release()

        return {"id": workflow_definition_id}

    def create_running_workflow(
        self,
        *,
        user_id: str,
        workflow_id: str,
        project_id: str,
        variables: dict[str, Any],
    ) -> dict[str, Any]:
        assert user_id
        assert isinstance(variables, dict)

        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_PICKLE_FILE, "rb") as pickle_file:
            running_workflow = Unpickler(pickle_file).load()

        next_id: int = len(running_workflow) + 1
        running_workflow_id: str = _RUNNING_WORKFLOW_ID_FORMAT.format(id=next_id)
        record = {
            "name": "test-running-workflow",
            "running_user": user_id,
            "running_user_api_token": "123456789",
            "done": False,
            "success": False,
            "workflow": {"id": workflow_id},
            "project": {"id": project_id},
            "variables": variables,
        }
        running_workflow[running_workflow_id] = record

        with open(_RUNNING_WORKFLOW_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(running_workflow)
        UnitTestWorkflowAPIAdapter.lock.release()

        return {"id": running_workflow_id}

    def create_instance(self, *, running_workflow_step_id: str) -> dict[str, Any]:
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_INSTANCE_PICKLE_FILE, "rb") as pickle_file:
            instances = Unpickler(pickle_file).load()

        next_id: int = len(instances) + 1
        instance_id: str = _INSTANCE_ID_FORMAT.format(id=next_id)
        record = {
            "running_workflow_step_id": running_workflow_step_id,
        }
        instances[instance_id] = record

        with open(_INSTANCE_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(instances)
        UnitTestWorkflowAPIAdapter.lock.release()

        return {"id": instance_id}

    def get_running_workflow_steps(self, *, running_workflow_id: str) -> dict[str, Any]:
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()
        UnitTestWorkflowAPIAdapter.lock.release()

        steps = []
        for key, value in running_workflow_step.items():
            if value["running_workflow"]["id"] == running_workflow_id:
                item = value
                item["id"] = key
                steps.append(item)
        return {"count": len(steps), "running_workflow_steps": steps}

    def get_running_workflow_step_outputs(
        self, *, running_workflow_step_id: str, output: str
    ) -> tuple[dict[str, Any], int]:
        del running_workflow_step_id
        del output
        return {"outputs": []}, HTTPStatus.OK

    def realise_outputs(
        self, *, running_workflow_step_id: str
    ) -> tuple[dict[str, Any], int]:
        del running_workflow_step_id
        return {}, HTTPStatus.OK
