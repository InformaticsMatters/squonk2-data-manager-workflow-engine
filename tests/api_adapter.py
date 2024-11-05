import os
from multiprocessing import Lock
from pickle import Pickler, Unpickler
from typing import Any, Dict, List, Optional

import yaml

from workflow.workflow_abc import APIAdapter

# Load the Unit test Job Definitions file now.
_JOB_DEFINITION_FILE: str = os.path.join(
    os.path.dirname(__file__), "job-definitions", "job-definitions.yaml"
)
with open(_JOB_DEFINITION_FILE, "r", encoding="utf8") as jd_file:
    _JOB_DEFINITIONS: Dict[str, Any] = yaml.load(jd_file, Loader=yaml.FullLoader)
assert _JOB_DEFINITIONS

# Table UUID formats
_INSTANCE_ID_FORMAT: str = "instance-00000000-0000-0000-0000-{id:012d}"
_TASK_ID_FORMAT: str = "task-00000000-0000-0000-0000-{id:012d}"
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
_TASK_PICKLE_FILE: str = f"{_PICKLE_DIRECTORY}/task.pickle"


class UnitTestAPIAdapter(APIAdapter):
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
        UnitTestAPIAdapter.lock.acquire()
        if not os.path.exists(_PICKLE_DIRECTORY):
            os.makedirs(_PICKLE_DIRECTORY)
        for file in [
            _WORKFLOW_PICKLE_FILE,
            _RUNNING_WORKFLOW_PICKLE_FILE,
            _RUNNING_WORKFLOW_STEP_PICKLE_FILE,
            _INSTANCE_PICKLE_FILE,
            _TASK_PICKLE_FILE,
        ]:
            with open(file, "wb") as pickle_file:
                Pickler(pickle_file).dump({})
        UnitTestAPIAdapter.lock.release()

    def create_workflow(self, *, workflow_definition: Dict[str, Any]) -> str:
        UnitTestAPIAdapter.lock.acquire()
        with open(_WORKFLOW_PICKLE_FILE, "rb") as pickle_file:
            workflow = Unpickler(pickle_file).load()

        next_id: int = len(workflow) + 1
        workflow_definition_id: str = _WORKFLOW_DEFINITION_ID_FORMAT.format(id=next_id)
        workflow[workflow_definition_id] = workflow_definition

        with open(_WORKFLOW_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(workflow)
        UnitTestAPIAdapter.lock.release()

        return {"id": workflow_definition_id}

    def get_workflow(self, *, workflow_id: str) -> Dict[str, Any]:
        UnitTestAPIAdapter.lock.acquire()
        with open(_WORKFLOW_PICKLE_FILE, "rb") as pickle_file:
            workflow = Unpickler(pickle_file).load()
        UnitTestAPIAdapter.lock.release()

        return {"workflow": workflow[workflow_id]} if workflow_id in workflow else {}

    def get_workflow_by_name(self, *, name: str, version: str) -> Dict[str, Any]:
        UnitTestAPIAdapter.lock.acquire()
        with open(_WORKFLOW_PICKLE_FILE, "rb") as pickle_file:
            workflow = Unpickler(pickle_file).load()
        UnitTestAPIAdapter.lock.release()

        item = {}
        for wfid, value in workflow.items():
            if value["name"] == name:
                item = {"id": wfid, "workflow": value}
        return item

    def create_running_workflow(
        self, *, workflow_id: str, project_id: str, variables: Dict[str, Any]
    ) -> str:
        assert isinstance(variables, dict)

        UnitTestAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_PICKLE_FILE, "rb") as pickle_file:
            running_workflow = Unpickler(pickle_file).load()

        next_id: int = len(running_workflow) + 1
        running_workflow_id: str = _RUNNING_WORKFLOW_ID_FORMAT.format(id=next_id)
        record = {
            "done": False,
            "success": False,
            "workflow": workflow_id,
            "project_id": project_id,
            "variables": variables,
        }
        running_workflow[running_workflow_id] = record

        with open(_RUNNING_WORKFLOW_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(running_workflow)
        UnitTestAPIAdapter.lock.release()

        return {"id": running_workflow_id}

    def set_running_workflow_done(
        self, *, running_workflow_id: str, success: bool
    ) -> None:
        UnitTestAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_PICKLE_FILE, "rb") as pickle_file:
            running_workflow = Unpickler(pickle_file).load()

        assert running_workflow_id in running_workflow
        running_workflow[running_workflow_id]["done"] = True
        running_workflow[running_workflow_id]["success"] = success

        with open(_RUNNING_WORKFLOW_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(running_workflow)
        UnitTestAPIAdapter.lock.release()

    def get_running_workflow(self, *, running_workflow_id: str) -> Dict[str, Any]:
        UnitTestAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_PICKLE_FILE, "rb") as pickle_file:
            running_workflow = Unpickler(pickle_file).load()
        UnitTestAPIAdapter.lock.release()

        if running_workflow_id not in running_workflow:
            return {}
        return {"running_workflow": running_workflow[running_workflow_id]}

    def create_running_workflow_step(
        self, *, running_workflow_id: str, step: str
    ) -> str:
        UnitTestAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()

        next_id: int = len(running_workflow_step) + 1
        running_workflow_step_id: str = _RUNNING_WORKFLOW_STEP_ID_FORMAT.format(
            id=next_id
        )
        record = {
            "step": step,
            "done": False,
            "success": False,
            "running_workflow": running_workflow_id,
        }
        running_workflow_step[running_workflow_step_id] = record

        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(running_workflow_step)
        UnitTestAPIAdapter.lock.release()

        return {"id": running_workflow_step_id}

    def get_running_workflow_step(
        self, *, running_workflow_step_id: str
    ) -> Dict[str, Any]:
        UnitTestAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()
        UnitTestAPIAdapter.lock.release()

        if running_workflow_step_id not in running_workflow_step:
            return {}
        return {
            "running_workflow_step": running_workflow_step[running_workflow_step_id]
        }

    def set_running_workflow_step_done(
        self, *, running_workflow_step_id: str, success: bool
    ) -> None:
        UnitTestAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()

        assert running_workflow_step_id in running_workflow_step
        running_workflow_step[running_workflow_step_id]["done"] = True
        running_workflow_step[running_workflow_step_id]["success"] = success

        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(running_workflow_step)
        UnitTestAPIAdapter.lock.release()

    def get_running_workflow_steps(
        self, *, running_workflow_id: str
    ) -> List[Dict[str, Any]]:
        UnitTestAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()
        UnitTestAPIAdapter.lock.release()

        steps = []
        for key, value in running_workflow_step.items():
            if value["running_workflow"] == running_workflow_id:
                item = {"running_workflow_step": value, "id": key}
                steps.append(item)
        return {"count": len(steps), "running_workflow_steps": steps}

    def create_instance(self, *, running_workflow_step_id: str) -> Dict[str, Any]:
        UnitTestAPIAdapter.lock.acquire()
        with open(_INSTANCE_PICKLE_FILE, "rb") as pickle_file:
            instances = Unpickler(pickle_file).load()

        next_id: int = len(instances) + 1
        instance_id: str = _INSTANCE_ID_FORMAT.format(id=next_id)
        record = {
            "running_workflow_step": running_workflow_step_id,
        }
        instances[instance_id] = record

        with open(_INSTANCE_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(instances)
        UnitTestAPIAdapter.lock.release()

        return {"id": instance_id}

    def get_instance(self, *, instance_id: str) -> Dict[str, Any]:
        UnitTestAPIAdapter.lock.acquire()
        with open(_INSTANCE_PICKLE_FILE, "rb") as pickle_file:
            instances = Unpickler(pickle_file).load()
        UnitTestAPIAdapter.lock.release()

        return {} if instance_id not in instances else instances[instance_id]

    def create_task(self, *, instance_id: str) -> Dict[str, Any]:
        UnitTestAPIAdapter.lock.acquire()
        with open(_TASK_PICKLE_FILE, "rb") as pickle_file:
            tasks = Unpickler(pickle_file).load()

        next_id: int = len(tasks) + 1
        task_id: str = _TASK_ID_FORMAT.format(id=next_id)
        record = {
            "done": False,
            "exit_code": 0,
        }
        tasks[task_id] = record

        with open(_TASK_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(tasks)
        UnitTestAPIAdapter.lock.release()

        return {"id": task_id}

    def get_task(self, *, task_id: str) -> Dict[str, Any]:
        UnitTestAPIAdapter.lock.acquire()
        with open(_TASK_PICKLE_FILE, "rb") as pickle_file:
            tasks = Unpickler(pickle_file).load()
        UnitTestAPIAdapter.lock.release()

        return {} if task_id not in tasks else tasks[task_id]

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
