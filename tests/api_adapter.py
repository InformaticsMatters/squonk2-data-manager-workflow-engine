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

_INSTANCE_ID_FORMAT: str = "instance-00000000-0000-0000-0000-{id:012d}"
_TASK_ID_FORMAT: str = "task-00000000-0000-0000-0000-{id:012d}"
_WORKFLOW_DEFINITION_ID_FORMAT: str = "workflow-00000000-0000-0000-0000-{id:012d}"
_RUNNING_WORKFLOW_ID_FORMAT: str = "r-workflow-00000000-0000-0000-0000-{id:012d}"
_RUNNING_WORKFLOW_STEP_ID_FORMAT: str = (
    "r-workflow-step-00000000-0000-0000-0000-{id:012d}"
)

_WORKFLOW_PICKLE_FILE: str = "workflow.pickle"
_RUNNING_WORKFLOW_PICKLE_FILE: str = "running-workflow.pickle"
_RUNNING_WORKFLOW_STEP_PICKLE_FILE: str = "running-workflow-step.pickle"
_INSTANCE_PICKLE_FILE: str = "instance.pickle"
_TASK_PICKLE_FILE: str = "task.pickle"


class UnitTestAPIAdapter(APIAdapter):
    """A minimal API adapter. It serves-up Job Definitions
    from the job-definitions/job-definitions.yaml file and provides basic
    (in-memory) storage for Workflow Definitions and related tables."""

    mp_lock = Lock()

    def __init__(self):
        super().__init__()
        # Safely initialise the pickle files
        UnitTestAPIAdapter.mp_lock.acquire()
        with open(f"tests/{_WORKFLOW_PICKLE_FILE}", "wb") as pickle_file:
            Pickler(pickle_file).dump({})
        with open(f"tests/{_RUNNING_WORKFLOW_PICKLE_FILE}", "wb") as pickle_file:
            Pickler(pickle_file).dump({})
        with open(f"tests/{_RUNNING_WORKFLOW_STEP_PICKLE_FILE}", "wb") as pickle_file:
            Pickler(pickle_file).dump({})
        with open(f"tests/{_INSTANCE_PICKLE_FILE}", "wb") as pickle_file:
            Pickler(pickle_file).dump({})
        with open(f"tests/{_TASK_PICKLE_FILE}", "wb") as pickle_file:
            Pickler(pickle_file).dump({})
        UnitTestAPIAdapter.mp_lock.release()

    def create_workflow(self, *, workflow_definition: Dict[str, Any]) -> str:
        UnitTestAPIAdapter.mp_lock.acquire()
        with open(f"tests/{_WORKFLOW_PICKLE_FILE}", "rb") as pickle_file:
            workflow = Unpickler(pickle_file).load()

        next_id: int = len(workflow) + 1
        workflow_definition_id: str = _WORKFLOW_DEFINITION_ID_FORMAT.format(id=next_id)
        workflow[workflow_definition_id] = workflow_definition

        with open(f"tests/{_WORKFLOW_PICKLE_FILE}", "wb") as pickle_file:
            Pickler(pickle_file).dump(workflow)
        UnitTestAPIAdapter.mp_lock.release()

        return {"id": workflow_definition_id}

    def get_workflow(self, *, workflow_definition_id: str) -> Dict[str, Any]:
        UnitTestAPIAdapter.mp_lock.acquire()
        with open(f"tests/{_WORKFLOW_PICKLE_FILE}", "rb") as pickle_file:
            workflow = Unpickler(pickle_file).load()
        UnitTestAPIAdapter.mp_lock.release()

        if workflow_definition_id not in workflow:
            return {}
        return {"workflow": workflow[workflow_definition_id]}

    def get_workflow_by_name(self, *, name: str, version: str) -> Dict[str, Any]:
        UnitTestAPIAdapter.mp_lock.acquire()
        with open(f"tests/{_WORKFLOW_PICKLE_FILE}", "rb") as pickle_file:
            workflow = Unpickler(pickle_file).load()
        UnitTestAPIAdapter.mp_lock.release()

        item = {}
        for wfid, value in workflow.items():
            if value["name"] == name:
                item = {"id": wfid, "workflow": value}
        return item

    def create_running_workflow(self, *, workflow_definition_id: str) -> str:
        UnitTestAPIAdapter.mp_lock.acquire()
        with open(f"tests/{_RUNNING_WORKFLOW_PICKLE_FILE}", "rb") as pickle_file:
            running_workflow = Unpickler(pickle_file).load()

        next_id: int = len(running_workflow) + 1
        running_workflow_id: str = _RUNNING_WORKFLOW_ID_FORMAT.format(id=next_id)
        record = {"done": False, "success": False, "workflow": workflow_definition_id}
        running_workflow[running_workflow_id] = record

        with open(f"tests/{_RUNNING_WORKFLOW_PICKLE_FILE}", "wb") as pickle_file:
            Pickler(pickle_file).dump(running_workflow)
        UnitTestAPIAdapter.mp_lock.release()

        return {"id": running_workflow_id}

    def get_running_workflow(self, *, running_workflow_id: str) -> Dict[str, Any]:
        UnitTestAPIAdapter.mp_lock.acquire()
        with open(f"tests/{_RUNNING_WORKFLOW_PICKLE_FILE}", "rb") as pickle_file:
            running_workflow = Unpickler(pickle_file).load()
        UnitTestAPIAdapter.mp_lock.release()

        if running_workflow_id not in running_workflow:
            return {}
        return {"running_workflow": running_workflow[running_workflow_id]}

    def create_running_workflow_step(
        self, *, running_workflow_id: str, step: str
    ) -> str:
        UnitTestAPIAdapter.mp_lock.acquire()
        with open(f"tests/{_RUNNING_WORKFLOW_STEP_PICKLE_FILE}", "rb") as pickle_file:
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

        with open(f"tests/{_RUNNING_WORKFLOW_STEP_PICKLE_FILE}", "wb") as pickle_file:
            Pickler(pickle_file).dump(running_workflow_step)
        UnitTestAPIAdapter.mp_lock.release()

        return {"id": running_workflow_step_id}

    def get_running_workflow_step(
        self, *, running_workflow_step_id: str
    ) -> Dict[str, Any]:
        UnitTestAPIAdapter.mp_lock.acquire()
        with open(f"tests/{_RUNNING_WORKFLOW_STEP_PICKLE_FILE}", "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()
        UnitTestAPIAdapter.mp_lock.release()

        if running_workflow_step_id not in running_workflow_step:
            return {}
        return {
            "running_workflow_step": running_workflow_step[running_workflow_step_id]
        }

    def get_running_workflow_steps(
        self, *, running_workflow_id: str
    ) -> List[Dict[str, Any]]:
        UnitTestAPIAdapter.mp_lock.acquire()
        with open(f"tests/{_RUNNING_WORKFLOW_STEP_PICKLE_FILE}", "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()
        UnitTestAPIAdapter.mp_lock.release()

        steps = []
        for key, value in running_workflow_step.items():
            if value["running_workflow"] == running_workflow_id:
                item = {"running_workflow_step": value, "id": key}
                steps.append(item)
        return {"count": len(steps), "running_workflow_steps": steps}

    def create_instance(self, *, running_workflow_step_id: str) -> Dict[str, Any]:
        UnitTestAPIAdapter.mp_lock.acquire()
        with open(f"tests/{_INSTANCE_PICKLE_FILE}", "rb") as pickle_file:
            instances = Unpickler(pickle_file).load()

        next_id: int = len(instances) + 1
        instance_id: str = _INSTANCE_ID_FORMAT.format(id=next_id)
        record = {
            "running_workflow_step": running_workflow_step_id,
        }
        instances[instance_id] = record

        with open(f"tests/{_INSTANCE_PICKLE_FILE}", "wb") as pickle_file:
            Pickler(pickle_file).dump(instances)
        UnitTestAPIAdapter.mp_lock.release()

        return {"id": instance_id}

    def get_instance(self, *, instance_id: str) -> Dict[str, Any]:
        UnitTestAPIAdapter.mp_lock.acquire()
        with open(f"tests/{_INSTANCE_PICKLE_FILE}", "rb") as pickle_file:
            instances = Unpickler(pickle_file).load()
        UnitTestAPIAdapter.mp_lock.release()

        if instance_id not in instances:
            return {}
        return instances[instance_id]

    def create_task(self, *, instance_id: str) -> Dict[str, Any]:
        UnitTestAPIAdapter.mp_lock.acquire()
        with open(f"tests/{_TASK_PICKLE_FILE}", "rb") as pickle_file:
            tasks = Unpickler(pickle_file).load()

        next_id: int = len(tasks) + 1
        task_id: str = _TASK_ID_FORMAT.format(id=next_id)
        record = {
            "done": False,
            "exit_code": 0,
        }
        tasks[task_id] = record

        with open(f"tests/{_TASK_PICKLE_FILE}", "wb") as pickle_file:
            Pickler(pickle_file).dump(tasks)
        UnitTestAPIAdapter.mp_lock.release()

        return {"id": task_id}

    def get_task(self, *, task_id: str) -> Dict[str, Any]:
        UnitTestAPIAdapter.mp_lock.acquire()
        with open(f"tests/{_TASK_PICKLE_FILE}", "rb") as pickle_file:
            tasks = Unpickler(pickle_file).load()
        UnitTestAPIAdapter.mp_lock.release()

        if task_id not in tasks:
            return {}
        return tasks[task_id]

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
