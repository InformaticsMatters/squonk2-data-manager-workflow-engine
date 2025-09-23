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

import copy
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
_MOCK_STEP_OUTPUT_FILE: str = f"{_PICKLE_DIRECTORY}/mock-output.pickle"


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
            _MOCK_STEP_OUTPUT_FILE,
        ]:
            with open(file, "wb") as pickle_file:
                Pickler(pickle_file).dump({})
        UnitTestWorkflowAPIAdapter.lock.release()

    def get_workflow(self, *, workflow_id: str) -> tuple[dict[str, Any], int]:
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

    def get_status_of_all_step_instances_by_name(
        self, *, running_workflow_id: str, name: str
    ) -> tuple[dict[str, Any], int]:
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()
        UnitTestWorkflowAPIAdapter.lock.release()

        steps: list[dict[str, Any]] = []
        for rwfs_id, record in running_workflow_step.items():
            if record["running_workflow"]["id"] != running_workflow_id:
                continue
            if record["name"] == name:
                response = record
                response["id"] = rwfs_id
                if record["replica"] == 0:
                    _ = response.pop("replica")
                steps.append(response)
        return {"count": len(steps), "status": steps}, 0

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
        instance_id: str,
        replica: int = 0,
        replicas: int = 1,
    ) -> tuple[dict[str, Any], int]:
        assert replica >= 0
        assert replicas > replica

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
            "replicas": replicas,
            "variables": {},
            "running_workflow": {"id": running_workflow_id},
            "instance_id": instance_id,
        }
        running_workflow_step[running_workflow_step_id] = record

        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(running_workflow_step)
        UnitTestWorkflowAPIAdapter.lock.release()

        return {"id": running_workflow_step_id}, 0

    def get_running_workflow_step(
        self, *, running_workflow_step_id: str
    ) -> tuple[dict[str, Any], int]:
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
    ) -> tuple[dict[str, Any], int]:
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

    def get_instance(self, *, instance_id: str) -> tuple[dict[str, Any], int]:
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_INSTANCE_PICKLE_FILE, "rb") as pickle_file:
            instances = Unpickler(pickle_file).load()
        UnitTestWorkflowAPIAdapter.lock.release()

        response = {} if instance_id not in instances else instances[instance_id]
        return response, 0

    def get_job(
        self, *, collection: str, job: str, version: str
    ) -> tuple[dict[str, Any], int]:
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

    def create_instance(self) -> dict[str, Any]:
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_INSTANCE_PICKLE_FILE, "rb") as pickle_file:
            instances = Unpickler(pickle_file).load()

        next_id: int = len(instances) + 1
        instance_id: str = _INSTANCE_ID_FORMAT.format(id=next_id)

        with open(_INSTANCE_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(instances)

        UnitTestWorkflowAPIAdapter.lock.release()

        return {"id": instance_id}

    def set_instance_running_workflow_id(
        self, *, instance_id: str, running_workflow_step_id: str
    ) -> None:
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_INSTANCE_PICKLE_FILE, "rb") as pickle_file:
            instances = Unpickler(pickle_file).load()

        assert instance_id in instances
        instances["running_workflow_step_id"] = running_workflow_step_id

        with open(_INSTANCE_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(instances)

        # Use the instance ID as the step's instance-directory (prefixing with '.')
        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "rb") as pickle_file:
            running_workflow_step = Unpickler(pickle_file).load()
        assert running_workflow_step_id in running_workflow_step
        running_workflow_step[running_workflow_step_id][
            "instance_directory"
        ] = f".{instance_id}"
        with open(_RUNNING_WORKFLOW_STEP_PICKLE_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(running_workflow_step)

        UnitTestWorkflowAPIAdapter.lock.release()

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

    def get_running_workflow_step_output_values_for_output(
        self, *, running_workflow_step_id: str, output_variable: str
    ) -> tuple[dict[str, Any], int]:
        """We use the 'mock' data to return output values, otherwise
        we return an empty list. And we need to get the step in order to get its name.
        """
        # The RunningWorkflowStep must exist...
        step, _ = self.get_running_workflow_step(
            running_workflow_step_id=running_workflow_step_id
        )
        assert step
        step_name: str = step["name"]
        # Now we can inspect the 'mock' data...
        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_MOCK_STEP_OUTPUT_FILE, "rb") as pickle_file:
            mock_output = Unpickler(pickle_file).load()
        UnitTestWorkflowAPIAdapter.lock.release()

        if step_name not in mock_output:
            return {"output": []}, 0
        # The record's output variable must match (there's only one record per step atm)
        assert mock_output[step_name]["output_variable"] == output_variable
        # Now return what was provided to the mock method...
        response = {"output": copy.copy(mock_output[step_name]["output"])}
        return response, 0

    def realise_outputs(
        self, *, running_workflow_step_id: str
    ) -> tuple[dict[str, Any], int]:
        del running_workflow_step_id
        return {}, HTTPStatus.OK

    # Custom (test) methods
    # Methods not declared in the ABC

    def mock_get_running_workflow_step_output_values_for_output(
        self, *, step_name: str, output_variable: str, output: list[str] | str
    ) -> None:
        """Sets the output response for a step.
        Limitation is that there can only be one record for each step name
        so, for now, the output_variable is superfluous and only used
        to check the output variable name matches."""
        assert isinstance(step_name, str)
        assert isinstance(output_variable, str)

        UnitTestWorkflowAPIAdapter.lock.acquire()
        with open(_MOCK_STEP_OUTPUT_FILE, "rb") as pickle_file:
            mock_output = Unpickler(pickle_file).load()

        record = {
            "output_variable": output_variable,
            "output": output,
        }
        mock_output[step_name] = record

        with open(_MOCK_STEP_OUTPUT_FILE, "wb") as pickle_file:
            Pickler(pickle_file).dump(mock_output)
        UnitTestWorkflowAPIAdapter.lock.release()
