import os
import subprocess
from subprocess import CompletedProcess
from typing import Any, Callable, Dict, List, Optional

from informaticsmatters.protobuf.datamanager.pod_message_pb2 import PodMessage

from workflow.workflow_abc import InstanceLauncher, LaunchResult

_JOB_DIRECTORY: str = os.path.join(os.path.dirname(__file__), "jobs")

_SUCCESS_LAUNCH_RESULT: LaunchResult = LaunchResult(
    error=0,
    error_msg=None,
    instance_id="instance-00000000-0000-0000-0000-000000000000",
    task_id="task-00000000-0000-0000-0000-000000000000",
)


class UnitTestInstanceLauncher(InstanceLauncher):
    """A unit test instance launcher, which runs the
    Python module that matches the job name in the provided specification.
    """

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
        assert project_id
        assert workflow_id
        assert step_specification

        # Just run the Python module that matched the 'job' in the step specification.
        # Don't care about 'version' or 'collection'.
        job: str = step_specification["job"]
        job_module = f"{_JOB_DIRECTORY}/{job}.py"
        assert os.path.isfile(job_module)

        job_cmd: List[str] = ["python", job_module]
        print(f"Running job command: {job_cmd}")
        completed_process: CompletedProcess = subprocess.run(job_cmd, check=True)
        assert completed_process.returncode == 0

        return _SUCCESS_LAUNCH_RESULT
