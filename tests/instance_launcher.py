import os
import subprocess
from datetime import datetime, timezone
from subprocess import CompletedProcess
from typing import Any, Dict, List

from informaticsmatters.protobuf.datamanager.pod_message_pb2 import PodMessage

from tests.api_adapter import UnitTestAPIAdapter
from tests.message_dispatcher import UnitTestMessageDispatcher
from workflow.workflow_abc import InstanceLauncher, LaunchResult

_JOB_DIRECTORY: str = os.path.join(os.path.dirname(__file__), "jobs")


class UnitTestInstanceLauncher(InstanceLauncher):
    """A unit test instance launcher, which runs the
    Python module that matches the job name in the provided specification.
    It also uses the UnitTestMessageDispatcher to send the simulated
    'end of instance' PodMessage (to the WorkflowEngine).
    """

    def __init__(
        self, api_adapter: UnitTestAPIAdapter, msg_dispatcher: UnitTestMessageDispatcher
    ):
        super().__init__()

        self._api_adapter = api_adapter
        self._msg_dispatcher = msg_dispatcher

    def launch(
        self,
        *,
        project_id: str,
        workflow_id: str,
        running_workflow_step_id: str,
        workflow_definition: Dict[str, Any],
        step: str,
        step_specification: Dict[str, Any],
    ) -> LaunchResult:
        assert project_id
        assert workflow_id
        assert step_specification

        # We're passed a RunningWorkflowStep ID but a record is expected to have been
        # created bt the caller, we simply create instance records.
        response = self._api_adapter.get_running_workflow_step(
            running_workflow_step_id=running_workflow_step_id
        )
        assert "running_workflow_step" in response
        # Now simulate the creation of an Instance record
        response = self._api_adapter.create_instance(
            running_workflow_step_id=running_workflow_step_id
        )
        instance_id = response["instance_id"]

        # Just run the Python module that matched the 'job' in the step specification.
        # Don't care about 'version' or 'collection'.
        job: str = step_specification["job"]
        job_module = f"{_JOB_DIRECTORY}/{job}.py"
        assert os.path.isfile(job_module)

        job_cmd: List[str] = ["python", job_module]
        print(f"Running job command: {job_cmd}")
        completed_process: CompletedProcess = subprocess.run(job_cmd, check=True)
        assert completed_process.returncode == 0

        # Simulate a PodMessage (that will contain the instance ID),
        # filling-in only the fields that are of use to the Engine.
        pod_message = PodMessage()
        pod_message.timestamp = f"{datetime.now(timezone.utc).isoformat()}Z"
        pod_message.phase = "Completed"
        pod_message.instance = instance_id
        pod_message.has_exit_code = True
        pod_message.exit_code = 0
        self._msg_dispatcher.send(pod_message)

        return LaunchResult(
            error=0,
            error_msg=None,
            instance_id=instance_id,
            task_id="task-00000000-0000-0000-0000-000000000000",
            command=" ".join(job_cmd),
        )
