import json
import os
import subprocess
from datetime import datetime, timezone
from subprocess import CompletedProcess
from typing import Any, Dict, List

from decoder import decoder as job_decoder
from decoder.decoder import TextEncoding
from informaticsmatters.protobuf.datamanager.pod_message_pb2 import PodMessage

from tests.api_adapter import UnitTestAPIAdapter
from tests.config import TEST_PROJECT_ID
from tests.message_dispatcher import UnitTestMessageDispatcher
from workflow.workflow_abc import InstanceLauncher, LaunchResult

_JOB_DIRECTORY: str = os.path.join(os.path.dirname(__file__), "jobs")


class UnitTestInstanceLauncher(InstanceLauncher):
    """A unit test instance launcher, which runs the
    Python module that matches the job name in the provided specification.

    The Python module used to satisfy the step matches the job name in the
    step specification. If the step_specification's 'job' is 'my_job', then the launcher
    will run the Python module 'my_job.py' in the 'jobs' directory. The
    module is run synchronously - i.e. the launch() method waits for the
    module to complete.

    It then uses the UnitTestMessageDispatcher to send a simulated
    'end of instance' PodMessage  that will be received by the WorkflowEngine's
    'handle_message()' method. The 'exit code' of the module is passed to the
    WorkflowEngine through the PodMessage - so if the module fails (i.e. returns
    a non-zero exit code) then the WorkflowEngine will see that the PodMessage.
    This allows you to write jobs that fail and see how the WorkflowEngine responds.
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
        running_workflow_id: str,
        running_workflow_step_id: str,
        step_specification: str,
        variables: Dict[str, Any],
    ) -> LaunchResult:
        assert project_id
        assert running_workflow_id
        assert step_specification
        assert isinstance(step_specification, str)
        assert isinstance(variables, dict)

        assert project_id == TEST_PROJECT_ID

        # We're passed a RunningWorkflowStep ID but a record is expected to have been
        # created bt the caller, we simply create instance records.
        response = self._api_adapter.get_running_workflow_step(
            running_workflow_step_id=running_workflow_step_id
        )
        assert "running_workflow_step" in response
        # Now simulate the creation of a Task and Instance record
        response = self._api_adapter.create_instance(
            running_workflow_step_id=running_workflow_step_id
        )
        instance_id = response["id"]
        response = self._api_adapter.create_task(instance_id=instance_id)
        task_id = response["id"]

        # Where to run the job (i.e. in the test project directory)
        execution_directory = f"tests/project-root/{project_id}"
        os.makedirs(execution_directory, exist_ok=True)

        # Apply variables to the step's Job command.
        step_specification_map = json.loads(step_specification)
        job = self._api_adapter.get_job(
            collection=step_specification_map["collection"],
            job=step_specification_map["job"],
            version="do-not-care",
        )
        assert job

        # Now apply the variables to the command
        decoded_command, status = job_decoder.decode(
            job["command"],
            variables,
            running_workflow_step_id,
            TextEncoding.JINJA2_3_0,
        )
        print(f"Decoded command: {decoded_command}")
        print(f"Status: {status}")
        assert status

        # Now run the decoded command, which will be in the _JOB_DIRECTORY
        command = f"{_JOB_DIRECTORY}/{decoded_command}"
        command_list = command.split()
        module = command_list[0]
        print(f"Module: {module}")
        assert os.path.isfile(module)
        subprocess_cmd: List[str] = ["python"] + command_list
        print(f"Subprocess command: {subprocess_cmd}")
        completed_process: CompletedProcess = subprocess.run(
            subprocess_cmd, check=False, cwd=execution_directory
        )

        # Simulate a PodMessage (that will contain the instance ID),
        # filling-in only the fields that are of use to the Engine.
        pod_message = PodMessage()
        pod_message.timestamp = f"{datetime.now(timezone.utc).isoformat()}Z"
        pod_message.phase = "Completed"
        pod_message.instance = instance_id
        pod_message.task = task_id
        pod_message.has_exit_code = True
        pod_message.exit_code = completed_process.returncode
        self._msg_dispatcher.send(pod_message)

        return LaunchResult(
            # The errors returned here are the launch errors, not the Job's errors.
            error=0,
            error_msg=None,
            instance_id=instance_id,
            task_id=task_id,
            command=" ".join(subprocess_cmd),
        )
