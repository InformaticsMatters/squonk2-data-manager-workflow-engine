"""The UnitTest Instance Launcher.

It runs Python module modules defined in the unit-test job-definitions file.

It also uses the UnitTestMessageDispatcher to send a simulated
'end of instance' PodMessage  that are normally sent to the WorkflowEngine's
'handle_message()' method by the underlying queue. The 'exit code' of the module is
passed to the WorkflowEngine through the PodMessage - so if the module fails
(i.e. returns a non-zero exit code) then the WorkflowEngine will see that the PodMessage.
This allows you to write jobs that fail and see how the WorkflowEngine responds.

Instances (jobs) are executed in a simulated project directory - actually
tests/project-root/project-00000000-0000-0000-0000-000000000001. The project directory
is created by the UnitTestInstanceLauncher and is also wiped as the launcher initialises
(so the start of each test begins with an empty project directory).
"""

import os
import shutil
import subprocess
from datetime import datetime, timezone
from subprocess import CompletedProcess
from typing import List

from decoder import decoder as job_decoder
from decoder.decoder import TextEncoding
from informaticsmatters.protobuf.datamanager.pod_message_pb2 import PodMessage

from tests.config import TEST_PROJECT_ID
from tests.message_dispatcher import UnitTestMessageDispatcher
from tests.wapi_adapter import UnitTestWorkflowAPIAdapter
from workflow.workflow_abc import InstanceLauncher, LaunchParameters, LaunchResult

# Relative path to the execution (project) directory
EXECUTION_DIRECTORY: str = os.path.join("tests", "project-root", TEST_PROJECT_ID)

# Full path to the 'jobs' directory
_JOB_PATH: str = os.path.join(os.path.dirname(__file__), "jobs")


def project_file_exists(file_name: str) -> bool:
    """A convenient test function to verify a file exists
    in the execution (project) directory."""
    return os.path.isfile(os.path.join(EXECUTION_DIRECTORY, file_name))


class UnitTestInstanceLauncher(InstanceLauncher):
    """A unit test instance launcher."""

    def __init__(
        self,
        wapi_adapter: UnitTestWorkflowAPIAdapter,
        msg_dispatcher: UnitTestMessageDispatcher,
    ):
        super().__init__()

        self._api_adapter = wapi_adapter
        self._msg_dispatcher = msg_dispatcher

        # Every launcher starts with an empty execution directory...
        print(f"Removing execution directory ({EXECUTION_DIRECTORY})")
        assert EXECUTION_DIRECTORY.startswith("tests/project-root")
        shutil.rmtree(EXECUTION_DIRECTORY, ignore_errors=True)

    def launch(self, launch_parameters: LaunchParameters) -> LaunchResult:
        assert launch_parameters
        assert launch_parameters.project_id == TEST_PROJECT_ID
        assert launch_parameters.specification
        assert isinstance(launch_parameters.specification, dict)

        os.makedirs(EXECUTION_DIRECTORY, exist_ok=True)

        # We're passed a RunningWorkflowStep ID but a record is expected to have been
        # created bt the caller, we simply create instance records.
        response, _ = self._api_adapter.get_running_workflow_step(
            running_workflow_step_id=launch_parameters.running_workflow_step_id
        )
        # Now simulate the creation of a Task and Instance record
        response = self._api_adapter.create_instance(
            running_workflow_step_id=launch_parameters.running_workflow_step_id
        )
        instance_id = response["id"]
        task_id = "task-00000000-0000-0000-0000-000000000001"

        # Apply variables to the step's Job command.
        job, _ = self._api_adapter.get_job(
            collection=launch_parameters.specification["collection"],
            job=launch_parameters.specification["job"],
            version="do-not-care",
        )
        assert job

        # Now apply the variables to the command
        decoded_command, status = job_decoder.decode(
            job["command"],
            launch_parameters.specification_variables,
            launch_parameters.running_workflow_step_id,
            TextEncoding.JINJA2_3_0,
        )
        print(f"Decoded command: {decoded_command}")
        print(f"Status: {status}")
        assert status

        # Now run the decoded command, which will be in the _JOB_DIRECTORY
        command = f"{_JOB_PATH}/{decoded_command}"
        command_list = command.split()
        module = command_list[0]
        print(f"Module: {module}")
        assert os.path.isfile(module)
        subprocess_cmd: List[str] = ["python"] + command_list
        print(f"Subprocess command: {subprocess_cmd}")
        print(f"Execution directory: {EXECUTION_DIRECTORY}")
        completed_process: CompletedProcess = subprocess.run(
            subprocess_cmd, check=False, cwd=EXECUTION_DIRECTORY
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
            instance_id=instance_id,
            task_id=task_id,
            command=" ".join(subprocess_cmd),
        )
