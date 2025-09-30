import os
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

from tests.test_decoder import _MINIMAL_WORKFLOW
from tests.wapi_adapter import UnitTestWorkflowAPIAdapter
from workflow.workflow_validator import ValidationLevel, WorkflowValidator

_NO_SUCH_JOB_WORKFLOW_FILE: str = os.path.join(
    os.path.dirname(__file__), "workflow-definitions", "no-such-job.yaml"
)
with open(_NO_SUCH_JOB_WORKFLOW_FILE, "r", encoding="utf8") as workflow_file:
    _NO_SUCH_JOB_WORKFLOW: dict[str, Any] = yaml.safe_load(workflow_file)
assert _NO_SUCH_JOB_WORKFLOW


@pytest.fixture
def wapi():
    wapi_adapter = UnitTestWorkflowAPIAdapter()
    yield wapi_adapter


def test_validate_minimal(wapi):
    # Arrange
    wapi_adapter = wapi

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.CREATE,
        workflow_definition=_MINIMAL_WORKFLOW,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_no_such_job(wapi):
    # Arrange
    wapi_adapter = wapi

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.CREATE,
        workflow_definition=_NO_SUCH_JOB_WORKFLOW,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_example_nop_file(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "example-nop-fail.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.CREATE,
        workflow_definition=workflow,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_example_smiles_to_file(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "example-smiles-to-file.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.CREATE,
        workflow_definition=workflow,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_example_two_step_nop(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "example-two-step-nop.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.CREATE,
        workflow_definition=workflow,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_shortcut_example_1(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "shortcut-example-1.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.CREATE,
        workflow_definition=workflow,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_simple_python_molprops(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__), "workflow-definitions", "simple-python-molprops.yaml"
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.CREATE,
        workflow_definition=workflow,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None


def test_validate_simple_python_molprops_with_options(wapi):
    # Arrange
    wapi_adapter = wapi
    workflow_filename: str = os.path.join(
        os.path.dirname(__file__),
        "workflow-definitions",
        "simple-python-molprops-with-options.yaml",
    )
    with open(workflow_filename, "r", encoding="utf8") as workflow_file:
        workflow: dict[str, Any] = yaml.load(workflow_file, Loader=yaml.FullLoader)
    assert workflow

    # Act
    error = WorkflowValidator.validate(
        level=ValidationLevel.CREATE,
        workflow_definition=workflow,
        wapi_adapter=wapi_adapter,
    )

    # Assert
    assert error.error_num == 0
    assert error.error_msg is None
