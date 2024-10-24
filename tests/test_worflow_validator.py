# Tests for the WorkflowValidator

import pytest

pytestmark = pytest.mark.unit

from tests.database_adapter import UnitTestDatabaseAdapter
from tests.message_dispatcher import UnitTestMessageDispatcher
from tests.message_queue import UnitTestMessageQueue
from tests.test_decoder_minimal import _MINIMAL_WORKFLOW
from workflow.worklfow_validator import ValidationLevel, WorkflowValidator


def test_validate_minimal_for_create():
    # Arrange
    msg_queue = UnitTestMessageQueue()
    db_adapter = UnitTestDatabaseAdapter()
    msg_dispatcher = UnitTestMessageDispatcher(msg_queue=msg_queue)
    validator = WorkflowValidator(db_adapter=db_adapter, msg_dispatcher=msg_dispatcher)

    # Act
    error = validator.validate(
        level=ValidationLevel.CREATE,
        workflow_definition=_MINIMAL_WORKFLOW,
    )

    # Assert
    assert error.error == 0
    assert error.error_msg is None
