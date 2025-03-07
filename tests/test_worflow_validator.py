import pytest

pytestmark = pytest.mark.unit

from tests.api_adapter import UnitTestAPIAdapter
from tests.message_dispatcher import UnitTestMessageDispatcher
from tests.message_queue import UnitTestMessageQueue
from tests.test_decoder_minimal import _MINIMAL_WORKFLOW
from workflow.worklfow_validator import ValidationLevel, WorkflowValidator


@pytest.fixture
def basic_validator():
    # A 'basic' unit-test WorkflowAdapter needs a DB adapter and Message Dispatcher.
    # For testing outside of the DM the Message Dispatcher also needs a Message Queue
    api_adapter = UnitTestAPIAdapter()
    msg_queue = UnitTestMessageQueue()
    msg_dispatcher = UnitTestMessageDispatcher(msg_queue=msg_queue)
    return WorkflowValidator(wapi_adapter=api_adapter, msg_dispatcher=msg_dispatcher)


def test_validate_minimal_for_create(basic_validator):
    # Arrange

    # Act
    error = basic_validator.validate(
        level=ValidationLevel.CREATE,
        workflow_definition=_MINIMAL_WORKFLOW,
    )

    # Assert
    assert error.error == 0
    assert error.error_msg is None
