# Tests for the decoder package.

import pytest

pytestmark = pytest.mark.unit

from tests.message_dispatcher import UnitTestMessageDispatcher


def test_get_nop_job():
    # Arrange
    utmd = UnitTestMessageDispatcher()

    # Act
    utmd.send(1)

    # Assert
