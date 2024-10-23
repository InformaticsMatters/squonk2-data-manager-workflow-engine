# Tests for the decoder package.
import os
from typing import Any, Dict

import pytest
import yaml

pytestmark = pytest.mark.unit

from tests.database_adapter import UnitTestDatabaseAdapter


def test_get_nop_job():
    # Arrange
    utda = UnitTestDatabaseAdapter()

    # Act
    jd = utda.get_job(
        collection="workflow-engine-unit-test-jobs", job="nop", version="1.0.0"
    )

    # Assert
    assert jd["command"] == "python --version"
