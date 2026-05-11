"""Test isolation: use Prefect's official test harness.

The harness provides an in-memory orchestration backend with a temporary SQLite
database, avoiding the subprocess server spawn that pytest's stdout capture
lifecycle races with during teardown.
"""

import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True, scope="session")
def _prefect_test_session():
    with prefect_test_harness():
        yield
