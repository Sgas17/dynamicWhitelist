import pytest
from src.whitelist.build_whitelist import Whitelist


@pytest.fixture(scope="session")
def whitelist():
    # Disable NATS for existing tests to avoid connection issues
    return Whitelist(enable_nats=False)