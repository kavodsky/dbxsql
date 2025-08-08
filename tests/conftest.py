"""Pytest configuration and shared fixtures."""

import pytest
from unittest.mock import Mock
from datetime import datetime

from dbxsql.settings import DatabricksSettings
from dbxsql.models import ConnectionInfo, QueryResult, QueryStatus
from dbxsql.auth import OAuthManager
from dbxsql.connection import ConnectionManager


@pytest.fixture
def sample_settings():
    """Sample settings fixture for testing."""
    return DatabricksSettings(
        client_id="test_client_id",
        client_secret="test_client_secret",
        server_hostname="test.databricks.com",
        http_path="/sql/1.0/warehouses/test",
        log_level="DEBUG",
        max_retries=3,
        query_timeout=300,
        connection_timeout=30
    )


@pytest.fixture
def mock_oauth_manager():
    """Mock OAuth manager fixture."""
    manager = Mock(spec=OAuthManager)
    manager.get_access_token.return_value = "mock_access_token"
    manager.is_authenticated.return_value = True
    manager.get_token_info.return_value = {
        'has_token': True,
        'is_expired': False,
        'expires_at': datetime.now().isoformat(),
        'expires_in_seconds': 3600
    }
    return manager


@pytest.fixture
def mock_connection_manager():
    """Mock connection manager fixture."""
    manager = Mock(spec=ConnectionManager)
    manager.connect.return_value = True
    manager.is_connected.return_value = True
    manager.test_connection.return_value = True
    manager.get_connection_info.return_value = ConnectionInfo(
        server_hostname="test.databricks.com",
        http_path="/sql/1.0/warehouses/test",
        is_connected=True,
        connection_time=datetime.now(),
        last_activity=datetime.now()
    )
    return manager


@pytest.fixture
def sample_query_result():
    """Sample query result fixture."""
    return QueryResult(
        status=QueryStatus.SUCCESS,
        data=[{"column1": "value1", "column2": "value2"}],
        raw_data=[("value1", "value2")],
        row_count=1,
        execution_time_seconds=0.123,
        query="SELECT column1, column2 FROM test_table"
    )


@pytest.fixture
def sample_failed_query_result():
    """Sample failed query result fixture."""
    return QueryResult(
        status=QueryStatus.FAILED,
        error_message="Table not found",
        query="SELECT * FROM nonexistent_table",
        execution_time_seconds=0.056
    )


@pytest.fixture(autouse=True)
def reset_model_registry():
    """Reset model registry after each test to prevent side effects."""
    from dbxsql.models import MODEL_REGISTRY

    # Store original registry
    original_registry = MODEL_REGISTRY.copy()

    yield

    # Restore original registry
    MODEL_REGISTRY.clear()
    MODEL_REGISTRY.update(original_registry)


@pytest.fixture
def mock_databricks_cursor():
    """Mock databricks cursor fixture with common methods."""
    cursor = Mock()
    cursor.description = [
        ('id', 'int'),
        ('name', 'string'),
        ('value', 'float')
    ]
    cursor.fetchall.return_value = [(1, 'test', 123.45)]
    cursor.fetchone.return_value = (1, 'test', 123.45)
    cursor.execute.return_value = None
    cursor.close.return_value = None
    return cursor


@pytest.fixture
def mock_databricks_connection():
    """Mock databricks connection fixture."""
    connection = Mock()
    cursor = Mock()
    cursor.description = [('test_column', 'string')]
    cursor.fetchall.return_value = [('test_value',)]
    connection.cursor.return_value = cursor
    connection.close.return_value = None
    return connection


# Test utilities
class TestDataBuilder:
    """Builder class for creating test data."""

    @staticmethod
    def create_file_info_data():
        """Create sample file info data."""
        return [
            ('path1', 'file1.txt', 1024, datetime.now(), False),
            ('path2', 'file2.txt', 2048, datetime.now(), False),
            ('path3', 'directory', None, datetime.now(), True)
        ]

    @staticmethod
    def create_nexsys_record_data():
        """Create sample nexsys record data."""
        return [
            (1, 'Record 1', datetime.now(), 'active', 100.50),
            (2, 'Record 2', datetime.now(), 'inactive', 200.75),
            (3, 'Record 3', datetime.now(), 'active', 300.25)
        ]

    @staticmethod
    def create_sales_record_data():
        """Create sample sales record data."""
        return [
            ('TXN001', 'CUST001', 'PROD001', 2, 10.50, 21.00, datetime.now()),
            ('TXN002', 'CUST002', 'PROD002', 1, 15.75, 15.75, datetime.now()),
            ('TXN003', 'CUST001', 'PROD003', 3, 8.25, 24.75, datetime.now())
        ]


@pytest.fixture
def test_data_builder():
    """Test data builder fixture."""
    return TestDataBuilder()


# Markers for test categories
def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "database: mark test as requiring database connection"
    )


# Skip integration tests by default unless explicitly requested
def pytest_collection_modifyitems(config, items):
    """Modify test collection to handle markers."""
    # if config.getoption("--integration"):
    #     # If --integration flag is passed, run all tests
    #     return

    skip_integration = pytest.mark.skip(reason="need --integration option to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)