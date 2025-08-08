"""Integration tests for Databricks SQL Handler."""

import pytest
import os
from unittest.mock import patch, Mock
from datetime import datetime

from src import QueryHandler, settings, GenericRecord, NexsysRecord
from src.models import QueryStatus
from src.exceptions import AuthenticationError, ConnectionError


@pytest.mark.integration
class TestQueryHandlerIntegration:
    """Integration tests for QueryHandler with mocked Databricks connection."""

    @pytest.fixture
    def mock_databricks_environment(self):
        """Mock the entire Databricks environment."""
        with patch('src.connection.sql') as mock_sql, \
                patch('src.auth.requests') as mock_requests:
            # Mock successful authentication
            mock_auth_response = Mock()
            mock_auth_response.status_code = 200
            mock_auth_response.json.return_value = {
                'access_token': 'test_token',
                'expires_in': 3600
            }
            mock_requests.post.return_value = mock_auth_response

            # Mock successful connection