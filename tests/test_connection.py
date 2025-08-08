"""Tests for connection module."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from contextlib import contextmanager

from dbxsql.connection import ConnectionManager, ConnectionManagerInterface, AuthenticationManagerProtocol
from dbxsql.settings import DatabricksSettings
from dbxsql.models import ConnectionInfo
from dbxsql.exceptions import ConnectionError


class TestConnectionManager:
    """Test cases for ConnectionManager."""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings fixture."""
        settings = Mock(spec=DatabricksSettings)
        settings.server_hostname = "test.databricks.com"
        settings.http_path = "/sql/1.0/warehouses/test"
        return settings

    @pytest.fixture
    def mock_auth_manager(self):
        """Mock authentication manager fixture."""
        auth_manager = Mock(spec=AuthenticationManagerProtocol)
        auth_manager.get_access_token.return_value = "test_access_token"
        return auth_manager

    @pytest.fixture
    def connection_manager(self, mock_settings, mock_auth_manager):
        """Connection manager fixture."""
        return ConnectionManager(mock_settings, mock_auth_manager)

    def test_connection_manager_implements_interface(self, connection_manager):
        """Test that ConnectionManager implements ConnectionManagerInterface."""
        assert isinstance(connection_manager, ConnectionManagerInterface)

    def test_connection_info_initialization(self, connection_manager, mock_settings):
        """Test connection info is properly initialized."""
        conn_info = connection_manager.connection_info
        assert conn_info.server_hostname == "test.databricks.com"
        assert conn_info.http_path == "/sql/1.0/warehouses/test"
        assert not conn_info.is_connected
        assert conn_info.connection_time is None
        assert conn_info.last_activity is None

    @patch('dbxsql.connection.sql.connect')
    def test_successful_connection(self, mock_sql_connect, connection_manager, mock_auth_manager):
        """Test successful database connection."""
        # Mock databricks sql objects
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_sql_connect.return_value = mock_connection

        result = connection_manager.connect()

        assert result is True
        assert connection_manager.is_connected()

        # Verify auth manager was called
        mock_auth_manager.get_access_token.assert_called_once()

        # Verify sql.connect was called with correct parameters
        mock_sql_connect.assert_called_once_with(
            server_hostname="test.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_access_token"
        )

        # Verify connection info is updated
        conn_info = connection_manager.connection_info
        assert conn_info.is_connected
        assert isinstance(conn_info.connection_time, datetime)
        assert isinstance(conn_info.last_activity, datetime)

    @patch('dbxsql.connection.sql.connect')
    def test_already_connected_scenario(self, mock_sql_connect, connection_manager):
        """Test connecting when already connected."""
        # Set up as already connected
        connection_manager._connection = Mock()
        connection_manager._cursor = Mock()
        connection_manager._connection_info.is_connected = True

        result = connection_manager.connect()

        assert result is True
        # Should not make new connection
        mock_sql_connect.assert_not_called()

    @patch('dbxsql.connection.sql.connect')
    def test_connection_failure(self, mock_sql_connect, connection_manager, mock_auth_manager):
        """Test connection failure."""
        mock_sql_connect.side_effect = Exception("Connection failed")

        with pytest.raises(ConnectionError) as exc_info:
            connection_manager.connect()

        assert "Failed to connect to Databricks" in str(exc_info.value)
        assert "Connection failed" in str(exc_info.value)
        assert not connection_manager.is_connected()

    def test_disconnect(self, connection_manager):
        """Test disconnection."""
        # Set up connected state
        mock_cursor = Mock()
        mock_connection = Mock()
        connection_manager._cursor = mock_cursor
        connection_manager._connection = mock_connection
        connection_manager._connection_info.is_connected = True

        connection_manager.disconnect()

        # Verify cleanup
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        assert not connection_manager.is_connected()
        assert connection_manager._cursor is None
        assert connection_manager._connection is None

    def test_disconnect_with_errors(self, connection_manager):
        """Test disconnection when cursor/connection close raises errors."""
        # Set up connected state with problematic objects
        mock_cursor = Mock()
        mock_cursor.close.side_effect = Exception("Cursor close error")
        mock_connection = Mock()
        mock_connection.close.side_effect = Exception("Connection close error")

        connection_manager._cursor = mock_cursor
        connection_manager._connection = mock_connection
        connection_manager._connection_info.is_connected = True

        # Should not raise exception despite errors
        connection_manager.disconnect()

        assert not connection_manager.is_connected()
        assert connection_manager._cursor is None
        assert connection_manager._connection is None

    def test_is_connected(self, connection_manager):
        """Test connection status checking."""
        # Initially not connected
        assert not connection_manager.is_connected()

        # Set up partial connection (missing cursor)
        connection_manager._connection = Mock()
        assert not connection_manager.is_connected()

        # Set up full connection
        connection_manager._cursor = Mock()
        connection_manager._connection_info.is_connected = True
        assert connection_manager.is_connected()

    @patch('dbxsql.connection.sql.connect')
    def test_ensure_connected_when_not_connected(self, mock_sql_connect, connection_manager):
        """Test ensure_connected when not connected."""
        # Mock successful connection
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_sql_connect.return_value = mock_connection

        connection_manager.ensure_connected()

        assert connection_manager.is_connected()
        mock_sql_connect.assert_called_once()

    def test_ensure_connected_when_already_connected(self, connection_manager):
        """Test ensure_connected when already connected."""
        # Set up as connected
        connection_manager._connection = Mock()
        connection_manager._cursor = Mock()
        connection_manager._connection_info.is_connected = True
        old_activity = connection_manager._connection_info.last_activity

        connection_manager.ensure_connected()

        # Should update activity time
        assert connection_manager._connection_info.last_activity != old_activity

    @patch('dbxsql.connection.sql.connect')
    def test_get_cursor_success(self, mock_sql_connect, connection_manager):
        """Test getting cursor successfully."""
        # Mock successful connection
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_sql_connect.return_value = mock_connection

        cursor = connection_manager.get_cursor()

        assert cursor == mock_cursor
        assert connection_manager.is_connected()

    def test_get_cursor_failure(self, connection_manager):
        """Test getting cursor when connection fails."""
        # Set up as connected but with None cursor (edge case)
        connection_manager._connection = Mock()
        connection_manager._cursor = None
        connection_manager._connection_info.is_connected = True

        with pytest.raises(ConnectionError) as exc_info:
            connection_manager.get_cursor()

        assert "Failed to get database cursor" in str(exc_info.value)

    @patch('dbxsql.connection.sql.connect')
    def test_refresh_connection(self, mock_sql_connect, connection_manager):
        """Test connection refresh."""
        # Set up initial connection
        old_connection = Mock()
        old_cursor = Mock()
        connection_manager._connection = old_connection
        connection_manager._cursor = old_cursor
        connection_manager._connection_info.is_connected = True

        # Mock new connection
        new_connection = Mock()
        new_cursor = Mock()
        new_connection.cursor.return_value = new_cursor
        mock_sql_connect.return_value = new_connection

        connection_manager.refresh_connection()

        # Verify old connection was closed
        old_cursor.close.assert_called_once()
        old_connection.close.assert_called_once()

        # Verify new connection was established
        assert connection_manager._connection == new_connection
        assert connection_manager._cursor == new_cursor