"""Database connection management for Databricks."""

from databricks import sql
from databricks.sql.client import Connection, Cursor
import logging
from typing import Optional, Protocol
from contextlib import contextmanager
from abc import ABC, abstractmethod

from dbxsql.settings import DatabricksSettings
from dbxsql.auth import OAuthManager
from dbxsql.models import ConnectionInfo
from dbxsql.exceptions import ConnectionError

logger = logging.getLogger(__name__)


class AuthenticationManagerProtocol(Protocol):
    """Protocol for authentication managers."""

    def get_access_token(self, force_refresh: bool = False) -> str:
        """Get valid access token."""
        ...


class ConnectionManagerInterface(ABC):
    """Abstract interface for connection managers."""

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection."""
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection."""
        ...

    @abstractmethod
    def is_connected(self) -> bool:
        """Check connection status."""
        ...

    @abstractmethod
    def get_connection_context(self):
        """Get connection context manager."""
        ...

    @abstractmethod
    def test_connection(self) -> bool:
        """Test database connection."""
        ...

    @abstractmethod
    def get_connection_info(self):
        """Get connection information."""
        ...


class ConnectionManager(ConnectionManagerInterface):
    """Manages database connections to Databricks."""

    def __init__(self, settings: DatabricksSettings, auth_manager: Optional[AuthenticationManagerProtocol] = None):
        self.settings = settings
        self.auth_manager = auth_manager or OAuthManager(settings)
        self._connection: Optional[Connection] = None
        self._cursor: Optional[Cursor] = None
        self._connection_info = ConnectionInfo(
            server_hostname=settings.server_hostname,
            http_path=settings.http_path
        )

    @property
    def connection_info(self) -> ConnectionInfo:
        """Get connection information."""
        return self._connection_info

    def connect(self) -> bool:
        """
        Establish connection to Databricks.

        Returns:
            True if connection successful

        Raises:
            ConnectionError: If connection fails
        """
        try:
            if self.is_connected():
                logger.info("Already connected to Databricks")
                return True

            # Get access token
            access_token = self.auth_manager.get_access_token()

            logger.info("Establishing connection to Databricks...")
            self._connection = sql.connect(
                server_hostname=self.settings.server_hostname,
                http_path=self.settings.http_path,
                access_token=access_token
            )

            self._cursor = self._connection.cursor()
            self._connection_info.mark_connected()

            logger.info("Successfully connected to Databricks")
            return True

        except Exception as e:
            error_msg = f"Failed to connect to Databricks: {str(e)}"
            logger.error(error_msg)
            self._cleanup_connection()
            raise ConnectionError(error_msg) from e

    def disconnect(self) -> None:
        """Safely close connection and cursor."""
        try:
            self._cleanup_connection()
            logger.info("Disconnected from Databricks")
        except Exception as e:
            logger.error(f"Error during disconnect: {str(e)}")
        finally:
            self._reset_connection_state()

    def _cleanup_connection(self) -> None:
        """Clean up connection resources."""
        if self._cursor:
            try:
                self._cursor.close()
                logger.debug("Cursor closed")
            except Exception as e:
                logger.warning(f"Error closing cursor: {str(e)}")

        if self._connection:
            try:
                self._connection.close()
                logger.debug("Connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {str(e)}")

    def _reset_connection_state(self) -> None:
        """Reset connection state."""
        self._cursor = None
        self._connection = None
        self._connection_info.is_connected = False

    def is_connected(self) -> bool:
        """Check if currently connected to database."""
        return (
                self._connection is not None
                and self._cursor is not None
                and self._connection_info.is_connected
        )

    def ensure_connected(self) -> None:
        """Ensure connection is established, reconnect if necessary."""
        if not self.is_connected():
            logger.info("Connection not available, reconnecting...")
            self.connect()
        else:
            # Update activity timestamp
            self._connection_info.update_activity()

    def get_cursor(self) -> Cursor:
        """
        Get database cursor, ensuring connection is active.

        Returns:
            Active database cursor

        Raises:
            ConnectionError: If connection cannot be established
        """
        self.ensure_connected()

        if not self._cursor:
            raise ConnectionError("Failed to get database cursor")

        return self._cursor

    def refresh_connection(self) -> None:
        """Refresh connection by disconnecting and reconnecting."""
        logger.info("Refreshing database connection...")
        self.disconnect()
        self.connect()

    @contextmanager
    def get_connection_context(self):
        """
        Context manager for database operations.
        Ensures proper connection and cleanup.

        Usage:
            with connection_manager.get_connection_context() as cursor:
                cursor.execute("SELECT * FROM table")
                result = cursor.fetchall()
        """
        cursor = None
        connection_refreshed = False

        try:
            cursor = self.get_cursor()
            yield cursor

        except Exception as e:
            logger.error(f"Error in connection context: {str(e)}")

            # Try to refresh connection on connection-related errors
            if self._is_connection_error(e) and not connection_refreshed:
                logger.info("Attempting to refresh connection due to error")
                try:
                    self.refresh_connection()
                    cursor = self.get_cursor()
                    connection_refreshed = True
                    yield cursor
                except Exception as refresh_error:
                    logger.error(f"Failed to refresh connection: {str(refresh_error)}")
                    raise e from refresh_error
            else:
                raise

        finally:
            # Update activity timestamp if still connected
            if self.is_connected():
                self._connection_info.update_activity()

    def _is_connection_error(self, error: Exception) -> bool:
        """Check if error is connection-related."""
        error_str = str(error).lower()
        connection_keywords = ["connection", "cursor", "closed", "disconnected", "timeout"]
        return any(keyword in error_str for keyword in connection_keywords)

    def test_connection(self) -> bool:
        """
        Test database connection with a simple query.

        Returns:
            True if connection test passes, False otherwise
        """
        try:
            with self.get_connection_context() as cursor:
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                return result is not None and result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False

    def get_connection_info(self) -> ConnectionInfo:
        """Get current connection information."""
        return self._connection_info.model_copy()