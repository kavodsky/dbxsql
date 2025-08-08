"""Tests for settings module."""

import pytest
from unittest.mock import patch, mock_open
from pydantic import ValidationError
import os
import tempfile
from pathlib import Path

from dbxsql.settings import DatabricksSettings


class TestDatabricksSettings:
    """Test cases for DatabricksSettings."""

    def test_valid_settings_creation(self):
        """Test creating settings with valid data."""
        settings = DatabricksSettings(
            client_id="test_client_id",
            client_secret="test_client_secret",
            server_hostname="test.databricks.com",
            http_path="/sql/1.0/warehouses/test"
        )

        assert settings.client_id == "test_client_id"
        assert settings.client_secret == "test_client_secret"
        assert settings.server_hostname == "test.databricks.com"
        assert settings.http_path == "/sql/1.0/warehouses/test"
        assert settings.log_level == "INFO"  # default value
        assert settings.max_retries == 3  # default value

    def test_settings_with_env_variables(self):
        """Test settings loading from environment variables."""
        env_vars = {
            "DATABRICKS_CLIENT_ID": "env_client_id",
            "DATABRICKS_CLIENT_SECRET": "env_client_secret",
            "DATABRICKS_SERVER_HOSTNAME": "env.databricks.com",
            "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/env",
            "DATABRICKS_LOG_LEVEL": "DEBUG",
            "DATABRICKS_MAX_RETRIES": "5"
        }

        with patch.dict(os.environ, env_vars):
            settings = DatabricksSettings()

            assert settings.client_id == "env_client_id"
            assert settings.client_secret == "env_client_secret"
            assert settings.server_hostname == "env.databricks.com"
            assert settings.http_path == "/sql/1.0/warehouses/env"
            assert settings.log_level == "DEBUG"
            assert settings.max_retries == 5

    def test_case_insensitive_env_variables(self):
        """Test that environment variables are case insensitive."""
        env_vars = {
            "databricks_client_id": "case_insensitive_id",
            "DATABRICKS_CLIENT_SECRET": "case_insensitive_secret",
            "Databricks_Server_Hostname": "case.databricks.com",
            "DATABRICKS_http_path": "/sql/1.0/warehouses/case"
        }

        with patch.dict(os.environ, env_vars):
            settings = DatabricksSettings()

            assert settings.client_id == "case_insensitive_id"
            assert settings.client_secret == "case_insensitive_secret"
            assert settings.server_hostname == "case.databricks.com"
            assert settings.http_path == "/sql/1.0/warehouses/case"

    def test_env_file_loading(self):
        """Test loading settings from .env file."""
        env_content = """
DATABRICKS_CLIENT_ID=file_client_id
DATABRICKS_CLIENT_SECRET=file_client_secret
DATABRICKS_SERVER_HOSTNAME=file.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/file
DATABRICKS_LOG_LEVEL=WARNING
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write(env_content)
            env_file_path = f.name

        try:
            # Patch the env_file path in the model config
            with patch.object(DatabricksSettings.model_config, 'env_file', env_file_path):
                settings = DatabricksSettings()

                assert settings.client_id == "file_client_id"
                assert settings.client_secret == "file_client_secret"
                assert settings.server_hostname == "file.databricks.com"
                assert settings.http_path == "/sql/1.0/warehouses/file"
                assert settings.log_level == "WARNING"
        finally:
            os.unlink(env_file_path)

    def test_invalid_log_level_validation(self):
        """Test validation of invalid log level."""
        with pytest.raises(ValidationError) as exc_info:
            DatabricksSettings(
                client_id="test",
                client_secret="test",
                server_hostname="test.com",
                http_path="/test",
                log_level="INVALID"
            )

        assert "Log level must be one of" in str(exc_info.value)

    def test_invalid_max_retries_validation(self):
        """Test validation of invalid max_retries."""
        with pytest.raises(ValidationError) as exc_info:
            DatabricksSettings(
                client_id="test",
                client_secret="test",
                server_hostname="test.com",
                http_path="/test",
                max_retries=-1
            )

        assert "max_retries must be between 0 and 10" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            DatabricksSettings(
                client_id="test",
                client_secret="test",
                server_hostname="test.com",
                http_path="/test",
                max_retries=15
            )

        assert "max_retries must be between 0 and 10" in str(exc_info.value)

    def test_invalid_timeout_validation(self):
        """Test validation of invalid timeout values."""
        with pytest.raises(ValidationError) as exc_info:
            DatabricksSettings(
                client_id="test",
                client_secret="test",
                server_hostname="test.com",
                http_path="/test",
                query_timeout=0
            )

        assert "Timeout must be greater than 0" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            DatabricksSettings(
                client_id="test",
                client_secret="test",
                server_hostname="test.com",
                http_path="/test",
                connection_timeout=-5
            )

        assert "Timeout must be greater than 0" in str(exc_info.value)

    def test_invalid_hostname_validation(self):
        """Test validation of invalid hostname."""
        with pytest.raises(ValidationError) as exc_info:
            DatabricksSettings(
                client_id="test",
                client_secret="test",
                server_hostname="invalid_hostname",  # no dot
                http_path="/test"
            )

        assert "Invalid server hostname" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            DatabricksSettings(
                client_id="test",
                client_secret="test",
                server_hostname="",  # empty
                http_path="/test"
            )

        assert "Invalid server hostname" in str(exc_info.value)

    def test_invalid_http_path_validation(self):
        """Test validation of invalid HTTP path."""
        with pytest.raises(ValidationError) as exc_info:
            DatabricksSettings(
                client_id="test",
                client_secret="test",
                server_hostname="test.com",
                http_path="invalid_path"  # doesn't start with /
            )

        assert "HTTP path must start with /" in str(exc_info.value)

    def test_get_token_url(self):
        """Test token URL generation."""
        settings = DatabricksSettings(
            client_id="test",
            client_secret="test",
            server_hostname="test.databricks.com",
            http_path="/test"
        )

        expected_url = "https://test.databricks.com/oidc/v1/token"
        assert settings.get_token_url() == expected_url

    def test_configure_logging(self):
        """Test logging configuration."""
        settings = DatabricksSettings(
            client_id="test",
            client_secret="test",
            server_hostname="test.com",
            http_path="/test",
            log_level="DEBUG"
        )

        # Test that configure_logging doesn't raise an exception
        settings.configure_logging()

        # Test with different log level
        settings.log_level = "ERROR"
        settings.configure_logging()

    def test_defaults_applied(self):
        """Test that default values are properly applied."""
        settings = DatabricksSettings(
            client_id="test",
            client_secret="test",
            server_hostname="test.com",
            http_path="/test"
        )

        assert settings.log_level == "INFO"
        assert settings.max_retries == 3
        assert settings.query_timeout == 300
        assert settings.connection_timeout == 30
        assert settings.oauth_scope == "all-apis"

    def test_missing_required_fields(self):
        """Test that missing required fields raise validation errors."""
        with pytest.raises(ValidationError) as exc_info:
            DatabricksSettings()

        error_str = str(exc_info.value)
        assert "client_id" in error_str
        assert "client_secret" in error_str
        assert "server_hostname" in error_str
        assert "http_path" in error_str

    def test_extra_fields_ignored(self):
        """Test that extra fields are ignored due to extra='ignore'."""
        # This should not raise an error even with extra fields
        settings = DatabricksSettings(
            client_id="test",
            client_secret="test",
            server_hostname="test.com",
            http_path="/test",
            extra_field="should_be_ignored"
        )

        assert settings.client_id == "test"
        assert not hasattr(settings, 'extra_field')