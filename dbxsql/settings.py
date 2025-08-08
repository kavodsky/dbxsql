"""Pydantic settings configuration with dotenv support."""

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
import logging
from pathlib import Path


class DatabricksSettings(BaseSettings):
    """Databricks configuration settings using Pydantic Settings with dotenv support."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="DATABRICKS_",
        case_sensitive=False,
        extra="ignore"
    )

    # Databricks connection settings
    client_id: str = Field(..., description="Databricks client ID")
    client_secret: str = Field(..., description="Databricks client secret")
    server_hostname: str = Field(..., description="Databricks server hostname")
    http_path: str = Field(..., description="HTTP path for the warehouse")

    # Optional settings with defaults
    log_level: str = Field(default="INFO", description="Logging level")
    max_retries: int = Field(default=3, description="Maximum retry attempts for queries")
    query_timeout: int = Field(default=300, description="Query timeout in seconds")
    connection_timeout: int = Field(default=30, description="Connection timeout in seconds")

    # OAuth settings
    oauth_scope: str = Field(default="all-apis", description="OAuth scope for authentication")

    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f'Log level must be one of {valid_levels}')
        return v.upper()

    @field_validator('max_retries')
    @classmethod
    def validate_max_retries(cls, v: int) -> int:
        """Validate max retries."""
        if v < 0 or v > 10:
            raise ValueError('max_retries must be between 0 and 10')
        return v

    @field_validator('query_timeout', 'connection_timeout')
    @classmethod
    def validate_timeouts(cls, v: int) -> int:
        """Validate timeout values."""
        if v <= 0:
            raise ValueError('Timeout must be greater than 0')
        return v

    @field_validator('server_hostname')
    @classmethod
    def validate_hostname(cls, v: str) -> str:
        """Basic hostname validation."""
        if not v or '.' not in v:
            raise ValueError('Invalid server hostname')
        return v

    @field_validator('http_path')
    @classmethod
    def validate_http_path(cls, v: str) -> str:
        """Validate HTTP path format."""
        if not v.startswith('/'):
            raise ValueError('HTTP path must start with /')
        return v

    def get_token_url(self) -> str:
        """Get the OAuth token URL."""
        return f"https://{self.server_hostname}/oidc/v1/token"

    def configure_logging(self) -> None:
        """Configure logging based on settings."""
        logging.basicConfig(
            level=getattr(logging, self.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )


# Global settings instance
settings = DatabricksSettings()

# Configure logging when module is imported
settings.configure_logging()