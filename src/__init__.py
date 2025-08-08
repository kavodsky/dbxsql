"""Databricks SQL Handler Package."""

from src.settings import DatabricksSettings, settings
from src.models import (
    QueryResult, QueryStatus, QueryMetrics, ConnectionInfo,
    FileInfo, TableInfo, NexsysRecord, SalesRecord, GenericRecord,
    MODEL_REGISTRY, get_model_class, register_model, list_available_models
)
from src.auth import OAuthManager, TokenProvider
from src.connection import ConnectionManager, ConnectionManagerInterface
from src.query_handler import QueryHandler, ResultParser, PydanticResultParser
from src.exceptions import (
    DatabricksHandlerError, AuthenticationError, ConnectionError,
    QueryExecutionError, SyntaxError, TimeoutError, DataParsingError
)

__version__ = "1.0.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

# Convenience imports for easier usage
__all__ = [
    # Main classes
    "QueryHandler",
    "DatabricksSettings",
    "settings",

    # Models
    "QueryResult",
    "QueryStatus",
    "QueryMetrics",
    "ConnectionInfo",
    "FileInfo",
    "TableInfo",
    "NexsysRecord",
    "SalesRecord",
    "GenericRecord",
    "MODEL_REGISTRY",
    "get_model_class",
    "register_model",
    "list_available_models",

    # Managers and Interfaces
    "OAuthManager",
    "TokenProvider",
    "ConnectionManager",
    "ConnectionManagerInterface",
    "ResultParser",
    "PydanticResultParser",

    # Exceptions
    "DatabricksHandlerError",
    "AuthenticationError",
    "ConnectionError",
    "QueryExecutionError",
    "SyntaxError",
    "TimeoutError",
    "DataParsingError",
]