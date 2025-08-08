"""Databricks SQL Handler Package."""

from dbxsql.settings import DatabricksSettings, settings
from dbxsql.models import (
    QueryResult, QueryStatus, QueryMetrics, ConnectionInfo,
    FileInfo, TableInfo, NexsysRecord, SalesRecord, GenericRecord,
    MODEL_REGISTRY, get_model_class, register_model, list_available_models
)
from dbxsql.auth import OAuthManager, TokenProvider
from dbxsql.connection import ConnectionManager, ConnectionManagerInterface
from dbxsql.query_handler import QueryHandler, ResultParser, PydanticResultParser
from dbxsql.exceptions import (
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