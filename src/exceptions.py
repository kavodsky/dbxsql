"""Custom exceptions for Databricks SQL handler."""


class DatabricksHandlerError(Exception):
    """Base exception for Databricks handler."""
    pass


class AuthenticationError(DatabricksHandlerError):
    """Raised when authentication fails."""
    pass


class ConnectionError(DatabricksHandlerError):
    """Raised when connection fails."""
    pass


class QueryExecutionError(DatabricksHandlerError):
    """Raised when query execution fails."""
    def __init__(self, message: str, query: str = None, original_error: Exception = None):
        super().__init__(message)
        self.query = query
        self.original_error = original_error


class SyntaxError(QueryExecutionError):
    """Raised when SQL syntax is invalid."""
    pass


class TimeoutError(DatabricksHandlerError):
    """Raised when operation times out."""
    pass


class ConfigurationError(DatabricksHandlerError):
    """Raised when configuration is invalid."""
    pass


class DataParsingError(DatabricksHandlerError):
    """Raised when data cannot be parsed into Pydantic models."""
    def __init__(self, message: str, raw_data: any = None, model_class: type = None):
        super().__init__(message)
        self.raw_data = raw_data
        self.model_class = model_class