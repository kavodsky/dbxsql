"""Query execution and result parsing with Pydantic models."""

from databricks import sql
from databricks.sql.client import Cursor
import logging
import time
from typing import Any, Dict, List, Optional, Type, TypeVar, Protocol
from pydantic import BaseModel, ValidationError
from abc import ABC, abstractmethod

from dbxsql.settings import DatabricksSettings
from dbxsql.connection import ConnectionManager, ConnectionManagerInterface
from dbxsql.models import (
    QueryResult, QueryStatus, QueryMetrics, FileInfo, TableInfo,
    GenericRecord, get_model_class
)
from dbxsql.exceptions import (
    QueryExecutionError, SyntaxError, TimeoutError, DataParsingError
)

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=BaseModel)


class ResultParser(ABC):
    """Abstract base class for result parsers."""

    @abstractmethod
    def parse_results(self, raw_data: List[Any], cursor: Cursor) -> List[BaseModel]:
        """Parse raw results into Pydantic models."""
        ...


class PydanticResultParser(ResultParser):
    """Parser for converting raw results to Pydantic models."""

    def __init__(self, model_class: Type[T]):
        self.model_class = model_class

    def parse_results(self, raw_data: List[Any], cursor: Cursor) -> List[T]:
        """Parse raw query results into Pydantic models."""
        if not raw_data:
            return []

        try:
            column_names = self._get_column_names(cursor)
            parsed_results = []
            parsing_errors = []

            for i, row in enumerate(raw_data):
                try:
                    row_dict = self._row_to_dict(row, column_names)
                    parsed_row = self._parse_single_row(row_dict)
                    parsed_results.append(parsed_row)

                except ValidationError as e:
                    error_msg = f"Row {i}: {str(e)}"
                    parsing_errors.append(error_msg)
                    logger.warning(f"Failed to parse row {i}: {error_msg}")

                    # Create GenericRecord as fallback
                    fallback_record = GenericRecord(data=row_dict)
                    parsed_results.append(fallback_record)

                except Exception as e:
                    error_msg = f"Row {i}: Unexpected error - {str(e)}"
                    parsing_errors.append(error_msg)
                    logger.error(error_msg)

            if parsing_errors:
                logger.warning(f"Encountered {len(parsing_errors)} parsing errors out of {len(raw_data)} rows")

            return parsed_results

        except Exception as e:
            error_msg = f"Failed to parse query results: {str(e)}"
            logger.error(error_msg)
            raise DataParsingError(error_msg, raw_data, self.model_class) from e

    def _get_column_names(self, cursor: Cursor) -> List[str]:
        """Extract column names from cursor description."""
        if cursor.description:
            return [desc[0] for desc in cursor.description]
        return []

    def _row_to_dict(self, row: Any, column_names: List[str]) -> Dict[str, Any]:
        """Convert a row to dictionary using column names."""
        if column_names and len(column_names) == len(row):
            return dict(zip(column_names, row))
        else:
            # Fallback: use generic column names
            return {f"column_{j}": value for j, value in enumerate(row)}

    def _parse_single_row(self, row_dict: Dict[str, Any]) -> T:
        """Parse a single row dictionary into the target model."""
        if self.model_class == GenericRecord:
            return GenericRecord(data=row_dict)
        else:
            return self.model_class(**row_dict)


class QueryExecutor:
    """Handles the execution logic for SQL queries."""

    def __init__(self, connection_manager: ConnectionManagerInterface, settings: DatabricksSettings):
        self._connection_manager = connection_manager
        self._settings = settings

    def execute_query(self, query: str, parser: Optional[ResultParser] = None, fetch_all: bool = True) -> QueryResult[T]:
        """Execute a single SQL query."""
        start_time = time.time()
        result = QueryResult[T](status=QueryStatus.FAILED, query=query.strip())

        try:
            with self._connection_manager.get_connection_context() as cursor:
                logger.info(f"Executing query: {query[:100]}...")
                cursor.execute(query)

                if fetch_all:
                    raw_data = cursor.fetchall()
                    result.raw_data = raw_data
                    result.row_count = len(raw_data) if raw_data else 0

                    # Parse data if parser provided
                    if parser and raw_data:
                        result.data = parser.parse_results(raw_data, cursor)

                result.status = QueryStatus.SUCCESS
                result.execution_time_seconds = time.time() - start_time

                logger.info(f"Query executed successfully. Rows: {result.row_count}, "
                          f"Time: {result.execution_time_seconds:.2f}s")

        except sql.exc.ServerOperationError as e:
            result = self._handle_server_error(e, result, query, start_time)
        except sql.exc.Error as e:
            result = self._handle_database_error(e, result, query, start_time)
        except Exception as e:
            result = self._handle_generic_error(e, result, query, start_time)

        return result

    def _handle_server_error(self, error: sql.exc.ServerOperationError, result: QueryResult, query: str, start_time: float) -> QueryResult:
        """Handle server operation errors."""
        result.execution_time_seconds = time.time() - start_time
        error_msg = str(error)

        if "PARSE_SYNTAX_ERROR" in error_msg:
            result.status = QueryStatus.SYNTAX_ERROR
            result.error_message = f"SQL syntax error: {error_msg}"
            logger.error(f"Syntax error in query: {error_msg}")
            raise SyntaxError(result.error_message, query, error)
        else:
            result.error_message = f"Server operation error: {error_msg}"
            logger.error(result.error_message)
            raise QueryExecutionError(result.error_message, query, error)

    def _handle_database_error(self, error: sql.exc.Error, result: QueryResult, query: str, start_time: float) -> QueryResult:
        """Handle database errors."""
        result.execution_time_seconds = time.time() - start_time
        result.error_message = f"Database error: {str(error)}"
        logger.error(result.error_message)
        raise QueryExecutionError(result.error_message, query, error)

    def _handle_generic_error(self, error: Exception, result: QueryResult, query: str, start_time: float) -> QueryResult:
        """Handle generic errors."""
        result.execution_time_seconds = time.time() - start_time

        if "timeout" in str(error).lower():
            result.status = QueryStatus.TIMEOUT
            result.error_message = f"Query timeout: {str(error)}"
            logger.error(result.error_message)
            raise TimeoutError(result.error_message)
        else:
            result.error_message = f"Unexpected error: {str(error)}"
            logger.error(result.error_message)
            raise QueryExecutionError(result.error_message, query, error)


class RetryPolicy:
    """Handles retry logic for query execution."""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay

    def execute_with_retry(self, operation, *args, **kwargs):
        """Execute operation with retry logic."""
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                logger.info(f"Attempt {attempt + 1}/{self.max_retries + 1}")
                return operation(*args, **kwargs)

            except SyntaxError:
                # Don't retry syntax errors
                raise

            except (QueryExecutionError, TimeoutError) as e:
                last_exception = e
                if attempt < self.max_retries:
                    wait_time = self.base_delay * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Operation failed, retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Operation failed after {self.max_retries + 1} attempts")

        # If we get here, all retries failed
        if last_exception:
            raise last_exception
        else:
            raise QueryExecutionError("Operation failed after all retry attempts")


class QueryHandler:
    """Handles query execution and result parsing with improved architecture."""

    def __init__(self, settings: DatabricksSettings, connection_manager: Optional[ConnectionManagerInterface] = None):
        self.settings = settings
        self.connection_manager = connection_manager or ConnectionManager(settings)
        self._executor = QueryExecutor(self.connection_manager, settings)
        self._retry_policy = RetryPolicy(max_retries=settings.max_retries)
        self.metrics = QueryMetrics()

    def connect(self) -> bool:
        """Connect to Databricks."""
        return self.connection_manager.connect()

    def disconnect(self) -> None:
        """Disconnect from Databricks."""
        self.connection_manager.disconnect()

    def execute_query(self, query: str, model_class: Optional[Type[T]] = None, fetch_all: bool = True) -> QueryResult[T]:
        """Execute SQL query and return structured result."""
        parser = PydanticResultParser(model_class) if model_class else None
        result = self._executor.execute_query(query, parser, fetch_all)
        self.metrics.add_query_result(result)
        return result

    def execute_query_with_retry(self, query: str, model_class: Optional[Type[T]] = None, max_retries: Optional[int] = None) -> QueryResult[T]:
        """Execute query with retry logic."""
        retry_policy = RetryPolicy(max_retries or self.settings.max_retries)
        return retry_policy.execute_with_retry(self.execute_query, query, model_class)

    def execute_multiple_queries(self, queries: List[str], model_classes: Optional[List[Type[BaseModel]]] = None) -> Dict[int, QueryResult]:
        """Execute multiple queries."""
        results = {}
        model_classes = model_classes or [None] * len(queries)

        for i, (query, model_class) in enumerate(zip(queries, model_classes)):
            logger.info(f"Executing query {i + 1}/{len(queries)}")

            try:
                result = self.execute_query_with_retry(query, model_class)
                results[i] = result
            except Exception as e:
                logger.error(f"Query {i + 1} failed: {str(e)}")
                result = QueryResult(
                    status=QueryStatus.FAILED,
                    query=query,
                    error_message=str(e)
                )
                results[i] = result

        return results

    # Convenience methods for common operations
    def list_files(self, path: str) -> QueryResult[FileInfo]:
        """List files in the given path and parse as FileInfo models."""
        query = f"LIST '{path}'"
        return self.execute_query(query, FileInfo)

    def show_tables(self, database: str = None) -> QueryResult[TableInfo]:
        """Show tables and parse as TableInfo models."""
        if database:
            query = f"SHOW TABLES IN {database}"
        else:
            query = "SHOW TABLES"
        return self.execute_query(query, TableInfo)

    def describe_table(self, table_name: str, database: str = None) -> QueryResult[GenericRecord]:
        """Describe table structure."""
        if database:
            query = f"DESCRIBE {database}.{table_name}"
        else:
            query = f"DESCRIBE {table_name}"
        return self.execute_query(query, GenericRecord)

    def query_with_model(self, query: str, model_name: str) -> QueryResult:
        """Execute query and parse results using a named model from the registry."""
        model_class = get_model_class(model_name)
        return self.execute_query(query, model_class)

    def get_metrics(self) -> QueryMetrics:
        """Get query execution metrics."""
        return self.metrics.model_copy()

    def reset_metrics(self) -> None:
        """Reset query metrics."""
        self.metrics = QueryMetrics()

    def test_connection(self) -> bool:
        """Test database connection."""
        return self.connection_manager.test_connection()

    def get_connection_info(self):
        """Get connection information."""
        return self.connection_manager.get_connection_info()

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()