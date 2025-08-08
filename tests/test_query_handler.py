"""Tests for query handler module."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from databricks import sql
import time

from dbxsql.query_handler import (
    QueryHandler, PydanticResultParser, QueryExecutor, RetryPolicy,
    ResultParser
)
from dbxsql.settings import DatabricksSettings
from dbxsql.connection import ConnectionManagerInterface
from dbxsql.models import (
    QueryResult, QueryStatus, QueryMetrics, GenericRecord,
    FileInfo, TableInfo, NexsysRecord
)
from dbxsql.exceptions import (
    QueryExecutionError, SyntaxError, TimeoutError, DataParsingError
)


class TestPydanticResultParser:
    """Test cases for PydanticResultParser."""

    @pytest.fixture
    def mock_cursor(self):
        """Mock cursor fixture."""
        cursor = Mock()
        cursor.description = [
            ('column1', 'string'),
            ('column2', 'int'),
            ('column3', 'float')
        ]
        return cursor

    def test_parser_initialization(self):
        """Test parser initialization."""
        parser = PydanticResultParser(GenericRecord)
        assert parser.model_class == GenericRecord

    def test_parse_empty_results(self, mock_cursor):
        """Test parsing empty results."""
        parser = PydanticResultParser(GenericRecord)
        result = parser.parse_results([], mock_cursor)

        assert result == []

    def test_parse_results_with_generic_record(self, mock_cursor):
        """Test parsing results with GenericRecord."""
        parser = PydanticResultParser(GenericRecord)
        raw_data = [
            ('value1', 123, 45.6),
            ('value2', 456, 78.9)
        ]

        result = parser.parse_results(raw_data, mock_cursor)

        assert len(result) == 1
        assert isinstance(result[0], NexsysRecord)
        assert result[0].id == 1
        assert result[0].name == 'Test Record'
        assert result[0].status == 'active'
        assert result[0].amount == 100.50

    def test_parse_results_validation_error_fallback(self, mock_cursor):
        """Test parsing with validation error falls back to GenericRecord."""
        parser = PydanticResultParser(NexsysRecord)
        # Data that doesn't match NexsysRecord structure
        raw_data = [
            ('invalid_field', 'invalid_value')
        ]

        result = parser.parse_results(raw_data, mock_cursor)

        assert len(result) == 1
        assert isinstance(result[0], GenericRecord)

    def test_parse_results_no_cursor_description(self):
        """Test parsing when cursor has no description."""
        mock_cursor = Mock()
        mock_cursor.description = None

        parser = PydanticResultParser(GenericRecord)
        raw_data = [('value1', 'value2')]

        result = parser.parse_results(raw_data, mock_cursor)

        assert len(result) == 1
        assert result[0].data['column_0'] == 'value1'
        assert result[0].data['column_1'] == 'value2'

    def test_parse_results_column_count_mismatch(self, mock_cursor):
        """Test parsing with column count mismatch."""
        parser = PydanticResultParser(GenericRecord)
        # More columns in data than in description
        raw_data = [('val1', 'val2', 'val3', 'val4')]

        result = parser.parse_results(raw_data, mock_cursor)

        assert len(result) == 1
        assert result[0].data['column_0'] == 'val1'
        assert result[0].data['column_3'] == 'val4'

    def test_parse_results_exception_handling(self, mock_cursor):
        """Test exception handling during parsing."""
        parser = PydanticResultParser(GenericRecord)

        # Mock data that causes an exception during parsing
        with patch.object(parser, '_parse_single_row', side_effect=Exception("Parse error")):
            raw_data = [('value1', 'value2')]

            with pytest.raises(DataParsingError) as exc_info:
                parser.parse_results(raw_data, mock_cursor)

            assert "Failed to parse query results" in str(exc_info.value)


class TestQueryExecutor:
    """Test cases for QueryExecutor."""

    @pytest.fixture
    def mock_connection_manager(self):
        """Mock connection manager fixture."""
        manager = Mock(spec=ConnectionManagerInterface)
        return manager

    @pytest.fixture
    def mock_settings(self):
        """Mock settings fixture."""
        settings = Mock(spec=DatabricksSettings)
        return settings

    @pytest.fixture
    def query_executor(self, mock_connection_manager, mock_settings):
        """Query executor fixture."""
        return QueryExecutor(mock_connection_manager, mock_settings)

    @pytest.fixture
    def mock_cursor(self):
        """Mock cursor fixture."""
        cursor = Mock()
        cursor.description = [('column1', 'string')]
        cursor.fetchall.return_value = [('test_value',)]
        return cursor

    def test_execute_query_success(self, query_executor, mock_connection_manager, mock_cursor):
        """Test successful query execution."""
        mock_connection_manager.get_connection_context.return_value.__enter__.return_value = mock_cursor
        mock_connection_manager.get_connection_context.return_value.__exit__.return_value = None

        result = query_executor.execute_query("SELECT 1")

        assert result.status == QueryStatus.SUCCESS
        assert result.row_count == 1
        assert result.raw_data == [('test_value',)]
        assert result.execution_time_seconds is not None

    def test_execute_query_with_parser(self, query_executor, mock_connection_manager, mock_cursor):
        """Test query execution with result parser."""
        mock_connection_manager.get_connection_context.return_value.__enter__.return_value = mock_cursor
        mock_connection_manager.get_connection_context.return_value.__exit__.return_value = None

        mock_parser = Mock(spec=ResultParser)
        mock_parser.parse_results.return_value = [GenericRecord(data={'column1': 'test_value'})]

        result = query_executor.execute_query("SELECT 1", mock_parser)

        assert result.status == QueryStatus.SUCCESS
        assert len(result.data) == 1
        assert isinstance(result.data[0], GenericRecord)
        mock_parser.parse_results.assert_called_once()

    def test_execute_query_syntax_error(self, query_executor, mock_connection_manager, mock_cursor):
        """Test query execution with syntax error."""
        mock_cursor.execute.side_effect = sql.exc.ServerOperationError("PARSE_SYNTAX_ERROR: Invalid syntax")
        mock_connection_manager.get_connection_context.return_value.__enter__.return_value = mock_cursor
        mock_connection_manager.get_connection_context.return_value.__exit__.return_value = None

        with pytest.raises(SyntaxError) as exc_info:
            query_executor.execute_query("INVALID SQL")

        assert "SQL syntax error" in str(exc_info.value)

    def test_execute_query_server_error(self, query_executor, mock_connection_manager, mock_cursor):
        """Test query execution with server error."""
        mock_cursor.execute.side_effect = sql.exc.ServerOperationError("Server error")
        mock_connection_manager.get_connection_context.return_value.__enter__.return_value = mock_cursor
        mock_connection_manager.get_connection_context.return_value.__exit__.return_value = None

        with pytest.raises(QueryExecutionError) as exc_info:
            query_executor.execute_query("SELECT 1")

        assert "Server operation error" in str(exc_info.value)

    def test_execute_query_database_error(self, query_executor, mock_connection_manager, mock_cursor):
        """Test query execution with database error."""
        mock_cursor.execute.side_effect = sql.exc.Error("Database error")
        mock_connection_manager.get_connection_context.return_value.__enter__.return_value = mock_cursor
        mock_connection_manager.get_connection_context.return_value.__exit__.return_value = None

        with pytest.raises(QueryExecutionError) as exc_info:
            query_executor.execute_query("SELECT 1")

        assert "Database error" in str(exc_info.value)

    def test_execute_query_timeout_error(self, query_executor, mock_connection_manager, mock_cursor):
        """Test query execution with timeout error."""
        mock_cursor.execute.side_effect = Exception("Query timeout exceeded")
        mock_connection_manager.get_connection_context.return_value.__enter__.return_value = mock_cursor
        mock_connection_manager.get_connection_context.return_value.__exit__.return_value = None

        with pytest.raises(TimeoutError) as exc_info:
            query_executor.execute_query("SELECT 1")

        assert "Query timeout" in str(exc_info.value)

    def test_execute_query_generic_error(self, query_executor, mock_connection_manager, mock_cursor):
        """Test query execution with generic error."""
        mock_cursor.execute.side_effect = ValueError("Generic error")
        mock_connection_manager.get_connection_context.return_value.__enter__.return_value = mock_cursor
        mock_connection_manager.get_connection_context.return_value.__exit__.return_value = None

        with pytest.raises(QueryExecutionError) as exc_info:
            query_executor.execute_query("SELECT 1")

        assert "Unexpected error" in str(exc_info.value)

    def test_execute_query_no_fetch(self, query_executor, mock_connection_manager, mock_cursor):
        """Test query execution without fetching results."""
        mock_connection_manager.get_connection_context.return_value.__enter__.return_value = mock_cursor
        mock_connection_manager.get_connection_context.return_value.__exit__.return_value = None

        result = query_executor.execute_query("CREATE TABLE test", fetch_all=False)

        assert result.status == QueryStatus.SUCCESS
        assert result.raw_data is None
        assert result.row_count == 0
        mock_cursor.fetchall.assert_not_called()


class TestRetryPolicy:
    """Test cases for RetryPolicy."""

    def test_retry_policy_initialization(self):
        """Test retry policy initialization."""
        policy = RetryPolicy(max_retries=5, base_delay=2.0)
        assert policy.max_retries == 5
        assert policy.base_delay == 2.0

    def test_successful_operation_no_retry(self):
        """Test successful operation without retry."""
        policy = RetryPolicy(max_retries=3)
        mock_operation = Mock(return_value="success")

        result = policy.execute_with_retry(mock_operation, "arg1", kwarg1="value1")

        assert result == "success"
        mock_operation.assert_called_once_with("arg1", kwarg1="value1")

    @patch('time.sleep')
    def test_operation_with_retry_success(self, mock_sleep):
        """Test operation that succeeds after retry."""
        policy = RetryPolicy(max_retries=2)
        mock_operation = Mock(side_effect=[QueryExecutionError("error"), "success"])

        result = policy.execute_with_retry(mock_operation)

        assert result == "success"
        assert mock_operation.call_count == 2
        mock_sleep.assert_called_once_with(1.0)  # base_delay * 2^0

    @patch('time.sleep')
    def test_operation_with_exponential_backoff(self, mock_sleep):
        """Test exponential backoff in retry."""
        policy = RetryPolicy(max_retries=3, base_delay=1.0)
        mock_operation = Mock(side_effect=[
            QueryExecutionError("error1"),
            TimeoutError("error2"),
            QueryExecutionError("error3"),
            "success"
        ])

        result = policy.execute_with_retry(mock_operation)

        assert result == "success"
        assert mock_operation.call_count == 4
        # Check exponential backoff: 1.0, 2.0, 4.0
        expected_calls = [
            pytest.approx(1.0),
            pytest.approx(2.0),
            pytest.approx(4.0)
        ]
        actual_calls = [call[0][0] for call in mock_sleep.call_args_list]
        assert actual_calls == expected_calls

    def test_syntax_error_no_retry(self):
        """Test that syntax errors are not retried."""
        policy = RetryPolicy(max_retries=3)
        mock_operation = Mock(side_effect=SyntaxError("syntax error"))

        with pytest.raises(SyntaxError):
            policy.execute_with_retry(mock_operation)

        mock_operation.assert_called_once()

    @patch('time.sleep')
    def test_max_retries_exceeded(self, mock_sleep):
        """Test behavior when max retries is exceeded."""
        policy = RetryPolicy(max_retries=2)
        mock_operation = Mock(side_effect=QueryExecutionError("persistent error"))

        with pytest.raises(QueryExecutionError) as exc_info:
            policy.execute_with_retry(mock_operation)

        assert "persistent error" in str(exc_info.value)
        assert mock_operation.call_count == 3  # initial + 2 retries
        assert mock_sleep.call_count == 2

    @patch('time.sleep')
    def test_no_last_exception_fallback(self, mock_sleep):
        """Test fallback when no last exception is captured."""
        policy = RetryPolicy(max_retries=1)

        # Create a mock that doesn't raise an exception but returns None
        def mock_operation():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise QueryExecutionError("error")
            return None

        call_count = 0

        # This is an edge case that shouldn't normally happen
        with patch.object(policy, 'max_retries', 1):
            with pytest.raises(QueryExecutionError) as exc_info:
                policy.execute_with_retry(mock_operation)

            assert "Operation failed after all retry attempts" in str(exc_info.value) or "error" in str(exc_info.value)


class TestQueryHandler:
    """Test cases for QueryHandler."""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings fixture."""
        settings = Mock(spec=DatabricksSettings)
        settings.max_retries = 3
        return settings

    @pytest.fixture
    def mock_connection_manager(self):
        """Mock connection manager fixture."""
        manager = Mock(spec=ConnectionManagerInterface)
        return manager

    @pytest.fixture
    def query_handler(self, mock_settings, mock_connection_manager):
        """Query handler fixture."""
        return QueryHandler(mock_settings, mock_connection_manager)

    def test_query_handler_initialization(self, query_handler, mock_settings, mock_connection_manager):
        """Test QueryHandler initialization."""
        assert query_handler.settings == mock_settings
        assert query_handler.connection_manager == mock_connection_manager
        assert isinstance(query_handler.metrics, QueryMetrics)

    def test_query_handler_initialization_default_connection_manager(self, mock_settings):
        """Test QueryHandler initialization with default connection manager."""
        with patch('dbxsql.query_handler.ConnectionManager') as mock_conn_class:
            mock_conn_instance = Mock()
            mock_conn_class.return_value = mock_conn_instance

            handler = QueryHandler(mock_settings)

            mock_conn_class.assert_called_once_with(mock_settings)
            assert handler.connection_manager == mock_conn_instance

    def test_connect_disconnect(self, query_handler, mock_connection_manager):
        """Test connect and disconnect methods."""
        mock_connection_manager.connect.return_value = True

        result = query_handler.connect()
        assert result is True
        mock_connection_manager.connect.assert_called_once()

        query_handler.disconnect()
        mock_connection_manager.disconnect.assert_called_once()

    @patch('dbxsql.query_handler.QueryExecutor')
    def test_execute_query(self, mock_executor_class, query_handler):
        """Test execute_query method."""
        mock_executor = Mock()
        mock_result = QueryResult(status=QueryStatus.SUCCESS)
        mock_executor.execute_query.return_value = mock_result
        mock_executor_class.return_value = mock_executor

        # Re-initialize to get the mocked executor
        query_handler._executor = mock_executor

        result = query_handler.execute_query("SELECT 1", GenericRecord)

        assert result == mock_result
        # Metrics should be updated
        assert query_handler.metrics.total_queries == 1

    @patch('dbxsql.query_handler.RetryPolicy')
    def test_execute_query_with_retry(self, mock_retry_class, query_handler):
        """Test execute_query_with_retry method."""
        mock_retry_policy = Mock()
        mock_result = QueryResult(status=QueryStatus.SUCCESS)
        mock_retry_policy.execute_with_retry.return_value = mock_result
        mock_retry_class.return_value = mock_retry_policy

        result = query_handler.execute_query_with_retry("SELECT 1", GenericRecord, max_retries=5)

        mock_retry_class.assert_called_once_with(5)
        mock_retry_policy.execute_with_retry.assert_called_once()
        assert result == mock_result

    def test_execute_query_with_retry_default_max_retries(self, query_handler, mock_settings):
        """Test execute_query_with_retry with default max_retries."""
        mock_settings.max_retries = 3

        with patch('dbxsql.query_handler.RetryPolicy') as mock_retry_class:
            mock_retry_policy = Mock()
            mock_retry_class.return_value = mock_retry_policy

            query_handler.execute_query_with_retry("SELECT 1")

            mock_retry_class.assert_called_once_with(3)

    def test_execute_multiple_queries_success(self, query_handler):
        """Test execute_multiple_queries with all successful queries."""
        with patch.object(query_handler, 'execute_query_with_retry') as mock_execute:
            mock_results = [
                QueryResult(status=QueryStatus.SUCCESS),
                QueryResult(status=QueryStatus.SUCCESS)
            ]
            mock_execute.side_effect = mock_results

            queries = ["SELECT 1", "SELECT 2"]
            model_classes = [GenericRecord, GenericRecord]

            results = query_handler.execute_multiple_queries(queries, model_classes)

            assert len(results) == 2
            assert results[0].status == QueryStatus.SUCCESS
            assert results[1].status == QueryStatus.SUCCESS

    def test_execute_multiple_queries_with_failure(self, query_handler):
        """Test execute_multiple_queries with some failures."""
        with patch.object(query_handler, 'execute_query_with_retry') as mock_execute:
            mock_execute.side_effect = [
                QueryResult(status=QueryStatus.SUCCESS),
                QueryExecutionError("Query failed")
            ]

            queries = ["SELECT 1", "INVALID SQL"]

            results = query_handler.execute_multiple_queries(queries)

            assert len(results) == 2
            assert results[0].status == QueryStatus.SUCCESS
            assert results[1].status == QueryStatus.FAILED
            assert "Query failed" in results[1].error_message

    def test_list_files(self, query_handler):
        """Test list_files convenience method."""
        with patch.object(query_handler, 'execute_query') as mock_execute:
            mock_result = QueryResult(status=QueryStatus.SUCCESS)
            mock_execute.return_value = mock_result

            result = query_handler.list_files("/test/path")

            mock_execute.assert_called_once_with("LIST '/test/path'", FileInfo)
            assert result == mock_result

    def test_show_tables(self, query_handler):
        """Test show_tables convenience method."""
        with patch.object(query_handler, 'execute_query') as mock_execute:
            mock_result = QueryResult(status=QueryStatus.SUCCESS)
            mock_execute.return_value = mock_result

            # Test without database
            result = query_handler.show_tables()
            mock_execute.assert_called_with("SHOW TABLES", TableInfo)

            # Test with database
            result = query_handler.show_tables("test_db")
            mock_execute.assert_called_with("SHOW TABLES IN test_db", TableInfo)

    def test_describe_table(self, query_handler):
        """Test describe_table convenience method."""
        with patch.object(query_handler, 'execute_query') as mock_execute:
            mock_result = QueryResult(status=QueryStatus.SUCCESS)
            mock_execute.return_value = mock_result

            # Test without database
            result = query_handler.describe_table("test_table")
            mock_execute.assert_called_with("DESCRIBE test_table", GenericRecord)

            # Test with database
            result = query_handler.describe_table("test_table", "test_db")
            mock_execute.assert_called_with("DESCRIBE test_db.test_table", GenericRecord)

    def test_query_with_model(self, query_handler):
        """Test query_with_model convenience method."""
        with patch.object(query_handler, 'execute_query') as mock_execute:
            with patch('dbxsql.query_handler.get_model_class') as mock_get_model:
                mock_get_model.return_value = GenericRecord
                mock_result = QueryResult(status=QueryStatus.SUCCESS)
                mock_execute.return_value = mock_result

                result = query_handler.query_with_model("SELECT 1", "generic")

                mock_get_model.assert_called_once_with("generic")
                mock_execute.assert_called_once_with("SELECT 1", GenericRecord)

    def test_get_metrics(self, query_handler):
        """Test get_metrics method."""
        # Add some metrics
        query_handler.metrics.total_queries = 5
        query_handler.metrics.successful_queries = 3

        metrics = query_handler.get_metrics()

        assert metrics.total_queries == 5
        assert metrics.successful_queries == 3
        # Should be a copy
        assert metrics is not query_handler.metrics

    def test_reset_metrics(self, query_handler):
        """Test reset_metrics method."""
        # Add some metrics
        query_handler.metrics.total_queries = 5
        query_handler.metrics.successful_queries = 3

        query_handler.reset_metrics()

        assert query_handler.metrics.total_queries == 0
        assert query_handler.metrics.successful_queries == 0

    def test_test_connection(self, query_handler, mock_connection_manager):
        """Test test_connection method."""
        mock_connection_manager.test_connection.return_value = True

        result = query_handler.test_connection()

        assert result is True
        mock_connection_manager.test_connection.assert_called_once()

    def test_get_connection_info(self, query_handler, mock_connection_manager):
        """Test get_connection_info method."""
        mock_info = Mock()
        mock_connection_manager.get_connection_info.return_value = mock_info

        result = query_handler.get_connection_info()

        assert result == mock_info
        mock_connection_manager.get_connection_info.assert_called_once()

    def test_context_manager(self, query_handler, mock_connection_manager):
        """Test QueryHandler as context manager."""
        mock_connection_manager.connect.return_value = True

        with query_handler as handler:
            assert handler == query_handler
            mock_connection_manager.connect.assert_called_once()

        mock_connection_manager.disconnect.assert_called_once()(result) == 2
        assert isinstance(result[0], GenericRecord)
        assert result[0].data['column1'] == 'value1'
        assert result[0].data['column2'] == 123
        assert result[0].data['column3'] == 45.6

    def test_parse_results_with_custom_model(self, mock_cursor):
        """Test parsing results with custom model."""
        # Mock cursor for NexsysRecord fields
        mock_cursor.description = [
            ('id', 'int'),
            ('name', 'string'),
            ('status', 'string'),
            ('amount', 'float')
        ]

        parser = PydanticResultParser(NexsysRecord)
        raw_data = [
            (1, 'Test Record', 'active', 100.50)
        ]

        result = parser.parse_results(raw_data, mock_cursor)

        assert len