"""Tests for models module."""

import pytest
from datetime import datetime
from pydantic import ValidationError
from typing import List

from dbxsql.models import (
    QueryStatus, FileInfo, TableInfo, QueryResult, QueryMetrics,
    ConnectionInfo, NexsysRecord, SalesRecord, GenericRecord,
    get_model_class, register_model, list_available_models, MODEL_REGISTRY
)


class TestQueryStatus:
    """Test cases for QueryStatus enum."""

    def test_query_status_values(self):
        """Test QueryStatus enum values."""
        assert QueryStatus.SUCCESS == "success"
        assert QueryStatus.FAILED == "failed"
        assert QueryStatus.TIMEOUT == "timeout"
        assert QueryStatus.SYNTAX_ERROR == "syntax_error"

    def test_query_status_membership(self):
        """Test QueryStatus membership."""
        assert "success" in QueryStatus
        assert "failed" in QueryStatus
        assert "invalid" not in QueryStatus


class TestFileInfo:
    """Test cases for FileInfo model."""

    def test_valid_file_info(self):
        """Test creating valid FileInfo."""
        file_info = FileInfo(
            path="/test/path/file.txt",
            name="file.txt",
            size=1024,
            modification_time=datetime.now(),
            is_directory=False
        )

        assert file_info.path == "/test/path/file.txt"
        assert file_info.name == "file.txt"
        assert file_info.size == 1024
        assert not file_info.is_directory

    def test_file_info_with_defaults(self):
        """Test FileInfo with default values."""
        file_info = FileInfo(path="/test/path", name="test")

        assert file_info.path == "/test/path"
        assert file_info.name == "test"
        assert file_info.size is None
        assert file_info.modification_time is None
        assert not file_info.is_directory

    def test_file_info_directory(self):
        """Test FileInfo for directory."""
        file_info = FileInfo(
            path="/test/directory",
            name="directory",
            is_directory=True
        )

        assert file_info.is_directory

    def test_file_info_path_validation_empty(self):
        """Test FileInfo path validation with empty path."""
        with pytest.raises(ValidationError) as exc_info:
            FileInfo(path="", name="test")

        assert "Path cannot be empty" in str(exc_info.value)

    def test_file_info_path_validation_whitespace(self):
        """Test FileInfo path validation with whitespace path."""
        with pytest.raises(ValidationError) as exc_info:
            FileInfo(path="   ", name="test")

        assert "Path cannot be empty" in str(exc_info.value)

    def test_file_info_path_stripping(self):
        """Test FileInfo path whitespace stripping."""
        file_info = FileInfo(path="  /test/path  ", name="test")
        assert file_info.path == "/test/path"


class TestTableInfo:
    """Test cases for TableInfo model."""

    def test_valid_table_info(self):
        """Test creating valid TableInfo."""
        table_info = TableInfo(
            database="test_db",
            table_name="test_table",
            is_temporary=True,
            table_type="MANAGED"
        )

        assert table_info.database == "test_db"
        assert table_info.table_name == "test_table"
        assert table_info.is_temporary
        assert table_info.table_type == "MANAGED"

    def test_table_info_with_defaults(self):
        """Test TableInfo with default values."""
        table_info = TableInfo(database="db", table_name="table")

        assert not table_info.is_temporary
        assert table_info.table_type is None

    def test_table_info_string_stripping(self):
        """Test TableInfo string field stripping."""
        table_info = TableInfo(
            database="  test_db  ",
            table_name="  test_table  "
        )

        assert table_info.database == "test_db"
        assert table_info.table_name == "test_table"


class TestQueryResult:
    """Test cases for QueryResult model."""

    def test_valid_query_result(self):
        """Test creating valid QueryResult."""
        data = [{"col1": "value1"}, {"col1": "value2"}]
        result = QueryResult[dict](
            status=QueryStatus.SUCCESS,
            data=data,
            row_count=2,
            execution_time_seconds=1.5,
            query="SELECT * FROM test"
        )

        assert result.status == QueryStatus.SUCCESS
        assert result.data == data
        assert result.row_count == 2
        assert result.execution_time_seconds == 1.5
        assert result.query == "SELECT * FROM test"

    def test_query_result_with_defaults(self):
        """Test QueryResult with default values."""
        result = QueryResult(status=QueryStatus.FAILED)

        assert result.status == QueryStatus.FAILED
        assert result.data is None
        assert result.raw_data is None
        assert result.row_count == 0
        assert result.execution_time_seconds is None
        assert result.error_message is None
        assert result.query is None

    def test_query_result_row_count_validation_negative(self):
        """Test QueryResult row_count validation with negative value."""
        result = QueryResult(status=QueryStatus.SUCCESS, row_count=-5)
        # Should be corrected to 0
        assert result.row_count == 0

    def test_query_result_row_count_validation_positive(self):
        """Test QueryResult row_count validation with positive value."""
        result = QueryResult(status=QueryStatus.SUCCESS, row_count=10)
        assert result.row_count == 10

    def test_query_result_with_error(self):
        """Test QueryResult with error information."""
        result = QueryResult(
            status=QueryStatus.FAILED,
            error_message="Syntax error in query",
            query="SELECT * FROM nonexistent"
        )

        assert result.status == QueryStatus.FAILED
        assert result.error_message == "Syntax error in query"
        assert result.query == "SELECT * FROM nonexistent"


class TestQueryMetrics:
    """Test cases for QueryMetrics model."""

    def test_query_metrics_initialization(self):
        """Test QueryMetrics initialization."""
        metrics = QueryMetrics()

        assert metrics.total_queries == 0
        assert metrics.successful_queries == 0
        assert metrics.failed_queries == 0
        assert metrics.total_execution_time == 0.0
        assert metrics.average_execution_time is None

    def test_add_successful_query_result(self):
        """Test adding successful query result to metrics."""
        metrics = QueryMetrics()
        result = QueryResult(
            status=QueryStatus.SUCCESS,
            execution_time_seconds=2.0
        )

        metrics.add_query_result(result)

        assert metrics.total_queries == 1
        assert metrics.successful_queries == 1
        assert metrics.failed_queries == 0
        assert metrics.total_execution_time == 2.0
        assert metrics.average_execution_time == 2.0

    def test_add_failed_query_result(self):
        """Test adding failed query result to metrics."""
        metrics = QueryMetrics()
        result = QueryResult(
            status=QueryStatus.FAILED,
            execution_time_seconds=1.5
        )

        metrics.add_query_result(result)

        assert metrics.total_queries == 1
        assert metrics.successful_queries == 0
        assert metrics.failed_queries == 1
        assert metrics.total_execution_time == 1.5
        assert metrics.average_execution_time == 1.5

    def test_add_multiple_query_results(self):
        """Test adding multiple query results to metrics."""
        metrics = QueryMetrics()

        # Add successful result
        success_result = QueryResult(
            status=QueryStatus.SUCCESS,
            execution_time_seconds=2.0
        )
        metrics.add_query_result(success_result)

        # Add failed result
        failed_result = QueryResult(
            status=QueryStatus.FAILED,
            execution_time_seconds=3.0
        )
        metrics.add_query_result(failed_result)

        assert metrics.total_queries == 2
        assert metrics.successful_queries == 1
        assert metrics.failed_queries == 1
        assert metrics.total_execution_time == 5.0
        assert metrics.average_execution_time == 2.5

    def test_add_query_result_without_execution_time(self):
        """Test adding query result without execution time."""
        metrics = QueryMetrics()
        result = QueryResult(status=QueryStatus.SUCCESS)

        metrics.add_query_result(result)

        assert metrics.total_queries == 1
        assert metrics.successful_queries == 1
        assert metrics.total_execution_time == 0.0
        assert metrics.average_execution_time == 0.0


class TestConnectionInfo:
    """Test cases for ConnectionInfo model."""

    def test_connection_info_initialization(self):
        """Test ConnectionInfo initialization."""
        conn_info = ConnectionInfo(
            server_hostname="test.databricks.com",
            http_path="/sql/1.0/warehouses/test"
        )

        assert conn_info.server_hostname == "test.databricks.com"
        assert conn_info.http_path == "/sql/1.0/warehouses/test"
        assert not conn_info.is_connected
        assert conn_info.connection_time is None
        assert conn_info.last_activity is None

    def test_mark_connected(self):
        """Test mark_connected method."""
        conn_info = ConnectionInfo(
            server_hostname="test.com",
            http_path="/test"
        )

        conn_info.mark_connected()

        assert conn_info.is_connected
        assert isinstance(conn_info.connection_time, datetime)
        assert isinstance(conn_info.last_activity, datetime)
        assert conn_info.connection_time == conn_info.last_activity

    def test_update_activity(self):
        """Test update_activity method."""
        conn_info = ConnectionInfo(
            server_hostname="test.com",
            http_path="/test"
        )
        conn_info.mark_connected()

        original_activity = conn_info.last_activity
        original_connection_time = conn_info.connection_time

        # Small delay to ensure different timestamp
        import time
        time.sleep(0.01)

        conn_info.update_activity()

        # Connection time should remain the same
        assert conn_info.connection_time == original_connection_time
        # Last activity should be updated
        assert conn_info.last_activity != original_activity
        assert conn_info.last_activity > original_activity


class TestNexsysRecord:
    """Test cases for NexsysRecord model."""

    def test_nexsys_record_with_all_fields(self):
        """Test NexsysRecord with all fields."""
        record = NexsysRecord(
            id=123,
            name="Test Record",
            created_date=datetime.now(),
            status="active",
            amount=999.99
        )

        assert record.id == 123
        assert record.name == "Test Record"
        assert record.status == "active"
        assert record.amount == 999.99

    def test_nexsys_record_with_defaults(self):
        """Test NexsysRecord with default values."""
        record = NexsysRecord()

        assert record.id is None
        assert record.name is None
        assert record.created_date is None
        assert record.status is None
        assert record.amount is None

    def test_nexsys_record_extra_fields_allowed(self):
        """Test NexsysRecord allows extra fields."""
        record = NexsysRecord(
            id=1,
            name="Test",
            extra_field="extra_value"
        )

        assert record.id == 1
        assert record.name == "Test"
        # Extra fields should be allowed but not directly accessible
        assert hasattr(record, 'extra_field')
        assert record.extra_field == "extra_value"


class TestSalesRecord:
    """Test cases for SalesRecord model."""

    def test_sales_record_valid(self):
        """Test creating valid SalesRecord."""
        record = SalesRecord(
            transaction_id="TXN123",
            customer_id="CUST456",
            product_id="PROD789",
            quantity=5,
            unit_price=19.99,
            total_amount=99.95,
            transaction_date=datetime.now()
        )

        assert record.transaction_id == "TXN123"
        assert record.customer_id == "CUST456"
        assert record.product_id == "PROD789"
        assert record.quantity == 5
        assert record.unit_price == 19.99
        assert record.total_amount == 99.95

    def test_sales_record_total_amount_calculation(self):
        """Test SalesRecord total_amount auto-calculation."""
        record = SalesRecord(
            transaction_id="TXN123",
            quantity=3,
            unit_price=10.50,
            transaction_date=datetime.now()
        )

        # total_amount should be auto-calculated
        assert record.total_amount == 31.50

    def test_sales_record_negative_quantity_validation(self):
        """Test SalesRecord quantity validation."""
        with pytest.raises(ValidationError) as exc_info:
            SalesRecord(
                transaction_id="TXN123",
                quantity=-1,
                unit_price=10.0,
                transaction_date=datetime.now()
            )

        assert "Quantity must be non-negative" in str(exc_info.value)

    def test_sales_record_negative_price_validation(self):
        """Test SalesRecord unit_price validation."""
        with pytest.raises(ValidationError) as exc_info:
            SalesRecord(
                transaction_id="TXN123",
                quantity=1,
                unit_price=-5.0,
                transaction_date=datetime.now()
            )

        assert "Price must be non-negative" in str(exc_info.value)

    def test_sales_record_provided_total_amount(self):
        """Test SalesRecord with provided total_amount."""
        record = SalesRecord(
            transaction_id="TXN123",
            quantity=2,
            unit_price=10.0,
            total_amount=25.0,  # Different from calculated
            transaction_date=datetime.now()
        )

        # Should use provided value, not calculate
        assert record.total_amount == 25.0


class TestGenericRecord:
    """Test cases for GenericRecord model."""

    def test_generic_record_creation(self):
        """Test GenericRecord creation."""
        data = {"name": "test", "value": 123, "active": True}
        record = GenericRecord(data=data)

        assert record.data == data

    def test_generic_record_dict_access(self):
        """Test GenericRecord dict-like access methods."""
        data = {"name": "test", "value": 123}
        record = GenericRecord(data=data)

        # Test __getitem__
        assert record["name"] == "test"
        assert record["value"] == 123
        assert record["nonexistent"] is None

        # Test __setitem__
        record["new_key"] = "new_value"
        assert record.data["new_key"] == "new_value"

        # Test get method
        assert record.get("name") == "test"
        assert record.get("nonexistent", "default") == "default"

    def test_generic_record_dict_methods(self):
        """Test GenericRecord dict-like methods."""
        data = {"name": "test", "value": 123, "active": True}
        record = GenericRecord(data=data)

        # Test keys
        keys = list(record.keys())
        assert "name" in keys
        assert "value" in keys
        assert "active" in keys

        # Test values
        values = list(record.values())
        assert "test" in values
        assert 123 in values
        assert True in values

        # Test items
        items = list(record.items())
        assert ("name", "test") in items
        assert ("value", 123) in items
        assert ("active", True) in items


class TestModelRegistry:
    """Test cases for model registry functions."""

    def test_get_model_class_existing(self):
        """Test getting existing model class."""
        model_class = get_model_class("nexsys")
        assert model_class == NexsysRecord

        model_class = get_model_class("sales")
        assert model_class == SalesRecord

    def test_get_model_class_case_insensitive(self):
        """Test getting model class is case insensitive."""
        model_class = get_model_class("NEXSYS")
        assert model_class == NexsysRecord

        model_class = get_model_class("Sales")
        assert model_class == SalesRecord

    def test_get_model_class_nonexistent(self):
        """Test getting nonexistent model class returns GenericRecord."""
        model_class = get_model_class("nonexistent")
        assert model_class == GenericRecord

    def test_register_model(self):
        """Test registering new model class."""

        class CustomModel(GenericRecord):
            pass

        register_model("custom", CustomModel)

        # Should be able to retrieve it
        model_class = get_model_class("custom")
        assert model_class == CustomModel

        # Should appear in available models
        available_models = list_available_models()
        assert "custom" in available_models

    def test_list_available_models(self):
        """Test listing available models."""
        models = list_available_models()

        # Should include default models
        assert "nexsys" in models
        assert "sales" in models
        assert "file_info" in models
        assert "table_info" in models
        assert "generic" in models

    def test_model_registry_contents(self):
        """Test MODEL_REGISTRY contents."""
        assert "nexsys" in MODEL_REGISTRY
        assert "sales" in MODEL_REGISTRY
        assert "file_info" in MODEL_REGISTRY
        assert "table_info" in MODEL_REGISTRY
        assert "generic" in MODEL_REGISTRY

        assert MODEL_REGISTRY["nexsys"] == NexsysRecord
        assert MODEL_REGISTRY["sales"] == SalesRecord
        assert MODEL_REGISTRY["file_info"] == FileInfo
        assert MODEL_REGISTRY["table_info"] == TableInfo
        assert MODEL_REGISTRY["generic"] == GenericRecord