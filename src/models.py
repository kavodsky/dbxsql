"""Pydantic models for Databricks SQL handler."""

from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Any, Dict, List, Optional, Union, Generic, TypeVar
from datetime import datetime
from enum import Enum

T = TypeVar('T', bound=BaseModel)


class QueryStatus(str, Enum):
    """Query execution status."""
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    SYNTAX_ERROR = "syntax_error"


class FileInfo(BaseModel):
    """Model for file information from LIST command."""
    model_config = ConfigDict(str_strip_whitespace=True)

    path: str
    name: str
    size: Optional[int] = None
    modification_time: Optional[datetime] = None
    is_directory: bool = False

    @field_validator('path')
    @classmethod
    def validate_path(cls, v: str) -> str:
        """Ensure path is not empty."""
        if not v or not v.strip():
            raise ValueError('Path cannot be empty')
        return v.strip()


class TableInfo(BaseModel):
    """Model for table information."""
    model_config = ConfigDict(
        # Allow field alias for different naming conventions
        populate_by_name=True,
        str_strip_whitespace=True
    )

    database: str
    table_name: str
    is_temporary: bool = False
    table_type: Optional[str] = None


class QueryResult(BaseModel, Generic[T]):
    """Generic query result wrapper."""
    status: QueryStatus
    data: Optional[List[T]] = None
    raw_data: Optional[List[Any]] = None
    row_count: int = 0
    execution_time_seconds: Optional[float] = None
    error_message: Optional[str] = None
    query: Optional[str] = None

    @field_validator('row_count')
    @classmethod
    def validate_row_count(cls, v: int) -> int:
        """Ensure row count is not negative."""
        return max(0, v)


class QueryMetrics(BaseModel):
    """Query execution metrics."""
    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    total_execution_time: float = 0.0
    average_execution_time: Optional[float] = None

    def add_query_result(self, result: QueryResult) -> None:
        """Add a query result to metrics."""
        self.total_queries += 1
        if result.status == QueryStatus.SUCCESS:
            self.successful_queries += 1
        else:
            self.failed_queries += 1

        if result.execution_time_seconds:
            self.total_execution_time += result.execution_time_seconds
            self.average_execution_time = self.total_execution_time / self.total_queries


class ConnectionInfo(BaseModel):
    """Database connection information."""
    server_hostname: str
    http_path: str
    is_connected: bool = False
    connection_time: Optional[datetime] = None
    last_activity: Optional[datetime] = None

    def mark_connected(self) -> None:
        """Mark connection as established."""
        self.is_connected = True
        self.connection_time = datetime.now()
        self.last_activity = datetime.now()

    def update_activity(self) -> None:
        """Update last activity timestamp."""
        self.last_activity = datetime.now()


# Example domain-specific models
class NexsysRecord(BaseModel):
    """Example model for NEXSYS data records."""
    model_config = ConfigDict(
        # Allow extra fields that might be in the data
        extra="allow",
        # Parse datetime strings automatically
        json_encoders={
            datetime: lambda v: v.isoformat() if v else None
        }
    )

    id: Optional[int] = None
    name: Optional[str] = None
    created_date: Optional[datetime] = None
    status: Optional[str] = None
    amount: Optional[float] = None


class SalesRecord(BaseModel):
    """Example model for sales data."""
    transaction_id: str
    customer_id: Optional[str] = None
    product_id: Optional[str] = None
    quantity: int = Field(ge=0, description="Quantity must be non-negative")
    unit_price: float = Field(ge=0, description="Price must be non-negative")
    total_amount: Optional[float] = None
    transaction_date: datetime

    @field_validator('total_amount', mode='before')
    @classmethod
    def calculate_total(cls, v: Optional[float], info) -> Optional[float]:
        """Calculate total amount if not provided."""
        if v is None:
            data = info.data
            quantity = data.get('quantity')
            unit_price = data.get('unit_price')
            if quantity is not None and unit_price is not None:
                return quantity * unit_price
        return v


class GenericRecord(BaseModel):
    """Generic record model for unknown data structures."""
    data: Dict[str, Any]

    def __getitem__(self, key: str) -> Any:
        """Allow dict-like access."""
        return self.data.get(key)

    def __setitem__(self, key: str, value: Any) -> None:
        """Allow dict-like assignment."""
        self.data[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Get value with default."""
        return self.data.get(key, default)

    def keys(self):
        """Get keys like a dictionary."""
        return self.data.keys()

    def values(self):
        """Get values like a dictionary."""
        return self.data.values()

    def items(self):
        """Get items like a dictionary."""
        return self.data.items()


# Model registry for dynamic model selection
MODEL_REGISTRY: Dict[str, type[BaseModel]] = {
    'nexsys': NexsysRecord,
    'sales': SalesRecord,
    'file_info': FileInfo,
    'table_info': TableInfo,
    'generic': GenericRecord,
}


def get_model_class(model_name: str) -> type[BaseModel]:
    """Get model class by name."""
    return MODEL_REGISTRY.get(model_name.lower(), GenericRecord)


def register_model(name: str, model_class: type[BaseModel]) -> None:
    """Register a new model class."""
    MODEL_REGISTRY[name.lower()] = model_class


def list_available_models() -> List[str]:
    """List all available model names."""
    return list(MODEL_REGISTRY.keys())