# Databricks SQL Handler

A comprehensive Python package for interacting with Databricks SQL warehouses using Pydantic models, OAuth authentication, and robust connection management.

## Features

- **OAuth Authentication**: Secure client credentials flow authentication
- **Pydantic Models**: Type-safe data models for query results
- **Connection Management**: Robust connection handling with automatic retry logic
- **Query Execution**: Execute single or multiple queries with structured results
- **Built-in Models**: Pre-defined models for common Databricks operations
- **Extensible**: Easy to extend with custom data models
- **CLI Interface**: Interactive command-line interface for testing and development

## Installation

```bash
pip install dbxsql
```

## Quick Start

### Basic Usage

```python
from dbxsql import QueryHandler, DatabricksSettings, NexsysRecord

# Configure settings (or use environment variables)
settings = DatabricksSettings(
    client_id="your_client_id",
    client_secret="your_client_secret", 
    server_hostname="your_databricks_hostname",
    http_path="/sql/1.0/warehouses/your_warehouse_id"
)

# Use as context manager
with QueryHandler(settings) as handler:
    # Execute a simple query
    result = handler.execute_query("SELECT * FROM my_table LIMIT 10")
    
    # Execute with Pydantic model parsing
    result = handler.execute_query(
        "SELECT * FROM nexsys_table LIMIT 10", 
        NexsysRecord
    )
    
    # Access structured data
    for record in result.data:
        print(f"ID: {record.id}, Name: {record.name}")
```

### Environment Variables

Create a `.env` file:

```env
DATABRICKS_CLIENT_ID=your_client_id
DATABRICKS_CLIENT_SECRET=your_client_secret
DATABRICKS_SERVER_HOSTNAME=your_hostname.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/warehouse_id
DATABRICKS_LOG_LEVEL=INFO
```

### CLI Usage

```bash
# Interactive mode
dbxsql --interactive

# Run example queries
dbxsql --examples

# Execute a single query
dbxsql --query "SELECT current_timestamp()"
```

## Custom Models

Create your own Pydantic models:

```python
from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class MyCustomModel(BaseModel):
    id: int
    name: str
    created_at: datetime
    amount: Optional[float] = None

# Register the model
from dbxsql import register_model
register_model("my_custom", MyCustomModel)

# Use it in queries
result = handler.execute_query("SELECT * FROM my_table", MyCustomModel)
```

## Configuration

All configuration can be done via environment variables with the `DATABRICKS_` prefix or programmatically:

| Setting | Environment Variable | Default | Description |
|---------|---------------------|---------|-------------|
| client_id | DATABRICKS_CLIENT_ID | Required | OAuth client ID |
| client_secret | DATABRICKS_CLIENT_SECRET | Required | OAuth client secret |
| server_hostname | DATABRICKS_SERVER_HOSTNAME | Required | Databricks hostname |
| http_path | DATABRICKS_HTTP_PATH | Required | SQL warehouse HTTP path |
| log_level | DATABRICKS_LOG_LEVEL | INFO | Logging level |
| max_retries | DATABRICKS_MAX_RETRIES | 3 | Query retry attempts |
| query_timeout | DATABRICKS_QUERY_TIMEOUT | 300 | Query timeout (seconds) |

## API Reference

### QueryHandler

Main class for executing queries and managing connections.

#### Methods

- `execute_query(query, model_class=None)`: Execute single query
- `execute_multiple_queries(queries, model_classes=None)`: Execute multiple queries  
- `execute_query_with_retry(query, model_class=None)`: Execute with automatic retry
- `list_files(path)`: List files in Databricks path
- `show_tables(database=None)`: Show tables in database
- `test_connection()`: Test database connectivity

### Built-in Models

- `NexsysRecord`: For NEXSYS system data
- `SalesRecord`: For sales transaction data  
- `FileInfo`: For file listing results
- `TableInfo`: For table information
- `GenericRecord`: For unknown data structures

## Error Handling

The package provides specific exceptions:

```python
from dbxsql import (
    AuthenticationError,
    ConnectionError, 
    QueryExecutionError,
    SyntaxError,
    TimeoutError,
    DataParsingError
)

try:
    result = handler.execute_query("SELECT * FROM table")
except AuthenticationError:
    print("Authentication failed")
except QueryExecutionError as e:
    print(f"Query failed: {e}")
```

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.