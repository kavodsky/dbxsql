"""Main application entry point for Databricks SQL Handler."""

import sys
import logging
import argparse
from pathlib import Path

from dbxsql import (
    QueryHandler, settings, QueryResult, NexsysRecord, SalesRecord,
    GenericRecord, get_model_class, list_available_models, DatabricksHandlerError
)

logger = logging.getLogger(__name__)


class ApplicationRunner:
    """Application runner with different execution modes."""

    def __init__(self, handler: QueryHandler):
        self.handler = handler

    def run_example_queries(self) -> None:
        """Run example queries to demonstrate functionality."""
        print("\n" + "=" * 60)
        print("DATABRICKS SQL HANDLER - EXAMPLE QUERIES")
        print("=" * 60)

        examples = [
            ("Current Timestamp Query", "SELECT current_timestamp() as current_time", None),
            # ("List Files", "LIST '/Volumes/...path..to..your..folder'", None),
            ("Generic Query", "SELECT 'sample' as name, 123 as value, current_date() as date_col", GenericRecord),
            # ("Parquet Files", "SELECT * FROM parquet.`/Volumes/...path..to..your..folder/*.parquet` LIMIT 5", NexsysRecord),
        ]

        for i, (title, query, model_class) in enumerate(examples, 1):
            print(f"\n{i}. {title}:")
            self._execute_example_query(query, model_class)

        # Multiple queries example
        print("\n5. Multiple Queries Execution:")
        self._execute_multiple_queries_example()

    def _execute_example_query(self, query: str, model_class=None) -> None:
        """Execute a single example query."""
        try:
            result = self.handler.execute_query(query, model_class)
            if result.status.value == "success":
                print(f"   Success! Rows: {result.row_count}")
                if result.data and len(result.data) <= 3:
                    for item in result.data:
                        print(f"   - {item}")
                elif result.raw_data and len(result.raw_data) <= 3:
                    for item in result.raw_data:
                        print(f"   - {item}")
            else:
                print(f"   Failed: {result.error_message}")
        except Exception as e:
            print(f"   Error: {str(e)}")

    def _execute_multiple_queries_example(self) -> None:
        """Execute multiple queries example."""
        queries = [
            "SELECT 1 as test_number",
            "SELECT current_user() as user",
            "SHOW DATABASES"
        ]

        try:
            results = self.handler.execute_multiple_queries(queries, [GenericRecord] * 3)
            for i, result in results.items():
                status = "Success" if result.status.value == "success" else "Failed"
                print(f"   Query {i + 1}: {status} ({result.row_count} rows)")
                if result.error_message:
                    print(f"     Error: {result.error_message}")
        except Exception as e:
            print(f"   Error: {str(e)}")

    def run_interactive_mode(self) -> None:
        """Run interactive SQL query mode."""
        print("\n" + "=" * 60)
        print("INTERACTIVE SQL MODE")
        print("=" * 60)
        print("Enter SQL queries (type 'quit' to exit)")
        self._show_help_commands()
        print()

        while True:
            try:
                query = input("SQL> ").strip()

                if query.lower() in ['quit', 'exit', 'q']:
                    break

                if not query:
                    continue

                if self._handle_special_command(query):
                    continue

                # Execute regular SQL query
                self._execute_interactive_query(query)

            except KeyboardInterrupt:
                print("\nExiting interactive mode...")
                break
            except EOFError:
                break

    def _show_help_commands(self) -> None:
        """Show help commands."""
        print("Available commands:")
        print("  quit, exit, q          - Exit interactive mode")
        print("  metrics               - Show query metrics")
        print("  connection            - Show connection info")
        print("  models                - List available models")
        print("  help                  - Show this help")

    def _handle_special_command(self, query: str) -> bool:
        """Handle special commands. Returns True if command was handled."""
        command = query.lower()

        if command == 'help':
            self._show_help_commands()
            return True

        if command == 'metrics':
            self._show_metrics()
            return True

        if command == 'connection':
            self._show_connection_info()
            return True

        if command == 'models':
            self._show_available_models()
            return True

        return False

    def _show_metrics(self) -> None:
        """Show query metrics."""
        metrics = self.handler.get_metrics()
        print("Query Metrics:")
        print(f"  Total queries: {metrics.total_queries}")
        print(f"  Successful: {metrics.successful_queries}")
        print(f"  Failed: {metrics.failed_queries}")
        avg_time = metrics.average_execution_time
        print(f"  Average execution time: {avg_time:.3f}s" if avg_time else "  Average execution time: N/A")

    def _show_connection_info(self) -> None:
        """Show connection information."""
        conn_info = self.handler.get_connection_info()
        print("Connection Info:")
        print(f"  Server: {conn_info.server_hostname}")
        print(f"  Connected: {conn_info.is_connected}")
        print(f"  Last activity: {conn_info.last_activity}")

    def _show_available_models(self) -> None:
        """Show available models."""
        models = list_available_models()
        print("Available models:")
        for model_name in models:
            model_class = get_model_class(model_name)
            print(f"  {model_name}: {model_class.__name__}")

    def _execute_interactive_query(self, query: str) -> None:
        """Execute a query in interactive mode."""
        # Ask for model class
        model_input = input("Model class (optional, press Enter for raw data): ").strip()
        model_class = None

        if model_input:
            model_class = get_model_class(model_input)
            print(f"Using model: {model_class.__name__}")

        try:
            result = self.handler.execute_query_with_retry(query, model_class)
            self._display_query_result(result)

        except Exception as e:
            print(f"✗ Error: {str(e)}")

    def _display_query_result(self, result: QueryResult) -> None:
        """Display query result."""
        if result.status.value == "success":
            print(f"✓ Success! Rows: {result.row_count}, Time: {result.execution_time_seconds:.3f}s")

            if result.data:
                self._display_parsed_data(result.data)
            elif result.raw_data:
                self._display_raw_data(result.raw_data)
        else:
            print(f"✗ Failed: {result.error_message}")

    def _display_parsed_data(self, data: list) -> None:
        """Display parsed data."""
        if len(data) <= 10:
            print("Results:")
            for i, row in enumerate(data):
                print(f"  {i + 1}: {row}")
        else:
            print("First 5 results:")
            for i, row in enumerate(data[:5]):
                print(f"  {i + 1}: {row}")
            print(f"  ... and {len(data) - 5} more rows")

    def _display_raw_data(self, data: list) -> None:
        """Display raw data."""
        print("Raw results:")
        for i, row in enumerate(data[:5]):
            print(f"  {i + 1}: {row}")
        if len(data) > 5:
            print(f"  ... and {len(data) - 5} more rows")

    def execute_single_query(self, query: str, model_name: str = None) -> None:
        """Execute a single query."""
        model_class = get_model_class(model_name) if model_name else None
        result = self.handler.execute_query_with_retry(query, model_class)

        print("\nQuery Result:")
        print(f"Status: {result.status.value}")
        print(f"Rows: {result.row_count}")
        print(f"Execution time: {result.execution_time_seconds:.3f}s")

        if result.status.value == "success" and result.data:
            print("Data:")
            for row in result.data[:10]:  # Show first 10 rows
                print(f"  {row}")
        elif result.error_message:
            print(f"Error: {result.error_message}")

    def execute_queries_from_file(self, file_path: str) -> None:
        """Execute queries from a file."""
        path = Path(file_path)
        if not path.exists():
            print(f"❌ File not found: {file_path}")
            sys.exit(1)

        queries = path.read_text().split(';')
        queries = [q.strip() for q in queries if q.strip()]

        print(f"\nExecuting {len(queries)} queries from {file_path}...")
        results = self.handler.execute_multiple_queries(queries)

        for i, result in results.items():
            print(f"Query {i + 1}: {result.status.value} ({result.row_count} rows)")
            if result.error_message:
                print(f"  Error: {result.error_message}")

    def show_session_summary(self) -> None:
        """Show session summary."""
        metrics = self.handler.get_metrics()
        if metrics.total_queries > 0:
            print("\nSession Summary:")
            print(f"  Total queries executed: {metrics.total_queries}")
            print(f"  Successful: {metrics.successful_queries}")
            print(f"  Failed: {metrics.failed_queries}")
            print(f"  Total execution time: {metrics.total_execution_time:.3f}s")
            avg_time = metrics.average_execution_time
            print(f"  Average execution time: {avg_time:.3f}s" if avg_time else "  Average execution time: N/A")


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(description="Databricks SQL Handler")
    parser.add_argument("--interactive", "-i", action="store_true",
                        help="Run in interactive mode")
    parser.add_argument("--examples", "-e", action="store_true",
                        help="Run example queries")
    parser.add_argument("--query", "-q", type=str,
                        help="Execute a single query")
    parser.add_argument("--model", "-m", type=str,
                        help="Model class to use for parsing results")
    parser.add_argument("--file", "-f", type=str,
                        help="Execute queries from file")
    return parser


def main():
    """Main application entry point."""
    parser = create_argument_parser()
    args = parser.parse_args()

    # Check if .env file exists
    env_file = Path(".env")
    if not env_file.exists():
        logger.warning("No .env file found. Make sure environment variables are set.")

    try:
        # Initialize handler with context manager
        with QueryHandler(settings) as handler:
            # Test connection
            print("Testing connection to Databricks...")
            if not handler.test_connection():
                print("❌ Connection test failed!")
                sys.exit(1)
            print("✅ Connected to Databricks successfully!")

            # Create application runner
            app_runner = ApplicationRunner(handler)

            # Handle different execution modes
            if args.examples:
                app_runner.run_example_queries()

            if args.query:
                app_runner.execute_single_query(args.query, args.model)

            if args.file:
                app_runner.execute_queries_from_file(args.file)

            if args.interactive or (not args.examples and not args.query and not args.file):
                app_runner.run_interactive_mode()

            # Show final metrics
            app_runner.show_session_summary()

    except DatabricksHandlerError as e:
        logger.error(f"Databricks handler error: {str(e)}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nApplication interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    # main()
    with QueryHandler(settings) as handler:
        # Create application runner
        app_runner = ApplicationRunner(handler)
        app_runner.run_example_queries()