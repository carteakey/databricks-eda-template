# ABOUTME: REST-based Databricks SQL query utility with safety checks
# ABOUTME: Reusable library for secure SQL execution returning pandas DataFrames

import os
import re
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional, Union
import warnings
import json


class DatabricksQueryClient:
    """
    A secure REST-based client for executing SQL queries on Databricks.

    Features:
    - SQL injection protection with dangerous pattern detection
    - Automatic environment variable loading
    - Built-in timeout and error handling
    - Returns pandas DataFrames for easy analysis
    - Detailed logging and debug options
    """

    def __init__(
        self, env_path: Optional[Union[str, Path]] = None, debug: bool = False
    ):
        """
        Initialize the Databricks query client.

        Args:
            env_path: Path to .env file. If None, tries multiple common locations.
            debug: Enable debug logging for troubleshooting.
        """
        self.debug = debug
        self._load_environment(env_path)
        self._validate_credentials()

        # Suppress SSL warnings for corporate environments
        if not self.debug:
            warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    def _load_environment(self, env_path: Optional[Union[str, Path]] = None):
        """Load environment variables from .env file with multiple fallback paths."""
        if env_path:
            env_paths = [Path(env_path)]
        else:
            # Try multiple common locations (add repo root first)
            env_paths = [
                Path.cwd()
                / ".env",  # Repository root when running scripts from project
                Path.cwd().parent / ".env",  # One level up (e.g., when inside utils/)
                Path.cwd().parent.parent
                / ".env",  # Two levels up (e.g., from notebooks/temp_code)
                Path.cwd().parent.parent.parent
                / ".env",  # Original notebook context attempt
                Path("..") / ".." / ".." / ".env",  # Relative path fallback
                Path(
                    "/Users/kchauhan/repos/ngm_dataset_eda_v2/.env"
                ),  # Absolute fallback (legacy)
            ]

        loaded = False
        for path in env_paths:
            if self.debug:
                print(f"🔍 Trying env_path: {path.resolve()}")

            if path.exists():
                load_dotenv(path, override=True)
                loaded = True
                if self.debug:
                    print(f"✅ Loaded environment from: {path.resolve()}")
                break

        if not loaded:
            raise EnvironmentError("Could not find .env file in any expected location")

    def _validate_credentials(self):
        """Validate that all required Databricks credentials are available."""
        self.token = os.getenv("DATABRICKS_ACCESS_TOKEN")
        self.hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")

        if self.debug:
            print(f"🔍 hostname: {self.hostname}")
            print(f"🔍 http_path: {self.http_path}")
            print(f"🔍 token: {self.token[:10] if self.token else None}...")

        # Check for placeholder values
        if (
            self.hostname == "your_hostname"
            or self.http_path == "/sql/1.0/warehouses/your_warehouse_id"
            or self.token == "your_token"
        ):
            raise ValueError(
                "Environment contains placeholder values! Check your .env file."
            )

        if not all([self.token, self.hostname, self.http_path]):
            missing = []
            if not self.token:
                missing.append("DATABRICKS_ACCESS_TOKEN")
            if not self.hostname:
                missing.append("DATABRICKS_SERVER_HOSTNAME")
            if not self.http_path:
                missing.append("DATABRICKS_HTTP_PATH")
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        # Extract warehouse ID
        self.warehouse_id = self.http_path.split("/")[-1]
        if self.debug:
            print(f"🔍 warehouse_id: {self.warehouse_id}")

    def _check_sql_safety(self, query: str) -> None:
        """
        Check SQL query for dangerous patterns that could modify data.

        Args:
            query: SQL query string to validate

        Raises:
            ValueError: If dangerous patterns are detected
        """
        query_upper = query.upper().strip()

        # Only allow SELECT and safe SHOW statements
        if not (query_upper.startswith('SELECT') or query_upper.startswith('SHOW')):
            raise ValueError('Only SELECT and SHOW queries are allowed')

        # If it's a SHOW statement, ensure it's a safe read-only SHOW command
        if query_upper.startswith("SHOW"):
            safe_show_patterns = [
                r"^SHOW\s+TABLES",
                r"^SHOW\s+DATABASES",
                r"^SHOW\s+SCHEMAS",
                r"^SHOW\s+COLUMNS",
                r"^SHOW\s+CREATE\s+TABLE",
                r"^SHOW\s+PARTITIONS",
                r"^SHOW\s+VIEWS",
            ]

            is_safe_show = any(
                re.match(pattern, query_upper) for pattern in safe_show_patterns
            )
            if not is_safe_show:
                raise ValueError(
                    "Only safe read-only SHOW commands are allowed (TABLES, DATABASES, SCHEMAS, COLUMNS, etc.)"
                )

        # Block dangerous statement patterns
        dangerous_patterns = [
            r"\bINSERT\s+INTO\b",
            r"\bUPDATE\s+\w+\s+SET\b",
            r"\bDELETE\s+FROM\b",
            r"\bDROP\s+(TABLE|VIEW|DATABASE|SCHEMA)\b",
            r"\bCREATE\s+(TABLE|VIEW|DATABASE|SCHEMA)\b",
            r"\bALTER\s+(TABLE|VIEW|DATABASE|SCHEMA)\b",
            r"\bTRUNCATE\s+TABLE\b",
        ]

        for pattern in dangerous_patterns:
            if re.search(pattern, query_upper):
                match = re.search(pattern, query_upper).group()
                raise ValueError(f"Dangerous SQL pattern detected: {match}")

    def execute_query(
        self, query: str, query_name: str = "Query", timeout: int = 30
    ) -> pd.DataFrame:
        """
        Execute a read-only SQL query on Databricks and return results as pandas DataFrame.

        Args:
            query: SQL SELECT query to execute
            query_name: Descriptive name for logging purposes
            timeout: Query timeout in seconds (5-50 seconds API limit)

        Returns:
            pandas.DataFrame: Query results

        Raises:
            ValueError: If query fails safety checks
            RuntimeError: If API call fails
        """
        # Safety checks
        self._check_sql_safety(query)

        # Ensure timeout is within API limits (5-50 seconds)
        api_timeout = min(max(timeout, 5), 50)

        # Prepare API request
        url = f"https://{self.hostname}/api/2.0/sql/statements"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        payload = {
            "statement": query,
            "warehouse_id": self.warehouse_id,
            "wait_timeout": f"{api_timeout}s",
        }

        if self.debug:
            print(f"🔄 Executing: {query_name}")
            print(f"🔍 Timeout: {api_timeout}s")

        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=api_timeout + 10,
                verify=False,
            )

            if response.status_code == 200:
                result = response.json()

                if self.debug:
                    print(f"🔍 API response keys: {list(result.keys())}")

                if "result" in result:
                    # Extract column information - try multiple locations
                    columns = []

                    # Try result.manifest.schema.columns first
                    if (
                        "manifest" in result["result"]
                        and "schema" in result["result"]["manifest"]
                    ):
                        columns = [
                            col["name"]
                            for col in result["result"]["manifest"]["schema"]["columns"]
                        ]

                    # Try top-level manifest.schema.columns as backup
                    elif "manifest" in result and "schema" in result["manifest"]:
                        columns = [
                            col["name"]
                            for col in result["manifest"]["schema"]["columns"]
                        ]

                    if self.debug and not columns:
                        print(f"🔍 Could not find columns in response structure")
                        if "manifest" in result:
                            print(
                                f"🔍 Top-level manifest keys: {list(result['manifest'].keys())}"
                            )
                        if "result" in result and "manifest" in result["result"]:
                            print(
                                f"🔍 Result manifest keys: {list(result['result']['manifest'].keys())}"
                            )

                    if self.debug and columns:
                        print(f"🔍 Found {len(columns)} columns: {columns}")

                    # Extract data
                    data = result["result"].get("data_array", [])

                    if data and columns:
                        df = pd.DataFrame(data, columns=columns)
                        if self.debug:
                            print(f"✅ Success: {len(df)} rows returned")
                        return df
                    elif data and not columns:
                        # Fallback: return data without column names
                        df = pd.DataFrame(data)
                        if self.debug:
                            print(f"⚠️ Got {len(data)} rows but no column info")
                        return df
                    else:
                        if self.debug:
                            print(
                                f"⚠️ No data returned (data: {len(data) if data else 0}, columns: {len(columns) if columns else 0})"
                            )
                        return pd.DataFrame()
                else:
                    # Handle query execution states
                    status = result.get("status", {})
                    state = status.get("state", "unknown")

                    if state == "FAILED" and "error" in status:
                        error_info = status["error"]
                        error_msg = error_info.get("message", "Unknown error")
                        error_code = error_info.get("error_code", "UNKNOWN")
                        raise RuntimeError(
                            f"Query failed: {error_msg} (Code: {error_code})"
                        )
                    else:
                        if self.debug:
                            print(f"⚠️ Query status: {state}")
                        return pd.DataFrame()

            else:
                error_msg = f"API call failed with status {response.status_code}"
                if response.text:
                    try:
                        error_detail = response.json()
                        if "message" in error_detail:
                            error_msg += f": {error_detail['message']}"
                    except:
                        error_msg += f": {response.text}"

                raise RuntimeError(error_msg)

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Network error: {e}")

    def test_connection(self) -> bool:
        """
        Test the connection to Databricks with a simple query.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            result = self.execute_query(
                "SELECT 1 as test, current_timestamp() as timestamp",
                "Connection Test",
                timeout=10,
            )
            return not result.empty
        except Exception as e:
            if self.debug:
                print(f"❌ Connection test failed: {e}")
            return False


# Convenience functions for quick usage
def query_databricks(
    query: str, query_name: str = "Query", timeout: int = 30, debug: bool = False
) -> pd.DataFrame:
    """
    Convenience function to execute a single query without managing client instance.

    Args:
        query: SQL SELECT query to execute
        query_name: Descriptive name for logging
        timeout: Query timeout in seconds
        debug: Enable debug logging

    Returns:
        pandas.DataFrame: Query results
    """
    client = DatabricksQueryClient(debug=debug)
    return client.execute_query(query, query_name, timeout)


def test_databricks_connection(debug: bool = False) -> bool:
    """
    Test Databricks connection.

    Args:
        debug: Enable debug logging

    Returns:
        bool: True if connection successful
    """
    client = DatabricksQueryClient(debug=debug)
    return client.test_connection()
