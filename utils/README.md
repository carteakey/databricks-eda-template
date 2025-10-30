# Databricks Query Utility

A secure, reusable REST-based library for executing SQL queries on Databricks with built-in safety checks.

## Features

- ✅ **SQL Injection Protection**: Blocks dangerous SQL patterns (INSERT, UPDATE, DELETE, DROP, etc.)
- ✅ **Automatic Environment Loading**: Finds .env files from multiple locations
- ✅ **Pandas Integration**: Returns results as pandas DataFrames
- ✅ **Error Handling**: Comprehensive error handling with detailed messages
- ✅ **Debug Mode**: Optional verbose logging for troubleshooting
- ✅ **Connection Testing**: Built-in connection validation

## Quick Start

### Simple Usage (Convenience Functions)

```python
from utils.databricks_query import query_databricks, test_databricks_connection

# Test connection
if test_databricks_connection():
    print("✅ Connected to Databricks")

# Execute a query
df = query_databricks(
    "SELECT * FROM my_table LIMIT 10",
    query_name="My Sample Query"
)
print(df.head())
```

### Advanced Usage (Client Instance)

```python
from utils.databricks_query import DatabricksQueryClient

# Create client with debug logging
client = DatabricksQueryClient(debug=True)

# Execute multiple queries
df1 = client.execute_query("SELECT COUNT(*) FROM table1", "Count Query")
df2 = client.execute_query("SELECT * FROM table2 LIMIT 5", "Sample Query")

# Test connection
if client.test_connection():
    print("Connection working!")
```

## Examples

### Steve's WPS Profile Query

```python
from utils.databricks_query import query_databricks

# Query WPS profile data
wps_query = '''
select device_id, product_seen_first, product_seen_last, 
       product_affiliate_id, device_manufacturer_name, 
       product_uninstalled, device_os_machine_guid 
from wps.profile_by_device 
where product_seen_first > '2025-08-16' 
  and product_seen_first <= '2025-08-18'
limit 10
'''

df = query_databricks(wps_query, "WPS Profile Sample")
print(f"Found {len(df)} records")
print(df.head())
```

### Error Handling

```python
from utils.databricks_query import DatabricksQueryClient

client = DatabricksQueryClient()

try:
    # This will be blocked by safety checks
    df = client.execute_query("DROP TABLE dangerous", "Bad Query")
except ValueError as e:
    print(f"Blocked dangerous query: {e}")

try:
    # This might fail due to network/API issues
    df = client.execute_query("SELECT * FROM nonexistent_table", "Test")
except RuntimeError as e:
    print(f"Query execution failed: {e}")
```

## Configuration

The utility automatically looks for `.env` files in these locations:
1. Path specified in constructor
2. `../../../.env` (relative to current directory)
3. `/Users/oansari/code/mcafee-eng/cpe-analytics/.env` (absolute fallback)

Required environment variables:
- `DATABRICKS_ACCESS_TOKEN`
- `DATABRICKS_SERVER_HOSTNAME`
- `DATABRICKS_HTTP_PATH`

## Safety Features

The utility blocks these dangerous SQL patterns:
- `INSERT INTO`
- `UPDATE ... SET`
- `DELETE FROM`
- `DROP TABLE/VIEW/DATABASE/SCHEMA`
- `CREATE TABLE/VIEW/DATABASE/SCHEMA`
- `ALTER TABLE/VIEW/DATABASE/SCHEMA`
- `TRUNCATE TABLE`

Only `SELECT` statements are allowed.

## API Reference

### DatabricksQueryClient

**Constructor:**
- `env_path` (optional): Path to .env file
- `debug` (bool): Enable debug logging

**Methods:**
- `execute_query(query, query_name, timeout)`: Execute SQL query
- `test_connection()`: Test Databricks connection

### Convenience Functions

- `query_databricks(query, query_name, timeout, debug)`: Execute single query
- `test_databricks_connection(debug)`: Test connection

## Testing

Run the test suite:
```bash
python utils/test_utility.py
```

This tests:
- Connection functionality
- Query execution
- Safety checks
- Error handling