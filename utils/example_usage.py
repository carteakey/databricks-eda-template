# ABOUTME: Example usage of the Databricks query utility
# ABOUTME: Demonstrates various ways to use the utility library

import sys
from pathlib import Path

# Add utils to path for importing
sys.path.append(str(Path(__file__).parent))

from databricks_query import DatabricksQueryClient, query_databricks

def main():
    """Demonstrate various usage patterns of the Databricks utility."""
    
    print("📚 Databricks Query Utility - Example Usage")
    print("=" * 55)
    
    # Example 1: Simple convenience function usage
    print("\n🔹 Example 1: Simple Query with Convenience Function")
    print("-" * 50)
    
    try:
        df = query_databricks(
            "SELECT 'Hello' as greeting, current_timestamp() as time",
            query_name="Greeting Query",
            debug=False
        )
        print(f"✅ Query returned {len(df)} row(s)")
        print(df)
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Example 2: Client instance for multiple queries
    print("\n🔹 Example 2: Client Instance for Multiple Queries")
    print("-" * 50)
    
    try:
        client = DatabricksQueryClient(debug=False)
        
        # Query 1: Basic test
        df1 = client.execute_query(
            "SELECT 1 as number, 'test' as text",
            "Basic Test"
        )
        print(f"✅ Basic test: {len(df1)} row(s)")
        
        # Query 2: Steve's WPS data (sample)
        wps_query = '''
        select device_id, product_seen_first, device_manufacturer_name
        from wps.profile_by_device 
        where product_seen_first > '2025-08-16' 
          and product_seen_first <= '2025-08-18'
        limit 3
        '''
        
        df2 = client.execute_query(wps_query, "WPS Sample")
        print(f"✅ WPS sample: {len(df2)} row(s)")
        print("Sample WPS data:")
        print(df2)
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Example 3: Safety demonstrations
    print("\n🔹 Example 3: Safety Check Demonstrations")
    print("-" * 50)
    
    dangerous_queries = [
        "DROP TABLE users",
        "INSERT INTO table VALUES (1,2,3)",
        "UPDATE users SET password = 'hacked'",
        "DELETE FROM important_data"
    ]
    
    client = DatabricksQueryClient()
    
    for query in dangerous_queries:
        try:
            client.execute_query(query, "Dangerous Query Test")
            print(f"❌ FAILED: '{query}' was not blocked!")
        except ValueError as e:
            print(f"✅ BLOCKED: '{query}' - {e}")
        except Exception as e:
            print(f"⚠️  UNEXPECTED: '{query}' - {e}")
    
    # Example 4: Connection testing
    print("\n🔹 Example 4: Connection Testing")
    print("-" * 50)
    
    try:
        from databricks_query import test_databricks_connection
        
        if test_databricks_connection():
            print("✅ Connection test successful")
        else:
            print("❌ Connection test failed")
            
    except Exception as e:
        print(f"❌ Connection test error: {e}")
    
    # Example 5: Error handling patterns
    print("\n🔹 Example 5: Error Handling Patterns")
    print("-" * 50)
    
    client = DatabricksQueryClient()
    
    # Test non-existent table
    try:
        df = client.execute_query(
            "SELECT * FROM definitely_does_not_exist_table",
            "Non-existent Table Test"
        )
        print(f"Unexpected success: {len(df)} rows")
    except RuntimeError as e:
        print(f"✅ Properly caught runtime error: {str(e)[:100]}...")
    except Exception as e:
        print(f"⚠️  Unexpected error type: {type(e).__name__}: {e}")
    
    print("\n" + "=" * 55)
    print("📚 Example usage complete!")
    print("\n💡 Tips:")
    print("   - Use debug=True for troubleshooting")
    print("   - Always handle RuntimeError for API failures")
    print("   - Use ValueError for safety check violations")
    print("   - The utility automatically finds your .env file")

if __name__ == "__main__":
    main()