#!/usr/bin/env python3
"""
01 - Initial Dataset Exploration
=================================

This script performs the initial exploration of the NGM (Next Generation McAfee) 
renewal dataset to understand its structure, size, and key characteristics.

Based on the SQL file, this appears to be a renewal investigation dataset 
focusing on subscription renewals, expirations, and customer segments.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Add utils to path for importing - use absolute path to avoid issues
import os
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
utils_path = os.path.join(project_root, 'utils')
sys.path.insert(0, utils_path)

from databricks_query import DatabricksQueryClient, query_databricks

def main():
    """Explore the dataset structure and basic statistics."""
    
    print("🔍 Dataset Initial Exploration")
    print("=" * 50)
    
    # Initialize client
    try:
        client = DatabricksQueryClient(debug=True)
        print("✅ Connected to Databricks")
    except Exception as e:
        print(f"❌ Failed to connect to Databricks: {e}")
        return
    
    # First, let's check if the table exists and get basic info
    table_name = "users.kartikey_chauhan.nar_renewal_investigation_2025_q3_mpd_ret_devices_ngm_v7"
    
    print(f"\n📊 Exploring table: {table_name}")
    print("-" * 70)
    
    # 1. Check table structure by getting first row and examining columns
    print("\n1️⃣ Getting table schema by examining first row...")
    first_row_query = f"""
    SELECT *
    FROM {table_name}
    LIMIT 1
    """
    
    try:
        first_row = client.execute_query(first_row_query, "First Row for Schema")
        print(f"✅ Table has {len(first_row.columns)} columns")
        print("\nColumn Names:")
        for i, col in enumerate(first_row.columns, 1):
            print(f"  {i:2d}. {col}")
    except Exception as e:
        print(f"❌ Error getting schema: {e}")
        return
    
    # 2. Get basic row count and date ranges
    print("\n2️⃣ Getting basic statistics...")
    basic_stats_query = f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT 9M_expiry_transaction_id) as unique_expiry_transactions,
        COUNT(DISTINCT renewed_transaction_id) as unique_renewal_transactions,
        MIN(9M_subscription_expiry_datetime) as earliest_expiry,
        MAX(9M_subscription_expiry_datetime) as latest_expiry,
        MIN(expiration_week) as earliest_expiration_week,
        MAX(expiration_week) as latest_expiration_week
    FROM {table_name}
    """
    
    try:
        basic_stats = client.execute_query(basic_stats_query, "Basic Statistics")
        print("\nDataset Overview:")
        for col in basic_stats.columns:
            print(f"  {col}: {basic_stats[col].iloc[0]}")
    except Exception as e:
        print(f"❌ Error getting basic stats: {e}")
        return
    
    # 3. Sample data preview
    print("\n3️⃣ Sample data preview...")
    sample_query = f"""
    SELECT *
    FROM {table_name}
    LIMIT 5
    """
    
    try:
        sample_data = client.execute_query(sample_query, "Sample Data")
        print(f"\nSample rows (showing first 10 columns):")
        print(sample_data.iloc[:, :10].to_string(index=False))
    except Exception as e:
        print(f"❌ Error getting sample data: {e}")
        return
    
    # 4. Check key dimensions
    print("\n4️⃣ Exploring key dimensions...")
    dimensions_query = f"""
    SELECT 
        COUNT(DISTINCT customer_type) as unique_customer_types,
        COUNT(DISTINCT subscription_renewed_geo) as unique_geos,
        COUNT(DISTINCT subscription_renewed_country) as unique_countries,
        COUNT(DISTINCT subscription_renewed_channel) as unique_channels,
        COUNT(DISTINCT renewal_transition_type) as unique_transition_types,
        COUNT(DISTINCT 9M_subscription_expiry_package_name) as unique_expiry_packages,
        COUNT(DISTINCT subscription_renewed_package_name) as unique_renewal_packages
    FROM {table_name}
    """
    
    try:
        dimensions = client.execute_query(dimensions_query, "Key Dimensions")
        print("\nDimensional Cardinality:")
        for col in dimensions.columns:
            print(f"  {col}: {dimensions[col].iloc[0]}")
    except Exception as e:
        print(f"❌ Error getting dimensions: {e}")
        return
    
    # 5. Customer type breakdown
    print("\n5️⃣ Customer type distribution...")
    customer_type_query = f"""
    SELECT 
        customer_type,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM {table_name}
    GROUP BY customer_type
    ORDER BY count DESC
    """
    
    try:
        customer_types = client.execute_query(customer_type_query, "Customer Types")
        print("\nCustomer Type Distribution:")
        print(customer_types.to_string(index=False))
    except Exception as e:
        print(f"❌ Error getting customer types: {e}")
        return
    
    print("\n✅ Initial exploration completed!")
    print("\n🎯 Key Insights from Initial Exploration:")
    print("   - Dataset contains subscription renewal and expiry transaction data")
    print("   - Multiple customer types and geographical dimensions available")
    print("   - Time-series data with expiry weeks and renewal tracking")
    print("   - Package transition analysis possible with expiry/renewal package fields")

if __name__ == "__main__":
    main()
