#!/usr/bin/env python3
"""
Simple script to query the Azure SQL database to verify data loaded correctly.
Uses the same connection string from the etl_kaggle_to_azuresql.py script.
"""

import os
import sys
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get connection string from environment variables
AZURE_SQL_CONN_STR = os.getenv("AZURE_SQL_CONN_STR")
ODBC_CONN_STR = os.getenv("ODBC_CONN_STR")
TABLE_NAME = os.getenv("TABLE_NAME", "dbo.airline_delay_causes")

def build_engine():
    """Build a SQLAlchemy engine using the connection string from environment variables."""
    if AZURE_SQL_CONN_STR:
        print("[INFO] Using AZURE_SQL_CONN_STR")
        return create_engine(AZURE_SQL_CONN_STR)
    if not ODBC_CONN_STR:
        raise RuntimeError("Provide AZURE_SQL_CONN_STR or ODBC_CONN_STR in .env")
    print("[INFO] Using ODBC_CONN_STR via odbc_connect")
    params = urllib.parse.quote_plus(ODBC_CONN_STR)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def run_query(query, engine, params=None):
    """Execute a SQL query and return the results as a pandas DataFrame."""
    print(f"[INFO] Executing query: {query}")
    try:
        return pd.read_sql(query, engine, params=params)
    except Exception as e:
        print(f"[ERROR] Query execution failed: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    """Main function to run queries against the database."""
    engine = build_engine()
    
    # Get count of rows in the table
    count_query = f"SELECT COUNT(*) AS row_count FROM {TABLE_NAME}"
    count_df = run_query(count_query, engine)
    print(f"[INFO] Total rows in {TABLE_NAME}: {count_df.iloc[0]['row_count']:,}")
    
    # Get the column names
    schema_query = f"SELECT TOP 1 * FROM {TABLE_NAME}"
    schema_df = run_query(schema_query, engine)
    print("\n[INFO] Column names in the table:")
    for column in schema_df.columns:
        print(f"  - {column}")
    
    # Get the first 10 rows to verify data
    sample_query = f"SELECT TOP 10 * FROM {TABLE_NAME}"
    sample_df = run_query(sample_query, engine)
    print("\n[INFO] Sample of data:")
    print(sample_df)
    
    # Get some basic statistics for arrival delay
    # Using 'arrdelay' instead of 'arr_delay' based on the actual column name
    stats_query = f"""
    SELECT 
        AVG(arrdelay) AS avg_arrival_delay,
        MAX(arrdelay) AS max_arrival_delay,
        MIN(arrdelay) AS min_arrival_delay,
        COUNT(DISTINCT origin) AS unique_origins,
        COUNT(DISTINCT dest) AS unique_destinations
    FROM {TABLE_NAME}
    """
    stats_df = run_query(stats_query, engine)
    print("\n[INFO] Basic statistics:")
    print(stats_df)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
