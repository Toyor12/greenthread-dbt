"""
Database Utilities Module
==========================

Provides database connection pooling, secret management, and utility functions
for the Books ETL Lambda functions.

Features:
- Connection pooling with psycopg2.pool.SimpleConnectionPool
- AWS Secrets Manager integration with caching
- Automatic connection cleanup on Lambda shutdown
- Configurable pool size via environment variables

Environment Variables Required:
- DB_SECRET_NAME: AWS Secrets Manager secret name (e.g., "books-etl/rds/credentials")
- DB_POOL_MIN_SIZE: Minimum connections in pool (default: 0)
- DB_POOL_MAX_SIZE: Maximum connections in pool (default: 2)

Usage:
    from db_utils import get_db_connection, return_db_connection

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM books LIMIT 10")
        rows = cursor.fetchall()
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            return_db_connection(conn)
"""

import json
import os
import boto3
import psycopg2
from psycopg2 import pool
from typing import Dict, Any, Optional

# Global connection pool (persists across warm Lambda invocations)
connection_pool: Optional[pool.SimpleConnectionPool] = None

# Secrets cache to avoid repeated API calls
secrets_cache: Dict[str, Dict[str, Any]] = {}


def get_secret(secret_name: str) -> Dict[str, Any]:
    """
    Retrieve database credentials from AWS Secrets Manager with caching.

    Caches secrets in memory to avoid repeated API calls during warm Lambda
    invocations. Secrets Manager charges $0.05 per 10,000 API calls.

    Args:
        secret_name: The name of the secret in Secrets Manager

    Returns:
        Dictionary containing secret key-value pairs:
        {
            "username": "etl_admin",
            "password": "...",
            "host": "books-etl-db.xxx.eu-central-1.rds.amazonaws.com",
            "port": 5432,
            "dbname": "books_etl"
        }

    Raises:
        Exception: If secret retrieval fails
    """
    # Return cached secret if available
    if secret_name in secrets_cache:
        print(f"Using cached secret: {secret_name}")
        return secrets_cache[secret_name]

    print(f"Retrieving secret from Secrets Manager: {secret_name}")

    try:
        client = boto3.client('secretsmanager')
        response = client.get_secret_value(SecretId=secret_name)

        # Parse secret string
        if 'SecretString' in response:
            secret = json.loads(response['SecretString'])
        else:
            # Binary secrets not expected for DB credentials
            raise ValueError("Secret is in binary format, expected JSON string")

        # Cache the secret
        secrets_cache[secret_name] = secret

        print(f"Successfully retrieved secret: {secret_name}")
        return secret

    except Exception as e:
        print(f"Error retrieving secret {secret_name}: {str(e)}")
        raise


def init_connection_pool() -> pool.SimpleConnectionPool:
    """
    Initialize the database connection pool.

    Creates a SimpleConnectionPool with configuration from environment variables.
    The pool is stored globally and reused across warm Lambda invocations.

    Connection Pool Benefits:
    - Reduces cold start overhead (don't create new connection each invocation)
    - Prevents connection exhaustion on RDS
    - Automatically handles connection cleanup

    Returns:
        SimpleConnectionPool instance

    Raises:
        Exception: If pool creation fails
    """
    # Get DB credentials from Secrets Manager
    secret_name = os.environ.get('DB_SECRET_NAME')
    if not secret_name:
        raise ValueError("DB_SECRET_NAME environment variable not set")

    db_config = get_secret(secret_name)

    # Validate required fields
    required_fields = ['host', 'port', 'dbname', 'username', 'password']
    missing_fields = [field for field in required_fields if field not in db_config]
    if missing_fields:
        raise ValueError(f"Missing required fields in secret: {', '.join(missing_fields)}")

    # Get pool size configuration from environment
    min_size = int(os.environ.get('DB_POOL_MIN_SIZE', '0'))
    max_size = int(os.environ.get('DB_POOL_MAX_SIZE', '2'))

    print(f"Initializing connection pool: min={min_size}, max={max_size}")
    print(f"Connecting to: {db_config['host']}:{db_config['port']}/{db_config['dbname']}")

    try:
        # Create connection pool
        pool_instance = psycopg2.pool.SimpleConnectionPool(
            minconn=min_size,
            maxconn=max_size,
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['dbname'],
            user=db_config['username'],
            password=db_config['password'],
            connect_timeout=10,  # 10 second connection timeout
            options='-c statement_timeout=30000'  # 30 second query timeout
        )

        print("Connection pool initialized successfully")
        return pool_instance

    except Exception as e:
        print(f"Error initializing connection pool: {str(e)}")
        raise


def get_db_connection():
    """
    Get a database connection from the pool.

    Initializes the pool on first call (lazy initialization). Subsequent calls
    reuse the existing pool if Lambda container is warm.

    Returns:
        psycopg2 connection object

    Raises:
        Exception: If connection cannot be obtained

    Example:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            conn.commit()
        finally:
            return_db_connection(conn)
    """
    global connection_pool

    # Initialize pool if not already created
    if connection_pool is None:
        print("Connection pool not initialized, creating new pool...")
        connection_pool = init_connection_pool()

    try:
        # Get connection from pool
        conn = connection_pool.getconn()

        if conn is None:
            raise Exception("Failed to get connection from pool (pool exhausted)")

        print("Successfully obtained connection from pool")
        return conn

    except Exception as e:
        print(f"Error getting connection from pool: {str(e)}")
        raise


def return_db_connection(conn) -> None:
    """
    Return a connection to the pool.

    IMPORTANT: Always call this in a finally block to ensure connections
    are returned to the pool. Unreturned connections will exhaust the pool.

    Args:
        conn: psycopg2 connection object to return to pool

    Example:
        conn = get_db_connection()
        try:
            # Use connection
            pass
        finally:
            return_db_connection(conn)
    """
    global connection_pool

    if connection_pool and conn:
        try:
            connection_pool.putconn(conn)
            print("Connection returned to pool")
        except Exception as e:
            print(f"Error returning connection to pool: {str(e)}")
    else:
        if not connection_pool:
            print("Warning: Connection pool is None, cannot return connection")
        if not conn:
            print("Warning: Connection is None, nothing to return")


def close_all_connections() -> None:
    """
    Close all connections in the pool.

    This should be called when the Lambda container is shutting down.
    However, AWS Lambda does not provide a shutdown hook, so this is
    primarily for manual cleanup during testing.

    In production, connections will be closed automatically when the
    Lambda container is recycled.

    Note:
        Connection pools persist across warm invocations, so calling this
        during normal operation will impact performance negatively.
    """
    global connection_pool

    if connection_pool:
        try:
            connection_pool.closeall()
            connection_pool = None
            print("All connections closed and pool destroyed")
        except Exception as e:
            print(f"Error closing connections: {str(e)}")
    else:
        print("Connection pool is None, nothing to close")


def execute_query(query: str, params: tuple = None, fetch: bool = False):
    """
    Execute a SQL query with automatic connection management.

    Convenience function that handles connection acquisition, execution,
    commit/rollback, and connection return automatically.

    Args:
        query: SQL query string (use %s for parameters)
        params: Tuple of query parameters for parameterized queries
        fetch: If True, returns query results; if False, returns row count

    Returns:
        If fetch=True: List of tuples (query results)
        If fetch=False: Integer (number of rows affected)

    Raises:
        Exception: If query execution fails

    Example:
        # Insert data
        execute_query(
            "INSERT INTO staging_books (title, price_gbp, batch_id) VALUES (%s, %s, %s)",
            ("Book Title", 25.99, "batch_123")
        )

        # Fetch data
        results = execute_query(
            "SELECT * FROM books WHERE price_gbp > %s",
            (30.0,),
            fetch=True
        )
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Execute query with parameters
        cursor.execute(query, params)

        if fetch:
            results = cursor.fetchall()
            conn.commit()
            return results
        else:
            row_count = cursor.rowcount
            conn.commit()
            return row_count

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error executing query: {str(e)}")
        print(f"Query: {query}")
        print(f"Params: {params}")
        raise

    finally:
        if conn:
            return_db_connection(conn)


def test_connection() -> bool:
    """
    Test database connectivity.

    Executes a simple SELECT 1 query to verify database connection.
    Useful for Lambda warming and health checks.

    Returns:
        True if connection successful, False otherwise

    Example:
        if test_connection():
            print("Database connection OK")
        else:
            print("Database connection FAILED")
    """
    try:
        result = execute_query("SELECT 1", fetch=True)
        if result and result[0][0] == 1:
            print("Database connection test: PASSED")
            return True
        else:
            print("Database connection test: FAILED (unexpected result)")
            return False
    except Exception as e:
        print(f"Database connection test: FAILED ({str(e)})")
        return False


def get_connection_stats() -> Dict[str, int]:
    """
    Get connection pool statistics.

    Returns:
        Dictionary with pool statistics:
        {
            "min_size": 0,
            "max_size": 2,
            "available": 2,
            "in_use": 0
        }

    Note:
        This is a best-effort implementation as psycopg2.pool.SimpleConnectionPool
        doesn't expose detailed statistics. Returns None if pool not initialized.
    """
    global connection_pool

    if connection_pool is None:
        return {
            "min_size": 0,
            "max_size": 0,
            "available": 0,
            "in_use": 0,
            "status": "not_initialized"
        }

    # SimpleConnectionPool doesn't expose detailed stats
    # We can only report configuration
    return {
        "min_size": connection_pool.minconn,
        "max_size": connection_pool.maxconn,
        "status": "initialized"
    }


# ============================================================================
# CDC HELPER FUNCTIONS
# ============================================================================

def generate_batch_id(prefix: str = "batch") -> str:
    """
    Generate a unique batch ID for CDC processing.

    Args:
        prefix: Prefix for batch ID (default: "batch")

    Returns:
        Batch ID string in format: "prefix_YYYY-MM-DD_HHMMSS"

    Example:
        >>> generate_batch_id("extract")
        "extract_2026-02-03_143052"
    """
    from datetime import datetime
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
    return f"{prefix}_{timestamp}"


def log_execution(
    execution_date: str,
    lambda_function: str,
    status: str,
    records_processed: int = 0,
    error_message: str = None,
    execution_duration_ms: int = None,
    s3_keys: dict = None
) -> None:
    """
    Log ETL execution to database for monitoring.

    NOTE: This requires an etl_execution_log table (not in current schema).
    Uncomment the CREATE TABLE statement below if you want execution logging.

    Args:
        execution_date: Execution date (YYYY-MM-DD)
        lambda_function: Lambda function name
        status: Execution status ('success', 'failed', 'partial')
        records_processed: Number of records processed
        error_message: Error message if failed
        execution_duration_ms: Execution time in milliseconds
        s3_keys: Dictionary of S3 keys used
    """
    # Uncomment to enable execution logging
    # execute_query(
    #     """
    #     INSERT INTO etl_execution_log
    #     (execution_date, lambda_function, status, records_processed,
    #      error_message, execution_duration_ms, s3_keys)
    #     VALUES (%s, %s, %s, %s, %s, %s, %s)
    #     """,
    #     (execution_date, lambda_function, status, records_processed,
    #      error_message, execution_duration_ms, json.dumps(s3_keys) if s3_keys else None)
    # )
    pass


# ============================================================================
# EXAMPLE USAGE (for testing)
# ============================================================================

if __name__ == "__main__":
    """
    Example usage and testing.

    To test locally:
    1. Set environment variables:
       export DB_SECRET_NAME="books-etl/rds/credentials"
       export DB_POOL_MIN_SIZE="0"
       export DB_POOL_MAX_SIZE="2"

    2. Ensure AWS credentials are configured
    3. Run: python db_utils.py
    """
    import sys

    print("=" * 60)
    print("Database Utilities Module - Test Mode")
    print("=" * 60)

    # Check required environment variables
    if not os.environ.get('DB_SECRET_NAME'):
        print("ERROR: DB_SECRET_NAME environment variable not set")
        print("Please set: export DB_SECRET_NAME='books-etl/rds/credentials'")
        sys.exit(1)

    # Test connection
    print("\n1. Testing database connection...")
    if test_connection():
        print("   ✓ Connection test PASSED")
    else:
        print("   ✗ Connection test FAILED")
        sys.exit(1)

    # Get pool stats
    print("\n2. Connection pool statistics:")
    stats = get_connection_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")

    # Test execute_query
    print("\n3. Testing execute_query (fetch)...")
    try:
        results = execute_query(
            "SELECT COUNT(*) FROM books WHERE is_current = TRUE",
            fetch=True
        )
        print(f"   Current books count: {results[0][0]}")
        print("   ✓ Query execution PASSED")
    except Exception as e:
        print(f"   ✗ Query execution FAILED: {e}")

    # Generate batch ID
    print("\n4. Testing batch ID generation...")
    batch_id = generate_batch_id("test")
    print(f"   Generated batch ID: {batch_id}")

    print("\n" + "=" * 60)
    print("All tests completed!")
    print("=" * 60)
