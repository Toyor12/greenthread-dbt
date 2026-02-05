"""
Transform Lambda Function - Version 2.0 (with RDS Integration and CDC)
========================================================================

Scheduled to run daily at 00:00 UTC via EventBridge.

Workflow:
1. Read raw data from S3
2. Apply transformations (currency conversion, filtering)
3. Save processed data to S3 (existing functionality)
4. Execute CDC batch processing in RDS (NEW)
5. Save summary to RDS fact tables (NEW)

Environment Variables Required:
- BUCKET: S3 bucket name (e.g., "books-etl-bucket")
- DB_SECRET_NAME: AWS Secrets Manager secret name (e.g., "books-etl/rds/credentials")
- DB_POOL_MIN_SIZE: Minimum connections in pool (default: 0)
- DB_POOL_MAX_SIZE: Maximum connections in pool (default: 2)

Dependencies:
- boto3
- psycopg2-binary (for RDS PostgreSQL connectivity)

VPC Configuration Required:
- Lambda must be attached to VPC private subnets
- Security group must allow outbound to RDS (port 5432)
- S3 access via VPC Gateway Endpoint (no NAT costs)
"""

import json
import os
import boto3
from datetime import datetime
from decimal import Decimal

# Import database utilities
from db_utils import (
    get_db_connection,
    return_db_connection,
    generate_batch_id,
    test_connection
)

# Initialize AWS clients
s3 = boto3.client("s3")

# Configuration from environment variables
BUCKET = os.environ["BUCKET"]


def read_from_s3(run_date):
    """
    Read raw book data from S3.

    Args:
        run_date: Date string in YYYY-MM-DD format

    Returns:
        list: List of book dictionaries from raw S3 folder

    Raises:
        Exception: If S3 read fails or file not found
    """
    raw_key = f"raw/books/date={run_date}/books.json"

    print(f"Reading raw data from S3: s3://{BUCKET}/{raw_key}")

    try:
        response = s3.get_object(Bucket=BUCKET, Key=raw_key)
        raw_data = json.loads(response["Body"].read())

        print(f"Successfully retrieved {len(raw_data)} raw records from S3")
        return raw_data

    except s3.exceptions.NoSuchKey:
        print(f"ERROR: Raw data not found at s3://{BUCKET}/{raw_key}")
        print("Ensure Extract Lambda ran successfully before Transform Lambda")
        raise Exception(f"Raw data file not found: {raw_key}")

    except Exception as e:
        print(f"Error reading from S3: {str(e)}")
        raise


def transform_data(raw_data):
    """
    Apply transformations to raw book data.

    Transformations:
    - Parse GBP price to float
    - Calculate USD and EUR prices using exchange rates
    - Filter to in-stock books only
    - Add processed timestamp

    Args:
        raw_data: List of raw book dictionaries from S3

    Returns:
        tuple: (processed_books, summary_stats)
    """
    print(f"Transforming {len(raw_data)} raw records...")

    processed = []
    total_value_gbp = 0
    in_stock_count = 0
    out_of_stock_count = 0

    for book in raw_data:
        try:
            # Extract price (remove £ symbol and encoded variants)
            price_str = book["price"].replace("Â£", "").replace("£", "").strip()
            price_gbp = float(price_str)

            # Check availability
            is_in_stock = "In stock" in book["availability"]

            if is_in_stock:
                in_stock_count += 1
                total_value_gbp += price_gbp

                # Create transformed record
                processed_book = {
                    "title": book["title"],
                    "price_gbp": round(price_gbp, 2),
                    "price_usd": round(price_gbp * 1.27, 2),  # GBP to USD
                    "price_eur": round(price_gbp * 1.17, 2),  # GBP to EUR
                    "in_stock": True,
                    "availability": book["availability"],
                    "processed_at": datetime.utcnow().isoformat()
                }
                processed.append(processed_book)
            else:
                out_of_stock_count += 1

        except Exception as e:
            print(f"Warning: Error transforming book '{book.get('title', 'Unknown')}': {str(e)}")
            continue

    # Calculate summary statistics
    summary = {
        "date": datetime.utcnow().strftime("%Y-%m-%d"),
        "total_books_raw": len(raw_data),
        "books_in_stock": in_stock_count,
        "books_out_of_stock": out_of_stock_count,
        "total_inventory_value_gbp": round(total_value_gbp, 2),
        "total_inventory_value_usd": round(total_value_gbp * 1.27, 2),
        "average_price_gbp": (
            round(total_value_gbp / in_stock_count, 2) if in_stock_count > 0 else 0
        ),
        "processed_at": datetime.utcnow().isoformat()
    }

    print(f"Transformation complete: {len(processed)} processed records")
    print(f"  In stock: {in_stock_count}")
    print(f"  Out of stock: {out_of_stock_count}")
    print(f"  Total value: £{summary['total_inventory_value_gbp']}")

    return processed, summary


def save_processed_to_s3(processed, summary, run_date):
    """
    Save processed data and summary to S3.

    Args:
        processed: List of processed book dictionaries
        summary: Summary statistics dictionary
        run_date: Date string in YYYY-MM-DD format

    Returns:
        tuple: (processed_key, summary_key)

    Raises:
        Exception: If S3 upload fails
    """
    # Save processed books
    processed_key = f"processed/books/date={run_date}/books.json"

    print(f"Saving {len(processed)} processed records to S3...")

    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=processed_key,
            Body=json.dumps(processed, indent=2),
            ContentType="application/json"
        )
        print(f"✓ Saved processed data: s3://{BUCKET}/{processed_key}")

        # Save summary
        summary_key = f"processed/summary/date={run_date}/summary.json"

        s3.put_object(
            Bucket=BUCKET,
            Key=summary_key,
            Body=json.dumps(summary, indent=2),
            ContentType="application/json"
        )
        print(f"✓ Saved summary: s3://{BUCKET}/{summary_key}")

        return processed_key, summary_key

    except Exception as e:
        print(f"Error saving to S3: {str(e)}")
        raise


def execute_cdc_batch_processing(batch_id, run_date):
    """
    Execute CDC batch processing in RDS.

    Calls the process_cdc_batch() stored procedure which:
    - Detects new books, price changes, stock changes, removed books
    - Updates SCD Type 2 records (books table)
    - Logs all changes to cdc_events table
    - Populates fact_daily_prices and fact_daily_summary

    Args:
        batch_id: Batch identifier (from Extract Lambda)
        run_date: Date string in YYYY-MM-DD format

    Returns:
        dict: CDC processing results
        {
            "new_books": 5,
            "removed_books": 0,
            "price_changes": 3,
            "stock_changes": 2,
            "total_processed": 20
        }

    Raises:
        Exception: If CDC processing fails
    """
    conn = None

    try:
        print(f"Executing CDC batch processing for batch: {batch_id}")

        conn = get_db_connection()
        cursor = conn.cursor()

        # Call the CDC stored procedure
        cursor.execute(
            "SELECT * FROM process_cdc_batch(%s, %s)",
            (batch_id, run_date)
        )

        # Fetch results
        result = cursor.fetchone()

        if result:
            new_books, removed_books, price_changes, stock_changes, total_processed = result

            cdc_summary = {
                "new_books": new_books,
                "removed_books": removed_books,
                "price_changes": price_changes,
                "stock_changes": stock_changes,
                "total_processed": total_processed
            }

            conn.commit()

            print(f"✓ CDC processing complete:")
            print(f"  New books: {new_books}")
            print(f"  Removed books: {removed_books}")
            print(f"  Price changes: {price_changes}")
            print(f"  Stock changes: {stock_changes}")
            print(f"  Total processed: {total_processed}")

            return cdc_summary
        else:
            raise Exception("CDC stored procedure returned no results")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error executing CDC batch processing: {str(e)}")
        raise

    finally:
        if conn:
            return_db_connection(conn)


def get_extract_batch_id(run_date):
    """
    Get the batch_id from staging_books for today's extract.

    The Extract Lambda creates a batch_id when inserting into staging_books.
    We need to retrieve it to process the CDC batch.

    Args:
        run_date: Date string in YYYY-MM-DD format

    Returns:
        str: Batch ID (e.g., "extract_2026-02-03_080512")

    Raises:
        Exception: If batch_id not found
    """
    conn = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Find the batch_id for today's scraped data
        cursor.execute(
            """
            SELECT DISTINCT batch_id
            FROM staging_books
            WHERE scraped_date = %s
            AND processed = FALSE
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (run_date,)
        )

        result = cursor.fetchone()

        if result:
            batch_id = result[0]
            print(f"Found batch_id for {run_date}: {batch_id}")
            return batch_id
        else:
            raise Exception(f"No unprocessed batch found for date: {run_date}")

    except Exception as e:
        print(f"Error retrieving batch_id: {str(e)}")
        raise

    finally:
        if conn:
            return_db_connection(conn)


def lambda_handler(event, context):
    """
    Main Lambda handler for Transform function.

    Workflow:
    1. Read raw data from S3
    2. Apply transformations
    3. Save processed data to S3 (data lake)
    4. Execute CDC processing in RDS (detect changes)
    5. Fact tables populated automatically by CDC procedure

    Args:
        event: Lambda event object (from EventBridge scheduler)
        context: Lambda context object

    Returns:
        dict: Execution summary with status and metrics
    """
    start_time = datetime.utcnow()
    run_date = start_time.strftime("%Y-%m-%d")

    print("=" * 60)
    print(f"Transform Lambda - Starting execution")
    print(f"Run Date: {run_date}")
    print("=" * 60)

    try:
        # Test database connection first
        print("\n[1/6] Testing database connection...")
        if not test_connection():
            raise Exception("Database connection test failed")
        print("✓ Database connection OK")

        # Step 1: Read raw data from S3
        print("\n[2/6] Reading raw data from S3...")
        raw_data = read_from_s3(run_date)
        print(f"✓ Read {len(raw_data)} raw records")

        # Step 2: Transform data
        print("\n[3/6] Transforming data...")
        processed, summary = transform_data(raw_data)
        print(f"✓ Transformed {len(processed)} records")

        # Step 3: Save to S3
        print("\n[4/6] Saving processed data to S3...")
        processed_key, summary_key = save_processed_to_s3(processed, summary, run_date)
        print("✓ Saved to S3")

        # Step 4: Get batch_id from Extract Lambda
        print("\n[5/6] Retrieving batch_id from staging table...")
        batch_id = get_extract_batch_id(run_date)
        print(f"✓ Batch ID: {batch_id}")

        # Step 5: Execute CDC processing (NEW)
        print("\n[6/6] Executing CDC batch processing in RDS...")
        cdc_summary = execute_cdc_batch_processing(batch_id, run_date)
        print("✓ CDC processing complete")

        # Calculate execution duration
        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()

        # Success response
        result = {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "run_date": run_date,
                "batch_id": batch_id,
                "processed_records": len(processed),
                "summary": summary,
                "cdc_summary": cdc_summary,
                "s3_keys": {
                    "processed": processed_key,
                    "summary": summary_key
                },
                "execution_time_seconds": round(duration_seconds, 2),
                "timestamp": end_time.isoformat()
            }, indent=2)
        }

        print("\n" + "=" * 60)
        print("Transform Lambda - Execution completed successfully")
        print(f"Duration: {duration_seconds:.2f} seconds")
        print(f"CDC Changes: {cdc_summary['new_books']} new, "
              f"{cdc_summary['price_changes']} price changes, "
              f"{cdc_summary['stock_changes']} stock changes")
        print("=" * 60)

        return result

    except Exception as e:
        # Error handling
        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()

        error_result = {
            "statusCode": 500,
            "body": json.dumps({
                "status": "error",
                "run_date": run_date,
                "error_message": str(e),
                "error_type": type(e).__name__,
                "execution_time_seconds": round(duration_seconds, 2),
                "timestamp": end_time.isoformat()
            }, indent=2)
        }

        print("\n" + "=" * 60)
        print("Transform Lambda - Execution FAILED")
        print(f"Error: {str(e)}")
        print(f"Duration: {duration_seconds:.2f} seconds")
        print("=" * 60)

        return error_result


# For local testing
if __name__ == "__main__":
    """
    Local testing mode.

    Usage:
        export BUCKET="books-etl-bucket"
        export DB_SECRET_NAME="books-etl/rds/credentials"
        export DB_POOL_MIN_SIZE="0"
        export DB_POOL_MAX_SIZE="2"
        python transform_lambda_v2.py
    """
    print("Running Transform Lambda in local test mode...")

    # Mock event and context
    test_event = {}
    test_context = type('obj', (object,), {
        'function_name': 'transform-books-lambda-test',
        'memory_limit_in_mb': 512,
        'invoked_function_arn': 'arn:aws:lambda:local:test',
        'aws_request_id': 'test-request-id'
    })

    # Execute
    result = lambda_handler(test_event, test_context)

    print("\n" + "=" * 60)
    print("Test Result:")
    print(result['body'] if 'body' in result else json.dumps(result, indent=2))
    print("=" * 60)
