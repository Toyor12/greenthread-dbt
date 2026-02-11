"""
Extract Lambda Function - Version 2.0 (with RDS Integration)
=============================================================

Scheduled to run daily at 08:00 UTC via EventBridge.

Workflow:
1. Scrape books.toscrape.com for book data
2. Save raw JSON to S3 (existing functionality)
3. Insert raw data into RDS staging_books table (NEW)

Environment Variables Required:
- BUCKET: S3 bucket name (e.g., "books-etl-bucket")
- DB_SECRET_NAME: AWS Secrets Manager secret name (e.g., "books-etl/rds/credentials")
- DB_POOL_MIN_SIZE: Minimum connections in pool (default: 0)
- DB_POOL_MAX_SIZE: Maximum connections in pool (default: 2)

Dependencies:
- requests
- beautifulsoup4
- boto3
- psycopg2-binary (for RDS PostgreSQL connectivity)

VPC Configuration Required:
- Lambda must be attached to VPC private subnets
- Security group must allow outbound to RDS (port 5432)
- NAT Gateway required for internet access (web scraping)
"""

import json
import os
import requests
import boto3
from bs4 import BeautifulSoup
from datetime import datetime

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
BASE_URL = "https://books.toscrape.com/"


def scrape_books():
    """
    Scrape book data from books.toscrape.com.

    Returns:
        list: List of dictionaries with book data
        [
            {
                "title": "A Light in the Attic",
                "price": "£51.77",
                "availability": "In stock (22 available)"
            },
            ...
        ]

    Raises:
        Exception: If web scraping fails
    """
    print(f"Scraping books from: {BASE_URL}")

    try:
        response = requests.get(BASE_URL, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        books = []

        for book in soup.select(".product_pod"):
            title = book.h3.a["title"]
            price = book.select_one(".price_color").text
            availability = book.select_one(".availability").text.strip()

            books.append({
                "title": title,
                "price": price,
                "availability": availability
            })

        print(f"Successfully scraped {len(books)} books")
        return books

    except Exception as e:
        print(f"Error scraping books: {str(e)}")
        raise


def save_to_s3(books, run_date):
    """
    Save raw book data to S3.

    Args:
        books: List of book dictionaries
        run_date: Date string in YYYY-MM-DD format

    Returns:
        str: S3 key where data was saved

    Raises:
        Exception: If S3 upload fails
    """
    key = f"raw/books/date={run_date}/books.json"

    print(f"Saving {len(books)} books to S3: s3://{BUCKET}/{key}")

    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(books, indent=2),
            ContentType="application/json"
        )

        print(f"Successfully saved to S3: {key}")
        return key

    except Exception as e:
        print(f"Error saving to S3: {str(e)}")
        raise


def parse_price(price_str):
    """
    Parse GBP price string to float.

    Handles variations like "£51.77" and "Â£51.77" (encoded pound symbol).

    Args:
        price_str: Price string from website (e.g., "£51.77")

    Returns:
        float: Price as decimal number

    Raises:
        ValueError: If price cannot be parsed
    """
    try:
        # Remove £ symbol and any encoded variants (Â£)
        cleaned = price_str.replace("£", "").replace("Â", "").strip()
        return float(cleaned)
    except ValueError as e:
        print(f"Error parsing price '{price_str}': {str(e)}")
        raise


def save_to_rds(books, run_date, s3_key, batch_id):
    """
    Save raw book data to RDS staging_books table.

    This data will be processed later by the CDC batch processor
    (triggered by Transform Lambda).

    Args:
        books: List of book dictionaries
        run_date: Date string in YYYY-MM-DD format
        s3_key: S3 key where raw data was saved
        batch_id: Unique batch identifier

    Returns:
        int: Number of records inserted

    Raises:
        Exception: If database insert fails
    """
    conn = None

    try:
        print(f"Inserting {len(books)} books into RDS staging table...")
        print(f"Batch ID: {batch_id}")

        conn = get_db_connection()
        cursor = conn.cursor()

        # Prepare batch insert data
        insert_query = """
            INSERT INTO staging_books
            (title, price_gbp, availability, is_in_stock, scraped_date, batch_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        batch_data = []
        for book in books:
            try:
                price_gbp = parse_price(book["price"])
                is_in_stock = "In stock" in book["availability"]

                batch_data.append((
                    book["title"],
                    price_gbp,
                    book["availability"],
                    is_in_stock,
                    run_date,
                    batch_id
                ))
            except Exception as e:
                print(f"Warning: Skipping book due to error: {book.get('title', 'Unknown')} - {str(e)}")
                continue

        # Execute batch insert
        cursor.executemany(insert_query, batch_data)
        conn.commit()

        inserted_count = len(batch_data)
        print(f"Successfully inserted {inserted_count} records into staging_books")

        return inserted_count

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error inserting into RDS: {str(e)}")
        raise

    finally:
        if conn:
            return_db_connection(conn)


def lambda_handler(event, context):
    """
    Main Lambda handler for Extract function.

    Workflow:
    1. Scrape website
    2. Save to S3 (data lake)
    3. Insert to RDS (structured database)

    Args:
        event: Lambda event object (from EventBridge scheduler)
        context: Lambda context object

    Returns:
        dict: Execution summary with status and metrics
    """
    start_time = datetime.utcnow()
    run_date = start_time.strftime("%Y-%m-%d")
    batch_id = generate_batch_id("extract")

    print("=" * 60)
    print(f"Extract Lambda - Starting execution")
    print(f"Run Date: {run_date}")
    print(f"Batch ID: {batch_id}")
    print("=" * 60)

    try:
        # Test database connection first (early failure detection)
        print("\n[1/4] Testing database connection...")
        if not test_connection():
            raise Exception("Database connection test failed")
        print("✓ Database connection OK")

        # Step 1: Scrape website
        print("\n[2/4] Scraping books from website...")
        books = scrape_books()
        print(f"✓ Scraped {len(books)} books")

        if len(books) == 0:
            raise Exception("No books scraped - possible website structure change")

        # Step 2: Save to S3 (existing functionality)
        print("\n[3/4] Saving raw data to S3...")
        s3_key = save_to_s3(books, run_date)
        print(f"✓ Saved to S3: {s3_key}")

        # Step 3: Insert to RDS (NEW functionality)
        print("\n[4/4] Inserting data into RDS staging table...")
        db_records = save_to_rds(books, run_date, s3_key, batch_id)
        print(f"✓ Inserted {db_records} records into RDS")

        # Calculate execution duration
        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()

        # Success response
        result = {
            "status": "success",
            "run_date": run_date,
            "batch_id": batch_id,
            "records_scraped": len(books),
            "records_inserted_rds": db_records,
            "s3_key": s3_key,
            "execution_time_seconds": round(duration_seconds, 2),
            "timestamp": end_time.isoformat()
        }

        print("\n" + "=" * 60)
        print("Extract Lambda - Execution completed successfully")
        print(f"Duration: {duration_seconds:.2f} seconds")
        print(f"Records: {len(books)} scraped, {db_records} inserted to RDS")
        print("=" * 60)

        return result

    except Exception as e:
        # Error handling
        end_time = datetime.utcnow()
        duration_seconds = (end_time - start_time).total_seconds()

        error_result = {
            "status": "error",
            "run_date": run_date,
            "batch_id": batch_id,
            "error_message": str(e),
            "error_type": type(e).__name__,
            "execution_time_seconds": round(duration_seconds, 2),
            "timestamp": end_time.isoformat()
        }

        print("\n" + "=" * 60)
        print("Extract Lambda - Execution FAILED")
        print(f"Error: {str(e)}")
        print(f"Duration: {duration_seconds:.2f} seconds")
        print("=" * 60)

        # Re-raise exception to mark Lambda as failed
        raise


# For local testing
if __name__ == "__main__":
    """
    Local testing mode.

    Usage:
        export BUCKET="books-etl-bucket"
        export DB_SECRET_NAME="books-etl/rds/credentials"
        export DB_POOL_MIN_SIZE="0"
        export DB_POOL_MAX_SIZE="2"
        python extract_lambda_v2.py
    """
    print("Running Extract Lambda in local test mode...")

    # Mock event and context
    test_event = {}
    test_context = type('obj', (object,), {
        'function_name': 'extract-books-lambda-test',
        'memory_limit_in_mb': 768,
        'invoked_function_arn': 'arn:aws:lambda:local:test',
        'aws_request_id': 'test-request-id'
    })

    # Execute
    result = lambda_handler(test_event, test_context)

    print("\n" + "=" * 60)
    print("Test Result:")
    print(json.dumps(result, indent=2))
    print("=" * 60)
