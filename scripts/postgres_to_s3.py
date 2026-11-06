
import csv
import os
import tempfile
from datetime import datetime, timezone

import boto3
import psycopg2
from psycopg2 import sql
from botocore.exceptions import ClientError

# PostgreSQL configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "dbname": "fraud_detection",
    "user": "fraud_user",
    "password": "fraud_password",
}

# Configure your S3 bucket through the S3_BUCKET environment variable.
S3_BUCKET = os.getenv("S3_BUCKET", "").strip()
AWS_REGION = "ap-south-1"

TABLES = ["transactions", "fraud_alerts"]

s3 = boto3.client("s3", region_name=AWS_REGION)


def export_table(cursor, table_name, file_path):
    """Export a PostgreSQL table to a local CSV file."""

    query = sql.SQL("COPY (SELECT * FROM {}) TO STDOUT WITH CSV HEADER").format(
        sql.Identifier("public", table_name)
    )

    with open(file_path, "w", newline="", encoding="utf-8") as file:
        cursor.copy_expert(query.as_string(cursor.connection), file)


def main():
    if not S3_BUCKET:
        raise ValueError(
            "Set S3_BUCKET to your existing private S3 bucket name."
        )

    run_time = datetime.now(timezone.utc)
    timestamp = run_time.strftime("%Y%m%dT%H%M%SZ")
    date_folder = run_time.strftime("%Y/%m/%d")

    print("Starting PostgreSQL to S3 export...")

    with tempfile.TemporaryDirectory(prefix="fraud_export_") as temp_dir:
        with psycopg2.connect(**DB_CONFIG) as connection:
            with connection.cursor() as cursor:

                for table in TABLES:
                    file_path = os.path.join(
                        temp_dir, f"{table}_{timestamp}.csv"
                    )

                    print(f"Exporting table: {table}")

                    export_table(cursor, table, file_path)

                    s3_key = (
                        f"fraud-detection/{table}/"
                        f"year={run_time:%Y}/"
                        f"month={run_time:%m}/"
                        f"day={run_time:%d}/"
                        f"{table}_{timestamp}.csv"
                    )

                    print(f"Uploading to s3://{S3_BUCKET}/{s3_key}")

                    s3.upload_file(
                        file_path,
                        S3_BUCKET,
                        s3_key,
                        ExtraArgs={
                            "ContentType": "text/csv",
                            "ServerSideEncryption": "AES256",
                        },
                    )

                    print(f"Uploaded successfully: {table}")

    print("Export completed successfully.")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, psycopg2.Error, ClientError, OSError) as error:
        print(f"Export failed: {error}")
        raise
