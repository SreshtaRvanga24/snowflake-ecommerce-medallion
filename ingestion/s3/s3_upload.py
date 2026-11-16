"""
Upload the S3-sourced CSVs into the landing bucket.

Usage:
    pip install boto3
    set -a; source .env; set +a
    python ingestion/s3/s3_upload.py --src datasets/raw --batch-date 2026-11-15

Lands each file as  <table>/<table>_<batch-date>.csv  in the bucket.
Existing objects are never overwritten, so a rerun is safe.

Credentials come from the standard AWS environment variables
(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION), which boto3 reads
automatically. The IAM user only needs s3:PutObject and s3:ListBucket.
"""
import argparse
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from dotenv import load_dotenv
load_dotenv()



# Source file -> landing folder. These are the tables on the S3 path.
FILES = {
    "CUSTOMERS.csv": "customers",
    "ORDERS_RAW.csv": "orders",
    "ORDER_ITEMS.csv": "order_items",
}


def object_exists(s3, bucket: str, key: str) -> bool:
    """True if the key is already in the bucket (uses ListBucket, not GetObject)."""
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
    return any(obj["Key"] == key for obj in resp.get("Contents", []))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True, help="Folder containing the CSVs")
    parser.add_argument("--batch-date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    bucket = os.environ["S3_BUCKET"]
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION"))

    for file_name, folder in FILES.items():
        path = Path(args.src) / file_name
        if not path.exists():
            print(f"SKIP   {file_name}: not found in {args.src}")
            continue
        key = f"{folder}/{folder}_{args.batch_date}.csv"
        if object_exists(s3, bucket, key):
            print(f"EXISTS s3://{bucket}/{key} (left unchanged)")
            continue
        try:
            s3.upload_file(str(path), bucket, key)
            print(f"OK     {file_name} -> s3://{bucket}/{key}")
        except ClientError as exc:
            print(f"FAILED {file_name}: {exc.response['Error']['Message']}")
            raise

    print("\nBucket contents:")
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            print(f"  {obj['Key']}  ({obj['Size']} bytes)")


if __name__ == "__main__":
    main()