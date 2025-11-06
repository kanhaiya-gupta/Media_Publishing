#!/usr/bin/env python3
"""
Script to create MinIO bucket for session metrics
"""

from minio import Minio
from minio.error import S3Error

# MinIO connection
client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "session-metrics"

try:
    # Check if bucket exists
    if client.bucket_exists(bucket_name):
        print(f"✓ Bucket '{bucket_name}' already exists")
    else:
        # Create bucket
        client.make_bucket(bucket_name)
        print(f"✓ Created bucket '{bucket_name}' successfully")
except S3Error as e:
    print(f"✗ Error: {e}")

