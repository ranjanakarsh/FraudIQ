#!/usr/bin/env python3
"""
Script to upload sample data to MinIO
"""

import json
import os
import boto3
from io import BytesIO

# MinIO configuration
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "raw"
TOPICS = ["bank_transactions", "gateway_transactions", "processor_transactions"]

def upload_sample_data():
    """Upload sample data to MinIO"""
    
    # Create S3 client for MinIO
    s3_client = boto3.client('s3',
                           endpoint_url=MINIO_ENDPOINT,
                           aws_access_key_id=MINIO_ACCESS_KEY,
                           aws_secret_access_key=MINIO_SECRET_KEY,
                           region_name='us-east-1')  # Region doesn't matter for MinIO
    
    # Create bucket if it doesn't exist
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
        print(f"Bucket {MINIO_BUCKET} already exists")
    except:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        print(f"Created bucket {MINIO_BUCKET}")
    
    # Get sample data
    with open("kafka/sample_data.json", "r") as f:
        data = json.load(f)
    
    # Upload to each topic
    for topic in TOPICS:
        # Create a file-like object in memory
        buffer = BytesIO()
        
        # Write data as JSON array
        buffer.write(json.dumps(data).encode('utf-8'))
        
        # Reset pointer to start of file
        buffer.seek(0)
        
        # Upload to MinIO
        key = f"{topic}/data.json"
        s3_client.upload_fileobj(buffer, MINIO_BUCKET, key)
        print(f"Uploaded sample data to {MINIO_BUCKET}/{key}")

if __name__ == "__main__":
    upload_sample_data() 