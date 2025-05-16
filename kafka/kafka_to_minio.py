#!/usr/bin/env python3
"""
Script to consume Kafka messages and store them in MinIO
"""

import json
import os
import time
from io import BytesIO
from datetime import datetime

from kafka import KafkaConsumer
import boto3

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "fraudiq-kafka:9092"
TOPICS = ["bank_transactions", "gateway_transactions", "processor_transactions"]

# MinIO configuration
MINIO_ENDPOINT = "http://fraudiq-minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "raw"

def save_to_minio(s3_client, bucket, topic, messages):
    """Save a batch of messages to MinIO"""
    if not messages:
        return
    
    # Create a file-like object in memory
    data = BytesIO()
    
    # Write each message as a JSON line
    for msg in messages:
        data.write(json.dumps(msg).encode('utf-8'))
        data.write(b'\n')
    
    # Reset pointer to start of file
    data.seek(0)
    
    # Create a filename with topic and timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    key = f"{topic}/data-{timestamp}.json"
    
    # Upload to MinIO
    s3_client.upload_fileobj(data, bucket, key)
    print(f"Uploaded {len(messages)} messages to {bucket}/{key}")

def main():
    # Create S3 client for MinIO
    s3_client = boto3.client('s3',
                           endpoint_url=MINIO_ENDPOINT,
                           aws_access_key_id=MINIO_ACCESS_KEY,
                           aws_secret_access_key=MINIO_SECRET_KEY,
                           region_name='us-east-1')  # Region doesn't matter for MinIO
    
    # Create bucket if it doesn't exist
    try:
        s3_client.head_bucket(Bucket=MINIO_BUCKET)
    except:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='kafka-minio-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    # Process messages
    message_buffer = {topic: [] for topic in TOPICS}
    batch_size = 100
    
    try:
        while True:
            # Poll for messages
            message_batch = consumer.poll(timeout_ms=1000, max_records=batch_size)
            
            if not message_batch:
                # No messages in this poll
                # Write any buffered messages
                for topic in TOPICS:
                    if message_buffer[topic]:
                        save_to_minio(s3_client, MINIO_BUCKET, topic, message_buffer[topic])
                        message_buffer[topic] = []
                
                print("No new messages, waiting...")
                time.sleep(5)
                continue
            
            # Process messages by topic
            for tp, messages in message_batch.items():
                topic = tp.topic
                
                # Add messages to buffer
                for msg in messages:
                    message_buffer[topic].append(msg.value)
                
                # If buffer is full, save to MinIO
                if len(message_buffer[topic]) >= batch_size:
                    save_to_minio(s3_client, MINIO_BUCKET, topic, message_buffer[topic])
                    message_buffer[topic] = []
    
    except KeyboardInterrupt:
        # Save any remaining messages
        for topic in TOPICS:
            if message_buffer[topic]:
                save_to_minio(s3_client, MINIO_BUCKET, topic, message_buffer[topic])
        
        print("Shutting down...")

if __name__ == "__main__":
    main() 