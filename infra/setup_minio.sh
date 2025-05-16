#!/bin/bash
# MinIO Setup Script
# Creates buckets and sets up permissions for FraudIQ

# Exit on error
set -e

# Load environment variables if .env file exists
if [ -f ../.env ]; then
    source ../.env
fi

# MinIO credentials and endpoint
MINIO_ENDPOINT=${S3_ENDPOINT_URL:-http://localhost:9000}
MINIO_ACCESS_KEY=${S3_ACCESS_KEY:-minioadmin}
MINIO_SECRET_KEY=${S3_SECRET_KEY:-minioadmin}

# Bucket names
BUCKET_RAW=${S3_BUCKET_RAW:-raw}
BUCKET_RECONCILED=${S3_BUCKET_RECONCILED:-reconciled}
BUCKET_FRAUD_FEATURES=${S3_BUCKET_FRAUD_FEATURES:-fraud-features}

# Install mc (MinIO client) if not already installed
if ! command -v mc &> /dev/null; then
    echo "Installing MinIO Client (mc)..."
    
    # Detect OS
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        wget https://dl.min.io/client/mc/release/linux-amd64/mc -O mc
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        wget https://dl.min.io/client/mc/release/darwin-amd64/mc -O mc
    else
        echo "Unsupported OS. Please install MinIO Client manually: https://min.io/docs/minio/linux/reference/minio-mc.html"
        exit 1
    fi
    
    chmod +x mc
    export PATH=$PATH:$(pwd)
    echo "MinIO Client installed."
else
    echo "MinIO Client already installed."
fi

# Configure mc
echo "Configuring MinIO Client..."
mc alias set fraudiq $MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
attempt=0
max_attempts=30
until mc ls fraudiq &> /dev/null || [ $attempt -ge $max_attempts ]; do
    attempt=$((attempt+1))
    echo "Attempt $attempt/$max_attempts: MinIO not ready, waiting..."
    sleep 2
done

if [ $attempt -ge $max_attempts ]; then
    echo "Error: MinIO did not become ready in time."
    echo "Please make sure MinIO is running at $MINIO_ENDPOINT and credentials are correct."
    exit 1
fi

# Create buckets
echo "Creating buckets..."

# Function to create bucket if it doesn't exist
create_bucket() {
    local bucket=$1
    if ! mc ls fraudiq/$bucket &> /dev/null; then
        echo "Creating bucket: $bucket"
        mc mb fraudiq/$bucket
        echo "Bucket created: $bucket"
    else
        echo "Bucket already exists: $bucket"
    fi
}

create_bucket $BUCKET_RAW
create_bucket $BUCKET_RECONCILED
create_bucket $BUCKET_FRAUD_FEATURES

# Set bucket policies
echo "Setting bucket policies..."

# Make buckets public (for demo purposes - in production, use proper IAM policies)
mc anonymous set download fraudiq/$BUCKET_RAW
mc anonymous set download fraudiq/$BUCKET_RECONCILED
mc anonymous set download fraudiq/$BUCKET_FRAUD_FEATURES

# Create kafka topic directories in raw bucket
echo "Creating Kafka topic directories in raw bucket..."
# Create temporary file
touch temp.txt

# Create directories by uploading a file and then removing it
mc cp temp.txt fraudiq/$BUCKET_RAW/bank_transactions/temp.txt
mc cp temp.txt fraudiq/$BUCKET_RAW/gateway_transactions/temp.txt
mc cp temp.txt fraudiq/$BUCKET_RAW/processor_transactions/temp.txt

# Remove the temporary files
mc rm fraudiq/$BUCKET_RAW/bank_transactions/temp.txt
mc rm fraudiq/$BUCKET_RAW/gateway_transactions/temp.txt
mc rm fraudiq/$BUCKET_RAW/processor_transactions/temp.txt

# Remove local temporary file
rm temp.txt

# Upload sample data if available
SAMPLE_DATA_DIR="../data/synthetic"
if [ -d "$SAMPLE_DATA_DIR" ] && [ -f "$SAMPLE_DATA_DIR/bank_transactions.json" ]; then
    echo "Uploading sample data to MinIO..."
    mc cp "$SAMPLE_DATA_DIR/bank_transactions.json" fraudiq/$BUCKET_RAW/bank_transactions/
    mc cp "$SAMPLE_DATA_DIR/gateway_transactions.json" fraudiq/$BUCKET_RAW/gateway_transactions/
    mc cp "$SAMPLE_DATA_DIR/processor_transactions.json" fraudiq/$BUCKET_RAW/processor_transactions/
    echo "Sample data uploaded."
else
    echo "No sample data found. Skipping upload."
fi

echo "MinIO setup completed successfully!"
echo "Buckets:"
echo "  - $BUCKET_RAW: raw transaction data"
echo "  - $BUCKET_RECONCILED: reconciled transaction data"
echo "  - $BUCKET_FRAUD_FEATURES: fraud detection features"
echo ""
echo "Access MinIO console at: ${MINIO_ENDPOINT/9000/9001}"
echo "Username: $MINIO_ACCESS_KEY"
echo "Password: $MINIO_SECRET_KEY" 