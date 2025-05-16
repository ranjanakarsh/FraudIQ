import os
import json
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

class Config:
    """Configuration manager for the FraudIQ platform"""
    
    def __init__(self):
        """Initialize configuration with defaults and environment vars"""
        # Kafka settings
        self.kafka = {
            'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),
            'topics_file': os.getenv('KAFKA_TOPICS_FILE', 'kafka/config/topics.json'),
        }
        
        # Flink settings
        self.flink = {
            'checkpoint_dir': os.getenv('FLINK_CHECKPOINT_DIR', '/tmp/flink-checkpoints'),
            'parallelism': int(os.getenv('FLINK_PARALLELISM', '2')),
        }
        
        # Spark settings
        self.spark = {
            'master': os.getenv('SPARK_MASTER', 'local[*]'),
            'app_name': os.getenv('SPARK_APP_NAME', 'fraudiq'),
            'checkpoint_dir': os.getenv('SPARK_CHECKPOINT_DIR', '/tmp/spark-checkpoints'),
        }
        
        # MinIO (S3) settings
        self.s3 = {
            'endpoint_url': os.getenv('S3_ENDPOINT_URL', 'http://localhost:9000'),
            'access_key': os.getenv('S3_ACCESS_KEY', 'minioadmin'),
            'secret_key': os.getenv('S3_SECRET_KEY', 'minioadmin'),
            'bucket_raw': os.getenv('S3_BUCKET_RAW', 'raw'),
            'bucket_reconciled': os.getenv('S3_BUCKET_RECONCILED', 'reconciled'),
            'bucket_fraud_features': os.getenv('S3_BUCKET_FRAUD_FEATURES', 'fraud-features'),
        }
        
        # Transaction settings
        self.transaction = {
            'schema_file': os.getenv('TRANSACTION_SCHEMA_FILE', 'data/schemas/transaction_schema.json'),
        }
        
        # Fraud detection settings
        self.fraud_detection = {
            'burst_count': int(os.getenv('FRAUD_BURST_COUNT', '5')),
            'burst_window_seconds': int(os.getenv('FRAUD_BURST_WINDOW_SECONDS', '30')),
        }
    
    def get_kafka_topics(self) -> Dict[str, Dict]:
        """Read Kafka topics configuration from file"""
        try:
            with open(self.kafka['topics_file'], 'r') as f:
                topics_config = json.load(f)
            
            # Convert the list to a dict for easier access
            return {topic['name']: topic for topic in topics_config.get('topics', [])}
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise Exception(f"Error loading Kafka topics configuration: {e}")
    
    def get_transaction_schema(self) -> Dict[str, Any]:
        """Read transaction schema from file"""
        try:
            with open(self.transaction['schema_file'], 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise Exception(f"Error loading transaction schema: {e}")


# Create a singleton instance
config = Config() 