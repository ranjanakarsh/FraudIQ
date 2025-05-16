#!/usr/bin/env python3
"""
Payment Gateway Transaction Producer for Kafka
Generates synthetic payment gateway transaction data and publishes to Kafka
"""

import sys
import json
import time
import argparse
import uuid
import random
from datetime import datetime
from typing import Dict, Any
from pathlib import Path

from faker import Faker
from kafka import KafkaProducer

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from utils.logger import setup_logger
from utils.config import config

logger = setup_logger("gateway-producer")
fake = Faker()

def generate_transaction() -> Dict[str, Any]:
    """Generate a single synthetic payment gateway transaction"""
    status_options = ["completed", "pending", "failed", "error"]
    card_types = ["visa", "mastercard", "amex", "discover"]
    gateway_names = ["Stripe", "PayPal", "Adyen", "Braintree", "Square"]
    
    amount = round(random.uniform(1.0, 1000.0), 2)
    
    # Gateway status probabilities
    status_weights = [0.88, 0.07, 0.03, 0.02]
    
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 1000)}",
        "timestamp": int(datetime.now().timestamp() * 1000),
        "amount": amount,
        "status": random.choices(status_options, weights=status_weights)[0],
        "source": random.choice(gateway_names),
        "merchant": fake.company(),
        "card_type": random.choice(card_types),
        "location": fake.city(),
        "ip_address": fake.ipv4(),
        "device_id": f"device_{random.randint(1, 500)}"
    }
    
    return transaction

def main():
    parser = argparse.ArgumentParser(description="Payment gateway transaction producer for Kafka")
    parser.add_argument("--rate", type=float, default=1.0, 
                        help="Number of transactions per second (default: 1.0)")
    parser.add_argument("--bootstrap-servers", type=str, 
                        default=config.kafka["bootstrap_servers"],
                        help=f"Kafka bootstrap servers (default: {config.kafka['bootstrap_servers']})")
    parser.add_argument("--topic", type=str, default="gateway_transactions",
                        help="Kafka topic name (default: gateway_transactions)")
    args = parser.parse_args()
    
    # Calculate sleep time between messages
    sleep_time = 1.0 / args.rate if args.rate > 0 else 0
    
    # Configure Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=str.encode
    )
    
    logger.info(f"Starting payment gateway transaction producer at rate of {args.rate} transactions per second")
    
    try:
        while True:
            transaction = generate_transaction()
            
            # Send the message asynchronously
            future = producer.send(
                args.topic,
                key=transaction["transaction_id"],
                value=transaction
            )
            
            # Optional: Wait for confirmation (can be removed for higher throughput)
            try:
                record_metadata = future.get(timeout=10)
                logger.info(f"Produced gateway transaction: {transaction['transaction_id']} - Source: {transaction['source']} - Amount: ${transaction['amount']:.2f} - Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            except Exception as e:
                logger.error(f"Message delivery failed: {e}")
            
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        logger.info("Shutting down payment gateway transaction producer")
    finally:
        # Wait for any outstanding messages to be delivered
        logger.info("Flushing producer...")
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main() 