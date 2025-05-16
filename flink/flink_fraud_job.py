#!/usr/bin/env python3
"""
Flink Fraud Detection Job
Real-time fraud pattern detection using Apache Flink
"""

import os
import sys
import re
import json
import argparse
from pathlib import Path

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Configuration
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

class TransactionProcessor(MapFunction):
    def __init__(self, threshold=200.0):
        self.threshold = threshold
        self.user_transaction_counts = {}
        self.user_total_amounts = {}
    
    def map(self, value):
        try:
            # First try to parse as JSON
            try:
                # Handle the case where we received JSON from Kafka
                transaction = json.loads(value)
                user_id = transaction.get("user_id")
                amount = transaction.get("amount")
                status = transaction.get("status", "unknown")
            except json.JSONDecodeError:
                # Handle the plain text format used in test mode
                match = re.match(r"Transaction: (\w+), amount=(\d+\.\d+)", value)
                if not match:
                    return f"Invalid transaction format: {value}"
                user_id = match.group(1)
                amount = float(match.group(2))
                status = "unknown"  # Status not provided in test data format
            
            # Validate required fields
            if not user_id or amount is None:
                return f"Missing required fields in transaction: {value}"
            
            # Convert amount to float if it's not already
            if not isinstance(amount, (int, float)):
                amount = float(amount)
            
            # Update user transaction counts and total amounts
            if user_id not in self.user_transaction_counts:
                self.user_transaction_counts[user_id] = 0
                self.user_total_amounts[user_id] = 0.0
            
            self.user_transaction_counts[user_id] += 1
            self.user_total_amounts[user_id] += amount
            
            # Check for suspicious activity
            is_suspicious = False
            reasons = []
            
            # Check 1: Large transaction amount
            if amount > self.threshold:
                is_suspicious = True
                reasons.append(f"Large transaction of {amount} exceeds threshold of {self.threshold}")
            
            # Check 2: Multiple transactions with large total
            if self.user_transaction_counts[user_id] >= 2 and self.user_total_amounts[user_id] > 300.0:
                is_suspicious = True
                reasons.append(f"Multiple transactions totaling {self.user_total_amounts[user_id]:.2f} from user {user_id}")
            
            # Check 3: Transaction with failed/declined status
            if status in ["failed", "declined"]:
                is_suspicious = True
                reasons.append(f"Transaction has suspicious status: {status}")
            
            if is_suspicious:
                reason_str = " & ".join(reasons)
                return f"FRAUD ALERT: {user_id} - {amount:.2f} - {reason_str}"
            else:
                return f"NORMAL: {user_id} - {amount:.2f}"
        except Exception as e:
            return f"Error processing: {value} - {str(e)}"

def main():
    parser = argparse.ArgumentParser(description="Flink Fraud Detection Job")
    parser.add_argument("--bootstrap-servers", type=str, 
                        default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--input-topics", type=str, nargs='+',
                        default=["bank_transactions", "gateway_transactions", "processor_transactions"],
                        help="Kafka topics to consume from")
    parser.add_argument("--group-id", type=str, 
                        default="fraud-detection-group",
                        help="Consumer group ID")
    parser.add_argument("--jars", type=str,
                        default="flink-connector-kafka-4.0.0-2.0.jar,kafka-clients-4.0.0.jar",
                        help="Comma-separated list of JAR files to include")
    parser.add_argument("--threshold", type=float, default=200.0,
                        help="Threshold for large transaction detection")
    parser.add_argument("--test-mode", action="store_true",
                       help="Run in test mode with sample data instead of Kafka")
    parser.add_argument("--parallelism", type=int, default=2,
                       help="Parallelism for Flink job")
    parser.add_argument("--checkpoint-interval", type=int, default=5000,
                       help="Checkpoint interval in milliseconds")
    args = parser.parse_args()
    
    # Collect JAR paths
    jar_paths = []
    for jar_name in args.jars.split(","):
        jar_path = os.path.abspath(jar_name)
        if os.path.exists(jar_path):
            jar_paths.append(jar_path)
            print(f"Added JAR: {jar_path}")
        else:
            print(f"Warning: JAR file not found: {jar_path}")
    
    # Set up Flink configuration
    config = Configuration()
    
    # This is important to make the job visible in Flink UI:
    # Ensure we use externalized checkpoints that are retained after job cancellation
    config.set_string("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION")
    config.set_string("execution.checkpointing.interval", f"{args.checkpoint_interval}ms")
    config.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    config.set_string("restart-strategy", "fixed-delay")
    config.set_string("restart-strategy.fixed-delay.attempts", "3")
    
    # Set up StreamExecutionEnvironment with the configuration
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(args.parallelism)
    
    # Add JARs to the environment
    for jar_path in jar_paths:
        env.add_jars(f"file://{jar_path}")
    
    # Create a data stream
    if args.test_mode:
        # Test mode: Use sample data
        print("Running in test mode with sample data")
        test_data = [
            "Transaction: user1, amount=100.0",
            "Transaction: user2, amount=250.0",  # Exceeds threshold
            "Transaction: user1, amount=300.0",  # Multiple transactions
            "Transaction: user3, amount=50.0",
            "Transaction: user2, amount=150.0",  # Multiple large transactions
            "Transaction: user3, amount=350.0"   # Exceeds threshold
        ]
        
        data_stream = env.from_collection(
            collection=test_data,
            type_info=Types.STRING()
        )
    else:
        # Kafka mode: Connect to Kafka topics
        print(f"Connecting to Kafka at {args.bootstrap_servers}")
        print(f"Reading from topics: {args.input_topics}")
        
        sources = []
        for topic in args.input_topics:
            source = KafkaSource.builder() \
                .set_bootstrap_servers(args.bootstrap_servers) \
                .set_topics(topic) \
                .set_group_id(args.group_id) \
                .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
                .set_value_only_deserializer(SimpleStringSchema()) \
                .build()
            
            topic_stream = env.from_source(
                source,
                WatermarkStrategy.no_watermarks(),
                f"Kafka Source - {topic}"
            )
            
            sources.append(topic_stream)
        
        # Union all streams if there are multiple
        if len(sources) > 1:
            data_stream = sources[0]
            for source in sources[1:]:
                data_stream = data_stream.union(source)
        else:
            data_stream = sources[0]
    
    # Process transactions and detect fraud
    fraud_detection_stream = data_stream \
        .map(TransactionProcessor(args.threshold), output_type=Types.STRING())

    # Name the operators for better visibility in the Flink UI
    fraud_detection_stream = fraud_detection_stream.name("Fraud Detection")
    
    # Add a sink to print the output
    fraud_detection_stream.print().name("Fraud Alerts")
    
    # Execute the job with the job name that will appear in the Flink UI
    print(f"Starting Flink job with parallelism {args.parallelism}")
    env.execute("FraudIQ Fraud Detection")

if __name__ == "__main__":
    main() 