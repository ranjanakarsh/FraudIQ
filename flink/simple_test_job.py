#!/usr/bin/env python3
"""
Simple Flink Test Job
Just reads from a Kafka topic to test connectivity
"""

import os
import sys
import json
import argparse
from pathlib import Path

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from utils.config import config

def main():
    parser = argparse.ArgumentParser(description="Simple Flink Kafka test job")
    parser.add_argument("--bootstrap-servers", type=str, 
                        default=config.kafka["bootstrap_servers"],
                        help=f"Kafka bootstrap servers (default: {config.kafka['bootstrap_servers']})")
    parser.add_argument("--topic", type=str, 
                        default="bank_transactions",
                        help="Kafka topic to read from (default: bank_transactions)")
    parser.add_argument("--jars", type=str,
                        help="Comma-separated list of paths to JAR files")
    
    args = parser.parse_args()
    
    # Set up Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    
    # Add JAR files if provided
    if args.jars:
        jar_paths = args.jars.split(",")
        for jar_path in jar_paths:
            jar_path = os.path.abspath(jar_path.strip())
            if os.path.exists(jar_path):
                env.add_jars(f"file://{jar_path}")
                print(f"Added JAR: {jar_path}")
            else:
                print(f"Warning: JAR file not found at {jar_path}")
    
    # Create Kafka source
    source = KafkaSource.builder() \
        .set_bootstrap_servers(args.bootstrap_servers) \
        .set_topics(args.topic) \
        .set_group_id("fraudiq-test-consumer") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    # Create data stream
    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        f"kafka-source-{args.topic}"
    )
    
    # Just print the data for testing
    stream.print()
    
    # Execute the job
    print(f"Starting simple Flink test job reading from {args.topic}")
    env.execute("FraudIQ Simple Test Job")

if __name__ == "__main__":
    main() 