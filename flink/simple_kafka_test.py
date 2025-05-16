#!/usr/bin/env python3
"""
Simple Flink Kafka test
"""

import os
import sys
from pathlib import Path

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common.typeinfo import Types

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

def main():
    # Set up Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    
    # Set parallelism to 1 for easier debugging
    env.set_parallelism(1)
    
    # Add JAR files
    jar_paths = ["flink-connector-kafka-1.17.1.jar", "kafka-clients-3.4.0.jar"]
    for jar_path in jar_paths:
        jar_path = os.path.abspath(jar_path)
        if os.path.exists(jar_path):
            env.add_jars(f"file://{jar_path}")
            print(f"Added JAR: {jar_path}")
        else:
            print(f"Warning: JAR file not found at {jar_path}")
    
    # Create a source that generates some data
    source = env.from_collection(
        collection=[
            '{"transaction_id": "tx1", "user_id": "user1", "amount": 100.0}',
            '{"transaction_id": "tx2", "user_id": "user2", "amount": 200.0}',
            '{"transaction_id": "tx3", "user_id": "user3", "amount": 300.0}'
        ],
        type_info=Types.STRING()
    )
    
    # Just print the data
    source.print()
    
    # Execute the job
    print("Starting simple test job...")
    env.execute("Simple Kafka Test")

if __name__ == "__main__":
    main() 