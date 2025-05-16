#!/usr/bin/env python3
"""
Spark Transaction Reconciliation Job
Reconciles transactions across different sources (bank, gateway, processor)
"""

import sys
import os
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp, when, lit, expr

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

try:
    # Try direct import first
    from utils.logger import setup_logger
    from utils.config import config
except ImportError:
    # If that fails, try with the fraudiq prefix
    try:
        from fraudiq.utils.logger import setup_logger
        from fraudiq.utils.config import config
    except ImportError:
        # If all else fails, create simple versions of these modules inline
        def setup_logger(name):
            import logging
            logger = logging.getLogger(name)
            logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            return logger
        
        class Config:
            def __init__(self):
                self.s3 = {
                    "endpoint_url": "http://minio:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin",
                    "bucket_raw": "raw",
                    "bucket_features": "fraud-features",
                    "bucket_reconciled": "reconciled"
                }
        config = Config()

logger = setup_logger("spark-reconcile")

def create_spark_session(app_name="fraudiq-reconciliation"):
    """Create a Spark session with Hudi support"""
    
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            # Hudi configs
            .config("spark.sql.extensions", "org.apache.hudi.spark3.HoodieSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            # S3 configs
            .config("spark.hadoop.fs.s3a.endpoint", config.s3["endpoint_url"])
            .config("spark.hadoop.fs.s3a.access.key", config.s3["access_key"])
            .config("spark.hadoop.fs.s3a.secret.key", config.s3["secret_key"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate())

def load_transactions(spark, topic, start_time, end_time):
    """Load transactions from a specified Kafka topic and time range"""
    
    # Format timestamps for Kafka query
    start_ts = int(start_time.timestamp() * 1000)
    end_ts = int(end_time.timestamp() * 1000)
    
    # Read from S3 instead of directly from Kafka since this is a batch job
    # We assume transaction data has been stored in S3 by a separate process
    s3_path = f"s3a://{config.s3['bucket_raw']}/{topic}"
    
    try:
        df = (spark.read.format("json")
              .load(s3_path)
              .filter(col("timestamp").between(start_ts, end_ts)))
        
        # Convert timestamp from milliseconds to timestamp
        df = df.withColumn("event_time", 
                          from_unixtime(col("timestamp") / 1000))
        
        logger.info(f"Loaded {df.count()} transactions from {topic}")
        return df
    except Exception as e:
        logger.error(f"Error loading transactions from {topic}: {e}")
        # Return empty dataframe with the same schema
        return spark.createDataFrame([], schema="transaction_id STRING, user_id STRING, timestamp LONG, amount DOUBLE, status STRING, source STRING, event_time TIMESTAMP")

def reconcile_transactions(bank_df, gateway_df, processor_df):
    """
    Reconcile transactions across different sources
    
    Performs a full outer join across all three sources to identify:
    - Missing transactions
    - Status mismatches
    - Amount mismatches
    """
    
    # Create views for easier querying
    bank_df.createOrReplaceTempView("bank_transactions")
    gateway_df.createOrReplaceTempView("gateway_transactions")
    processor_df.createOrReplaceTempView("processor_transactions")
    
    # Perform full outer join on transaction_id
    reconciled_df = bank_df.sparkSession.sql("""
        SELECT 
            COALESCE(b.transaction_id, g.transaction_id, p.transaction_id) AS transaction_id,
            COALESCE(b.user_id, g.user_id, p.user_id) AS user_id,
            COALESCE(b.event_time, g.event_time, p.event_time) AS event_time,
            b.amount AS bank_amount,
            g.amount AS gateway_amount,
            p.amount AS processor_amount,
            b.status AS bank_status,
            g.status AS gateway_status,
            p.status AS processor_status,
            b.source AS bank_source,
            g.source AS gateway_source,
            p.source AS processor_source,
            CASE 
                WHEN b.transaction_id IS NULL THEN 'missing'
                ELSE 'present' 
            END AS bank_presence,
            CASE 
                WHEN g.transaction_id IS NULL THEN 'missing'
                ELSE 'present' 
            END AS gateway_presence,
            CASE 
                WHEN p.transaction_id IS NULL THEN 'missing'
                ELSE 'present' 
            END AS processor_presence,
            CASE
                WHEN b.amount IS NULL OR g.amount IS NULL THEN NULL
                WHEN ABS(b.amount - g.amount) > 0.01 THEN 'mismatch'
                ELSE 'match'
            END AS bank_gateway_amount_check,
            CASE
                WHEN b.amount IS NULL OR p.amount IS NULL THEN NULL
                WHEN ABS(b.amount - p.amount) > 0.01 THEN 'mismatch'
                ELSE 'match'
            END AS bank_processor_amount_check,
            CASE
                WHEN g.amount IS NULL OR p.amount IS NULL THEN NULL
                WHEN ABS(g.amount - p.amount) > 0.01 THEN 'mismatch'
                ELSE 'match'
            END AS gateway_processor_amount_check,
            CURRENT_TIMESTAMP() AS reconciliation_time
        FROM 
            bank_transactions b
            FULL OUTER JOIN gateway_transactions g ON b.transaction_id = g.transaction_id
            FULL OUTER JOIN processor_transactions p ON b.transaction_id = p.transaction_id
    """)
    
    # Add a reconciliation status column
    reconciled_df = reconciled_df.withColumn("reconciliation_status",
        when((col("bank_presence") == "missing") | 
             (col("gateway_presence") == "missing") | 
             (col("processor_presence") == "missing"), "incomplete")
        .when((col("bank_gateway_amount_check") == "mismatch") | 
              (col("bank_processor_amount_check") == "mismatch") | 
              (col("gateway_processor_amount_check") == "mismatch"), "amount_mismatch")
        .when((col("bank_status") != col("gateway_status")) | 
              (col("bank_status") != col("processor_status")) | 
              (col("gateway_status") != col("processor_status")), "status_mismatch")
        .otherwise("reconciled")
    )
    
    return reconciled_df

def write_to_hudi(df, table_name, s3_bucket):
    """Write the dataframe to a Hudi table in S3"""
    
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'transaction_id',
        'hoodie.datasource.write.partitionpath.field': 'event_time',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'reconciliation_time',
        'hoodie.upsert.shuffle.parallelism': '2',
        'hoodie.insert.shuffle.parallelism': '2',
        'hoodie.datasource.hive_sync.enable': 'false'
    }
    
    s3_path = f"s3a://{s3_bucket}/{table_name}"
    
    try:
        (df.write
         .format("hudi")
         .options(**hudi_options)
         .mode("append")
         .save(s3_path))
        
        logger.info(f"Successfully wrote data to Hudi table: {table_name} at {s3_path}")
    except Exception as e:
        logger.error(f"Error writing to Hudi table {table_name}: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Spark transaction reconciliation job")
    parser.add_argument("--start-time", type=str, 
                        help="Start time for reconciliation (format: YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--end-time", type=str, 
                        help="End time for reconciliation (format: YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--hours-ago", type=int, default=6,
                        help="Hours ago to start reconciliation (default: 6)")
    parser.add_argument("--bank-topic", type=str, default="bank_transactions",
                        help="Kafka topic for bank transactions")
    parser.add_argument("--gateway-topic", type=str, default="gateway_transactions",
                        help="Kafka topic for gateway transactions")
    parser.add_argument("--processor-topic", type=str, default="processor_transactions",
                        help="Kafka topic for processor transactions")
    parser.add_argument("--output-bucket", type=str, 
                        default=config.s3["bucket_reconciled"],
                        help=f"S3 bucket for output (default: {config.s3['bucket_reconciled']})")
    parser.add_argument("--table-name", type=str, default="reconciled_transactions",
                        help="Hudi table name for reconciled transactions")
    
    args = parser.parse_args()
    
    # Set time range for reconciliation
    end_time = datetime.now()
    if args.end_time:
        end_time = datetime.strptime(args.end_time, "%Y-%m-%d %H:%M:%S")
    
    start_time = end_time - timedelta(hours=args.hours_ago)
    if args.start_time:
        start_time = datetime.strptime(args.start_time, "%Y-%m-%d %H:%M:%S")
    
    logger.info(f"Reconciling transactions from {start_time} to {end_time}")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load transactions from each source
        bank_df = load_transactions(spark, args.bank_topic, start_time, end_time)
        gateway_df = load_transactions(spark, args.gateway_topic, start_time, end_time)
        processor_df = load_transactions(spark, args.processor_topic, start_time, end_time)
        
        # Reconcile transactions
        reconciled_df = reconcile_transactions(bank_df, gateway_df, processor_df)
        
        # Log reconciliation summary
        total = reconciled_df.count()
        reconciled = reconciled_df.filter(col("reconciliation_status") == "reconciled").count()
        incomplete = reconciled_df.filter(col("reconciliation_status") == "incomplete").count()
        amount_mismatch = reconciled_df.filter(col("reconciliation_status") == "amount_mismatch").count()
        status_mismatch = reconciled_df.filter(col("reconciliation_status") == "status_mismatch").count()
        
        logger.info(f"Reconciliation summary:")
        logger.info(f"  Total transactions: {total}")
        logger.info(f"  Fully reconciled: {reconciled} ({reconciled/total*100:.2f}%)")
        logger.info(f"  Incomplete: {incomplete} ({incomplete/total*100:.2f}%)")
        logger.info(f"  Amount mismatch: {amount_mismatch} ({amount_mismatch/total*100:.2f}%)")
        logger.info(f"  Status mismatch: {status_mismatch} ({status_mismatch/total*100:.2f}%)")
        
        # Write reconciled transactions to Hudi
        write_to_hudi(reconciled_df, args.table_name, args.output_bucket)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 