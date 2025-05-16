#!/usr/bin/env python3
"""
Spark Transaction Feature Extraction Job
Extracts features from transaction data for fraud analysis
"""

import sys
import os
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, from_unixtime, count, sum, avg, stddev, min, max,
    datediff, hour, dayofweek, month, year, expr, 
    collect_set, size, when, lit
)

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
                    "bucket_features": "fraud-features"
                }
        config = Config()

logger = setup_logger("spark-features")

def create_spark_session(app_name="fraudiq-feature-extraction"):
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

def load_transactions(spark, topics, start_time, end_time):
    """Load transactions from multiple topics and time range"""
    
    # Format timestamps for query
    start_ts = int(start_time.timestamp() * 1000)
    end_ts = int(end_time.timestamp() * 1000)
    
    dfs = []
    
    for topic in topics:
        # Read from S3
        s3_path = f"s3a://{config.s3['bucket_raw']}/{topic}"
        
        try:
            df = (spark.read.format("json")
                  .load(s3_path)
                  .filter(col("timestamp").between(start_ts, end_ts)))
            
            # Convert timestamp from milliseconds to timestamp
            df = df.withColumn("event_time", from_unixtime(col("timestamp") / 1000))
            
            logger.info(f"Loaded {df.count()} transactions from {topic}")
            dfs.append(df)
        except Exception as e:
            logger.error(f"Error loading transactions from {topic}: {e}")
    
    if not dfs:
        # Return empty dataframe with basic schema if no data loaded
        return spark.createDataFrame([], schema="transaction_id STRING, user_id STRING, timestamp LONG, amount DOUBLE, status STRING, source STRING, event_time TIMESTAMP")
    
    # Union all dataframes
    return dfs[0] if len(dfs) == 1 else dfs[0].unionByName(*dfs[1:], allowMissingColumns=True)

def extract_user_features(transactions_df):
    """Extract user-level features from transaction data"""
    
    # Define windows for different time periods
    hourly_window = Window.partitionBy("user_id", hour("event_time"))
    daily_window = Window.partitionBy("user_id", expr("date(event_time)"))
    weekly_window = Window.partitionBy("user_id", expr("weekofyear(event_time)"))
    user_window = Window.partitionBy("user_id")
    
    # Extract features
    user_features = transactions_df.select(
        "user_id",
        "transaction_id",
        "amount",
        "status",
        "source",
        "location",
        "event_time",
        "card_type",
        "ip_address",
        "device_id"
    ).withColumn(
        "txn_count_hourly", count("transaction_id").over(hourly_window)
    ).withColumn(
        "txn_count_daily", count("transaction_id").over(daily_window)
    ).withColumn(
        "txn_count_weekly", count("transaction_id").over(weekly_window)
    ).withColumn(
        "txn_amount_hourly", sum("amount").over(hourly_window)
    ).withColumn(
        "txn_amount_daily", sum("amount").over(daily_window)
    ).withColumn(
        "txn_amount_weekly", sum("amount").over(weekly_window)
    ).withColumn(
        "avg_txn_amount", avg("amount").over(user_window)
    ).withColumn(
        "stddev_txn_amount", stddev("amount").over(user_window)
    ).withColumn(
        "max_txn_amount", max("amount").over(user_window)
    ).withColumn(
        "min_txn_amount", min("amount").over(user_window)
    ).withColumn(
        "location_count", size(collect_set("location").over(user_window))
    ).withColumn(
        "device_count", size(collect_set("device_id").over(user_window))
    ).withColumn(
        "ip_count", size(collect_set("ip_address").over(user_window))
    ).withColumn(
        "card_type_count", size(collect_set("card_type").over(user_window))
    ).withColumn(
        "failed_txn_count", count(when(col("status").isin("failed", "declined", "rejected", "error", "timeout"), 1)).over(user_window)
    ).withColumn(
        "failed_txn_ratio", col("failed_txn_count") / count("transaction_id").over(user_window)
    ).withColumn(
        "feature_extraction_time", lit(datetime.now())
    )
    
    # Group by user_id to get one row per user
    user_features_agg = user_features.groupBy("user_id").agg(
        max("txn_count_hourly").alias("max_txn_count_hourly"),
        max("txn_count_daily").alias("max_txn_count_daily"),
        max("txn_count_weekly").alias("max_txn_count_weekly"),
        max("txn_amount_hourly").alias("max_txn_amount_hourly"),
        max("txn_amount_daily").alias("max_txn_amount_daily"),
        max("txn_amount_weekly").alias("max_txn_amount_weekly"),
        max("avg_txn_amount").alias("avg_txn_amount"),
        max("stddev_txn_amount").alias("stddev_txn_amount"),
        max("max_txn_amount").alias("max_txn_amount"),
        min("min_txn_amount").alias("min_txn_amount"),
        max("location_count").alias("location_count"),
        max("device_count").alias("device_count"),
        max("ip_count").alias("ip_count"),
        max("card_type_count").alias("card_type_count"),
        max("failed_txn_count").alias("failed_txn_count"),
        max("failed_txn_ratio").alias("failed_txn_ratio"),
        max("feature_extraction_time").alias("feature_extraction_time")
    )
    
    # Add a risk score (simple example)
    user_features_agg = user_features_agg.withColumn(
        "risk_score",
        (col("max_txn_count_hourly") * 0.5) +
        (col("location_count") * 0.1) +
        (col("device_count") * 0.1) +
        (col("ip_count") * 0.1) +
        (col("failed_txn_ratio") * 5.0)
    )
    
    return user_features_agg

def write_to_hudi(df, table_name, s3_bucket):
    """Write the dataframe to a Hudi table in S3"""
    
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'user_id',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'feature_extraction_time',
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
    parser = argparse.ArgumentParser(description="Spark transaction feature extraction job")
    parser.add_argument("--start-time", type=str, 
                        help="Start time for feature extraction (format: YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--end-time", type=str, 
                        help="End time for feature extraction (format: YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--days-ago", type=int, default=7,
                        help="Days ago to start feature extraction (default: 7)")
    parser.add_argument("--topics", type=str, 
                        default="bank_transactions,gateway_transactions,processor_transactions",
                        help="Comma-separated list of Kafka topics for input data")
    parser.add_argument("--output-bucket", type=str, 
                        default=config.s3["bucket_features"],
                        help=f"S3 bucket for output (default: {config.s3['bucket_features']})")
    parser.add_argument("--table-name", type=str, default="user_features",
                        help="Hudi table name for extracted features")
    
    args = parser.parse_args()
    
    # Set time range for feature extraction
    end_time = datetime.now()
    if args.end_time:
        end_time = datetime.strptime(args.end_time, "%Y-%m-%d %H:%M:%S")
    
    start_time = end_time - timedelta(days=args.days_ago)
    if args.start_time:
        start_time = datetime.strptime(args.start_time, "%Y-%m-%d %H:%M:%S")
    
    logger.info(f"Extracting features from {start_time} to {end_time}")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Parse topics
        topics = [topic.strip() for topic in args.topics.split(",")]
        
        # Load transactions
        transactions_df = load_transactions(spark, topics, start_time, end_time)
        
        # Extract features
        features_df = extract_user_features(transactions_df)
        
        # Log feature extraction summary
        user_count = features_df.count()
        high_risk_count = features_df.filter(col("risk_score") > 5.0).count()
        
        logger.info(f"Feature extraction summary:")
        logger.info(f"  Total users: {user_count}")
        logger.info(f"  High risk users: {high_risk_count} ({high_risk_count/user_count*100:.2f}%)")
        
        # Write features to Hudi
        write_to_hudi(features_df, args.table_name, args.output_bucket)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 