"""
Fraud Feature Extraction DAG
Runs the Spark feature extraction job daily
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'fraudiq',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Path to the Spark job
SPARK_JOB_PATH = "/opt/airflow/spark/extract_features.py"

# Create the DAG
dag = DAG(
    'fraud_feature_extraction',
    default_args=default_args,
    description='Extract fraud detection features from transaction data',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['fraudiq', 'fraud', 'features'],
)

# Start task
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Run the Spark feature extraction job
extract_features = SparkSubmitOperator(
    task_id='extract_fraud_features',
    application=SPARK_JOB_PATH,
    name='FraudIQ Feature Extraction',
    conn_id='spark_default',
    verbose=True,
    conf={
        'spark.executor.memory': '2g',
        'spark.driver.memory': '2g',
        'spark.executor.cores': '2',
    },
    application_args=[
        '--days-ago', '7',
        '--topics', 'bank_transactions,gateway_transactions,processor_transactions',
    ],
    dag=dag,
)

# Check if feature extraction was successful
check_success = BashOperator(
    task_id='check_success',
    bash_command='echo "Feature extraction job completed successfully."',
    dag=dag,
)

# End task
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set up the task dependencies
start >> extract_features >> check_success >> end 