"""
Transaction Reconciliation DAG
Runs the Spark reconciliation job every 6 hours
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
SPARK_JOB_PATH = "/opt/airflow/spark/reconcile.py"

# Create the DAG
dag = DAG(
    'transaction_reconciliation',
    default_args=default_args,
    description='Reconcile transactions across different sources',
    schedule_interval=timedelta(hours=6),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['fraudiq', 'reconciliation'],
)

# Start task
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Run the Spark reconciliation job
reconcile = SparkSubmitOperator(
    task_id='reconcile_transactions',
    application=SPARK_JOB_PATH,
    name='FraudIQ Transaction Reconciliation',
    conn_id='spark_default',
    verbose=True,
    conf={
        'spark.executor.memory': '2g',
        'spark.driver.memory': '2g',
        'spark.executor.cores': '2',
    },
    application_args=[
        '--hours-ago', '6',
        '--bank-topic', 'bank_transactions',
        '--gateway-topic', 'gateway_transactions',
        '--processor-topic', 'processor_transactions',
    ],
    dag=dag,
)

# Check if reconciliation was successful
check_success = BashOperator(
    task_id='check_success',
    bash_command='echo "Reconciliation job completed successfully."',
    dag=dag,
)

# End task
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set up the task dependencies
start >> reconcile >> check_success >> end 