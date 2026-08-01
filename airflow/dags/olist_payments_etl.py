"""
Standalone DAG for Payments dataset ETL
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'olist_payments_etl',
    default_args=default_args,
    description='ETL pipeline for Payments dataset',
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['olist', 'payments'],
)

start = DummyOperator(task_id='start', dag=dag)

# File sensor
wait_for_file = FileSensor(
    task_id='wait_for_payments_file',
    filepath='/opt/hadoop/data/raw/olist_order_payments_dataset/year={{ ds[:4] }}/month={{ ds[5:7] }}/day={{ ds[8:10] }}/_SUCCESS',
    poke_interval=60,
    timeout=3600,
    dag=dag,
)

# Ingestion
ingest = SparkSubmitOperator(
    task_id='ingest_payments',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/payments_ingestion.py',
    conn_id='spark_default',
    verbose=True,
    application_args=['--year', '{{ ds[:4] }}', '--month', '{{ ds[5:7] }}', '--day', '{{ ds[8:10] }}'],
    dag=dag,
)

# Data quality checks
dq_checks = SparkSubmitOperator(
    task_id='payments_dq_checks',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/data_quality_checks.py',
    conn_id='spark_default',
    verbose=True,
    application_args=['--dataset', 'payments', '--date', '{{ ds }}'],
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

start >> wait_for_file >> ingest >> dq_checks >> end