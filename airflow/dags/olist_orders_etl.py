"""
Standalone DAG for Orders dataset ETL
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
    'olist_orders_etl',
    default_args=default_args,
    description='ETL pipeline for Orders dataset',
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['olist', 'orders'],
)

start = DummyOperator(task_id='start', dag=dag)

# File sensor
wait_for_file = FileSensor(
    task_id='wait_for_orders_file',
    filepath='/opt/hadoop/data/raw/olist_orders_dataset/year={{ ds[:4] }}/month={{ ds[5:7] }}/day={{ ds[8:10] }}/_SUCCESS',
    poke_interval=60,
    timeout=3600,
    dag=dag,
)

# Ingestion
ingest = SparkSubmitOperator(
    task_id='ingest_orders',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/orders_ingestion.py',
    conn_id='spark_default',
    verbose=True,
    application_args=['--year', '{{ ds[:4] }}', '--month', '{{ ds[5:7] }}', '--day', '{{ ds[8:10] }}'],
    dag=dag,
)

# Data quality checks
dq_checks = SparkSubmitOperator(
    task_id='orders_dq_checks',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/data_quality_checks.py',
    conn_id='spark_default',
    verbose=True,
    application_args=['--dataset', 'orders', '--date', '{{ ds }}'],
    dag=dag,
)

# Fact builder (when dimensions are ready)
fact_builder = SparkSubmitOperator(
    task_id='orders_fact_builder',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/orders_fact_builder.py',
    conn_id='spark_default',
    verbose=True,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# Dependencies
start >> wait_for_file >> ingest >> dq_checks >> fact_builder >> end