"""
Standalone DAG for Data Quality Monitoring
Runs comprehensive quality checks across all datasets
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import sys
sys.path.append('/home/hadoop-user/olist-ecommerce/airflow/plugins')
from operators.data_quality_operator import DataQualityOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-quality@olist.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'olist_data_quality_dag',
    default_args=default_args,
    description='Comprehensive data quality monitoring for all Olist datasets',
    schedule_interval='0 6 * * *',  # Run daily at 6 AM (after ETL completes)
    catchup=False,
    tags=['olist', 'data-quality', 'monitoring'],
)

start = DummyOperator(task_id='start', dag=dag)

# Main data quality validation
dq_validation = DataQualityOperator(
    task_id='full_data_quality_validation',
    config_path='/home/hadoop-user/olist-ecommerce/airflow/dags/config/data_quality_rules.yaml',
    datasets=['customers', 'orders', 'products', 'sellers', 'geolocation', 
              'reviews', 'payments', 'order_items', 'category_translation'],
    execution_date='{{ ds }}',
    dag=dag,
)

# Generate DQ report
generate_report = SparkSubmitOperator(
    task_id='generate_dq_report',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/dq_alerting.py',
    conn_id='spark_default',
    verbose=True,
    application_args=['--date', '{{ ds }}', '--action', 'report'],
    dag=dag,
)

# Send alerts if DQ score below threshold
send_alerts = SparkSubmitOperator(
    task_id='send_dq_alerts',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/dq_alerting.py',
    conn_id='spark_default',
    verbose=True,
    application_args=['--date', '{{ ds }}', '--action', 'alert', '--threshold', '90'],
    dag=dag,
)

# Quarantine failed records
quarantine_records = SparkSubmitOperator(
    task_id='quarantine_failed_records',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/dq_alerting.py',
    conn_id='spark_default',
    verbose=True,
    application_args=['--date', '{{ ds }}', '--action', 'quarantine'],
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# Dependencies
start >> dq_validation >> generate_report >> [send_alerts, quarantine_records] >> end