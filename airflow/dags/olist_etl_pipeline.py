"""
Main DAG for Olist Ecommerce ETL Pipeline
Orchestrates ingestion, data quality checks, dimension/fact building, and Iceberg optimization
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

import sys
import os
sys.path.append('/home/hadoop-user/olist-ecommerce/airflow/plugins')

from operators.data_quality_operator import DataQualityOperator
from sensors.dq_threshold_sensor import DQThresholdSensor

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-team@olist.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=4),
}

# Define the DAG
dag = DAG(
    'olist_etl_pipeline',
    default_args=default_args,
    description='End-to-end ETL pipeline for Olist Ecommerce data',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['olist', 'ecommerce', 'etl', 'iceberg'],
)

# Start marker
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# =========================================================
# File Sensors - Wait for all datasets to arrive
# =========================================================

datasets = [
    'olist_customers_dataset',
    'olist_orders_dataset',
    'olist_products_dataset',
    'olist_sellers_dataset',
    'olist_geolocation_dataset',
    'olist_order_reviews_dataset',
    'olist_order_payments_dataset',
    'olist_order_items_dataset',
    'product_category_name_translation',
]

sensor_tasks = {}
for dataset in datasets:
    sensor_tasks[dataset] = FileSensor(
        task_id=f'wait_for_{dataset}',
        filepath=f'/opt/hadoop/data/raw/{dataset}/year={{{{{ ds[:4] }}}}}/month={{{{{ ds[5:7] }}}}}/day={{{{{ ds[8:10] }}}}}/_SUCCESS',
        poke_interval=60,
        timeout=7200,
        mode='poke',
        dag=dag,
    )

# =========================================================
# Ingestion Tasks - Load raw CSV data into staging
# =========================================================

ingestion_tasks = {}

# Customers ingestion
ingestion_tasks['customers'] = SparkSubmitOperator(
    task_id='ingest_customers',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/customers_ingestion.py',
    conn_id='spark_default',
    verbose=True,
    conf={
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hive',
    },
    application_args=[
        '--year', '{{ ds[:4] }}',
        '--month', '{{ ds[5:7] }}',
        '--day', '{{ ds[8:10] }}',
    ],
    dag=dag,
)

# Orders ingestion
ingestion_tasks['orders'] = SparkSubmitOperator(
    task_id='ingest_orders',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/orders_ingestion.py',
    conn_id='spark_default',
    verbose=True,
    application_args=[
        '--year', '{{ ds[:4] }}',
        '--month', '{{ ds[5:7] }}',
        '--day', '{{ ds[8:10] }}',
    ],
    dag=dag,
)

# Products ingestion
ingestion_tasks['products'] = SparkSubmitOperator(
    task_id='ingest_products',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/products_ingestion.py',
    conn_id='spark_default',
    verbose=True,
    application_args=[
        '--year', '{{ ds[:4] }}',
        '--month', '{{ ds[5:7] }}',
        '--day', '{{ ds[8:10] }}',
    ],
    dag=dag,
)

# Sellers ingestion
ingestion_tasks['sellers'] = SparkSubmitOperator(
    task_id='ingest_sellers',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/sellers_ingestion.py',
    conn_id='spark_default',
    verbose=True,
    application_args=[
        '--year', '{{ ds[:4] }}',
        '--month', '{{ ds[5:7] }}',
        '--day', '{{ ds[8:10] }}',
    ],
    dag=dag,
)

# Geolocation ingestion
ingestion_tasks['geolocation'] = SparkSubmitOperator(
    task_id='ingest_geolocation',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/geolocation_ingestion.py',
    conn_id='spark_default',
    verbose=True,
    application_args=[
        '--year', '{{ ds[:4] }}',
        '--month', '{{ ds[5:7] }}',
        '--day', '{{ ds[8:10] }}',
    ],
    dag=dag,
)

# Reviews ingestion
ingestion_tasks['reviews'] = SparkSubmitOperator(
    task_id='ingest_reviews',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/reviews_ingestion.py',
    conn_id='spark_default',
    verbose=True,
    application_args=[
        '--year', '{{ ds[:4] }}',
        '--month', '{{ ds[5:7] }}',
        '--day', '{{ ds[8:10] }}',
    ],
    dag=dag,
)

# Payments ingestion
ingestion_tasks['payments'] = SparkSubmitOperator(
    task_id='ingest_payments',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/payments_ingestion.py',
    conn_id='spark_default',
    verbose=True,
    application_args=[
        '--year', '{{ ds[:4] }}',
        '--month', '{{ ds[5:7] }}',
        '--day', '{{ ds[8:10] }}',
    ],
    dag=dag,
)

# Order items ingestion
ingestion_tasks['order_items'] = SparkSubmitOperator(
    task_id='ingest_order_items',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/order_items_ingestion.py',
    conn_id='spark_default',
    verbose=True,
    application_args=[
        '--year', '{{ ds[:4] }}',
        '--month', '{{ ds[5:7] }}',
        '--day', '{{ ds[8:10] }}',
    ],
    dag=dag,
)

# Category translation ingestion
ingestion_tasks['category_translation'] = SparkSubmitOperator(
    task_id='ingest_category_translation',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/category_translation_ingestion.py',
    conn_id='spark_default',
    verbose=True,
    application_args=[
        '--year', '{{ ds[:4] }}',
        '--month', '{{ ds[5:7] }}',
        '--day', '{{ ds[8:10] }}',
    ],
    dag=dag,
)

# =========================================================
# Data Quality Validation
# =========================================================

dq_validation = DataQualityOperator(
    task_id='data_quality_validation',
    config_path='/home/hadoop-user/olist-ecommerce/airflow/dags/config/data_quality_rules.yaml',
    datasets=['customers', 'orders', 'products', 'sellers', 'geolocation', 
              'reviews', 'payments', 'order_items'],
    execution_date='{{ ds }}',
    dag=dag,
)

# DQ threshold sensor - ensures quality meets SLA before proceeding
dq_sensor = DQThresholdSensor(
    task_id='dq_threshold_sensor',
    dq_score_threshold=95.0,
    dq_reports_path='/opt/hadoop/data/quality/dq_reports/',
    datasets=['customers', 'orders', 'products', 'sellers', 'order_items'],
    poke_interval=60,
    timeout=1800,
    mode='poke',
    dag=dag,
)

# =========================================================
# Dimension Building
# =========================================================

# Customer dimension (SCD Type 2)
dim_customers = SparkSubmitOperator(
    task_id='dim_customers',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/dim_customers.py',
    conn_id='spark_default',
    verbose=True,
    application_args=['--date', '{{ ds }}'],
    dag=dag,
)

# Product dimension (with category translation)
dim_products = SparkSubmitOperator(
    task_id='dim_products',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/dim_products.py',
    conn_id='spark_default',
    verbose=True,
    application_args=['--date', '{{ ds }}'],
    dag=dag,
)

# Seller dimension
dim_sellers = SparkSubmitOperator(
    task_id='dim_sellers',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/dim_sellers.py',
    conn_id='spark_default',
    verbose=True,
    application_args=['--date', '{{ ds }}'],
    dag=dag,
)

# =========================================================
# Fact Building
# =========================================================

# Orders fact table
fact_orders = SparkSubmitOperator(
    task_id='fact_orders',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/fact_orders.py',
    conn_id='spark_default',
    verbose=True,
    application_args=['--date', '{{ ds }}'],
    dag=dag,
)

# Order items fact table
fact_order_items = SparkSubmitOperator(
    task_id='fact_order_items',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/fact_order_items.py',
    conn_id='spark_default',
    verbose=True,
    application_args=['--date', '{{ ds }}'],
    dag=dag,
)

# Payments fact table
fact_payments = SparkSubmitOperator(
    task_id='fact_order_payments',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/fact_order_payments.py',
    conn_id='spark_default',
    verbose=True,
    application_args=['--date', '{{ ds }}'],
    dag=dag,
)

# =========================================================
# Iceberg Optimization
# =========================================================

iceberg_optimize = SparkSubmitOperator(
    task_id='iceberg_optimization',
    application='/home/hadoop-user/olist-ecommerce/spark/jobs/iceberg_optimization.py',
    conn_id='spark_default',
    verbose=True,
    application_args=['--date', '{{ ds }}', '--operation', 'all'],
    dag=dag,
)

# =========================================================
# End marker
# =========================================================

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# =========================================================
# Task Dependencies
# =========================================================

# All sensors must succeed before ingestion
for sensor in sensor_tasks.values():
    start >> sensor >> list(ingestion_tasks.values())

# All ingestion tasks must complete before DQ validation
for task in ingestion_tasks.values():
    task >> dq_validation

# DQ validation -> DQ sensor -> Dimension building
dq_validation >> dq_sensor >> [dim_customers, dim_products, dim_sellers]

# Dimensions must complete before facts
[dim_customers, dim_products, dim_sellers] >> [fact_orders, fact_order_items, fact_payments]

# Facts must complete before optimization
[fact_orders, fact_order_items, fact_payments] >> iceberg_optimize >> end