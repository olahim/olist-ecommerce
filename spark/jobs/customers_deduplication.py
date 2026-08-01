#!/usr/bin/env python3
"""
Customer Deduplication Job
Identifies and removes duplicate customer records
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, row_number, count, sum as spark_sum, 
    desc, lit, current_timestamp
)
from pyspark.sql.window import Window
import argparse

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Customer Deduplication") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

def main(date):
    spark = create_spark_session()
    
    try:
        # Read latest customers data
        customers_df = spark.read.parquet("/opt/hadoop/data/staging/customers/") \
            .filter(col("ingestion_date") == date) \
            .filter(col("dq_status") == "VALIDATED")
        
        print(f"Found {customers_df.count()} records before deduplication")
        
        # Identify duplicates by customer_id
        window_spec = Window.partitionBy("customer_id").orderBy(desc("ingestion_timestamp"))
        
        deduplicated_df = customers_df \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")
        
        # Also check for duplicates by customer_unique_id (should be unique per customer)
        unique_window = Window.partitionBy("customer_unique_id").orderBy(desc("ingestion_timestamp"))
        
        final_df = deduplicated_df \
            .withColumn("unique_row_num", row_number().over(unique_window)) \
            .filter(col("unique_row_num") == 1) \
            .drop("unique_row_num")
        
        record_count = final_df.count()
        duplicates_removed = customers_df.count() - record_count
        
        print(f"After deduplication: {record_count} records (removed {duplicates_removed} duplicates)")
        
        # Write deduplicated data
        output_path = "/opt/hadoop/data/staging/customers/deduplicated/"
        final_df.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        # Log deduplication metrics
        metrics_df = spark.createDataFrame([{
            "dataset": "customers",
            "date": date,
            "records_before": customers_df.count(),
            "records_after": record_count,
            "duplicates_removed": duplicates_removed,
            "ingestion_timestamp": current_timestamp()
        }])
        
        metrics_df.write \
            .mode("append") \
            .parquet("/opt/hadoop/data/quality/dq_metrics/deduplication_metrics.parquet")
        
    except Exception as e:
        print(f"Error during deduplication: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date of processing")
    args = parser.parse_args()
    main(args.date)