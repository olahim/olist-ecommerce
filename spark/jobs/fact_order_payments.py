#!/usr/bin/env python3
"""
Order Payments Fact Table Builder
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import argparse

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Payments Fact Builder") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

def main(date):
    spark = create_spark_session()
    
    try:
        # Read payments from staging
        payments_df = spark.read.parquet("/opt/hadoop/data/staging/payments/")
        
        # Add calculated fields
        fact_payments = payments_df \
            .withColumn("ingestion_timestamp", current_timestamp())
        
        # Write to Iceberg fact table
        fact_payments.writeTo("olist_warehouse.fact_order_payments_iceberg") \
            .tableProperty("write.format.default", "parquet") \
            .createOrReplace()
        
        print(f"Payments fact table built with {fact_payments.count()} records")
        
    except Exception as e:
        print(f"Error building payments fact table: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    main(args.date)
