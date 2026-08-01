#!/usr/bin/env python3
"""
Order Items Fact Table Builder
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import argparse

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Order Items Fact Builder") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

def main(date):
    spark = create_spark_session()
    
    try:
        # Read order items from staging
        order_items_df = spark.read.parquet("/opt/hadoop/data/staging/order_items/")
        
        # Add calculated fields
        fact_order_items = order_items_df \
            .withColumn("total_line_value", col("price") * col("quantity")) \
            .withColumn("ingestion_timestamp", current_timestamp())
        
        # Write to Iceberg fact table
        fact_order_items.writeTo("olist_warehouse.fact_order_items_iceberg") \
            .tableProperty("write.format.default", "parquet") \
            .createOrReplace()
        
        print(f"Order items fact table built with {fact_order_items.count()} records")
        
    except Exception as e:
        print(f"Error building order items fact table: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    main(args.date)
