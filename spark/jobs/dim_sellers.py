#!/usr/bin/env python3
"""
Seller Dimension Builder
Creates seller dimension with performance metrics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, avg, count, sum as spark_sum
import argparse

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Seller Dimension Builder") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

def main(date):
    spark = create_spark_session()
    
    try:
        # Read sellers from staging
        sellers_df = spark.read.parquet("/opt/hadoop/data/staging/sellers/")
        
        # Read order items for seller metrics
        order_items_df = spark.read.parquet("/opt/hadoop/data/staging/order_items/")
        
        # Calculate seller performance metrics
        seller_metrics = order_items_df.groupBy("seller_id").agg(
            spark_sum(col("price")).alias("total_sales"),
            count(col("order_id")).alias("total_orders"),
            avg(col("price")).alias("avg_order_value")
        )
        
        # Join metrics with seller info
        dim_sellers = sellers_df.join(seller_metrics, on="seller_id", how="left") \
            .withColumn("total_sales", col("total_sales").fillna(0)) \
            .withColumn("total_orders", col("total_orders").fillna(0)) \
            .withColumn("is_active", col("total_orders") > 0) \
            .withColumn("ingestion_timestamp", current_timestamp())
        
        # Write to Iceberg dimension table
        dim_sellers.writeTo("olist_warehouse.dim_sellers_iceberg") \
            .tableProperty("write.format.default", "parquet") \
            .createOrReplace()
        
        print(f"Seller dimension built with {dim_sellers.count()} records")
        
    except Exception as e:
        print(f"Error building seller dimension: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    main(args.date)
