#!/usr/bin/env python3
"""
Customer Dimension Builder
Creates and maintains customer dimension table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, current_timestamp, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
import argparse

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Customer Dimension Builder") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

def main(date):
    spark = create_spark_session()
    
    try:
        # Read customer data from staging
        customers_df = spark.read.parquet("/opt/hadoop/data/staging/customers/deduplicated/")
        
        # Read orders to get customer metrics
        orders_df = spark.read.parquet("/opt/hadoop/data/staging/orders/")
        
        # Calculate customer metrics
        customer_metrics = orders_df.groupBy("customer_id").agg(
            spark_sum(col("total_value")).alias("lifetime_value"),
            spark_count(col("order_id")).alias("total_orders"),
            spark_min(col("order_purchase_date")).alias("first_order_date"),
            spark_max(col("order_purchase_date")).alias("last_order_date")
        )
        
        # Join customer info with metrics
        dim_customers = customers_df.join(customer_metrics, on="customer_id", how="left") \
            .withColumn("lifetime_value", col("lifetime_value").fillna(0)) \
            .withColumn("total_orders", col("total_orders").fillna(0)) \
            .withColumn("customer_segment", 
                        when(col("lifetime_value") > 10000, "VIP")
                        .when(col("lifetime_value") > 5000, "High Value")
                        .when(col("lifetime_value") > 1000, "Regular")
                        .otherwise("Low Value")) \
            .withColumn("is_current", lit(True)) \
            .withColumn("start_date", current_date()) \
            .withColumn("end_date", lit(None)) \
            .withColumn("ingestion_timestamp", current_timestamp())
        
        # Generate surrogate key
        window_spec = Window.orderBy("customer_id")
        dim_customers = dim_customers.withColumn("customer_sk", 
                                                  row_number().over(window_spec) + 1000000)
        
        # Write to Iceberg dimension table
        dim_customers.writeTo("olist_warehouse.dim_customers_iceberg") \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("write.parquet.compression-codec", "snappy") \
            .createOrReplace()
        
        print(f"Customer dimension built with {dim_customers.count()} records")
        
    except Exception as e:
        print(f"Error building customer dimension: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    main(args.date)