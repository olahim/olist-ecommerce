#!/usr/bin/env python3
"""
Orders Fact Table Builder
Creates fact table by joining orders with dimensions and payments
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, sum as spark_sum, count, when, datediff, to_date
import argparse

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Orders Fact Builder") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

def main(date):
    spark = create_spark_session()
    
    try:
        # Read dimensions
        customers_df = spark.table("olist_warehouse.dim_customers_iceberg").filter(col("is_current") == True)
        products_df = spark.table("olist_warehouse.dim_products_iceberg")
        sellers_df = spark.table("olist_warehouse.dim_sellers_iceberg")
        
        # Read staging tables
        orders_df = spark.read.parquet("/opt/hadoop/data/staging/orders/")
        order_items_df = spark.read.parquet("/opt/hadoop/data/staging/order_items/")
        payments_df = spark.read.parquet("/opt/hadoop/data/staging/payments/")
        reviews_df = spark.read.parquet("/opt/hadoop/data/staging/reviews/")
        
        # Calculate order totals from items
        order_totals = order_items_df.groupBy("order_id").agg(
            spark_sum(col("price")).alias("total_value"),
            spark_sum(col("freight_value")).alias("total_freight"),
            count(col("order_item_id")).alias("item_count")
        )
        
        # Aggregate payments per order
        order_payments = payments_df.groupBy("order_id").agg(
            spark_sum(col("payment_value")).alias("payment_value"),
            spark_max(col("payment_installments")).alias("payment_installments"),
            spark_max(col("payment_type")).alias("payment_type")
        )
        
        # Get review scores
        order_reviews = reviews_df.select("order_id", "review_score").dropDuplicates(["order_id"])
        
        # Join all components
        fact_orders = orders_df \
            .join(customers_df, on="customer_id", how="left") \
            .join(order_totals, on="order_id", how="left") \
            .join(order_payments, on="order_id", how="left") \
            .join(order_reviews, on="order_id", how="left") \
            .withColumn("order_purchase_date", to_date(col("order_purchase_timestamp"))) \
            .withColumn("delivery_days", 
                        when(col("order_delivered_customer_timestamp").isNotNull(),
                             datediff(col("order_delivered_customer_timestamp"), 
                                      col("order_purchase_timestamp")))
                        .otherwise(None)) \
            .withColumn("is_delayed",
                        when(col("order_delivered_customer_timestamp") > col("order_estimated_delivery_date"),
                             True).otherwise(False)) \
            .withColumn("ingestion_timestamp", current_timestamp())
        
        # Write to Iceberg fact table
        fact_orders.writeTo("olist_warehouse.fact_orders_iceberg") \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("write.parquet.compression-codec", "snappy") \
            .partitionedBy("order_purchase_date") \
            .createOrReplace()
        
        print(f"Orders fact table built with {fact_orders.count()} records")
        
    except Exception as e:
        print(f"Error building orders fact table: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    main(args.date)