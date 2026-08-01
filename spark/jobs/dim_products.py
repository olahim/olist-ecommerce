#!/usr/bin/env python3
"""
Product Dimension Builder
Joins products with category translation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import argparse

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Product Dimension Builder") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

def main(date):
    spark = create_spark_session()
    
    try:
        # Read products from staging
        products_df = spark.read.parquet("/opt/hadoop/data/staging/products/")
        
        # Read category translation
        categories_df = spark.read.parquet("/opt/hadoop/data/staging/categories/")
        
        # Join with category translation
        dim_products = products_df.join(
            categories_df, 
            on="product_category_name", 
            how="left"
        ).withColumn("ingestion_timestamp", current_timestamp())
        
        # Write to Iceberg dimension table
        dim_products.writeTo("olist_warehouse.dim_products_iceberg") \
            .tableProperty("write.format.default", "parquet") \
            .createOrReplace()
        
        print(f"Product dimension built with {dim_products.count()} records")
        
    except Exception as e:
        print(f"Error building product dimension: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    main(args.date)
