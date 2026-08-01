#!/usr/bin/env python3
"""
Orders Dataset Ingestion Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, input_file_name, lit, trim, 
    col, to_timestamp, when
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import argparse
import sys

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Orders Ingestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

def get_schema():
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
        StructField("order_approved_timestamp", StringType(), True),
        StructField("order_delivered_carrier_timestamp", StringType(), True),
        StructField("order_delivered_customer_timestamp", StringType(), True),
        StructField("order_estimated_delivery_date", StringType(), True),
    ])

def standardize_timestamps(df):
    """Convert string timestamps to proper timestamp type"""
    timestamp_columns = [
        "order_purchase_timestamp",
        "order_approved_timestamp",
        "order_delivered_carrier_timestamp",
        "order_delivered_customer_timestamp",
        "order_estimated_delivery_date"
    ]
    
    for col_name in timestamp_columns:
        df = df.withColumn(col_name, 
                           when(col(col_name).isNotNull(), 
                                to_timestamp(col(col_name), "yyyy-MM-dd HH:mm:ss"))
                           .otherwise(None))
    
    return df

def clean_data(df):
    return df \
        .withColumn("order_id", trim(col("order_id"))) \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("order_status", trim(col("order_status"))) \
        .transform(standardize_timestamps)

def main(year, month, day):
    spark = create_spark_session()
    schema = get_schema()
    
    try:
        input_path = f"/opt/hadoop/data/raw/olist_orders_dataset/year={year}/month={month}/day={day}/"
        print(f"Reading orders data from: {input_path}")
        
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", ",") \
            .schema(schema) \
            .csv(input_path)
        
        df = clean_data(df)
        
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("source_file", input_file_name()) \
               .withColumn("ingestion_year", lit(year)) \
               .withColumn("ingestion_month", lit(month)) \
               .withColumn("ingestion_day", lit(day)) \
               .withColumn("dq_status", lit("PENDING"))
        
        output_path = f"/opt/hadoop/data/staging/orders/year={year}/month={month}/day={day}/"
        df.write.mode("overwrite").parquet(output_path)
        
        # Write to Iceberg staging
        df.writeTo("olist_staging.orders") \
            .tableProperty("write.format.default", "parquet") \
            .createOrReplace()
        
        print(f"Successfully ingested {df.count()} orders records")
        
    except Exception as e:
        print(f"Error ingesting orders: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()
    main(args.year, args.month, args.day)