#!/usr/bin/env python3
"""
Customers Dataset Ingestion Job
Reads CSV from raw zone, validates, and writes to staging
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, input_file_name, lit, col, 
    when, regexp_replace, trim, upper
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import argparse
import sys

def create_spark_session():
    """Create and configure Spark session with Iceberg support"""
    return SparkSession.builder \
        .appName("Olist Customers Ingestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.spark_catalog.uri", "thrift://localhost:9083") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def get_schema():
    """Define schema for customers dataset"""
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_unique_id", StringType(), True),
        StructField("customer_zip_code_prefix", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
    ])

def clean_data(df):
    """Clean and standardize customer data"""
    return df \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("customer_unique_id", trim(col("customer_unique_id"))) \
        .withColumn("customer_zip_code_prefix", 
                    when(col("customer_zip_code_prefix").isNotNull(), 
                         regexp_replace(col("customer_zip_code_prefix"), "[^0-9]", ""))
                    .otherwise(None)) \
        .withColumn("customer_city", upper(trim(col("customer_city")))) \
        .withColumn("customer_state", upper(trim(col("customer_state")))) \
        .withColumn("customer_city", 
                    when(col("customer_city") == "Sao Paulo", "SAO PAULO")
                    .otherwise(col("customer_city")))

def main(year, month, day):
    spark = create_spark_session()
    schema = get_schema()
    
    try:
        # Read from raw zone
        input_path = f"/opt/hadoop/data/raw/olist_customers_dataset/year={year}/month={month}/day={day}/"
        
        print(f"Reading customers data from: {input_path}")
        
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .option("mode", "PERMISSIVE") \
            .schema(schema) \
            .csv(input_path)
        
        # Clean and standardize data
        df = clean_data(df)
        
        # Add metadata columns
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("source_file", input_file_name()) \
               .withColumn("ingestion_year", lit(year)) \
               .withColumn("ingestion_month", lit(month)) \
               .withColumn("ingestion_day", lit(day)) \
               .withColumn("dq_status", lit("PENDING"))
        
        # Write to staging as Parquet
        output_path = f"/opt/hadoop/data/staging/customers/year={year}/month={month}/day={day}/"
        
        df.write \
            .mode("overwrite") \
            .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
            .parquet(output_path)
        
        # Also write to Iceberg staging table
        df.writeTo("olist_staging.customers") \
            .tableProperty("write.format.default", "parquet") \
            .createOrReplace()
        
        record_count = df.count()
        print(f"Successfully ingested {record_count} customers records for {year}-{month}-{day}")
        
    except Exception as e:
        print(f"Error ingesting customers data: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True, help="Year of data partition")
    parser.add_argument("--month", required=True, help="Month of data partition")
    parser.add_argument("--day", required=True, help="Day of data partition")
    args = parser.parse_args()
    main(args.year, args.month, args.day)
