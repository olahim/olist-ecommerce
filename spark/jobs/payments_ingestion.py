#!/usr/bin/env python3
"""
Order Payments Dataset Ingestion Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit, trim, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import argparse

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Payments Ingestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def get_schema():
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", IntegerType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", DoubleType(), True),
    ])

def clean_data(df):
    return df \
        .withColumn("order_id", trim(col("order_id"))) \
        .withColumn("payment_type", trim(col("payment_type")))

def main(year, month, day):
    spark = create_spark_session()
    schema = get_schema()
    
    try:
        input_path = f"/opt/hadoop/data/raw/olist_order_payments_dataset/year={year}/month={month}/day={day}/"
        print(f"Reading payments data from: {input_path}")
        
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
               .withColumn("ingestion_day", lit(day))
        
        output_path = f"/opt/hadoop/data/staging/payments/year={year}/month={month}/day={day}/"
        df.write.mode("overwrite").parquet(output_path)
        
        print(f"Successfully ingested {df.count()} payment records")
        
    except Exception as e:
        print(f"Error ingesting payments: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()
    main(args.year, args.month, args.day)