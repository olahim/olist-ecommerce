#!/usr/bin/env python3
"""
Geolocation Dataset Ingestion Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit, trim, col, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import argparse

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Geolocation Ingestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def get_schema():
    return StructType([
        StructField("geolocation_zip_code_prefix", StringType(), True),
        StructField("geolocation_lat", DoubleType(), True),
        StructField("geolocation_lng", DoubleType(), True),
        StructField("geolocation_city", StringType(), True),
        StructField("geolocation_state", StringType(), True),
    ])

def clean_data(df):
    return df \
        .withColumn("geolocation_zip_code_prefix", 
                    regexp_replace(col("geolocation_zip_code_prefix"), "[^0-9]", "")) \
        .withColumn("geolocation_city", trim(col("geolocation_city"))) \
        .withColumn("geolocation_state", trim(col("geolocation_state")))

def main(year, month, day):
    spark = create_spark_session()
    schema = get_schema()
    
    try:
        input_path = f"/opt/hadoop/data/raw/olist_geolocation_dataset/year={year}/month={month}/day={day}/"
        print(f"Reading geolocation data from: {input_path}")
        
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", ",") \
            .schema(schema) \
            .csv(input_path)
        
        df = clean_data(df)
        
        # Remove duplicates (same zip code may have multiple coordinates)
        df = df.dropDuplicates(["geolocation_zip_code_prefix"])
        
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("source_file", input_file_name()) \
               .withColumn("ingestion_year", lit(year)) \
               .withColumn("ingestion_month", lit(month)) \
               .withColumn("ingestion_day", lit(day))
        
        output_path = f"/opt/hadoop/data/staging/geolocation/year={year}/month={month}/day={day}/"
        df.write.mode("overwrite").parquet(output_path)
        
        print(f"Successfully ingested {df.count()} geolocation records")
        
    except Exception as e:
        print(f"Error ingesting geolocation: {str(e)}")
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