#!/usr/bin/env python3
"""
Category Translation Dataset Ingestion Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit, trim, col, lower
from pyspark.sql.types import StructType, StructField, StringType
import argparse

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Category Translation Ingestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def get_schema():
    return StructType([
        StructField("product_category_name", StringType(), True),
        StructField("product_category_name_english", StringType(), True),
    ])

def clean_data(df):
    return df \
        .withColumn("product_category_name", lower(trim(col("product_category_name")))) \
        .withColumn("product_category_name_english", lower(trim(col("product_category_name_english"))))

def main(year, month, day):
    spark = create_spark_session()
    schema = get_schema()
    
    try:
        input_path = f"/opt/hadoop/data/raw/product_category_name_translation/year={year}/month={month}/day={day}/"
        print(f"Reading category translation from: {input_path}")
        
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
        
        output_path = f"/opt/hadoop/data/staging/categories/year={year}/month={month}/day={day}/"
        df.write.mode("overwrite").parquet(output_path)
        
        # Write to permanent table for reference
        df.writeTo("olist_warehouse.category_translation") \
            .tableProperty("write.format.default", "parquet") \
            .createOrReplace()
        
        print(f"Successfully ingested {df.count()} category translations")
        
    except Exception as e:
        print(f"Error ingesting category translation: {str(e)}")
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