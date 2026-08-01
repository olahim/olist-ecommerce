#!/usr/bin/env python3
"""
Products Dataset Ingestion Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit, trim, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import argparse

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Products Ingestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def get_schema():
    return StructType([
        StructField("product_id", StringType(), True),
        StructField("product_category_name", StringType(), True),
        StructField("product_name_length", IntegerType(), True),
        StructField("product_description_length", IntegerType(), True),
        StructField("product_photos_qty", IntegerType(), True),
        StructField("product_weight_g", IntegerType(), True),
        StructField("product_length_cm", IntegerType(), True),
        StructField("product_height_cm", IntegerType(), True),
        StructField("product_width_cm", IntegerType(), True),
    ])

def clean_data(df):
    return df \
        .withColumn("product_id", trim(col("product_id"))) \
        .withColumn("product_category_name", 
                    when(col("product_category_name").isNotNull(), 
                         trim(col("product_category_name")))
                    .otherwise(None)) \
        .withColumn("product_name_length", 
                    when(col("product_name_length") > 0, col("product_name_length"))
                    .otherwise(None)) \
        .withColumn("product_weight_g", 
                    when(col("product_weight_g") > 0, col("product_weight_g"))
                    .otherwise(None))

def main(year, month, day):
    spark = create_spark_session()
    schema = get_schema()
    
    try:
        input_path = f"/opt/hadoop/data/raw/olist_products_dataset/year={year}/month={month}/day={day}/"
        print(f"Reading products data from: {input_path}")
        
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
        
        output_path = f"/opt/hadoop/data/staging/products/year={year}/month={month}/day={day}/"
        df.write.mode("overwrite").parquet(output_path)
        
        print(f"Successfully ingested {df.count()} products records")
        
    except Exception as e:
        print(f"Error ingesting products: {str(e)}")
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