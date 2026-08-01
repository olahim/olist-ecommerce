#!/usr/bin/env python3
"""
Order Reviews Dataset Ingestion Job
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit, trim, col, to_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import argparse

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Reviews Ingestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def get_schema():
    return StructType([
        StructField("review_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("review_score", IntegerType(), True),
        StructField("review_comment_title", StringType(), True),
        StructField("review_comment_message", StringType(), True),
        StructField("review_creation_date", StringType(), True),
        StructField("review_answer_timestamp", StringType(), True),
    ])

def clean_data(df):
    return df \
        .withColumn("review_id", trim(col("review_id"))) \
        .withColumn("order_id", trim(col("order_id"))) \
        .withColumn("review_comment_title", 
                    when(col("review_comment_title").isNotNull(), 
                         trim(col("review_comment_title")))
                    .otherwise(None)) \
        .withColumn("review_comment_message", 
                    when(col("review_comment_message").isNotNull(), 
                         trim(col("review_comment_message")))
                    .otherwise(None)) \
        .withColumn("review_creation_date", 
                    to_timestamp(col("review_creation_date"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("review_answer_timestamp", 
                    to_timestamp(col("review_answer_timestamp"), "yyyy-MM-dd HH:mm:ss"))

def main(year, month, day):
    spark = create_spark_session()
    schema = get_schema()
    
    try:
        input_path = f"/opt/hadoop/data/raw/olist_order_reviews_dataset/year={year}/month={month}/day={day}/"
        print(f"Reading reviews data from: {input_path}")
        
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
        
        output_path = f"/opt/hadoop/data/staging/reviews/year={year}/month={month}/day={day}/"
        df.write.mode("overwrite").parquet(output_path)
        
        print(f"Successfully ingested {df.count()} review records")
        
    except Exception as e:
        print(f"Error ingesting reviews: {str(e)}")
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