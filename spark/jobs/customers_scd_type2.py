#!/usr/bin/env python3
"""
SCD Type 2 Processing for Customers Dimension
Tracks historical changes to customer attributes
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, lit, current_date, current_timestamp,
    when, row_number, monotonically_increasing_id
)
from pyspark.sql.window import Window
import argparse
from datetime import datetime

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Customers SCD Type 2") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

def get_existing_dimension(spark):
    """Read existing customer dimension table"""
    try:
        return spark.table("olist_warehouse.dim_customers_iceberg")
    except Exception:
        # Table doesn't exist yet
        return None

def identify_changes(new_df, existing_df):
    """Identify which records have changed and need SCD processing"""
    
    # Create natural key join
    if existing_df is None:
        # No existing data, all records are new
        return new_df.withColumn("change_type", lit("INSERT"))
    
    # Compare current and new records
    joined = new_df.alias("new").join(
        existing_df.filter(col("is_current") == True).alias("old"),
        on=["customer_unique_id"],
        how="left"
    )
    
    # Determine change type
    changes_df = joined.select(
        col("new.*"),
        when(col("old.customer_sk").isNull(), lit("INSERT"))
        .when(
            (col("new.customer_city") != col("old.customer_city")) |
            (col("new.customer_state") != col("old.customer_state")) |
            (col("new.customer_zip_code_prefix") != col("old.customer_zip_code_prefix")),
            lit("UPDATE")
        )
        .otherwise(lit("NOCHANGE"))
        .alias("change_type")
    )
    
    return changes_df

def apply_scd_updates(spark, changes_df, existing_df):
    """Apply SCD Type 2 updates to dimension table"""
    
    # Separate change types
    inserts = changes_df.filter(col("change_type") == "INSERT")
    updates = changes_df.filter(col("change_type") == "UPDATE")
    no_changes = changes_df.filter(col("change_type") == "NOCHANGE")
    
    print(f"Records to insert: {inserts.count()}")
    print(f"Records to update: {updates.count()}")
    print(f"Records with no change: {no_changes.count()}")
    
    # Generate surrogate keys for inserts
    if inserts.count() > 0:
        max_sk = 0
        if existing_df is not None:
            max_sk = existing_df.selectExpr("max(customer_sk)").collect()[0][0] or 0
        
        inserts_with_sk = inserts.withColumn(
            "customer_sk", 
            monotonically_increasing_id() + max_sk + 1
        ).withColumn("is_current", lit(True)) \
         .withColumn("start_date", current_date()) \
         .withColumn("end_date", lit(None)) \
         .withColumn("ingestion_timestamp", current_timestamp())
        
        inserts_with_sk.writeTo("olist_warehouse.dim_customers_iceberg").append()
    
    # Update existing records (close old, insert new)
    if updates.count() > 0:
        # Close current records
        for row in updates.collect():
            spark.sql(f"""
                UPDATE olist_warehouse.dim_customers_iceberg 
                SET is_current = FALSE, end_date = CURRENT_DATE()
                WHERE customer_unique_id = '{row['customer_unique_id']}' 
                AND is_current = TRUE
            """)
        
        # Insert new records with updated attributes
        updates_with_sk = updates.withColumn(
            "customer_sk", 
            monotonically_increasing_id() + 1000000
        ).withColumn("is_current", lit(True)) \
         .withColumn("start_date", current_date()) \
         .withColumn("end_date", lit(None)) \
         .withColumn("ingestion_timestamp", current_timestamp())
        
        updates_with_sk.writeTo("olist_warehouse.dim_customers_iceberg").append()
    
    print("SCD Type 2 processing completed successfully")

def main(date):
    spark = create_spark_session()
    
    try:
        # Read new customer data
        new_customers = spark.read.parquet("/opt/hadoop/data/staging/customers/deduplicated/")
        
        # Get existing dimension table
        existing_dim = get_existing_dimension(spark)
        
        # Identify changes
        changes = identify_changes(new_customers, existing_dim)
        
        # Apply SCD updates
        apply_scd_updates(spark, changes, existing_dim)
        
        print(f"SCD Type 2 processing completed for date: {date}")
        
    except Exception as e:
        print(f"Error in SCD Type 2 processing: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Date of processing")
    args = parser.parse_args()
    main(args.date)