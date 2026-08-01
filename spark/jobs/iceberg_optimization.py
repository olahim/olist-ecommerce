#!/usr/bin/env python3
"""
Iceberg Table Optimization Job
Performs compaction, snapshot expiration, and manifest optimization
"""

from pyspark.sql import SparkSession
import argparse
from datetime import datetime, timedelta

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist Iceberg Optimization") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

def main(operation, table_name):
    spark = create_spark_session()
    
    try:
        if operation == "compact":
            print(f"Compacting table: {table_name}")
            spark.sql(f"""
                CALL spark_catalog.system.rewrite_data_files(
                    table => '{table_name}',
                    strategy => 'binpack',
                    options => map('min-input-files', '5', 'target-file-size-bytes', '536870912')
                )
            """)
            
        elif operation == "expire_snapshots":
            print(f"Expiring snapshots for table: {table_name}")
            spark.sql(f"""
                CALL spark_catalog.system.expire_snapshots(
                    table => '{table_name}',
                    older_than => TIMESTAMP '{(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")}',
                    retain_last => 5
                )
            """)
            
        elif operation == "rewrite_manifests":
            print(f"Rewriting manifests for table: {table_name}")
            spark.sql(f"""
                CALL spark_catalog.system.rewrite_manifests(
                    table => '{table_name}',
                    options => map('target-compression-size-bytes', '134217728')
                )
            """)
            
        elif operation == "all":
            tables = [
                "olist_warehouse.dim_customers_iceberg",
                "olist_warehouse.dim_products_iceberg",
                "olist_warehouse.dim_sellers_iceberg",
                "olist_warehouse.fact_orders_iceberg",
                "olist_warehouse.fact_order_items_iceberg",
                "olist_warehouse.fact_order_payments_iceberg"
            ]
            
            for tbl in tables:
                print(f"Optimizing table: {tbl}")
                # Compact
                spark.sql(f"""
                    CALL spark_catalog.system.rewrite_data_files(
                        table => '{tbl}',
                        strategy => 'binpack',
                        options => map('min-input-files', '5')
                    )
                """)
                # Expire snapshots
                spark.sql(f"""
                    CALL spark_catalog.system.expire_snapshots(
                        table => '{tbl}',
                        retain_last => 5
                    )
                """)
                
        print(f"Optimization completed for {operation}")
        
    except Exception as e:
        print(f"Error during optimization: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--operation", required=True, choices=["compact", "expire_snapshots", "rewrite_manifests", "all"])
    parser.add_argument("--table", default=None, help="Table name (required for single operations)")
    args = parser.parse_args()
    main(args.operation, args.table)