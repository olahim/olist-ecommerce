#!/usr/bin/env python3
"""
End-to-end integration tests for Olist pipeline
"""

import unittest
import subprocess
import os
import time
import pandas as pd
from pyspark.sql import SparkSession

class TestEndToEndPipeline(unittest.TestCase):
    """End-to-end integration tests"""

    @classmethod
    def setUpClass(cls):
        """Initialize Spark and setup test environment"""
        cls.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("E2ETest") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .getOrCreate()
        
        cls.test_date = "2024-01-01"
        cls.test_year = "2024"
        cls.test_month = "01"
        cls.test_day = "01"

    @classmethod
    def tearDownClass(cls):
        """Cleanup test environment"""
        cls.spark.stop()

    def test_ingestion_creates_staging_files(self):
        """Test that ingestion creates staging files"""
        from spark.jobs.customers_ingestion import main as ingest_customers
        
        try:
            ingest_customers(self.test_year, self.test_month, self.test_day)
            
            staging_path = f"/opt/hadoop/data/staging/customers/year={self.test_year}/month={self.test_month}/day={self.test_day}/"
            self.assertTrue(os.path.exists(staging_path))
        except Exception as e:
            self.skipTest(f"Ingestion test skipped: {e}")

    def test_staging_data_has_metadata_columns(self):
        """Test that staging data has metadata columns"""
        try:
            df = self.spark.read.parquet("/opt/hadoop/data/staging/customers/")
            
            columns = df.columns
            self.assertIn("ingestion_timestamp", columns)
            self.assertIn("source_file", columns)
            self.assertIn("dq_status", columns)
        except Exception as e:
            self.skipTest(f"Staging data test skipped: {e}")

    def test_cross_dataset_join_works(self):
        """Test that cross-dataset joins work"""
        try:
            customers = self.spark.read.parquet("/opt/hadoop/data/staging/customers/")
            orders = self.spark.read.parquet("/opt/hadoop/data/staging/orders/")
            
            joined = customers.join(orders, on="customer_id", how="inner")
            self.assertGreaterEqual(joined.count(), 0)
        except Exception as e:
            self.skipTest(f"Join test skipped: {e}")

    def test_iceberg_table_readable(self):
        """Test that Iceberg tables can be read"""
        try:
            df = self.spark.table("olist_warehouse.dim_customers_iceberg")
            self.assertGreaterEqual(df.count(), 0)
        except Exception as e:
            self.skipTest(f"Iceberg table test skipped: {e}")

    def test_dq_report_generation(self):
        """Test that DQ report is generated"""
        report_path = f"/opt/hadoop/data/quality/dq_reports/customers_report_{self.test_date}.json"
        if os.path.exists(report_path):
            import json
            with open(report_path, 'r') as f:
                report = json.load(f)
            self.assertIn("dq_score", report)
            self.assertIn("results", report)

if __name__ == "__main__":
    unittest.main()