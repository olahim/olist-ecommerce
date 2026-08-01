#!/usr/bin/env python3
"""
Unit tests for Spark jobs
Tests individual functions and transformations
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pandas as pd
import tempfile
import os

class TestSparkJobs(unittest.TestCase):
    """Test suite for Spark job functions"""

    @classmethod
    def setUpClass(cls):
        """Initialize Spark session for testing"""
        cls.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("UnitTest") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Stop Spark session"""
        cls.spark.stop()

    def test_customers_ingestion_schema(self):
        """Test customers ingestion schema definition"""
        from spark.jobs.customers_ingestion import get_schema
        
        schema = get_schema()
        self.assertIsNotNone(schema)
        self.assertEqual(len(schema.fields), 5)
        
        field_names = [f.name for f in schema.fields]
        expected_fields = ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 
                          'customer_city', 'customer_state']
        self.assertListEqual(field_names, expected_fields)

    def test_customers_cleaning_logic(self):
        """Test customer data cleaning logic"""
        from spark.jobs.customers_ingestion import clean_data
        
        # Create test data
        test_data = self.spark.createDataFrame([
            ("  abc123  ", "  def456  ", "01234", "  Sao Paulo  ", "  SP  "),
            (None, "ghi789", "abcde", "Rio de Janeiro", "RJ"),
        ], ["customer_id", "customer_unique_id", "customer_zip_code_prefix", 
            "customer_city", "customer_state"])
        
        cleaned_df = clean_data(test_data)
        
        # Check trimming
        row = cleaned_df.first()
        self.assertEqual(row.customer_id, "abc123")
        self.assertEqual(row.customer_city, "SAO PAULO")
        self.assertEqual(row.customer_state, "SP")
        
        # Check zip code cleaning (remove non-digits)
        rows = cleaned_df.collect()
        self.assertEqual(rows[1].customer_zip_code_prefix, "")
        self.assertIsNone(rows[1].customer_zip_code_prefix)

    def test_data_quality_null_checks(self):
        """Test null check function"""
        from spark.jobs.data_quality_checks import check_null_conditions
        
        test_data = self.spark.createDataFrame([
            ("id1", "name1", 100),
            ("id2", None, 200),
            (None, "name3", None),
        ], ["id", "name", "value"])
        
        result = check_null_conditions(test_data, "id", "ERROR", "test_dataset")
        self.assertEqual(result["failed_count"], 1)
        self.assertFalse(result["passed"])
        
        result = check_null_conditions(test_data, "name", "WARNING", "test_dataset")
        self.assertEqual(result["failed_count"], 1)
        self.assertFalse(result["passed"])
        
        result = check_null_conditions(test_data.filter("id IS NOT NULL"), "id", "ERROR", "test_dataset")
        self.assertTrue(result["passed"])

    def test_data_quality_duplicate_checks(self):
        """Test duplicate detection function"""
        from spark.jobs.data_quality_checks import check_duplicates
        
        test_data = self.spark.createDataFrame([
            ("id1", "val1"),
            ("id1", "val1"),  # Duplicate
            ("id2", "val2"),
            ("id3", "val3"),
        ], ["id", "value"])
        
        result = check_duplicates(test_data, ["id"], "ERROR", "test_dataset")
        self.assertEqual(result["failed_count"], 1)
        self.assertFalse(result["passed"])
        
        # No duplicates
        unique_data = test_data.distinct()
        result = check_duplicates(unique_data, ["id"], "ERROR", "test_dataset")
        self.assertEqual(result["failed_count"], 0)
        self.assertTrue(result["passed"])

    def test_data_quality_range_checks(self):
        """Test range validation function"""
        from spark.jobs.data_quality_checks import check_range
        
        test_data = self.spark.createDataFrame([
            (10,),
            (25,),
            (100,),
            (150,),
        ], ["value"])
        
        # Values should be between 0 and 100
        result = check_range(test_data, "value", 0, 100, "ERROR", "test_dataset")
        self.assertEqual(result["failed_count"], 1)
        self.assertFalse(result["passed"])
        
        # All values in range
        result = check_range(test_data.filter("value <= 100"), "value", 0, 100, "ERROR", "test_dataset")
        self.assertTrue(result["passed"])

    def test_dq_score_calculation(self):
        """Test DQ score calculation"""
        from spark.jobs.data_quality_checks import calculate_dq_score
        
        results = [
            {"passed": True, "dataset": "test", "weight": 1.0},
            {"passed": True, "dataset": "test", "weight": 1.0},
            {"passed": False, "dataset": "test", "weight": 1.0},
        ]
        
        score = calculate_dq_score(results, {"default": 1.0})
        self.assertEqual(score, 66.66666666666666)
        
        # Test with weights
        results_weighted = [
            {"passed": True, "dataset": "important", "weight": 2.0},
            {"passed": False, "dataset": "less_important", "weight": 1.0},
        ]
        
        score = calculate_dq_score(results_weighted, {"important": 2.0, "less_important": 1.0})
        self.assertEqual(score, 66.66666666666666)

    def test_customer_deduplication(self):
        """Test customer deduplication logic"""
        test_data = self.spark.createDataFrame([
            ("customer1", "unique1", "zip1", "city1", "state1", "2024-01-01", "file1"),
            ("customer1", "unique1", "zip1", "city1", "state1", "2024-01-02", "file2"),  # Duplicate customer_id
            ("customer2", "unique2", "zip2", "city2", "state2", "2024-01-01", "file1"),
        ], ["customer_id", "customer_unique_id", "customer_zip_code_prefix", 
            "customer_city", "customer_state", "ingestion_date", "source_file"])
        
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy("customer_id").orderBy(F.desc("ingestion_date"))
        deduplicated = test_data.withColumn("row_num", F.row_number().over(window_spec)) \
                                .filter(F.col("row_num") == 1) \
                                .drop("row_num")
        
        self.assertEqual(deduplicated.count(), 2)

    def test_referential_integrity(self):
        """Test referential integrity validation"""
        from spark.jobs.data_quality_checks import check_referential_integrity
        
        orders = self.spark.createDataFrame([
            ("order1", "valid_cust"),
            ("order2", "valid_cust"),
            ("order3", "invalid_cust"),
        ], ["order_id", "customer_id"])
        
        customers = self.spark.createDataFrame([
            ("valid_cust",),
        ], ["customer_id"])
        
        result = check_referential_integrity(orders, "customer_id", customers, "customer_id", 
                                              "ERROR", "orders")
        self.assertEqual(result["failed_count"], 1)
        self.assertFalse(result["passed"])

    def test_iceberg_optimization_sql_generation(self):
        """Test Iceberg optimization SQL generation"""
        from spark.jobs.iceberg_optimization import generate_optimization_sql
        
        sql = generate_optimization_sql("test_table", "compact")
        self.assertIn("rewrite_data_files", sql)
        self.assertIn("test_table", sql)
        
        sql = generate_optimization_sql("test_table", "expire_snapshots")
        self.assertIn("expire_snapshots", sql)

    def test_category_translation_join(self):
        """Test product category translation join logic"""
        products = self.spark.createDataFrame([
            ("product1", "electronics"),
            ("product2", "clothing"),
            ("product3", "unknown"),
        ], ["product_id", "category_pt"])
        
        translation = self.spark.createDataFrame([
            ("electronics", "Electronics"),
            ("clothing", "Clothing"),
        ], ["category_pt", "category_en"])
        
        from pyspark.sql import functions as F
        joined = products.join(translation, on="category_pt", how="left")
        
        self.assertEqual(joined.count(), 3)
        results = joined.collect()
        self.assertEqual(results[0]["category_en"], "Electronics")
        self.assertIsNone(results[2]["category_en"])  # Unknown category

    def test_fact_order_calculations(self):
        """Test fact order calculations"""
        order_items = self.spark.createDataFrame([
            ("order1", "item1", 100.0, 10.0, 2),
            ("order1", "item2", 50.0, 5.0, 1),
            ("order2", "item3", 200.0, 20.0, 1),
        ], ["order_id", "item_id", "price", "freight", "quantity"])
        
        from pyspark.sql import functions as F
        order_totals = order_items.groupBy("order_id").agg(
            F.sum("price").alias("total_value"),
            F.sum("freight").alias("total_freight"),
            F.count("item_id").alias("item_count")
        )
        
        self.assertEqual(order_totals.count(), 2)
        
        order1 = order_totals.filter(F.col("order_id") == "order1").first()
        self.assertEqual(order1["total_value"], 150.0)
        self.assertEqual(order1["total_freight"], 15.0)
        self.assertEqual(order1["item_count"], 2)

if __name__ == "__main__":
    unittest.main()