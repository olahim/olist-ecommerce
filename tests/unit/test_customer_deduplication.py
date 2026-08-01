#!/usr/bin/env python3
"""
Unit tests for customer deduplication logic
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class TestCustomerDeduplication(unittest.TestCase):
    """Test suite for customer deduplication"""

    @classmethod
    def setUpClass(cls):
        """Initialize Spark session for testing"""
        cls.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("CustomerDeduplicationTest") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Stop Spark session"""
        cls.spark.stop()

    def create_test_data(self):
        """Create test customer data"""
        return self.spark.createDataFrame([
            # Customer with multiple records (duplicates)
            ("cust1", "unique1", "12345", "Sao Paulo", "SP", "2024-01-01", "file1.csv"),
            ("cust1", "unique1", "12345", "Sao Paulo", "SP", "2024-01-15", "file2.csv"),
            ("cust1", "unique1", "12345", "Sao Paulo", "SP", "2024-02-01", "file3.csv"),
            # Customer with changed address
            ("cust2", "unique2", "54321", "Rio", "RJ", "2024-01-10", "file1.csv"),
            ("cust2", "unique2", "98765", "Rio", "RJ", "2024-02-01", "file2.csv"),
            # Unique customer
            ("cust3", "unique3", "11111", "Belo Horizonte", "MG", "2024-01-20", "file1.csv"),
        ], ["customer_id", "customer_unique_id", "zip_code", "city", "state", 
            "ingestion_date", "source_file"])

    def test_remove_duplicate_customer_id(self):
        """Test removing duplicates by customer_id"""
        test_data = self.create_test_data()
        
        window_spec = Window.partitionBy("customer_id").orderBy(F.desc("ingestion_date"))
        deduplicated = test_data.withColumn("row_num", F.row_number().over(window_spec)) \
                                .filter(F.col("row_num") == 1) \
                                .drop("row_num")
        
        # Should have 3 unique customers (cust1, cust2, cust3)
        self.assertEqual(deduplicated.count(), 3)
        
        # Verify we kept the latest record for cust1
        cust1 = deduplicated.filter(F.col("customer_id") == "cust1").first()
        self.assertEqual(cust1["ingestion_date"], "2024-02-01")

    def test_unique_customer_id_should_be_unique(self):
        """Test that customer_unique_id should be unique"""
        test_data = self.create_test_data()
        
        # Check for duplicates in customer_unique_id
        duplicates = test_data.groupBy("customer_unique_id").count().filter(F.col("count") > 1)
        
        # In test data, unique1 appears multiple times (cust1 records)
        self.assertEqual(duplicates.count(), 1)
        
        # Deduplicate by customer_unique_id
        window_spec = Window.partitionBy("customer_unique_id").orderBy(F.desc("ingestion_date"))
        deduplicated = test_data.withColumn("row_num", F.row_number().over(window_spec)) \
                                .filter(F.col("row_num") == 1) \
                                .drop("row_num")
        
        self.assertEqual(deduplicated.count(), 3)

    def test_scd_type_2_change_detection(self):
        """Test SCD Type 2 change detection"""
        test_data = self.create_test_data()
        
        # Get current state (latest record for each customer_id)
        window_spec = Window.partitionBy("customer_id").orderBy(F.desc("ingestion_date"))
        current_state = test_data.withColumn("row_num", F.row_number().over(window_spec)) \
                                 .filter(F.col("row_num") == 1) \
                                 .drop("row_num")
        
        # For cust2, compare current vs previous to detect change
        cust2_records = test_data.filter(F.col("customer_id") == "cust2") \
                                 .orderBy(F.asc("ingestion_date"))
        rows = cust2_records.collect()
        
        if len(rows) >= 2:
            # Check if zip_code changed
            changed = rows[0]["zip_code"] != rows[1]["zip_code"]
            self.assertTrue(changed)

    def test_surrogate_key_generation(self):
        """Test surrogate key generation for SCD"""
        test_data = self.create_test_data().distinct()
        
        # Generate surrogate keys
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        
        window_spec = Window.orderBy("customer_unique_id")
        with_surrogate = test_data.withColumn("customer_sk", 
                                               F.row_number().over(window_spec) + 1000000)
        
        # Each record should have a unique surrogate key
        self.assertEqual(with_surrogate.select("customer_sk").distinct().count(), 
                        with_surrogate.count())

    def test_customer_segment_calculation(self):
        """Test customer segment calculation based on lifetime value"""
        customers = self.spark.createDataFrame([
            ("cust1", 15000.0, 10),  # VIP
            ("cust2", 8000.0, 6),    # High Value
            ("cust3", 3000.0, 4),    # Regular
            ("cust4", 500.0, 1),     # Low Value
        ], ["customer_id", "lifetime_value", "total_orders"])
        
        from pyspark.sql import functions as F
        
        segmented = customers.withColumn("customer_segment",
            F.when(F.col("lifetime_value") >= 10000, "VIP")
             .when(F.col("lifetime_value") >= 5000, "High Value")
             .when(F.col("lifetime_value") >= 1000, "Regular")
             .otherwise("Low Value"))
        
        segments = segmented.select("customer_id", "customer_segment").collect()
        segments_dict = {row["customer_id"]: row["customer_segment"] for row in segments}
        
        self.assertEqual(segments_dict["cust1"], "VIP")
        self.assertEqual(segments_dict["cust2"], "High Value")
        self.assertEqual(segments_dict["cust3"], "Regular")
        self.assertEqual(segments_dict["cust4"], "Low Value")

    def test_valid_zip_code_format(self):
        """Test zip code format validation"""
        zip_codes = self.spark.createDataFrame([
            ("12345",),
            ("1234",),
            ("abcde",),
            (None,),
        ], ["zip_code"])
        
        from pyspark.sql import functions as F
        
        validated = zip_codes.withColumn("is_valid",
            F.when(F.col("zip_code").rlike("^[0-9]{5}$"), True)
             .otherwise(False))
        
        results = validated.collect()
        self.assertTrue(results[0]["is_valid"])
        self.assertFalse(results[1]["is_valid"])
        self.assertFalse(results[2]["is_valid"])
        self.assertFalse(results[3]["is_valid"])

if __name__ == "__main__":
    unittest.main()
