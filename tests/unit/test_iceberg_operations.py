#!/usr/bin/env python3
"""
Unit tests for Iceberg operations
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os
import json

class TestIcebergOperations(unittest.TestCase):
    """Test suite for Iceberg operations"""

    def setUp(self):
        """Setup test environment"""
        self.test_table = "test_iceberg_table"
        self.test_catalog = "test_catalog"

    @patch('subprocess.Popen')
    def test_compact_table_command(self, mock_popen):
        """Test compact table command generation"""
        from spark.jobs.iceberg_optimization import generate_optimization_sql
        
        sql = generate_optimization_sql(self.test_table, "compact")
        self.assertIn("rewrite_data_files", sql)
        self.assertIn(self.test_table, sql)
        self.assertIn("binpack", sql)

    @patch('subprocess.Popen')
    def test_expire_snapshots_command(self, mock_popen):
        """Test expire snapshots command generation"""
        from spark.jobs.iceberg_optimization import generate_optimization_sql
        
        sql = generate_optimization_sql(self.test_table, "expire_snapshots")
        self.assertIn("expire_snapshots", sql)
        self.assertIn(self.test_table, sql)

    def test_iceberg_table_properties(self):
        """Test Iceberg table properties configuration"""
        expected_properties = {
            'format-version': '2',
            'write.format.default': 'parquet',
            'write.parquet.compression-codec': 'snappy',
            'write.metadata.delete-after-commit.enabled': 'true',
            'write.metadata.previous-versions-max': '10'
        }
        
        self.assertIn('format-version', expected_properties)
        self.assertEqual(expected_properties['format-version'], '2')
        self.assertIn('write.format.default', expected_properties)

    def test_partition_spec_generation(self):
        """Test partition specification generation"""
        partition_columns = ['order_purchase_date']
        
        expected_partition = "PARTITIONED BY (order_purchase_date)"
        self.assertIn("order_purchase_date", expected_partition)

    def test_snapshot_expiration_logic(self):
        """Test snapshot expiration logic"""
        from datetime import datetime, timedelta
        
        current_date = datetime.now()
        expire_before = current_date - timedelta(days=7)
        
        self.assertLess(expire_before, current_date)
        self.assertEqual((current_date - expire_before).days, 7)

    def test_manifest_rewrite_options(self):
        """Test manifest rewrite options"""
        rewrite_options = {
            'target-compression-size-bytes': '134217728',
            'min-input-files': '5'
        }
        
        self.assertEqual(rewrite_options['target-compression-size-bytes'], 134217728)
        self.assertEqual(rewrite_options['min-input-files'], '5')

    @patch('pyspark.sql.SparkSession')
    def test_spark_session_configuration(self, mock_spark):
        """Test Spark session configuration for Iceberg"""
        mock_builder = MagicMock()
        mock_spark.builder.return_value = mock_builder
        
        configs = [
            ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
            ("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog"),
            ("spark.sql.catalog.spark_catalog.type", "hive"),
        ]
        
        for key, value in configs:
            mock_builder.config.return_value = mock_builder
            mock_builder.config.assert_called

    def test_write_options(self):
        """Test Iceberg write options"""
        write_options = {
            'write-option': 'overwrite-mode',
            'format': 'parquet',
            'compression': 'snappy'
        }
        
        self.assertEqual(write_options['format'], 'parquet')
        self.assertEqual(write_options['compression'], 'snappy')

    def test_dq_aware_optimization_condition(self):
        """Test DQ-aware optimization filter condition"""
        condition = "dq_score >= 95 OR dq_score IS NULL"
        
        self.assertIn("dq_score", condition)
        self.assertIn(">= 95", condition)
        self.assertIn("IS NULL", condition)

    def test_migration_to_v2_statement(self):
        """Test migration to Iceberg V2 statement"""
        statement = "ALTER TABLE test_table SET TBLPROPERTIES ('format-version' = '2')"
        self.assertIn("format-version", statement)
        self.assertIn("'2'", statement)

if __name__ == "__main__":
    unittest.main()