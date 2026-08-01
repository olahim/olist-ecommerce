#!/usr/bin/env python3
"""
Unit tests for data quality rules
"""

import unittest
import yaml
import os
import json
from pyspark.sql import SparkSession

class TestDataQualityRules(unittest.TestCase):
    """Test suite for data quality rules configuration"""

    @classmethod
    def setUpClass(cls):
        """Initialize Spark session and load rules"""
        cls.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("DQRulesTest") \
            .getOrCreate()
        
        rules_path = '/home/hadoop-user/olist-ecommerce/airflow/dags/config/data_quality_rules.yaml'
        if os.path.exists(rules_path):
            with open(rules_path, 'r') as f:
                cls.rules = yaml.safe_load(f)
        else:
            cls.rules = None

    @classmethod
    def tearDownClass(cls):
        """Stop Spark session"""
        cls.spark.stop()

    def test_rules_configuration_exists(self):
        """Test that DQ rules configuration exists"""
        self.assertIsNotNone(self.rules, "DQ rules configuration not found")

    def test_global_settings_exist(self):
        """Test that global settings exist"""
        if self.rules:
            self.assertIn('global', self.rules)
            self.assertIn('sla_threshold', self.rules['global'])

    def test_customers_rules_complete(self):
        """Test that customers DQ rules are complete"""
        if self.rules and 'customers' in self.rules:
            customers_rules = self.rules['customers']
            self.assertIn('null_checks', customers_rules)
            self.assertIn('duplicate_checks', customers_rules)
            self.assertIn('format_checks', customers_rules)

    def test_orders_referential_integrity(self):
        """Test that orders have referential integrity rules"""
        if self.rules and 'orders' in self.rules:
            orders_rules = self.rules['orders']
            self.assertIn('referential_integrity', orders_rules)
            
            fk_rules = orders_rules['referential_integrity']
            self.assertTrue(any(rule['foreign_key'] == 'customer_id' for rule in fk_rules))

    def test_null_check_severity_levels(self):
        """Test that null checks have proper severity levels"""
        if self.rules:
            datasets = ['customers', 'orders', 'products', 'sellers']
            valid_severities = ['ERROR', 'WARNING', 'INFO']
            
            for dataset in datasets:
                if dataset in self.rules:
                    for rule in self.rules[dataset].get('null_checks', []):
                        self.assertIn(rule['severity'], valid_severities)

    def test_range_checks_have_bounds(self):
        """Test that range checks have min and max values"""
        if self.rules:
            for dataset in self.rules:
                if dataset != 'global':
                    for rule in self.rules[dataset].get('range_checks', []):
                        self.assertIn('min', rule)
                        self.assertIn('max', rule)

    def test_duplicate_check_columns(self):
        """Test that duplicate checks specify columns"""
        if self.rules:
            for dataset in self.rules:
                if dataset != 'global':
                    for rule in self.rules[dataset].get('duplicate_checks', []):
                        self.assertIn('columns', rule)
                        self.assertIsInstance(rule['columns'], list)

    def test_sla_threshold_reasonable(self):
        """Test that SLA threshold is reasonable"""
        if self.rules:
            threshold = self.rules['global'].get('sla_threshold', 0)
            self.assertGreaterEqual(threshold, 80)
            self.assertLessEqual(threshold, 100)

    def test_products_category_mapping(self):
        """Test that products have category mapping validation"""
        if self.rules and 'products' in self.rules:
            products_rules = self.rules['products']
            fk_rules = products_rules.get('referential_integrity', [])
            category_fk = [r for r in fk_rules if r['foreign_key'] == 'product_category_name']
            self.assertTrue(len(category_fk) > 0)

    def test_order_items_multiple_fks(self):
        """Test that order_items has multiple foreign key checks"""
        if self.rules and 'order_items' in self.rules:
            items_rules = self.rules['order_items']
            fk_rules = items_rules.get('referential_integrity', [])
            fk_columns = [rule['foreign_key'] for rule in fk_rules]
            
            self.assertIn('order_id', fk_columns)
            self.assertIn('product_id', fk_columns)
            self.assertIn('seller_id', fk_columns)

    def test_reviews_score_range(self):
        """Test that review score has proper range"""
        if self.rules and 'reviews' in self.rules:
            reviews_rules = self.rules['reviews']
            range_checks = reviews_rules.get('range_checks', [])
            review_range = [r for r in range_checks if r['column'] == 'review_score']
            
            if review_range:
                self.assertEqual(review_range[0]['min'], 1)
                self.assertEqual(review_range[0]['max'], 5)

if __name__ == "__main__":
    unittest.main()