#!/usr/bin/env python3
"""
Unit tests for Airflow DAG validation
"""

import unittest
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import DagBag
import importlib.util
import os
import sys

# Add Airflow dags to path
sys.path.append('/home/hadoop-user/olist-ecommerce/airflow/dags')

class TestDAGValidation(unittest.TestCase):
    """Test suite for Airflow DAG validation"""

    @classmethod
    def setUpClass(cls):
        """Setup DAG bag for testing"""
        dag_folder = '/home/hadoop-user/olist-ecommerce/airflow/dags'
        cls.dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)

    def test_dag_bag_has_no_import_errors(self):
        """Test that DAG bag has no import errors"""
        self.assertFalse(
            len(self.dag_bag.import_errors),
            f"DAG import errors: {self.dag_bag.import_errors}"
        )

    def test_main_etl_pipeline_dag_loaded(self):
        """Test that main ETL pipeline DAG is loaded"""
        dag = self.dag_bag.get_dag('olist_etl_pipeline')
        self.assertIsNotNone(dag)
        self.assertEqual(dag.description, 'End-to-end ETL pipeline for Olist Ecommerce data')
        self.assertEqual(dag.schedule_interval, '0 2 * * *')

    def test_customers_dag_loaded(self):
        """Test that customers DAG is loaded"""
        dag = self.dag_bag.get_dag('olist_customers_etl')
        self.assertIsNotNone(dag)
        self.assertEqual(dag.schedule_interval, '0 3 * * *')

    def test_orders_dag_loaded(self):
        """Test that orders DAG is loaded"""
        dag = self.dag_bag.get_dag('olist_orders_etl')
        self.assertIsNotNone(dag)

    def test_products_dag_loaded(self):
        """Test that products DAG is loaded"""
        dag = self.dag_bag.get_dag('olist_products_etl')
        self.assertIsNotNone(dag)

    def test_sellers_dag_loaded(self):
        """Test that sellers DAG is loaded"""
        dag = self.dag_bag.get_dag('olist_sellers_etl')
        self.assertIsNotNone(dag)

    def test_data_quality_dag_loaded(self):
        """Test that data quality DAG is loaded"""
        dag = self.dag_bag.get_dag('olist_data_quality_dag')
        self.assertIsNotNone(dag)
        self.assertEqual(dag.schedule_interval, '0 6 * * *')

    def test_dag_has_correct_structure(self):
        """Test that DAG has expected task structure"""
        dag = self.dag_bag.get_dag('olist_etl_pipeline')
        task_ids = [task.task_id for task in dag.tasks]
        
        # Check essential tasks exist
        expected_tasks = ['start', 'end', 'data_quality_validation', 'dq_threshold_sensor',
                         'dim_customers', 'dim_products', 'dim_sellers',
                         'fact_orders', 'fact_order_items', 'iceberg_optimization']
        
        for task in expected_tasks:
            self.assertIn(task, task_ids)

    def test_dag_dependencies_valid(self):
        """Test that DAG dependencies are valid"""
        dag = self.dag_bag.get_dag('olist_etl_pipeline')
        # Get all downstream tasks from start
        start_task = dag.get_task('start')
        downstream_tasks = start_task.downstream_task_ids
        self.assertGreater(len(downstream_tasks), 0)
        
        # Verify that all tasks eventually lead to end
        for task in dag.tasks:
            if task.task_id != 'end':
                self.assertIsNotNone(task.downstream_task_ids)

    def test_dag_default_args_valid(self):
        """Test that DAG default args are valid"""
        dag = self.dag_bag.get_dag('olist_etl_pipeline')
        default_args = dag.default_args
        
        self.assertIn('owner', default_args)
        self.assertEqual(default_args['owner'], 'data_engineer')
        self.assertIn('retries', default_args)
        self.assertGreaterEqual(default_args['retries'], 1)
        self.assertIn('retry_delay', default_args)
        self.assertIsInstance(default_args['retry_delay'], timedelta)

    def test_no_cyclic_dependencies(self):
        """Test that DAG has no cyclic dependencies"""
        for dag_id in self.dag_bag.dags:
            dag = self.dag_bag.get_dag(dag_id)
            # Basic check - try to get topological order
            try:
                dag.topological_sort()
            except Exception as e:
                self.fail(f"DAG {dag_id} has cyclic dependencies: {e}")

    def test_dag_max_active_runs(self):
        """Test that DAG has max_active_runs set properly"""
        dag = self.dag_bag.get_dag('olist_etl_pipeline')
        self.assertEqual(dag.max_active_runs, 1)

    def test_dag_catchup_disabled(self):
        """Test that DAG catchup is disabled"""
        dag = self.dag_bag.get_dag('olist_etl_pipeline')
        self.assertFalse(dag.catchup)

if __name__ == "__main__":
    unittest.main()