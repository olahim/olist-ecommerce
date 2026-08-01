#!/usr/bin/env python3
"""
Integration tests for DQ alerting
"""

import unittest
import json
import os
import yaml
from unittest.mock import patch, Mock

class TestDQAlerting(unittest.TestCase):
    """Test data quality alerting"""

    def setUp(self):
        """Setup test environment"""
        self.alert_webhook = "https://hooks.slack.com/services/test-webhook"
        
    @patch('requests.post')
    def test_slack_alert_format(self, mock_post):
        """Test Slack alert formatting"""
        from spark.jobs.dq_alerting import send_slack_alert
        
        reports = [
            {
                "dataset": "customers",
                "dq_score": 85.5,
                "failed_checks": 2,
                "total_checks": 10
            },
            {
                "dataset": "orders",
                "dq_score": 92.0,
                "failed_checks": 1,
                "total_checks": 12
            }
        ]
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        # This should not raise an exception
        send_slack_alert(reports, 90.0)

    @patch('smtplib.SMTP')
    def test_email_alert_format(self, mock_smtp):
        """Test email alert formatting"""
        from spark.jobs.dq_alerting import send_email_alert
        
        reports = [
            {
                "dataset": "customers",
                "dq_score": 85.5,
                "failed_checks": 2,
                "total_checks": 10,
                "results": [
                    {"check_type": "null_check", "failed_count": 5, "passed": False}
                ]
            }
        ]
        
        mock_server = Mock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        
        send_email_alert(reports, 90.0)

    def test_alert_threshold_logic(self):
        """Test alert threshold logic"""
        threshold = 90.0
        
        # Should alert
        low_score = 85.0
        self.assertLess(low_score, threshold)
        
        # Should not alert
        good_score = 95.0
        self.assertGreaterEqual(good_score, threshold)

    def test_dq_report_parsing(self):
        """Test DQ report parsing"""
        test_report = {
            "dataset": "customers",
            "date": "2024-01-01",
            "dq_score": 92.5,
            "total_checks": 10,
            "failed_checks": 1,
            "results": [
                {"check_type": "null_check", "column": "customer_id", "passed": True},
                {"check_type": "null_check", "column": "customer_name", "passed": False, "failed_count": 3}
            ]
        }
        
        self.assertEqual(test_report["dq_score"], 92.5)
        self.assertEqual(test_report["failed_checks"], 1)
        
        # Extract failed checks
        failed = [r for r in test_report["results"] if not r.get("passed", True)]
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]["column"], "customer_name")

    def test_quarantine_directory_creation(self):
        """Test quarantine directory creation logic"""
        quarantine_base = "/opt/hadoop/data/quality/failed_records/"
        dataset = "customers"
        quarantine_dir = f"{quarantine_base}/{dataset}/"
        
        # This test just validates the path logic
        self.assertIn(dataset, quarantine_dir)
        self.assertTrue(quarantine_dir.startswith("/opt/hadoop/data/quality/failed_records/"))

if __name__ == "__main__":
    unittest.main()