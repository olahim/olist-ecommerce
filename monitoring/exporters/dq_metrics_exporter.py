#!/usr/bin/env python3
"""
DQ Metrics Prometheus Exporter
Exports data quality metrics to Prometheus
"""

import json
import os
import time
from prometheus_client import start_http_server, Gauge, Counter, Info
import glob
from datetime import datetime

# Metrics definitions
DQ_SCORE = Gauge('dq_score_current', 'Current DQ score', ['dataset'])
DQ_SCORE_DAILY = Gauge('dq_score_daily', 'Daily DQ score', ['dataset', 'date'])
FAILED_RECORDS = Gauge('failed_records_count', 'Failed records count', ['dataset'])
DQ_CHECKS_TOTAL = Counter('dq_checks_total', 'Total DQ checks', ['dataset', 'check_type'])
DQ_CHECKS_FAILED = Counter('dq_checks_failed', 'Failed DQ checks', ['dataset', 'check_type'])
LATEST_DATA_TIMESTAMP = Gauge('latest_data_timestamp', 'Timestamp of latest data ingestion')

def load_latest_dq_metrics():
    """Load latest DQ metrics from report files"""
    reports_dir = '/opt/hadoop/data/quality/dq_reports/'
    datasets = ['customers', 'orders', 'products', 'sellers', 'order_items']
    
    for dataset in datasets:
        # Find latest report for this dataset
        pattern = f"{reports_dir}/{dataset}_report_*.json"
        files = glob.glob(pattern)
        
        if files:
            latest_file = max(files, key=os.path.getctime)
            with open(latest_file, 'r') as f:
                report = json.load(f)
            
            dq_score = report.get('dq_score', 0)
            DQ_SCORE.labels(dataset=dataset).set(dq_score)
            
            date = report.get('date', datetime.now().strftime('%Y-%m-%d'))
            DQ_SCORE_DAILY.labels(dataset=dataset, date=date).set(dq_score)
            
            # Update counters
            for result in report.get('results', []):
                check_type = result.get('check_type', 'unknown')
                DQ_CHECKS_TOTAL.labels(dataset=dataset, check_type=check_type).inc()
                if not result.get('passed', True):
                    DQ_CHECKS_FAILED.labels(dataset=dataset, check_type=check_type).inc()

def load_failed_records():
    """Count failed records in quarantine"""
    failed_dir = '/opt/hadoop/data/quality/failed_records/'
    datasets = ['customers', 'orders', 'products', 'sellers']
    
    for dataset in datasets:
        dataset_dir = os.path.join(failed_dir, dataset)
        if os.path.exists(dataset_dir):
            file_count = len([f for f in os.listdir(dataset_dir) if f.endswith('.parquet')])
            FAILED_RECORDS.labels(dataset=dataset).set(file_count)

def update_data_timestamp():
    """Update latest data timestamp"""
    # Look for latest ingestion file
    staging_dir = '/opt/hadoop/data/staging/'
    latest_time = 0
    
    for root, dirs, files in os.walk(staging_dir):
        if '_SUCCESS' in files:
            mtime = os.path.getmtime(os.path.join(root, '_SUCCESS'))
            if mtime > latest_time:
                latest_time = mtime
    
    if latest_time > 0:
        LATEST_DATA_TIMESTAMP.set(latest_time)

def main():
    """Start Prometheus exporter"""
    start_http_server(9103)
    print("DQ Metrics exporter started on port 9103")
    
    while True:
        load_latest_dq_metrics()
        load_failed_records()
        update_data_timestamp()
        time.sleep(60)

if __name__ == "__main__":
    main()
