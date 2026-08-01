"""
Sensor that waits for DQ score to meet threshold
"""

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import json
import os
import logging

class DQThresholdSensor(BaseSensorOperator):
    """
    Waits for data quality score to meet or exceed threshold
    """
    
    template_fields = ('dq_score_threshold', 'execution_date')
    
    @apply_defaults
    def __init__(
        self,
        dq_score_threshold: float = 95.0,
        dq_reports_path: str = '/opt/hadoop/data/quality/dq_reports/',
        datasets: list = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.dq_score_threshold = dq_score_threshold
        self.dq_reports_path = dq_reports_path
        self.datasets = datasets or [
            'customers', 'orders', 'products', 'sellers', 
            'geolocation', 'reviews', 'payments', 'order_items'
        ]
    
    def poke(self, context):
        """Check if all datasets meet DQ threshold"""
        execution_date = context['ds']
        all_passed = True
        
        for dataset in self.datasets:
            report_file = f"{self.dq_reports_path}/{dataset}_report_{execution_date}.json"
            
            if not os.path.exists(report_file):
                self.log.info(f"Report not yet available for {dataset}")
                return False
            
            with open(report_file, 'r') as f:
                report = json.load(f)
            
            dq_score = report.get('dq_score', 0)
            
            if dq_score < self.dq_score_threshold:
                self.log.warning(f"DQ score for {dataset} is {dq_score:.2f}% < {self.dq_score_threshold}%")
                all_passed = False
            else:
                self.log.info(f"DQ score for {dataset} is {dq_score:.2f}% - OK")
        
        if all_passed:
            self.log.info("All datasets met DQ threshold")
        return all_passed
