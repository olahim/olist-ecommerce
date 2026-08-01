"""
Custom Airflow operator for data quality validation
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from subprocess import Popen, PIPE
import json
import yaml
import logging

class DataQualityOperator(BaseOperator):
    """
    Executes data quality checks across multiple datasets
    """
    
    template_fields = ('config_path', 'datasets', 'execution_date')
    
    @apply_defaults
    def __init__(
        self,
        config_path: str,
        datasets: list,
        execution_date: str,
        spark_submit_bin: str = '/opt/spark/bin/spark-submit',
        dq_script_path: str = '/home/hadoop-user/olist-ecommerce/spark/jobs/data_quality_checks.py',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.config_path = config_path
        self.datasets = datasets
        self.execution_date = execution_date
        self.spark_submit_bin = spark_submit_bin
        self.dq_script_path = dq_script_path
    
    def execute(self, context):
        self.log.info(f"Running DQ checks for datasets: {self.datasets}")
        results = []
        
        # Load config to get thresholds
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        sla_threshold = config.get('global', {}).get('sla_threshold', 95)
        
        for dataset in self.datasets:
            self.log.info(f"Checking dataset: {dataset}")
            
            cmd = [
                self.spark_submit_bin,
                self.dq_script_path,
                '--dataset', dataset,
                '--config', self.config_path,
                '--date', self.execution_date
            ]
            
            process = Popen(cmd, stdout=PIPE, stderr=PIPE)
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                self.log.error(f"DQ checks failed for {dataset}: {stderr.decode()}")
                raise Exception(f"DQ checks failed for {dataset}")
            
            # Read results
            report_path = f"/opt/hadoop/data/quality/dq_reports/{dataset}_report_{self.execution_date}.json"
            
            try:
                with open(report_path, 'r') as f:
                    report = json.load(f)
                    results.append(report)
                    
                    dq_score = report.get('dq_score', 0)
                    if dq_score < sla_threshold:
                        self.log.warning(f"Dataset {dataset} DQ score {dq_score}% < {sla_threshold}%")
                    else:
                        self.log.info(f"Dataset {dataset} DQ score {dq_score}% passed")
                        
            except FileNotFoundError:
                self.log.error(f"Report not found for {dataset}")
                raise Exception(f"DQ report missing for {dataset}")
        
        # Check if any dataset failed SLA
        failed = [r for r in results if r.get('dq_score', 0) < sla_threshold]
        
        if failed:
            failed_list = ', '.join([f['dataset'] for f in failed])
            self.log.error(f"Datasets failed DQ SLA: {failed_list}")
            raise Exception(f"DQ SLA failed for datasets: {failed_list}")
        
        self.log.info("All DQ checks passed successfully")
        return results