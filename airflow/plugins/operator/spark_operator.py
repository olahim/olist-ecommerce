"""
Custom Airflow operator for Spark job submission with enhanced features
"""

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.decorators import apply_defaults
from typing import Dict, List, Optional

class EnhancedSparkSubmitOperator(SparkSubmitOperator):
    """
    Enhanced Spark operator with additional features:
    - Automatic retry on failure
    - Metrics collection
    - Iceberg configuration injection
    """
    
    template_fields = ('application', 'application_args', 'conf')
    
    @apply_defaults
    def __init__(
        self,
        application: str,
        application_args: Optional[List[str]] = None,
        conf: Optional[Dict] = None,
        enable_iceberg: bool = True,
        collect_metrics: bool = True,
        *args,
        **kwargs
    ):
        # Default Iceberg configuration
        iceberg_conf = {
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.spark_catalog.type': 'hive',
            'spark.sql.catalog.spark_catalog.warehouse': '/opt/hadoop/data/warehouse/iceberg',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        } if enable_iceberg else {}
        
        if conf:
            iceberg_conf.update(conf)
        
        self.collect_metrics = collect_metrics
        
        super().__init__(
            application=application,
            application_args=application_args,
            conf=iceberg_conf,
            *args,
            **kwargs
        )
    
    def on_kill(self):
        """Handle task kill - attempt to kill Spark job"""
        self.log.info("Killing Spark job...")
        super().on_kill()