"""
Custom Airflow operator for Apache Iceberg operations
Supports table maintenance, optimization, and DDL operations
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from subprocess import Popen, PIPE
import logging

class IcebergOperator(BaseOperator):
    """
    Operator for executing Iceberg table operations via Spark
    """
    
    template_fields = ('table_name', 'operation', 'catalog_name', 'warehouse_path')
    
    @apply_defaults
    def __init__(
        self,
        table_name: str,
        operation: str,
        catalog_name: str = 'spark_catalog',
        warehouse_path: str = '/opt/hadoop/data/warehouse/iceberg',
        spark_submit_bin: str = '/opt/spark/bin/spark-submit',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.operation = operation
        self.catalog_name = catalog_name
        self.warehouse_path = warehouse_path
        self.spark_submit_bin = spark_submit_bin
    
    def execute(self, context):
        self.log.info(f"Executing Iceberg operation: {self.operation} on table: {self.table_name}")
        
        # Build Spark SQL command based on operation type
        spark_sql = self._build_spark_sql()
        
        # Build spark-submit command
        cmd = [
            self.spark_submit_bin,
            '--conf', 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            '--conf', f'spark.sql.catalog.{self.catalog_name}=org.apache.iceberg.spark.SparkCatalog',
            '--conf', f'spark.sql.catalog.{self.catalog_name}.type=hive',
            '--conf', f'spark.sql.catalog.{self.catalog_name}.warehouse={self.warehouse_path}',
            '--class', 'org.apache.spark.sql.SparkSession',
            self._create_temp_script(spark_sql)
        ]
        
        process = Popen(cmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            self.log.error(f"Iceberg operation failed: {stderr.decode()}")
            raise Exception(f"Iceberg {self.operation} failed on {self.table_name}")
        
        self.log.info(f"Iceberg operation completed successfully: {stdout.decode()}")
        return True
    
    def _build_spark_sql(self):
        """Build Spark SQL for the requested operation"""
        operations = {
            'rewrite_data_files': f"CALL {self.catalog_name}.system.rewrite_data_files(table => '{self.table_name}')",
            'expire_snapshots': f"CALL {self.catalog_name}.system.expire_snapshots(table => '{self.table_name}', retain_last => 5)",
            'rewrite_manifests': f"CALL {self.catalog_name}.system.rewrite_manifests(table => '{self.table_name}')",
            'migrate_to_v2': f"ALTER TABLE {self.table_name} SET TBLPROPERTIES ('format-version' = '2')",
            'compact': self._build_compact_sql(),
        }
        return operations.get(self.operation, f"SHOW TABLES IN {self.catalog_name}")
    
    def _build_compact_sql(self):
        """Build compaction SQL with optimal settings"""
        return f"""
        CALL {self.catalog_name}.system.rewrite_data_files(
            table => '{self.table_name}',
            strategy => 'binpack',
            options => map('min-input-files', '5', 'target-file-size-bytes', '536870912')
        )
        """
    
    def _create_temp_script(self, spark_sql):
        """Create temporary Spark SQL script"""
        import tempfile
        script_content = f'''
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sql("{spark_sql}")
spark.stop()
'''
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(script_content)
            return f.name