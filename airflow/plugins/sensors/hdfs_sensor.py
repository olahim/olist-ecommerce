"""
Custom Airflow sensor for HDFS file detection
"""

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from subprocess import Popen, PIPE
import logging

class HDFSSensor(BaseSensorOperator):
    """
    Sensor that waits for a file/directory to appear in HDFS
    """
    
    template_fields = ('filepath',)
    
    @apply_defaults
    def __init__(
        self,
        filepath: str,
        hdfs_command: str = 'hdfs',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.filepath = filepath
        self.hdfs_command = hdfs_command
    
    def poke(self, context):
        """Check if file exists in HDFS"""
        cmd = [self.hdfs_command, 'dfs', '-test', '-e', self.filepath]
        
        process = Popen(cmd, stdout=PIPE, stderr=PIPE)
        process.communicate()
        
        exists = process.returncode == 0
        
        if exists:
            self.log.info(f"File exists: {self.filepath}")
        else:
            self.log.info(f"Waiting for file: {self.filepath}")
        
        return exists
