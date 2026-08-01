"""
Custom Airflow sensor for S3 file detection
"""

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import boto3
from botocore.exceptions import ClientError
import logging

class S3Sensor(BaseSensorOperator):
    """
    Sensor that waits for a file to appear in S3
    """
    
    template_fields = ('bucket', 'key')
    
    @apply_defaults
    def __init__(
        self,
        bucket: str,
        key: str,
        aws_conn_id: str = 'aws_default',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.key = key
        self.aws_conn_id = aws_conn_id
        
        # Initialize S3 client (connection would be resolved from Airflow)
        self.s3_client = boto3.client('s3')
    
    def poke(self, context):
        """Check if object exists in S3"""
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=self.key)
            self.log.info(f"File exists: s3://{self.bucket}/{self.key}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.log.info(f"Waiting for file: s3://{self.bucket}/{self.key}")
                return False
            else:
                self.log.error(f"Error checking S3: {str(e)}")
                return False