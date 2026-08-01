"""
Custom Airflow operator for Trino query execution
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import logging

class TrinoOperator(BaseOperator):
    """
    Operator for executing SQL queries on Trino
    """
    
    template_fields = ('sql', 'catalog', 'schema')
    
    @apply_defaults
    def __init__(
        self,
        sql: str,
        catalog: str = 'iceberg',
        schema: str = 'olist_warehouse',
        host: str = 'localhost',
        port: int = 8080,
        user: str = 'airflow',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.catalog = catalog
        self.schema = schema
        self.host = host
        self.port = port
        self.user = user
    
    def execute(self, context):
        self.log.info(f"Executing Trino query: {self.sql[:100]}...")
        
        conn = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
        )
        
        cursor = conn.cursor()
        
        try:
            cursor.execute(self.sql)
            results = cursor.fetchall()
            self.log.info(f"Query executed successfully, returned {len(results)} rows")
            return results
        except Exception as e:
            self.log.error(f"Trino query failed: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()