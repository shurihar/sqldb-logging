"""
Databricks Logging Tests.
"""

import logging
import os
import sys

from sqldb_logging.handlers import SQLHandler
from .common_functions import run_logger


def test_databricks():
    """Checks if SQLHandler can write logs to Databricks."""
    handler = SQLHandler(
        table=f'log_table_py_{sys.version_info.major}_{sys.version_info.minor}',
        drivername='databricks',
        username='token',
        password=os.getenv('DATABRICKS_TOKEN'),
        host=os.getenv('DATABRICKS_SERVER_HOSTNAME'),
        buffer_size=10,  # this doesn't seem to work with Databricks (each record is committed in a separate txn)
        flush_level=logging.CRITICAL,
        connect_args={
            'http_path': os.getenv('DATABRICKS_HTTP_PATH'),
            'catalog': 'workspace',
            'schema': 'default'
        },
        echo=True
    )
    run_logger(__name__, handler)
