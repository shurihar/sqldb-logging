"""
Databricks Logging Tests.
"""

import os
import sys

from sqldb_logging.handlers import SQLHandler
from .common_functions import run_logger


def test_databricks(buffer_size, flush_level):
    """Checks if SQLHandler can write logs to Databricks."""
    handler = SQLHandler(
        table=f'log_table_py_{sys.version_info.major}_{sys.version_info.minor}',
        drivername='databricks',
        username='token',
        password=os.getenv('DATABRICKS_TOKEN'),
        host=os.getenv('DATABRICKS_SERVER_HOSTNAME'),
        buffer_size=buffer_size,
        flush_level=flush_level,
        connect_args={
            'http_path': os.getenv('DATABRICKS_HTTP_PATH'),
            'catalog': 'workspace',
            'schema': 'default'
        },
        echo=True
    )
    rowcount = run_logger(__name__, handler)
    assert rowcount == 6
    with handler.engine.connect() as conn:
        conn.exec_driver_sql(f'optimize {handler.log_table.name}')
