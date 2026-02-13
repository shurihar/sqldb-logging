"""
MySQL Logging Tests.
"""

import os

from sqldb_logging.handlers import SQLHandler
from .common_functions import run_logger


def test_mysql(buffer_size, flush_level):
    """Checks whether SQLHandler can write logs to the MySQL database."""
    handler = SQLHandler(
        table='log_table',
        drivername='mysql+mysqldb',
        username='root',
        password=os.getenv('MYSQL_ROOT_PASSWORD'),
        host='localhost',
        port=3306,
        database='mysqldb',
        buffer_size=buffer_size,
        flush_level=flush_level,
        echo=True
    )
    rowcount = run_logger(__name__, handler)
    assert rowcount == 6
