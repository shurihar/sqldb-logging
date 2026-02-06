"""
MySQL Logging Tests.
"""

import logging
import os

from tests.common_functions import run_logger
from sqldb_logging.handlers import SQLHandler


def test_mysql():
    """Checks whether SQLHandler can write logs to the MySQL database."""
    handler = SQLHandler(
        table='log_table',
        drivername='mysql+mysqldb',
        username='root',
        password=os.getenv('MYSQL_PASSWORD'),
        host='localhost',
        port=3306,
        database='mysqldb',
        buffer_size=10,
        flush_level=logging.CRITICAL,
        echo=True
    )
    run_logger(__name__, handler)
