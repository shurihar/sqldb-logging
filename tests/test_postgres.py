"""
PostgreSQL Logging Tests.
"""

import logging
import os

from sqldb_logging.handlers import SQLHandler
from .common_functions import run_logger


def test_postgres():
    """Checks whether SQLHandler can write logs to the PostgreSQL database."""
    handler = SQLHandler(
        table='log_table',
        drivername='postgresql+psycopg',
        username='postgres',
        password=os.getenv('POSTGRES_PASSWORD'),
        host='localhost',
        port=5432,
        database='postgres',
        schema='public',
        buffer_size=10,
        flush_level=logging.CRITICAL,
        echo=True
    )
    rowcount = run_logger(__name__, handler)
    assert rowcount == 6
