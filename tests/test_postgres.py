"""
PostgreSQL Logging Tests.
"""

import os

from sqldb_logging.handlers import SQLHandler
from .common_functions import run_logger


def test_postgres(buffer_size, flush_level):
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
        buffer_size=buffer_size,
        flush_level=flush_level,
        echo=True
    )
    rowcount = run_logger(__name__, handler)
    assert rowcount == 6
