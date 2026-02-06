"""
SQLite Logging Tests.
"""

import logging
import os

from sqldb_logging.handlers import SQLHandler
from .common_functions import run_logger


def test_sqlite(tmp_path):
    """Checks whether the SQLHandler can write logs to the SQLite database."""
    handler = SQLHandler(
        table='log_table',
        drivername='sqlite',
        database=os.path.join(tmp_path, 'sqlite.db'),
        buffer_size=10,
        flush_level=logging.CRITICAL,
        echo=True
    )
    run_logger(__name__, handler)
