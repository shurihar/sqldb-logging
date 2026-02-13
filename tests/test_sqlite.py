"""
SQLite Logging Tests.
"""

import os

from sqldb_logging.handlers import SQLHandler
from .common_functions import run_logger


def test_sqlite(buffer_size, flush_level, tmp_path):
    """Checks whether the SQLHandler can write logs to the SQLite database."""
    handler = SQLHandler(
        table='log_table',
        drivername='sqlite',
        database=os.path.join(tmp_path, 'sqlite.db'),
        buffer_size=buffer_size,
        flush_level=flush_level,
        echo=True
    )
    rowcount = run_logger(__name__, handler)
    assert rowcount == 6
