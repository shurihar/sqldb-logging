"""
Microsoft SQL Server logging tests.
"""

import os

from sqldb_logging.handlers import SQLHandler
from .common_functions import run_logger


def test_mssql(buffer_size, flush_level):
    """
    Checks whether SQLHandler can write logs to Microsoft SQL Server.

    If setinputsizes support is enabled, SQLAlchemy produces the following error when checking for table existence:
    pyodbc.Error: ('HY104', '[HY104] [Microsoft][ODBC SQL Server Driver]Invalid precision value (0) (SQLBindParameter)')
    """
    handler = SQLHandler(
        table='log_table',
        drivername='mssql+pyodbc',
        username='sa',
        password=os.getenv('MSSQL_SA_PASSWORD'),
        host='localhost',
        port=1433,
        database='master',
        schema='dbo',
        buffer_size=buffer_size,
        flush_level=flush_level,
        connect_args={'driver': 'SQL Server'},
        echo=True,
        use_setinputsizes=False
    )
    rowcount = run_logger(__name__, handler)
    assert rowcount == 6
