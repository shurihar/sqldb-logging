"""
Tests for the SQLHandler class.
"""

import logging
import os
import sys
import time
from decimal import Decimal

from sqlalchemy import select

from sqldb_logging.handlers import SQLHandler


def run_logger(handler: SQLHandler) -> int:
    """
    Creates a logger with the given handler,
    writes messages to the database specified by the handler,
    and returns the number of records inserted into the log table.
    """
    start_time = time.time()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.debug('This is a %s message', logging.getLevelName(logging.DEBUG), stack_info=True)
    logger.info('This is an %s message', logging.getLevelName(logging.INFO), stack_info=True)
    logger.warning('This is a %s message', logging.getLevelName(logging.WARNING), stack_info=True)
    logger.error('This is an %s message', logging.getLevelName(logging.ERROR), stack_info=True)
    try:
        1 / 0
    except ZeroDivisionError as error:
        logger.exception(error, stack_info=True)
    logger.critical('This is a %s message', logging.getLevelName(logging.CRITICAL), stack_info=True)
    end_time = time.time()
    stmt = select(handler.log_table) \
        .where(handler.log_table.c['created'] >= Decimal(str(start_time))) \
        .where(handler.log_table.c['created'] < Decimal(str(end_time)))
    with handler.engine.connect() as conn:
        return len(conn.execute(stmt).fetchall())


def test_databricks(buffer_size, flush_level):
    """Checks if SQLHandler can write logs to Databricks."""
    handler = SQLHandler(
        table=f'log_table_py_{sys.version_info.major}_{sys.version_info.minor}',
        drivername='databricks',
        username='token',
        password=os.getenv('DATABRICKS_TOKEN'),
        host=os.getenv('DATABRICKS_SERVER_HOSTNAME'),
        buffer_size=buffer_size,  # this doesn't seem to work with Databricks, each record is written separately
        flush_level=flush_level,
        connect_args={
            'http_path': os.getenv('DATABRICKS_HTTP_PATH'),
            'catalog': 'workspace',
            'schema': 'default'
        },
        echo=True
    )
    rowcount = run_logger(handler)
    assert rowcount == 6
    with handler.engine.connect() as conn:
        conn.exec_driver_sql(f'optimize {handler.log_table.name}')
        # conn.exec_driver_sql(f'vacuum {handler.log_table.name}')


def test_mssql(buffer_size, flush_level):
    """
    Checks whether SQLHandler can write logs to Microsoft SQL Server.

    If setinputsizes support is enabled,
    SQLAlchemy produces the following error when checking for table existence:
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
    rowcount = run_logger(handler)
    assert rowcount == 6


def test_mysql(buffer_size, flush_level):
    """Checks whether SQLHandler can write logs to MySQL."""
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
    rowcount = run_logger(handler)
    assert rowcount == 6


def test_postgres(buffer_size, flush_level):
    """Checks whether SQLHandler can write logs to Postgres."""
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
    rowcount = run_logger(handler)
    assert rowcount == 6


def test_sqlite(buffer_size, flush_level, tmp_path):
    """Checks whether SQLHandler can write logs to SQLite."""
    handler = SQLHandler(
        table='log_table',
        drivername='sqlite',
        database=os.path.join(tmp_path, 'sqlite.db'),
        buffer_size=buffer_size,
        flush_level=flush_level,
        echo=True
    )
    rowcount = run_logger(handler)
    assert rowcount == 6
