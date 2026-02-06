import logging
import os
import sys
import time
from decimal import Decimal

from sqlalchemy import select

from sqldb_logging.handlers import SQLHandler


class TestDatabricks:

    def test_databricks(self):
        start_time = time.time()
        print(start_time)
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
        logger = logging.getLogger(self.__class__.__name__)
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
            .where(handler.log_table.c['created'] > Decimal(str(start_time))) \
            .where(handler.log_table.c['created'] < Decimal(str(end_time)))
        with handler.engine.connect() as conn:
            assert len(conn.execute(stmt).fetchall()) == 6
