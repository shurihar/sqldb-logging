import logging
import traceback
from datetime import datetime
from logging.handlers import MemoryHandler
from typing import Any

from sqlalchemy import (
    URL,
    create_engine,
    MetaData,
    Table,
    Column,
    DateTime,
    Double,
    Text,
    String,
    SmallInteger,
    Integer,
    BigInteger,
    insert
)


class SQLHandler(MemoryHandler):
    """
    A handler class which buffers logging records in memory, periodically
    flushing them to a database table using SQLAlchemy. Flushing occurs whenever the buffer
    is full, or when an event of a certain severity or greater is seen.
    :param table: The name of the database table where to write logs.
    :param drivername: the name of the database backend. This name will
        correspond to a module in sqlalchemy/databases or a third party
        plug-in.
    :param username: The user name.
    :param password: database password.  Is typically a string, but may
        also be an object that can be stringified with ``str()``.
    :param host: The name of the host.
    :param port: The port number.
    :param database: The database name.
    :param schema: The schema name for the log table, which is required if
        the table resides in a schema other than the default selected schema
        for the engine's database connection.  Defaults to ``None``.
    :param buffer_size: The number of records to buffer in memory before flushing to the database.
        The default is 1, which means that each record is flushed immediately.
        Setting this parameter to a higher value reduces the number of round trips to the database, but requires
        calling flush() or close() method before exiting the program to flush all pending records to the database.
        It may also result in the loss of buffered records if the program terminates unexpectedly.
        Using the with statement when instantiating the handler can help avoid this problem.
    :param flush_level: the level at which flushing should occur.
    :param kwargs: takes a wide variety of options which are routed towards their appropriate components.
        For more information, see https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine
    """

    def __init__(
            self,
            table: str,
            drivername: str,
            username: str | None = None,
            password: str | None = None,
            host: str | None = None,
            port: int | None = None,
            database: str | None = None,
            schema: str | None = None,
            buffer_size: int = 1,
            flush_level: int = logging.ERROR,
            **kwargs: Any
    ):
        super().__init__(buffer_size, flush_level)
        url = URL.create(
            drivername=drivername,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database
        )
        self.engine = create_engine(url, **kwargs)
        metadata = MetaData()
        self.log_table = Table(
            table,
            metadata,
            Column('asctime', DateTime, nullable=False),
            Column('created', Double, nullable=False),
            Column('exc_info', Text),
            Column('filename', Text),
            Column('func_name', Text),
            Column('levelname', String(8), nullable=False),
            Column('levelno', SmallInteger, nullable=False),
            Column('lineno', Integer),
            Column('message', Text, nullable=False),
            Column('module_name', Text),
            Column('msecs', Double, nullable=False),
            Column('logger_name', Text, nullable=False),
            Column('pathname', Text),
            Column('process_id', BigInteger),
            Column('process_name', Text),
            Column('relative_created', Double, nullable=False),
            Column('stack_info', Text),
            Column('thread_id', BigInteger),
            Column('thread_name', Text),
            schema=schema
        )
        metadata.create_all(self.engine)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def flush(self):
        self.acquire()
        try:
            if self.buffer:
                parameters = []
                for record in self.buffer:
                    exc_info = None
                    if record.exc_info is not None:
                        exc_type, exc_val, exc_tb = record.exc_info
                        exc_info = ''.join(traceback.format_exception(exc_type, value=exc_val, tb=exc_tb))
                    parameters.append(
                        {
                            'asctime': datetime.fromtimestamp(record.created),
                            'created': record.created,
                            'exc_info': exc_info,
                            'filename': record.filename,
                            'func_name': record.funcName,
                            'levelname': record.levelname,
                            'levelno': record.levelno,
                            'lineno': record.lineno,
                            'message': str(record.msg) % record.args,
                            'module_name': record.module,
                            'msecs': record.msecs,
                            'logger_name': record.name,
                            'pathname': record.pathname,
                            'process_id': record.process,
                            'process_name': record.processName,
                            'relative_created': record.relativeCreated,
                            'stack_info': record.stack_info,
                            'thread_id': record.thread,
                            'thread_name': record.threadName
                        }
                    )
                with self.engine.connect() as conn:
                    conn.execute(insert(self.log_table), parameters)
                    conn.commit()
                self.buffer.clear()
        finally:
            self.release()
