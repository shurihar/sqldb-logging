# sqldb-logging
An extension to the Python logging library that allows logging to SQL databases using [SQLAlchemy](https://www.sqlalchemy.org/)

## Requirements
- Python 3.11 or later

## Installation

Basic installation:

```shell
pip install sqldb-logging
```

Installation with optional dependencies:

```shell
pip install sqldb-logging[databricks,mssql,mysql,postgresql]
```

## Usage

```python
import logging

from sqldb_logging.handlers import SQLHandler

handler = SQLHandler(
    table='log_table',
    drivername='postgresql+psycopg',
    username='postgres',
    password='postgres',
    host='localhost',
    port=5432,
    database='postgres',
    schema='public'
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)
logger.info('Doing something')
```
**Note**: The database user must have CREATE TABLE permission to create a log table if it doesn't exist.
If the log table already exists,
its schema must reflect the structure of [LogRecord](https://docs.python.org/3/library/logging.html#logrecord-objects)

## Supported databases
In theory, any database supported by SQLAlchemy should work. The following databases have been confirmed to work:
- MySQL (using [mysqlclient](https://pypi.org/project/mysqlclient/))
- PostgreSQL (using [psycopg](https://pypi.org/project/psycopg/))
- SQLite (using the Python built-in module sqlite3)
- Databricks (using [databricks-sqlalchemy](https://pypi.org/project/databricks-sqlalchemy/))
- Microsoft SQL Server (using [pyodbc](https://pypi.org/project/pyodbc/))
