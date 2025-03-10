import logging
from sqlalchemy import create_engine
import pandas as pd
from abc import ABC, abstractmethod

logger = logging.getLogger("QUALITY_CHECKS")

class DatabaseFactory(ABC):
    @abstractmethod
    def create_engine(self):
        pass

    @abstractmethod
    def create_connection(self):
        pass

class MySQLDatabaseFactory(DatabaseFactory):

    def __init__(self, db_user, db_password, db_host, db_port, db_name, conn_idle_timeout):
        self.connection_string = (f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
        self.conn_idle_timeout = conn_idle_timeout

    def create_engine(self):
        engine = create_engine(self.connection_string, pool_recycle=self.conn_idle_timeout)
        logger.info("MySQL engine created.")
        return engine

    def create_connection(self):
        conn = self.create_engine().raw_connection()
        logger.info("MySQL connection established.")
        return conn

def get_database_factory(db_type, db_user, db_password, db_host, db_port, db_name, conn_idle_timeout) -> DatabaseFactory:
    if db_type == "mysql":
        return MySQLDatabaseFactory(db_user, db_password, db_host, db_port, db_name, conn_idle_timeout)
    else:
        # Add support for other database types here
        pass

class Database:
    def __init__(self, db_type, db_user, db_password, db_host, db_port, db_name, conn_idle_timeout):
        self.db_factory = get_database_factory(db_type, db_user, db_password, db_host, db_port, db_name, conn_idle_timeout)
        self.engine = self.db_factory.create_engine()

    def open_connection(self):
        return self.db_factory.create_connection()

    def close_connection(self, conn):
        logger.info("Closing database connection.")
        conn.close()

    def fetch_table_in_chunks(self, conn, chunksize: int, table_name: str, columns: list = None, where_clause: str = None):
        select_columns = "*"
        if columns:
            select_columns = ", ".join([f"{col}" for col in columns])

        query = f"SELECT {select_columns} FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        return pd.read_sql(query, conn, chunksize=chunksize)
