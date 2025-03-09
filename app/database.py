import logging
from sqlalchemy import create_engine
import pandas as pd
import config as config
from abc import ABC, abstractmethod

class DatabaseFactory(ABC):
    @abstractmethod
    def create_engine(self):
        pass

    @abstractmethod
    def create_connection(self):
        pass

class MySQLDatabaseFactory(DatabaseFactory):
    def create_engine(self):
        engine = create_engine(config.CONNECTION_STRING, pool_recycle=config.CONN_IDLE_TIMEOUT)
        logging.info("MySQL engine created.")
        return engine

    def create_connection(self):
        conn = self.create_engine().raw_connection()
        logging.info("MySQL connection established.")
        return conn

def get_database_factory(db_type: str) -> DatabaseFactory:
    if db_type == config.DB_TYPE_MYSQL:
        return MySQLDatabaseFactory()
    else:
        # Add support for other database types here
        pass
    
class Database:
    def __init__(self):
        self.db_factory = get_database_factory(config.DB_TYPE_MYSQL)
        self.engine = self.db_factory.create_engine()

    def open_connection(self):
        return self.db_factory.create_connection()

    def close_connection(self, conn):
        logging.info("Closing database connection.")
        conn.close()

    def fetch_table_in_chunks(self, conn, table_name: str, columns: list = None, where_clause: str = None, chunksize: int = config.DEFAULT_CHUNK_SIZE):
        select_columns = "*"
        if columns:
            select_columns = ", ".join([f"{col}" for col in columns])

        query = f"SELECT {select_columns} FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        return pd.read_sql(query, conn, chunksize=chunksize)

