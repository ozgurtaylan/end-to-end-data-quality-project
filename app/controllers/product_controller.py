import pandas as pd
from .base_controller import BaseController

class ProductController(BaseController):

    # Open to improvements...
    STOCK_THRESHOLD: int = 4000
    VALID_CATEGORIES: list = ["Elektronik", "Moda", "Ev & Yaşam", "Spor", "Otomotiv"]

    def __init__(
        self,
        db_type: str,
        db_user: str,
        db_password: str,
        db_host: str,
        db_port: int,
        db_name: str,
        table_name: str,
        conn_idle_timeout: int,
        chunk_size: int,
        datahub_server_url: str,
        datahub_platform_urn: str,
        datahub_entity_urn: str,
        column_operations: list,
    ):
        super().__init__(
            db_type,
            db_user,
            db_password,
            db_host,
            db_port,
            db_name,
            table_name,
            conn_idle_timeout,
            chunk_size,
            datahub_server_url,
            datahub_platform_urn,
            datahub_entity_urn,
            column_operations,
        )

    # Product Price Validations
    # Open to improvements...
    def count_null_prices(self, df: pd.DataFrame, column: str) -> tuple:
        self._validate_columns(df, [column])
        result = self._count_null(df, column)
        status = "SUCCESS" if result == 0 else "FAILURE"
        return result, status

    def count_invalid_prices(self, df: pd.DataFrame, column: str) -> tuple:
        self._validate_columns(df, [column])
        result = int((df[column] <= 0).sum())
        status = "SUCCESS" if result == 0 else "FAILURE"
        return result, status

    # Product Stock Validations
    def count_null_stocks(self, df: pd.DataFrame, column: str) -> tuple:
        self._validate_columns(df, [column])
        result = self._count_null(df, column)
        status = "SUCCESS" if result == 0 else "FAILURE"
        return result, status

    def count_negative_stocks(self, df: pd.DataFrame, column: str) -> tuple:
        self._validate_columns(df, [column])
        result = self._count_negative(df, column)
        status = "SUCCESS" if result == 0 else "FAILURE"
        return result, status

    def count_overvalued_stocks(self, df: pd.DataFrame, column: str) -> tuple:
        self._validate_columns(df, [column])
        result = self._count_overvalued(df, column, self.STOCK_THRESHOLD)
        status = "SUCCESS" if result == 0 else "FAILURE"
        return result, status

    # Product Category Validations
    def count_null_categories(self, df: pd.DataFrame, column: str) -> tuple:
        self._validate_columns(df, [column])
        result = self._count_null(df, column)
        status = "SUCCESS" if result == 0 else "FAILURE"
        return result, status

    def count_uncategorized_categories(self, df: pd.DataFrame, column: str) -> tuple:
        self._validate_columns(df, [column])
        result = self._count_off_list(df, column, self.VALID_CATEGORIES)
        status = "SUCCESS" if result == 0 else "FAILURE"
        return result, status