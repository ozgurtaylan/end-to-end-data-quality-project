import pandas as pd
from abc import ABC, abstractmethod
import logging
from tabulate import tabulate

class BaseController(ABC):
    def __init__(self, db_name, table_name, columns):
        self.db_name = db_name
        self.table_name = table_name
        self.columns = columns
        self.controls = self._initialize_controls()

    @abstractmethod
    def _initialize_controls(self):
        pass

    @staticmethod
    def _validate_columns(df: pd.DataFrame, column_names: list):
        missing_columns = [col for col in column_names if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Columns not found in DataFrame: {', '.join(missing_columns)}")

    @staticmethod
    def _count_negative(df: pd.DataFrame, column_name: str) -> int:
        BaseController._validate_columns(df, [column_name])
        return int((df[column_name] < 0).sum())

    @staticmethod
    def _count_null(df: pd.DataFrame, column_name: str) -> int:
        BaseController._validate_columns(df, [column_name])
        return int(df[column_name].isnull().sum())

    @staticmethod
    def _count_off_list(df: pd.DataFrame, column_name: str, preset_list: list) -> int:
        BaseController._validate_columns(df, [column_name])
        non_null_values = df[column_name].dropna().str.lower()
        preset_list_lower = [item.lower() for item in preset_list]
        return int((~non_null_values.isin(preset_list_lower)).sum())

    @staticmethod
    def _count_overvalued(df: pd.DataFrame, column_name: str, threshold: int) -> int:
        BaseController._validate_columns(df, [column_name])
        return int((df[column_name] > threshold).sum())