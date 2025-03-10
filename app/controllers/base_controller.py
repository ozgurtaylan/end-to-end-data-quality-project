import pandas as pd
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger("QUALITY_CHEKS")

class BaseController(ABC):

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
        self.db_type = db_type
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.table_name = table_name
        self.conn_idle_timeout = conn_idle_timeout
        self.chunk_size = chunk_size
        self.datahub_server_url = datahub_server_url
        self.datahub_platform_urn = datahub_platform_urn
        self.datahub_entity_urn = datahub_entity_urn
        self._column_operations = column_operations
        self.executor_feed = self._init_executor_feed()

    def _init_executor_feed(self):
        logger.info("Initializing executor feed.")
        output = []
        for column in self._column_operations:
            if "controls" in column:
                for control in column["controls"]:
                    result = 0
                    output.append(
                        {
                            "column": column.get("name"),
                            "control_name": control.get("name"),
                            "method": control.get("responsible_method"),
                            "result": result,
                            "status": "Not Run",
                        }
                    )
        return output

    @staticmethod
    def _validate_columns(df: pd.DataFrame, column_names: list):
        missing_columns = [col for col in column_names if col not in df.columns]
        if missing_columns:
            raise ValueError(
                f"Columns not found in DataFrame: {', '.join(missing_columns)}"
            )

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