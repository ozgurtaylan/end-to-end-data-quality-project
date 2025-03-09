import pandas as pd
from .base_controller import BaseController

class ProductController(BaseController):

    def __init__(self,db_name,table_name,columns,valid_categories, stock_threshold):
        super().__init__(db_name,table_name,columns)
        self._valid_categories = valid_categories
        self._stock_threshold = stock_threshold

    def _initialize_controls(self):
        controls = [
                {"column_name": "price", "control_name": "missing_price_count", "method": self._count_null_prices},
                {"column_name": "price", "control_name": "non_positive_price_count", "method": self._count_invalid_prices},
                {"column_name": "stock", "control_name": "missing_stock_count", "method": self._count_null_stocks},
                {"column_name": "stock", "control_name": "negative_stock_count", "method": self._count_negative_stocks},
                {"column_name": "stock", "control_name": "overvalued_stock_count", "method": self._count_overvalued_stocks},
                {"column_name": "category", "control_name": "missing_category_count", "method": self._count_null_categories},
                {"column_name": "category", "control_name": "uncategorized_category_count", "method": self._count_uncategorized_categories,},
            ]
        
        for control in controls:
            control["result"] = 0
            control["status"] = "Not Run"
        return controls
    
    # Product Price Validations
    def _count_null_prices(self, df: pd.DataFrame, column):
        return self._count_null(df, column)

    def _count_invalid_prices(self, df: pd.DataFrame, column):
        self._validate_columns(df, [column])
        return int((df[column] <= 0).sum())

    # Product Stock Validations
    def _count_null_stocks(self, df: pd.DataFrame, column):
        return self._count_null(df, column)

    def _count_negative_stocks(self, df: pd.DataFrame, column):
        return self._count_negative(df, column)

    def _count_overvalued_stocks(self, df: pd.DataFrame, column):
        return self._count_overvalued(df, column, self._stock_threshold)

    # Product Category Validations
    def _count_null_categories(self, df: pd.DataFrame, column):
        return self._count_null(df, column)

    def _count_uncategorized_categories(self, df: pd.DataFrame, column):
        return self._count_off_list(df, column, self._valid_categories)