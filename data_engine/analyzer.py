import pandas as pd
import numpy as np
import sqlite3
from typing import Dict, List, Any, Union
from scipy.stats import zscore

from utils.logger import logger


def profile_column(df: pd.DataFrame, column: str) -> Dict[str, Any]:
    """
    Generate a profile summary for a single column in the DataFrame.
    """
    try:
        if column not in df.columns:
            return {"error": f"Column '{column}' not found in dataset."}

        col_data = df[column]
        non_null = col_data.dropna()
        col_type = str(col_data.dtype)
        unique_ratio = non_null.nunique() / len(non_null) if len(non_null) > 0 else 0

        profile = {
            "column": column,
            "inferred_type": col_type,
            "null_percent": round(col_data.isna().mean() * 100, 2),
            "unique_percent": round(unique_ratio * 100, 2),
            "top_5_values": non_null.value_counts().head(5).to_dict()
        }

        if pd.api.types.is_numeric_dtype(col_data):
            profile.update({
                "mean": col_data.mean(),
                "std": col_data.std(),
                "min": col_data.min(),
                "max": col_data.max(),
                "outlier_count": int(np.sum(np.abs(zscore(non_null)) > 3))
            })

        return profile

    except Exception as e:
        logger.error(f"[PROFILE] Failed to profile column '{column}': {e}")
        return {"column": column, "error": str(e)}


def analyze_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform basic analysis on a DataFrame.
    """
    try:
        num_rows, num_columns = df.shape
        column_types = df.dtypes.astype(str).to_dict()
        null_summary = df.isnull().sum().to_dict()
        high_null_cols = [col for col, val in null_summary.items() if val / len(df) > 0.3]
        high_card_cols = [col for col in df.columns if df[col].nunique() / len(df) > 0.7]

        frequent_values = {
            col: df[col].value_counts().head(3).to_dict()
            for col in df.columns if df[col].dtype == 'object'
        }

        memory_usage = round(df.memory_usage(deep=True).sum() / 1024**2, 2)

        return {
            "status": "success",
            "num_rows": num_rows,
            "num_columns": num_columns,
            "columns": list(df.columns),
            "column_types": column_types,
            "null_summary": null_summary,
            "high_null_columns": high_null_cols,
            "high_cardinality_columns": high_card_cols,
            "frequent_values": frequent_values,
            "memory_usage_mb": memory_usage,
            "describe": df.describe(include='all').fillna("").to_dict(),
            "correlation": df.corr(numeric_only=True).fillna(0).to_dict(),
            "error": None
        }

    except Exception as e:
        logger.error(f"[ANALYZE] Error analyzing data: {e}")
        return {"status": "error", "error": str(e)}


def compare_datasets(df1: pd.DataFrame, df2: pd.DataFrame) -> Dict[str, Any]:
    """
    Compare two datasets and identify schema, shape, and numeric drifts.
    """
    try:
        col_df1 = set(df1.columns)
        col_df2 = set(df2.columns)
        shared_cols = col_df1 & col_df2

        column_diff = {
            "only_in_df1": list(col_df1 - col_df2),
            "only_in_df2": list(col_df2 - col_df1),
        }

        type_mismatches = {
            col: {"df1": str(df1[col].dtype), "df2": str(df2[col].dtype)}
            for col in shared_cols if str(df1[col].dtype) != str(df2[col].dtype)
        }

        shape_diff = {
            "df1_shape": df1.shape,
            "df2_shape": df2.shape
        }

        numeric_drift = {}
        for col in shared_cols:
            if pd.api.types.is_numeric_dtype(df1[col]) and pd.api.types.is_numeric_dtype(df2[col]):
                numeric_drift[col] = {
                    "df1": {
                        "mean": df1[col].mean(),
                        "std": df1[col].std(),
                        "min": df1[col].min(),
                        "max": df1[col].max()
                    },
                    "df2": {
                        "mean": df2[col].mean(),
                        "std": df2[col].std(),
                        "min": df2[col].min(),
                        "max": df2[col].max()
                    }
                }

        return {
            "status": "success",
            "column_diff": column_diff,
            "type_mismatches": type_mismatches,
            "shape_diff": shape_diff,
            "numeric_drift": numeric_drift,
            "error": None
        }

    except Exception as e:
        logger.error(f"[COMPARE] Failed to compare datasets: {e}")
        return {"status": "error", "error": str(e)}


def generate_cleaning_report(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Summarize cleaning results.
    """
    try:
        return {
            "shape": df.shape,
            "columns": df.columns.tolist(),
            "missing_values": df.isnull().sum().to_dict(),
            "data_types": df.dtypes.astype(str).to_dict(),
            "duplicate_rows": int(df.duplicated().sum())
        }
    except Exception as e:
        logger.error(f"[REPORT] Failed to generate cleaning report: {e}")
        return {"error": str(e)}


def run_sql_query(db_path: str, query: str) -> Union[List[Dict[str, Any]], Dict[str, str]]:
    """
    Execute SQL query against SQLite DB and return records.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            result = pd.read_sql_query(query, conn)
        return result.to_dict(orient="records")
    except Exception as e:
        logger.error(f"[SQL] Query failed: {e}")
        return {"error": str(e)}


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load data from a file path to a pandas DataFrame.
    Supports: CSV, Excel, JSON, Parquet.
    """
    try:
        ext = file_path.split('.')[-1].lower()
        if ext == 'csv':
            return pd.read_csv(file_path)
        elif ext in ['xlsx', 'xls']:
            return pd.read_excel(file_path)
        elif ext == 'json':
            return pd.read_json(file_path)
        elif ext == 'parquet':
            return pd.read_parquet(file_path)
        else:
            raise ValueError(f"Unsupported file type: {ext}")
    except Exception as e:
        logger.error(f"[LOAD] Failed to load file {file_path}: {e}")
        raise
        
