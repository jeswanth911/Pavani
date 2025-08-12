# data_engine/cleaner.py

import os
import csv
import json
import numpy as np
import pandas as pd
import chardet
from typing import Tuple, Dict, List
from scipy.stats import zscore
from typing import Tuple

from utils.logger import logger
from utils.file_parser import (
    parse_file,
    parse_sql_file,
    parse_pdf_file,
    parse_csv_file,
    parse_excel_file,
    parse_json_file,
    parse_parquet_file,
)

# Ensure folders
for folder in ["data/cleaned", "data/analyzed", "data/output", "data/exports", "data/temp", "data/uploaded"]:
    os.makedirs(folder, exist_ok=True)


def detect_encoding(file_path: str) -> str:
    with open(file_path, 'rb') as f:
        raw = f.read(10000)
    result = chardet.detect(raw)
    return result['encoding'] or 'utf-8'


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names by:
    - Lowercasing
    - Replacing spaces/special chars with underscores
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w\s]", "", regex=True)
        .str.replace(r"\s+", "_", regex=True)
    )
    return df
    

def replace_na_like_values(df: pd.DataFrame) -> pd.DataFrame:
    na_values = ["", "na", "n/a", "null", "NULL", "NaN", "-", "--"]
    return df.replace(na_values, np.nan)


def detect_outliers(df: pd.DataFrame) -> Dict[str, int]:
    outlier_counts = {}
    numeric = df.select_dtypes(include=[np.number])
    if numeric.empty:
        return outlier_counts

    z_scores = np.abs(zscore(numeric, nan_policy='omit'))
    for idx, col in enumerate(numeric.columns):
        count = int((z_scores[:, idx] > 3).sum())
        outlier_counts[col] = count
    return outlier_counts


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main cleaning logic:
    - Strip strings
    - Drop completely empty columns
    - Fill or drop missing values (basic logic)
    """
    # Strip string columns
    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].astype(str).str.strip()

    # Drop fully empty columns
    df.dropna(axis=1, how='all', inplace=True)

    # Fill missing values for numeric columns
    for col in df.select_dtypes(include=['number']).columns:
        df[col] = df[col].fillna(df[col].median())

    # Fill missing values for categorical columns
    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].fillna('unknown')

    return df
    
    
def clean_data(df: pd.DataFrame, output_path: str = None) -> Tuple[pd.DataFrame, str]:
    """
    Full cleaning pipeline:
    - Normalize column names
    - Clean data
    - Save cleaned file (optional)
    Returns:
        cleaned DataFrame and output path (if saved)
    """
    try:
        if df is None or df.empty:
            raise ValueError("Parsed DataFrame is empty.")

        logger.info(f"✅ Starting cleaning on DataFrame with shape: {df.shape}")

        # Normalize and clean
        df = normalize_column_names(df)
        df = clean_dataframe(df)

        # Save if output path provided
        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, index=False)
            logger.info(f"📁 Cleaned file saved to: {output_path}")
            return df, output_path

        return df, ""

    except Exception as e:
        logger.error(f"❌ Failed to clean data: {str(e)}")
        raise
        
        


def generate_cleaning_report(df: pd.DataFrame) -> dict:
    return {
        "columns": list(df.columns),
        "null_counts": df.isnull().sum().to_dict(),
        "dtypes": df.dtypes.apply(lambda x: str(x)).to_dict(),
        "num_rows": len(df)
    }


def clean_data_file(df: pd.DataFrame, output_path: str) -> pd.DataFrame:
    try:
        df.dropna(axis=1, how='all', inplace=True)
        df.drop_duplicates(inplace=True)

        # ✅ Safely clean column names
        df.columns = [str(col).strip().lower().replace(" ", "_") for col in df.columns]

        # Fill missing values in numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df[col].isnull().any():
                median_val = df[col].median()
                df[col].fillna(median_val, inplace=True)

        # Fill missing values in categorical columns
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        for col in categorical_cols:
            if df[col].isnull().any():
                mode_val = df[col].mode()[0] if not df[col].mode().empty else "Unknown"
                df[col].fillna(mode_val, inplace=True)

        # Strip whitespace from categorical values
        for col in categorical_cols:
            df[col] = df[col].astype(str).str.strip()

        # Save cleaned file
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)

        logger.info(f"[CLEANING] Data cleaned and saved to: {output_path}")
        return df

    except Exception as e:
        logger.error(f"[CLEANING ERROR] {e}", exc_info=True)
        raise
        
        
def clean_and_report(file_path: str, output_dir: str = "data/cleaned") -> Tuple[str, Dict]:
    try:
        logger.info(f"Cleaning and reporting for: {file_path}")
        df = parse_file(file_path)
        if isinstance(df, dict) and "dataframe" in df:
            df = df["dataframe"]
        if df is None or df.empty:
            raise ValueError("Parsed DataFrame is empty")

        original_rows = df.shape[0]
        df = clean_dataframe(df)
        cleaned_rows = df.shape[0]
        normalized_cols = normalize_column_names(df)
        null_summary = df.isnull().sum().to_dict()
        outlier_summary = detect_outliers(df)

        base = os.path.basename(file_path).rsplit(".", 1)[0]
        cleaned_path = os.path.join(output_dir, f"{base}_cleaned.csv")
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(cleaned_path, index=False)

        return cleaned_path, {
            "status": "success",
            "original_file": file_path,
            "cleaned_file_path": cleaned_path,
            "original_rows": original_rows,
            "cleaned_rows": cleaned_rows,
            "num_columns": df.shape[1],
            "duplicates_removed": original_rows - cleaned_rows,
            "normalized_columns": normalized_cols,
            "null_summary": null_summary,
            "outliers_detected": outlier_summary,
            "error": None
        }

    except Exception as e:
        logger.error(f"clean_and_report failed: {e}", exc_info=True)
        return "", {
            "status": "error",
            "original_file": file_path,
            "cleaned_file_path": "",
            "original_rows": 0,
            "cleaned_rows": 0,
            "num_columns": 0,
            "duplicates_removed": 0,
            "normalized_columns": [],
            "null_summary": {},
            "outliers_detected": {},
            "error": str(e)
        }
        
def save_dataframe_to_sqlite(df: pd.DataFrame, table_name: str = "data", db_path: str = "data/mydb.sqlite") -> str:
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    return db_path

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].astype(str).str.strip()
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    return df
    
