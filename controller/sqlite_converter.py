# controller/sqlite_converter.py

import os
import sqlite3
import pandas as pd
from utils.logger import logger

def convert_to_sqlite_df(df: pd.DataFrame, db_path: str = "data/temp.db", table_name: str = "data") -> str:
    """
    Converts a DataFrame to SQLite database.
    
    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        db_path (str): Path to SQLite DB file.
        table_name (str): Table name to create/overwrite.

    Returns:
        str: Path to the SQLite DB file.
    """
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        logger.info(f"DataFrame saved to SQLite DB at {db_path} in table {table_name}")
        return db_path
    except Exception as e:
        logger.error(f"Failed to convert to SQLite: {e}")
        raise
      
