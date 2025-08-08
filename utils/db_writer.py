# utils/db_writer.py

import sqlite3
import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def save_dataframe_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str = "uploaded_table") -> str:
    """Saves a DataFrame into a SQLite database as a new table."""
    if df.empty:
        raise ValueError("❌ DataFrame is empty. Cannot save to database.")

    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()

        logger.info(f"✅ Saved DataFrame to '{db_path}' as table '{table_name}'")

        # Optional: log tables
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        logger.info(f"📋 Tables in DB: {tables}")
        conn.close()

        return db_path
    except Exception as e:
        logger.error(f"❌ Failed to save DataFrame to SQLite: {e}")
        raise
      
