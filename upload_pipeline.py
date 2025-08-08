# controller/upload_pipeline.py

import os
import pandas as pd
from utils.file_parser import parse_file
from data_engine.cleaner import clean_data
from utils.db_writer import save_dataframe_to_sqlite

def process_uploaded_file(file_path: str, table_name: str = "uploaded_table") -> str:
    """Complete upload pipeline: parse, clean, and save to SQLite."""
    try:
        # Step 1: Parse file to DataFrame
        df = parse_file(file_path)

        # Step 2: Clean the DataFrame
        cleaned_df = clean_data(df)

        # Step 3: Save to SQLite
        db_path = "data/output/cleaned_data.db"
        save_dataframe_to_sqlite(cleaned_df, db_path, table_name)

        return db_path

    except Exception as e:
        return f"❌ Failed to process file: {e}"
      
