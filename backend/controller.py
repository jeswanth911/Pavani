import os
import pandas as pd
from utils.logger import logger
from utils.file_parser import parse_file
from data_engine.cleaner import clean_data_file
from data_engine.analyzer import analyze_data
from controller.sqlite_converter import convert_to_sqlite_df

# Define storage paths
UPLOAD_DIR = "data/uploaded"
CLEANED_DIR = "data/cleaned"
ANALYZED_DIR = "data/analyzed"
OUTPUT_DIR = "data/output"

# Ensure folders exist
for folder in [UPLOAD_DIR, CLEANED_DIR, ANALYZED_DIR, OUTPUT_DIR]:
    os.makedirs(folder, exist_ok=True)

def upload_csv_controller(file):
    try:
        filename = file.filename
        ext = os.path.splitext(filename)[1].lower()
        filepath = os.path.join(UPLOAD_DIR, filename)

        with open(filepath, "wb") as f:
            f.write(file.file.read())
        
        logger.info(f"[UPLOAD] File saved at {filepath}")
        return {"status": "success", "file_path": filepath}
    except Exception as e:
        logger.error(f"[UPLOAD ERROR] {e}", exc_info=True)
        return {"status": "error", "file_path": "", "error": str(e)}

def clean_data_controller(file_path):
    try:
        # Generate output cleaned file path
        base = os.path.basename(file_path)
        name, _ = os.path.splitext(base)
        cleaned_file_path = os.path.join(CLEANED_DIR, f"{name}_cleaned.csv")

        # Parse raw file → dataframe
        df = parse_file(file_path)
        if df.empty:
            raise ValueError("Parsed dataframe is empty")

        # Clean the data
        cleaned_df = clean_data_file(df, cleaned_file_path)

        logger.info(f"[CLEAN] Cleaned file saved at {cleaned_file_path}")
        return {"status": "success", "cleaned_file": cleaned_file_path, "dataframe": cleaned_df}
    except Exception as e:
        logger.error(f"[CLEAN ERROR] {e}", exc_info=True)
        return {"status": "error", "cleaned_file": "", "dataframe": None, "error": str(e)}

def analyze_data_controller(df: pd.DataFrame):
    try:
        summary = analyze_data(df)
        logger.info(f"[ANALYZE] Analysis summary generated")
        return {"status": "success", "analysis_summary": summary}
    except Exception as e:
        logger.error(f"[ANALYZE ERROR] {e}", exc_info=True)
        return {"status": "error", "analysis_summary": {}, "error": str(e)}

def convert_to_sqlite_controller(df: pd.DataFrame):
    try:
        db_filename = f"data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.db"
        db_path = os.path.join(OUTPUT_DIR, db_filename)

        convert_to_sqlite_df(df, db_path)

        logger.info(f"[SQLITE] DB created at {db_path}")
        return {"status": "success", "sqlite_db_path": db_path}
    except Exception as e:
        logger.error(f"[SQLITE ERROR] {e}", exc_info=True)
        return {"status": "error", "sqlite_db_path": "", "error": str(e)}
        
