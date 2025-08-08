from data_engine.cleaner import clean_data
from utils.file_parser import parse_file
import os
from fastapi import APIRouter
from fastapi import APIRouter, UploadFile, File

router = APIRouter()

def clean_file_pipeline(file_path: str, save_cleaned: bool = True) -> dict:
    try:
        df = parse_file(file_path)

        if df is None or df.empty:
            raise ValueError("Parsed dataframe is empty")

        cleaned_path = ""
        if save_cleaned:
            base_name = os.path.basename(file_path)
            cleaned_path = os.path.join("data/cleaned", f"cleaned_{base_name}")

        cleaned_df, saved_path = clean_data(df, output_path=cleaned_path if save_cleaned else None)

        return {
            "status": "success",
            "cleaned_file": saved_path,
            "rows": cleaned_df.shape[0],
            "columns": cleaned_df.shape[1],
            "preview": cleaned_df.head(5).to_dict(orient="records"),
        }

    except Exception as e:
        return {
            "status": "error",
            "cleaned_file": "",
            "rows": 0,
            "columns": 0,
            "preview": [],
            "error": str(e)
        }


@router.post("/clean-file")
async def clean_file_pipeline(file: UploadFile = File(...)):
    try:
        # 1. Save file to disk
        raw_path = f"data/uploads/{file.filename}"
        save_uploaded_file(file, raw_path)

        # 2. Parse
        df = parse_file(raw_path)
        if df is None or df.empty:
            raise ValueError("Parsed dataframe is empty")

        # 3. Clean
        df = clean_data(df)

        # 4. Save cleaned
        cleaned_path = f"data/cleaned/cleaned_{file.filename}"
        df.to_csv(cleaned_path, index=False)

        return {
            "status": "success",
            "cleaned_file": cleaned_path,
            "rows": len(df),
            "columns": df.columns.tolist()
        }
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        return {
            "status": "error",
            "cleaned_file": "",
            "error": str(e)
        }
        
