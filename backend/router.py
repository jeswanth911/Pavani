
import os
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from controller.predictor import predict_from_file
from data_engine.sql_agent import NL2SQLAgent
from controller.upload_pipeline import process_uploaded_file
from controller.workflow_manager import run_full_workflow
from controller.cleaner_controller import clean_file_pipeline
from controller.workflow_controller import router as workflow_router
from utils.logger import logger
from controller.question_controller import router as question_router
from controller.upload_controller import router as upload_router
from utils.file_parser import save_uploaded_file
from backend.controller import (
    clean_data_controller,
    analyze_data_controller,
    convert_to_sqlite_controller,
)


router = APIRouter(
    prefix="/api",
    tags=["Data Ingestion"]
)

ALLOWED_EXTENSIONS = [
    ".csv", ".xlsx", ".xls", ".json", ".txt", ".xml", ".pdf", ".hl7",
    ".parquet", ".sql", ".log", ".eml"
]

class UploadResponse(BaseModel):
    status: str
    cleaned_file: Optional[str] = ""
    analysis_summary: Optional[dict] = {}
    sqlite_db_path: Optional[str] = ""
    error: Optional[str] = None


class QuestionInput(BaseModel):
    question: str
    db_path: str 


@router.post("/upload/", response_model=UploadResponse, tags=["Data Ingestion"])
async def upload_file(file: UploadFile = File(...)):
    """
    Upload and process a file: clean → analyze → convert to SQLite DB.
    """
    try:
        filename = file.filename
        ext = os.path.splitext(filename)[1].lower()

        if ext not in ALLOWED_EXTENSIONS:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {ext}")

        # Step 1: Save the uploaded file
        uploaded_path = save_uploaded_file(file, destination_folder="data/uploaded")
        logger.info(f"[UPLOAD] File saved: {uploaded_path}")

        # Step 2: Clean the data
        clean_result = clean_data_controller(uploaded_path)
        if clean_result["status"] != "success":
            raise Exception(clean_result.get("error", "Unknown cleaning error"))
        
        cleaned_file = clean_result["cleaned_file"]
        df = clean_result["dataframe"]
        logger.info(f"[CLEAN] Cleaned file: {cleaned_file}")

        # Step 3: Analyze the cleaned data
        analysis_result = analyze_data_controller(df)
        if analysis_result["status"] != "success":
            raise Exception(analysis_result.get("error", "Unknown analysis error"))
        
        logger.info("[ANALYZE] Analysis completed.")

        # Step 4: Convert to SQLite
        db_result = convert_to_sqlite_controller(df)
        if db_result["status"] != "success":
            raise Exception(db_result.get("error", "SQLite conversion failed."))
        
        sqlite_path = db_result["sqlite_db_path"]
        logger.info(f"[SQLITE] DB created at: {sqlite_path}")

        return UploadResponse(
            status="success",
            cleaned_file=cleaned_file,
            analysis_summary=analysis_result["analysis_summary"],
            sqlite_db_path=sqlite_path
        )

    except Exception as e:
        logger.error(f"[ERROR] Upload pipeline failed: {e}", exc_info=True)
        return UploadResponse(
            status="error",
            error=str(e)
        )


@router.post("/ask/")
def ask_question(input_data: QuestionInput):
    try:
        agent = NL2SQLAgent(input_data.db_path)
        result = agent.ask(input_data.question)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
        


@router.post("/debug/db-tables")
def debug_db(db_path: str):
    import sqlite3

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()

        if not tables:
            return {"status": "error", "error": "No tables found in the database."}

        table_info = {}

        for table in tables:
            name = table[0]
            columns = cursor.execute(f"PRAGMA table_info({name});").fetchall()
            column_names = [col[1] for col in columns]
            row_count = cursor.execute(f"SELECT COUNT(*) FROM {name};").fetchone()[0]
            table_info[name] = {"columns": column_names, "rows": row_count}

        conn.close()
        return {"status": "success", "tables": table_info}

    except Exception as e:
        return {"status": "error", "error": str(e)}
        

