
# controller/workflow_controller.py

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from utils.logger import logger
from utils.file_parser import parse_file
from data_engine.cleaner import clean_data
from data_engine.analyzer import analyze_data
from data_engine.sql_agent import NL2SQLAgent
from pathlib import Path
import shutil
import os
import uuid

router = APIRouter()


@router.post("/run-workflow/")
async def run_workflow(
    file: UploadFile = File(...),
    question: str = Form(...),
    method: str = Form("query")  # Can be 'query', 'ask', or 'run'
):
    try:
        # Step 1: Save uploaded file
        upload_dir = "data/uploads"
        os.makedirs(upload_dir, exist_ok=True)
        temp_filename = f"{uuid.uuid4().hex}_{file.filename}"
        file_path = os.path.join(upload_dir, temp_filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Step 2: Parse the file into DataFrame
        df = parse_file(file_path)

        # Step 3: Clean the data
        cleaned_df, cleaning_report = clean_data(df)

        # Step 4: Analyze the cleaned data
        analysis_summary = analyze_data(cleaned_df)

        # Step 5: Save cleaned DataFrame to SQLite
        sqlite_dir = "data/sqlite"
        os.makedirs(sqlite_dir, exist_ok=True)
        table_name = Path(file.filename).stem.replace(" ", "_")
        sqlite_filename = f"{Path(temp_filename).stem}_cleaned.db"
        sqlite_path = os.path.join(sqlite_dir, sqlite_filename)
        cleaned_df.to_sql(table_name, f"sqlite:///{sqlite_path}", index=False, if_exists="replace")

        # Step 6: Use NL2SQLAgent with selected method
        agent = NL2SQLAgent(sqlite_path)

        # Only pass question and table_name (not 3 arguments!)
        if method == "query":
            output = agent.query(question, table_name)
        elif method == "ask":
            output = agent.ask(question, table_name)
        elif method == "run":
            output = agent.run(question, table_name)
        else:
            raise ValueError(f"❌ Invalid method: {method}. Use 'query', 'ask', or 'run'.")

        # Handle return type: single dict or tuple
        if isinstance(output, tuple):
            result, sql_query, explanation = output
        else:
            result = output
            sql_query = explanation = None

        # Step 7: Return results
        return {
            "status": "success",
            "message": "Workflow completed successfully.",
            "analysis_summary": analysis_summary,
            "cleaning_report": cleaning_report,
            "sqlite_path": sqlite_path,
            "question": question,
            "method_used": method,
            "sql_used": sql_query,
            "answer": result,
            "explanation": explanation
        }

    except Exception as e:
        logger.error(f"❌ Workflow failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Workflow failed: {str(e)}")
        
