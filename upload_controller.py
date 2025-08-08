import os
import uuid
from fastapi import FastAPI
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from controller.upload_pipeline import process_uploaded_file
from utils.file_parser import save_uploaded_file
from utils.logger import logger

router = APIRouter(tags=["Data Ingestion"])

UPLOAD_DIR = "data/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Upload a file and trigger full data processing pipeline:
    - Parse → Clean → Analyze → Convert to SQLite
    """
    try:
        # Generate safe, unique filename
        ext = os.path.splitext(file.filename)[-1]
        filename = f"{uuid.uuid4().hex}{ext}"
        file_path = os.path.join(UPLOAD_DIR, filename)

        # Save uploaded file to disk
        saved_path = await save_uploaded_file(file, UPLOAD_DIR)
        logger.info(f"File uploaded and saved to: {saved_path}")

        # Run full data pipeline
        pipeline_result = run_upload_pipeline(saved_path)

        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "db_path": pipeline_result.get("db_path"),
                "analysis_summary": pipeline_result.get("analysis_summary"),
                "message": "Upload successful. You can now ask a question."
            }
        )

    except HTTPException as e:
        logger.error(f"Upload failed: {e.detail}")
        raise

    except Exception as e:
        logger.exception(f"Unhandled error during upload: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error during file upload.")
      
def include_routes(app: FastAPI):
    app.include_router(upload_router)
