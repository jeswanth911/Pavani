from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from data_engine.sql_agent import NL2SQLAgent
from utils.logger import logger

router = APIRouter(tags=["Data Ingestion"])


class AskRequest(BaseModel):
    question: str
    db_path: str
    return_explanation: Optional[bool] = False


@router.post("/ask/")
async def ask_question(request: AskRequest):
    """
    Handle natural language questions, convert them to SQL,
    query the SQLite DB, and return structured results.
    """
    try:
        logger.info(f"[NLQ] Received question: {request.question}")
        agent = NL2SQLAgent(db_path=request.db_path)

        # Generate SQL from question
        sql_query, explanation = agent.question_to_sql(request.question)

        if not sql_query:
            raise HTTPException(status_code=400, detail="Could not generate SQL from the given question.")

        logger.info(f"[NLQ] Generated SQL: {sql_query}")

        # Execute SQL and return results
        results = agent.execute_sql(sql_query)
        logger.info(f"[NLQ] SQL executed successfully with {len(results)} rows.")

        response = {
            "status": "success",
            "question": request.question,
            "generated_sql": sql_query,
            "results": results
        }

        if request.return_explanation:
            response["explanation"] = explanation

        return response

    except FileNotFoundError:
        logger.error(f"Database not found at: {request.db_path}")
        raise HTTPException(status_code=404, detail="Database file not found.")

    except Exception as e:
        logger.error(f"[NLQ] Error processing question: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing question: {str(e)}")
      
