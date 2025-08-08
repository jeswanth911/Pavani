import os
import pandas as pd
from utils.logger import logger
from utils.file_parser import parse_file
from data_engine.cleaner import clean_data_file
from data_engine.analyzer import analyze_data
from controller.sqlite_converter import convert_to_sqlite_df
from data_engine.sql_agent import NL2SQLAgent
from typing import Optional, Dict

def run_full_workflow(file_path: str, question: Optional[str] = None, method: str = "query") -> Dict:
    """
    Orchestrates the full data pipeline from file parsing to analysis, DB storage, and optional NLQ.

    Args:
        file_path (str): Path to the uploaded file.
        question (str, optional): Natural language question about the data.
        method (str): Which method to use for the NL2SQL agent: "ask", "query", or "run"

    Returns:
        dict: Structured JSON-like response containing processing results.
    """
    try:
        logger.info(f"Workflow started for file: {file_path}")

        # 1. Parse the uploaded file
        df = parse_file(file_path)
        if df is None or df.empty:
            raise ValueError("Failed to parse file or file is empty.")

        logger.info("File parsed successfully.")

        # 2. Clean the data
        cleaned_df, cleaning_report = clean_data_file(df)
        if cleaned_df is None or cleaned_df.empty:
            raise ValueError("Cleaning failed. Data is empty after cleaning.")

        logger.info("Data cleaned successfully.")

        # 3. Analyze the cleaned data
        analysis_summary = analyze_data(cleaned_df)
        logger.info("Data analysis completed.")

        # 4. Convert cleaned data to SQLite DB
        db_path = convert_to_sqlite_df(cleaned_df, original_file_path=file_path)
        logger.info(f"Data converted to SQLite DB at: {db_path}")

        response = {
            "status": "success",
            "data_summary": {
                "cleaning_report": cleaning_report,
                "analysis_summary": analysis_summary,
                "sqlite_db_path": db_path
            },
            "query_result": None,
            "explanation": None,
            "visual_path": None
        }

        # 5. If a question is provided, run NL2SQL
        if question:
            logger.info(f"Running NL2SQL agent with method '{method}' for question: {question}")
            agent = NL2SQLAgent(db_path)

            if method == "query":
                query_result, explanation, sql_query = agent.query(question)
            elif method == "ask":
                result = agent.ask(question)
                query_result = result.get("result")
                explanation = result.get("explanation", "")
                sql_query = result.get("sql_query", "")
            elif method == "run":
                result = agent.run(question)
                query_result = result.get("result")
                explanation = result.get("explanation", "")
                sql_query = result.get("sql_query", "")
            else:
                raise ValueError("Invalid method. Choose from 'query', 'ask', or 'run'.")

            logger.info("SQL query executed successfully.")
            logger.debug(f"Generated SQL: {sql_query}")

            # 6. Generate visualization
            visual_path = generate_visualization(query_result, question)
            logger.info(f"Visualization saved at: {visual_path}")

            response.update({
                "query_result": query_result.to_dict(orient="records") if isinstance(query_result, pd.DataFrame) else query_result,
                "explanation": explanation,
                "visual_path": visual_path
            })

        return response

    except Exception as e:
        logger.exception(f"Workflow failed: {str(e)}")
        return {
            "status": "error",
            "data_summary": {},
            "query_result": None,
            "explanation": None,
            "visual_path": None,
            "error": str(e)
        }
        
