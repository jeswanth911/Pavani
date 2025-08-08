from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from data_engine.sql_agent import NL2SQLAgent
from fastapi import APIRouter
import sqlite3
import os




router = APIRouter(prefix="/api")

class QuestionInput(BaseModel):
    question: str
    db_path: str  # e.g., "data/output/mydata.sqlite"

@router.post("/ask/")
def ask_question(input_data: QuestionInput):
    try:
        agent = NL2SQLAgent(input_data.db_path)
        result = agent.ask(input_data.question)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
      
@router.post("/fix-db")
def fix_db():
    db_path = "data/mydb.sqlite"  # replace if you use a different path
    os.makedirs("data", exist_ok=True)

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create test table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            email TEXT
        );
        """)

        # Insert sample data
        cursor.execute("INSERT INTO customers (name, age, email) VALUES (?, ?, ?)", ("Alice", 30, "alice@example.com"))
        cursor.execute("INSERT INTO customers (name, age, email) VALUES (?, ?, ?)", ("Bob", 42, "bob@example.com"))

        conn.commit()
        conn.close()

        return {"status": "success", "message": "✅ Database created with test data."}

    except Exception as e:
        return {"status": "error", "error": str(e)}
