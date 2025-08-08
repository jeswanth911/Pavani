# sql_agent.py

import os
import json
import sqlite3
import logging
import requests
import pandas as pd
from typing import List, Dict
from dotenv import load_dotenv
from sqlite3 import Error

# Load API key
load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class NL2SQLAgent:
    def __init__(self, db_path: str, model: str = "mistralai/mistral-7b-instruct:free"):
        self.db_path = db_path
        self.model = model
        self.api_key = os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise EnvironmentError("❌ OPENROUTER_API_KEY not set.")

        self.api_url = "https://openrouter.ai/api/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost",
            "X-Title": "MyBAI-SQLAgent"
        }

    def ask(self, question: str, table_name: str = None):
        """
        Entry point: answer a question by generating SQL → executing it → returning results.
        """
        try:
            schema = self.get_schema()
            prompt = self.generate_prompt(question, schema)
            sql_query = self.call_llm(prompt)
            result = self.execute_sql(sql_query)
            explanation = f"SQL generated: {sql_query}"
            return result, sql_query, explanation

        except Exception as e:
            return [], "", f"❌ Error: {e}"

    def query(self, question: str, table_name: str = None):
        return self.ask(question, table_name)

    def run(self, question: str, table_name: str = None):
        return self.ask(question, table_name)

    def get_schema(self) -> str:
        """Extracts schema from SQLite DB and returns readable string."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            if not tables:
                raise ValueError("No tables found in DB.")

            schema = ""
            for (table_name,) in tables:
                schema += f"\nTable: {table_name}\n"
                cursor.execute(f"PRAGMA table_info({table_name});")
                for col in cursor.fetchall():
                    schema += f" - {col[1]} ({col[2]})\n"

            return schema.strip()

        except Exception as e:
            logger.error(f"⚠️ Schema extraction failed: {e}")
            raise
        finally:
            conn.close()

    def generate_prompt(self, question: str, schema: str) -> str:
        """Generates structured prompt for LLM based on question and DB schema."""
        return f"""
You are a senior data analyst. Write a valid SQLite SELECT query only.

## Database Schema:
{schema}

## User Question:
{question}

## Instructions:
- Use only SELECT queries
- Use correct table and column names
- Do not include explanations, markdown, or comments
- Return only raw SQL
"""

    def call_llm(self, prompt: str) -> str:
        """Sends prompt to OpenRouter and extracts SQL from response."""
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "You convert natural language into SQL queries."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.2,
            "max_tokens": 300
        }

        try:
            logger.info("📤 Sending prompt to OpenRouter...")
            response = requests.post(self.api_url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()

            content = response.json()["choices"][0]["message"]["content"]
            sql_query = self._extract_sql(content)
            logger.info(f"✅ SQL generated: {sql_query}")
            return sql_query

        except requests.RequestException as e:
            logger.error(f"🛑 API error: {e}")
            raise RuntimeError(f"LLM API request failed: {e}")

        except Exception as e:
            logger.error(f"🔴 LLM response error: {e}")
            raise RuntimeError(f"Failed to parse SQL: {e}")

    def _extract_sql(self, content: str) -> str:
        """Extracts raw SQL from LLM response."""
        content = content.strip()
        if content.startswith("```sql"):
            content = content.replace("```sql", "").replace("```", "").strip()
        if not content.lower().startswith("select"):
            raise ValueError("Generated query is not a SELECT statement.")
        return content.strip().rstrip(";")

    def execute_sql(self, query: str) -> List[Dict]:
        """Executes SQL against SQLite DB and returns result rows as dicts."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"❌ SQL execution error: {e}")
            raise RuntimeError(f"SQL execution failed: {e}")
        finally:
            conn.close()


# ✅ Utility Function (bottom)
def convert_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str = "data"):
    """Utility: Convert DataFrame to SQLite."""
    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        logger.info(f"✅ Data written to table `{table_name}` at {db_path}")
    except Exception as e:
        logger.error(f"❌ Failed to convert to SQLite: {e}")
        raise
    finally:
        conn.close()
        
