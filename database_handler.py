import sqlite3
import pandas as pd
import os
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
import hashlib
from datetime import datetime

logger = logging.getLogger(__name__)


class DatabaseHandler:
    """Handle SQLite database operations for parsed data"""
    
    def __init__(self, db_path: str = "data/processed_data.db"):
        self.db_path = db_path
        self.ensure_db_directory()
        self.init_database()
    
    def ensure_db_directory(self):
        """Ensure database directory exists"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
    
    def init_database(self):
        """Initialize database with metadata tables"""
        with sqlite3.connect(self.db_path) as conn:
            # Create metadata table to track uploaded files
            conn.execute("""
                CREATE TABLE IF NOT EXISTS file_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    original_filename TEXT NOT NULL,
                    table_name TEXT NOT NULL UNIQUE,
                    file_type TEXT NOT NULL,
                    upload_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    file_hash TEXT,
                    row_count INTEGER,
                    column_count INTEGER,
                    file_size_bytes INTEGER,
                    processing_status TEXT DEFAULT 'success'
                )
            """)
            
            # Create table to store column information
            conn.execute("""
                CREATE TABLE IF NOT EXISTS column_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    data_type TEXT,
                    null_count INTEGER,
                    unique_count INTEGER,
                    sample_values TEXT,
                    FOREIGN KEY (table_name) REFERENCES file_metadata (table_name)
                )
            """)
            
            conn.commit()
            logger.info("Database initialized successfully")
    
    def generate_table_name(self, filename: str) -> str:
        """Generate a clean table name from filename"""
        # Remove extension and clean name
        base_name = Path(filename).stem
        # Replace special characters with underscores
        clean_name = ''.join(c if c.isalnum() else '_' for c in base_name.lower())
        # Ensure it doesn't start with a number
        if clean_name[0].isdigit():
            clean_name = 'table_' + clean_name
        
        # Add timestamp to make it unique
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{clean_name}_{timestamp}"
    
    def calculate_file_hash(self, file_path: str) -> str:
        """Calculate MD5 hash of file for duplicate detection"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def dataframe_to_sql(self, 
                        df: pd.DataFrame, 
                        filename: str,
                        file_path: str,
                        file_type: str,
                        if_exists: str = 'replace') -> Dict[str, Any]:
        """
        Convert DataFrame to SQL table
        
        Args:
            df: Pandas DataFrame
            filename: Original filename
            file_path: Path to the file
            file_type: Type of file (csv, excel, etc.)
            if_exists: What to do if table exists ('fail', 'replace', 'append')
        
        Returns:
            Dictionary with operation results
        """
        try:
            table_name = self.generate_table_name(filename)
            file_hash = self.calculate_file_hash(file_path)
            file_size = os.path.getsize(file_path)
            
            with sqlite3.connect(self.db_path) as conn:
                # Store the DataFrame as a table
                df.to_sql(table_name, conn, if_exists=if_exists, index=False)
                
                # Store metadata
                conn.execute("""
                    INSERT OR REPLACE INTO file_metadata 
                    (original_filename, table_name, file_type, file_hash, 
                     row_count, column_count, file_size_bytes)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (filename, table_name, file_type, file_hash, 
                      len(df), len(df.columns), file_size))
                
                # Store column metadata
                for column in df.columns:
                    col_data = df[column]
                    null_count = col_data.isnull().sum()
                    unique_count = col_data.nunique()
                    
                    # Get sample values (non-null, first 5)
                    sample_values = col_data.dropna().head(5).astype(str).tolist()
                    sample_str = ', '.join(sample_values) if sample_values else 'No data'
                    
                    conn.execute("""
                        INSERT INTO column_metadata 
                        (table_name, column_name, data_type, null_count, unique_count, sample_values)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (table_name, column, str(col_data.dtype), 
                          int(null_count), int(unique_count), sample_str))
                
                conn.commit()
            
            logger.info(f"Successfully saved DataFrame to table: {table_name}")
            
            return {
                'status': 'success',
                'table_name': table_name,
                'rows_inserted': len(df),
                'columns': list(df.columns),
                'database_path': self.db_path,
                'file_hash': file_hash
            }
            
        except Exception as e:
            logger.error(f"Failed to save DataFrame to database: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'table_name': None
            }
    
    def get_table_list(self) -> List[Dict[str, Any]]:
        """Get list of all tables with metadata"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT table_name, original_filename, file_type, 
                       upload_timestamp, row_count, column_count
                FROM file_metadata 
                ORDER BY upload_timestamp DESC
            """)
            
            tables = []
            for row in cursor.fetchall():
                tables.append({
                    'table_name': row[0],
                    'original_filename': row[1],
                    'file_type': row[2],
                    'upload_timestamp': row[3],
                    'row_count': row[4],
                    'column_count': row[5]
                })
            
            return tables
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific table"""
        with sqlite3.connect(self.db_path) as conn:
            # Get basic table info
            cursor = conn.execute("""
                SELECT * FROM file_metadata WHERE table_name = ?
            """, (table_name,))
            
            metadata = cursor.fetchone()
            if not metadata:
                return {'error': 'Table not found'}
            
            # Get column info
            cursor = conn.execute("""
                SELECT column_name, data_type, null_count, unique_count, sample_values
                FROM column_metadata WHERE table_name = ?
            """, (table_name,))
            
            columns = []
            for col_row in cursor.fetchall():
                columns.append({
                    'name': col_row[0],
                    'type': col_row[1],
                    'null_count': col_row[2],
                    'unique_count': col_row[3],
                    'sample_values': col_row[4]
                })
            
            return {
                'table_name': metadata[2],
                'original_filename': metadata[1],
                'file_type': metadata[3],
                'upload_timestamp': metadata[4],
                'row_count': metadata[6],
                'column_count': metadata[7],
                'columns': columns
            }
    
    def query_data(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query(query, conn)
                return df
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def get_sample_data(self, table_name: str, limit: int = 10) -> pd.DataFrame:
        """Get sample data from a table"""
        query = f"SELECT * FROM [{table_name}] LIMIT {limit}"
        return self.query_data(query)
    
    def search_data(self, table_name: str, search_term: str, limit: int = 50) -> pd.DataFrame:
        """Search for data in a table"""
        try:
            # Get column names first
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(f"PRAGMA table_info([{table_name}])")
                columns = [row[1] for row in cursor.fetchall()]
            
            # Build search query for text columns
            search_conditions = []
            for col in columns:
                search_conditions.append(f"CAST([{col}] AS TEXT) LIKE ?")
            
            search_query = f"""
                SELECT * FROM [{table_name}] 
                WHERE {' OR '.join(search_conditions)}
                LIMIT {limit}
            """
            
            search_params = [f"%{search_term}%"] * len(columns)
            
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query(search_query, conn, params=search_params)
                return df
                
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            return pd.DataFrame()
    
    def get_column_stats(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """Get statistics for a specific column"""
        try:
            query = f"SELECT [{column_name}] FROM [{table_name}] WHERE [{column_name}] IS NOT NULL"
            df = self.query_data(query)
            
            if df.empty:
                return {'error': 'No data found'}
            
            column_data = df[column_name]
            stats = {
                'count': len(column_data),
                'unique_values': column_data.nunique(),
                'most_common': column_data.mode().iloc[0] if not column_data.mode().empty else None
            }
            
            # Add numeric statistics if column is numeric
            if pd.api.types.is_numeric_dtype(column_data):
                stats.update({
                    'mean': float(column_data.mean()),
                    'median': float(column_data.median()),
                    'std': float(column_data.std()),
                    'min': float(column_data.min()),
                    'max': float(column_data.max())
                })
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get column stats: {str(e)}")
            return {'error': str(e)}
    
    def delete_table(self, table_name: str) -> bool:
        """Delete a table and its metadata"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Delete the actual table
                conn.execute(f"DROP TABLE IF EXISTS [{table_name}]")
                
                # Delete metadata
                conn.execute("DELETE FROM file_metadata WHERE table_name = ?", (table_name,))
                conn.execute("DELETE FROM column_metadata WHERE table_name = ?", (table_name,))
                
                conn.commit()
            
            logger.info(f"Successfully deleted table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete table {table_name}: {str(e)}")
            return False
    
    def export_table_to_csv(self, table_name: str, output_path: str) -> bool:
        """Export table data to CSV"""
        try:
            df = self.query_data(f"SELECT * FROM [{table_name}]")
            df.to_csv(output_path, index=False)
            logger.info(f"Table {table_name} exported to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to export table {table_name}: {str(e)}")
            return False
    
    def get_database_summary(self) -> Dict[str, Any]:
        """Get overall database summary"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT 
                        COUNT(*) as total_tables,
                        SUM(row_count) as total_rows,
                        SUM(column_count) as total_columns,
                        SUM(file_size_bytes) as total_file_size
                    FROM file_metadata
                """)
                
                summary = cursor.fetchone()
                
                # Get file type distribution
                cursor = conn.execute("""
                    SELECT file_type, COUNT(*) as count
                    FROM file_metadata
                    GROUP BY file_type
                """)
                
                file_types = {row[0]: row[1] for row in cursor.fetchall()}
                
                return {
                    'total_tables': summary[0] or 0,
                    'total_rows': summary[1] or 0,
                    'total_columns': summary[2] or 0,
                    'total_file_size_mb': round((summary[3] or 0) / (1024 * 1024), 2),
                    'file_type_distribution': file_types,
                    'database_size_mb': round(os.path.getsize(self.db_path) / (1024 * 1024), 2) if os.path.exists(self.db_path) else 0
                }
        
        except Exception as e:
            logger.error(f"Failed to get database summary: {str(e)}")
            return {'error': str(e)}


class NaturalLanguageQueryProcessor:
    """Process natural language queries and convert to SQL"""
    
    def __init__(self, db_handler: DatabaseHandler):
        self.db_handler = db_handler
    
    def process_query(self, question: str, table_name: str = None) -> Dict[str, Any]:
        """
        Process natural language question and return SQL query and results
        This is a simple implementation - you can enhance it with AI/ML models
        """
        question_lower = question.lower()
        
        # Get available tables if no specific table mentioned
        if not table_name:
            tables = self.db_handler.get_table_list()
            if not tables:
                return {'error': 'No tables available in database'}
            table_name = tables[0]['table_name']  # Use most recent table
        
        # Get table info
        table_info = self.db_handler.get_table_info(table_name)
        if 'error' in table_info:
            return table_info
        
        columns = [col['name'] for col in table_info['columns']]
        
        try:
            # Simple query patterns
            if any(word in question_lower for word in ['count', 'how many', 'total']):
                sql_query = f"SELECT COUNT(*) as total_count FROM [{table_name}]"
                
            elif any(word in question_lower for word in ['show', 'display', 'list', 'what']):
                limit = 10
                if 'all' in question_lower:
                    sql_query = f"SELECT * FROM [{table_name}]"
                else:
                    sql_query = f"SELECT * FROM [{table_name}] LIMIT {limit}"
                    
            elif 'average' in question_lower or 'mean' in question_lower:
                # Try to find numeric columns
                numeric_cols = []
                for col_info in table_info['columns']:
                    if 'int' in col_info['type'].lower() or 'float' in col_info['type'].lower():
                        numeric_cols.append(col_info['name'])
                
                if numeric_cols:
                    avg_queries = [f"AVG([{col}]) as avg_{col}" for col in numeric_cols[:3]]
                    sql_query = f"SELECT {', '.join(avg_queries)} FROM [{table_name}]"
                else:
                    sql_query = f"SELECT COUNT(*) as total_rows FROM [{table_name}]"
                    
            elif 'unique' in question_lower or 'distinct' in question_lower:
                # Find the first text column
                text_col = columns[0] if columns else 'id'
                sql_query = f"SELECT DISTINCT [{text_col}] FROM [{table_name}] LIMIT 20"
                
            elif 'maximum' in question_lower or 'max' in question_lower:
                # Find numeric columns
                numeric_cols = []
                for col_info in table_info['columns']:
                    if 'int' in col_info['type'].lower() or 'float' in col_info['type'].lower():
                        numeric_cols.append(col_info['name'])
                
                if numeric_cols:
                    max_queries = [f"MAX([{col}]) as max_{col}" for col in numeric_cols[:3]]
                    sql_query = f"SELECT {', '.join(max_queries)} FROM [{table_name}]"
                else:
                    sql_query = f"SELECT * FROM [{table_name}] LIMIT 5"
                    
            else:
                # Default: show sample data
                sql_query = f"SELECT * FROM [{table_name}] LIMIT 10"
            
            # Execute query
            result_df = self.db_handler.query_data(sql_query)
            
            return {
                'status': 'success',
                'question': question,
                'sql_query': sql_query,
                'results': result_df.to_dict('records'),
                'row_count': len(result_df),
                'table_used': table_name
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'question': question
            }
    
    def suggest_questions(self, table_name: str) -> List[str]:
        """Suggest possible questions for a table"""
        table_info = self.db_handler.get_table_info(table_name)
        
        if 'error' in table_info:
            return []
        
        suggestions = [
            f"How many records are in {table_info['original_filename']}?",
            f"Show me the first 10 rows of data",
            f"What are the unique values in the data?",
            f"Show me all the data"
        ]
        
        # Add column-specific suggestions
        for col in table_info['columns'][:3]:  # First 3 columns
            if 'int' in col['type'].lower() or 'float' in col['type'].lower():
                suggestions.append(f"What is the average {col['name']}?")
                suggestions.append(f"What is the maximum {col['name']}?")
            else:
                suggestions.append(f"What are the unique {col['name']} values?")
        
        return suggestions[:8]  # Return max 8 suggestions


# Example usage and testing functions
def test_database_operations():
    """Test function to demonstrate database operations"""
    
    # Initialize database handler
    db_handler = DatabaseHandler("test_data.db")
    
    # Create sample DataFrame
    sample_data = pd.DataFrame({
        'name': ['John', 'Jane', 'Bob', 'Alice'],
        'age': [25, 30, 35, 28],
        'city': ['New York', 'London', 'Paris', 'Tokyo'],
        'salary': [50000, 60000, 70000, 55000]
    })
    
    # Save to database
    result = db_handler.dataframe_to_sql(
        df=sample_data,
        filename="sample_data.csv",
        file_path="sample_data.csv",  # This would be actual file path
        file_type="csv"
    )
    
    print("Save result:", result)
    
    # Get table list
    tables = db_handler.get_table_list()
    print("Tables:", tables)
    
    # Query data
    if tables:
        table_name = tables[0]['table_name']
        sample_query_result = db_handler.get_sample_data(table_name, 5)
        print("Sample data:")
        print(sample_query_result)
        
        # Test natural language queries
        nlq_processor = NaturalLanguageQueryProcessor(db_handler)
        
        questions = [
            "How many people are in the data?",
            "What is the average age?",
            "Show me all the data",
            "What are the unique cities?"
        ]
        
        for question in questions:
            result = nlq_processor.process_query(question, table_name)
            print(f"\nQuestion: {question}")
            print(f"SQL: {result.get('sql_query', 'N/A')}")
            print(f"Results: {result.get('results', 'Error')}")


if __name__ == "__main__":
    test_database_operations()