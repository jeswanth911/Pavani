import os
import pandas as pd
import shutil
import json
import sqlite3
import re
import mimetypes
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Union
from io import BytesIO, StringIO

# Import libraries with error handling
try:
    import xml.etree.ElementTree as ET
except ImportError:
    ET = None

try:
    import pdfplumber
    from PyPDF2 import PdfReader
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

try:
    from email import policy
    from email.parser import BytesParser
    EMAIL_AVAILABLE = True
except ImportError:
    EMAIL_AVAILABLE = False

try:
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Supported file formats
SUPPORTED_FORMATS = {
    ".csv": "Comma-separated values",
    ".xlsx": "Excel spreadsheet (new format)",
    ".xls": "Excel spreadsheet (old format)",
    ".json": "JSON data",
    ".txt": "Plain text/Tab-separated",
    ".tsv": "Tab-separated values",
    ".xml": "XML document",
    ".html": "HTML document",
    ".log": "Log file",
    ".sql": "SQL script",
    ".parquet": "Parquet file",
    ".pdf": "PDF document",
    ".eml": "Email file",
    ".hl7": "HL7 medical data"
}

UPLOAD_DIR = "data/uploaded"


class FileParsingError(Exception):
    """Custom exception for file parsing errors"""
    pass


def ensure_upload_directory(directory: str = UPLOAD_DIR) -> None:
    """Ensure upload directory exists"""
    Path(directory).mkdir(parents=True, exist_ok=True)


def is_supported_format(filename: str) -> bool:
    """Check if file format is supported"""
    ext = Path(filename).suffix.lower()
    return ext in SUPPORTED_FORMATS


def get_file_info(file_path: str) -> Dict[str, Any]:
    """Get basic file information"""
    path = Path(file_path)
    return {
        "filename": path.name,
        "extension": path.suffix.lower(),
        "size_bytes": path.stat().st_size if path.exists() else 0,
        "is_supported": is_supported_format(path.name)
    }


def save_uploaded_file(file, destination_folder: str = UPLOAD_DIR) -> str:
    """
    Save uploaded file to destination folder
    Works with FastAPI UploadFile or file-like objects
    """
    ensure_upload_directory(destination_folder)
    
    # Handle different file object types
    if hasattr(file, 'filename'):
        filename = file.filename
        file_content = file.file
    else:
        filename = getattr(file, 'name', 'uploaded_file')
        file_content = file
    
    file_path = os.path.join(destination_folder, filename)
    
    try:
        with open(file_path, "wb") as buffer:
            if hasattr(file_content, 'read'):
                shutil.copyfileobj(file_content, buffer)
            else:
                buffer.write(file_content)
        
        logger.info(f"File saved successfully: {file_path}")
        return file_path
        
    except Exception as e:
        logger.error(f"Failed to save file {filename}: {str(e)}")
        raise FileParsingError(f"Failed to save file: {str(e)}")


def parse_csv_file(file_path: str) -> pd.DataFrame:
    """Parse CSV files with multiple encoding attempts"""
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
            logger.info(f"Successfully parsed CSV with {encoding} encoding")
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.warning(f"Failed to parse CSV with {encoding}: {str(e)}")
            continue
    
    raise FileParsingError("Unable to parse CSV file with any supported encoding")


def parse_excel_file(file_path: str) -> pd.DataFrame:
    """Parse Excel files (.xlsx, .xls) with proper engine handling"""
    try:
        ext = Path(file_path).suffix.lower()

        if ext == ".xlsx":
            df = pd.read_excel(file_path, engine="openpyxl")
        elif ext == ".xls":
            df = pd.read_excel(file_path, engine="xlrd")
        else:
            raise FileParsingError("Unsupported Excel file extension")

        return df

    except Exception as e:
        raise FileParsingError(f"Failed to parse Excel file: {str(e)}")
        
        
def parse_json_file(file_path: str) -> pd.DataFrame:
    """Parse JSON files"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Handle different JSON structures
        if isinstance(data, list):
            df = pd.json_normalize(data)
        elif isinstance(data, dict):
            # If it's a single object, wrap in list
            if all(isinstance(v, (str, int, float, bool, type(None))) for v in data.values()):
                df = pd.DataFrame([data])
            else:
                df = pd.json_normalize(data)
        else:
            # Convert to string representation
            df = pd.DataFrame({'json_data': [str(data)]})
        
        return df
        
    except Exception as e:
        raise FileParsingError(f"Failed to parse JSON file: {str(e)}")


def parse_xml_file(file_path: str) -> pd.DataFrame:
    """Parse XML files"""
    if ET is None:
        raise FileParsingError("XML parsing not available. Install required dependencies.")
    
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Try to extract data in a structured way
        data = []
        
        # If root has direct children with text
        for child in root:
            row = {}
            if child.text and child.text.strip():
                row[child.tag] = child.text.strip()
            
            # Add attributes
            for attr_name, attr_value in child.attrib.items():
                row[f"{child.tag}_{attr_name}"] = attr_value
            
            # Add sub-elements
            for subchild in child:
                if subchild.text and subchild.text.strip():
                    row[f"{child.tag}_{subchild.tag}"] = subchild.text.strip()
            
            if row:
                data.append(row)
        
        if not data:
            # Fallback: just store the raw XML
            with open(file_path, 'r', encoding='utf-8') as f:
                xml_content = f.read()
            data = [{'xml_content': xml_content}]
        
        return pd.DataFrame(data)
        
    except Exception as e:
        raise FileParsingError(f"Failed to parse XML file: {str(e)}")


def parse_txt_file(file_path: str) -> pd.DataFrame:
    """Parse text files (assume tab-separated or plain text)"""
    try:
        # First try as tab-separated
        try:
            df = pd.read_csv(file_path, delimiter='\t', encoding='utf-8', low_memory=False)
            if len(df.columns) > 1:
                return df
        except:
            pass
        
        # Try as comma-separated
        try:
            df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
            if len(df.columns) > 1:
                return df
        except:
            pass
        
        # Treat as plain text
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        return pd.DataFrame({'text_line': [line.strip() for line in lines if line.strip()]})
        
    except Exception as e:
        raise FileParsingError(f"Failed to parse text file: {str(e)}")


def parse_pdf_file(file_path: str) -> pd.DataFrame:
    """Parse PDF files"""
    if not PDF_AVAILABLE:
        raise FileParsingError("PDF parsing not available. Install PyPDF2 and pdfplumber.")
    
    try:
        # Try with pdfplumber first (better text extraction)
        try:
            import pdfplumber
            text_content = []
            with pdfplumber.open(file_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    text = page.extract_text()
                    if text:
                        text_content.append({
                            'page_number': page_num,
                            'text_content': text.strip()
                        })
            
            if text_content:
                return pd.DataFrame(text_content)
        except:
            pass
        
        # Fallback to PyPDF2
        reader = PdfReader(file_path)
        text_content = []
        for page_num, page in enumerate(reader.pages, 1):
            text = page.extract_text()
            if text:
                text_content.append({
                    'page_number': page_num,
                    'text_content': text.strip()
                })
        
        if not text_content:
            text_content = [{'page_number': 1, 'text_content': 'No text could be extracted from PDF'}]
        
        return pd.DataFrame(text_content)
        
    except Exception as e:
        raise FileParsingError(f"Failed to parse PDF file: {str(e)}")


def parse_parquet_file(file_path: str) -> pd.DataFrame:
    """Parse Parquet files"""
    if not PARQUET_AVAILABLE:
        raise FileParsingError("Parquet parsing not available. Install pyarrow.")
    
    try:
        return pd.read_parquet(file_path)
    except Exception as e:
        raise FileParsingError(f"Failed to parse Parquet file: {str(e)}")


def parse_email_file(file_path: str) -> pd.DataFrame:
    """Parse email (.eml) files"""
    try:
        with open(file_path, 'rb') as f:
            if EMAIL_AVAILABLE:
                parser = BytesParser(policy=policy.default)
                msg = parser.parse(f)
                
                data = {
                    'subject': msg.get('Subject', ''),
                    'from': msg.get('From', ''),
                    'to': msg.get('To', ''),
                    'date': msg.get('Date', ''),
                    'body': ''
                }
                
                # Extract body
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            data['body'] = part.get_content()
                            break
                else:
                    data['body'] = msg.get_content()
                
                return pd.DataFrame([data])
            else:
                # Simple text parsing
                content = f.read().decode('utf-8', errors='ignore')
                return pd.DataFrame({'email_content': [content]})
        
    except Exception as e:
        raise FileParsingError(f"Failed to parse email file: {str(e)}")


def parse_log_file(file_path: str) -> pd.DataFrame:
    """Parse log files"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Try to parse common log formats
        log_data = []
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if line:
                # Simple log parsing - can be enhanced based on log format
                log_data.append({
                    'line_number': line_num,
                    'log_entry': line
                })
        
        return pd.DataFrame(log_data)
        
    except Exception as e:
        raise FileParsingError(f"Failed to parse log file: {str(e)}")


def parse_sql_file(file_path: str) -> pd.DataFrame:
    """Parse SQL files"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Split SQL statements
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        data = []
        for i, statement in enumerate(statements, 1):
            data.append({
                'statement_number': i,
                'sql_statement': statement
            })
        
        return pd.DataFrame(data)
        
    except Exception as e:
        raise FileParsingError(f"Failed to parse SQL file: {str(e)}")


def parse_hl7_file(file_path: str) -> pd.DataFrame:
    """Parse HL7 medical data files"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Split by message separators (usually line breaks)
        segments = [seg.strip() for seg in content.split('\n') if seg.strip()]
        
        data = []
        for i, segment in enumerate(segments, 1):
            # Basic HL7 segment parsing
            if '|' in segment:
                fields = segment.split('|')
                data.append({
                    'segment_number': i,
                    'segment_type': fields[0] if fields else '',
                    'segment_data': segment
                })
            else:
                data.append({
                    'segment_number': i,
                    'segment_type': 'UNKNOWN',
                    'segment_data': segment
                })
        
        return pd.DataFrame(data)
        
    except Exception as e:
        raise FileParsingError(f"Failed to parse HL7 file: {str(e)}")


def parse_html_file(file_path: str) -> pd.DataFrame:
    """Parse HTML files"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if BS4_AVAILABLE:
            soup = BeautifulSoup(content, 'html.parser')
            text_content = soup.get_text()
        else:
            # Simple HTML tag removal
            text_content = re.sub('<[^<]+?>', '', content)
        
        return pd.DataFrame({'html_content': [text_content.strip()]})
        
    except Exception as e:
        raise FileParsingError(f"Failed to parse HTML file: {str(e)}")


def parse_file(file_path: str) -> pd.DataFrame:
    """
    Main function to parse any supported file format
    Returns a pandas DataFrame
    """
    if not os.path.exists(file_path):
        raise FileParsingError(f"File not found: {file_path}")
    
    file_info = get_file_info(file_path)
    
    if not file_info['is_supported']:
        raise FileParsingError(f"Unsupported file format: {file_info['extension']}")
    
    ext = file_info['extension']
    
    try:
        logger.info(f"Parsing file: {file_path} (format: {ext})")
        
        # Route to appropriate parser based on file extension
        if ext == '.csv':
            return parse_csv_file(file_path)
        elif ext in ['.xlsx', '.xls']:
            return parse_excel_file(file_path)
        elif ext == '.json':
            return parse_json_file(file_path)
        elif ext == '.xml':
            return parse_xml_file(file_path)
        elif ext in ['.txt', '.tsv']:
            return parse_txt_file(file_path)
        elif ext == '.pdf':
            return parse_pdf_file(file_path)
        elif ext == '.parquet':
            return parse_parquet_file(file_path)
        elif ext == '.eml':
            return parse_email_file(file_path)
        elif ext == '.log':
            return parse_log_file(file_path)
        elif ext == '.sql':
            return parse_sql_file(file_path)
        elif ext == '.hl7':
            return parse_hl7_file(file_path)
        elif ext == '.html':
            return parse_html_file(file_path)
        else:
            raise FileParsingError(f"Parser not implemented for {ext}")
    
    except FileParsingError:
        raise
    except Exception as e:
        raise FileParsingError(f"Unexpected error parsing {ext} file: {str(e)}")


def get_dataframe_info(df: pd.DataFrame) -> Dict[str, Any]:
    """Get summary information about the parsed DataFrame"""
    return {
        'shape': df.shape,
        'columns': list(df.columns),
        'dtypes': df.dtypes.to_dict(),
        'null_counts': df.isnull().sum().to_dict(),
        'memory_usage': df.memory_usage(deep=True).sum(),
        'sample_data': df.head().to_dict('records') if not df.empty else []
    }


def validate_and_clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Basic data validation and cleaning"""
    cleaned_df = df.copy()
    
    # Remove completely empty rows
    cleaned_df = cleaned_df.dropna(how='all')
    
    # Remove duplicate rows
    cleaned_df = cleaned_df.drop_duplicates()
    
    # Clean column names (remove special characters, spaces)
    cleaned_df.columns = [re.sub(r'[^\w]', '_', str(col)).lower() for col in cleaned_df.columns]
    
    logger.info(f"Data cleaning complete. Shape: {cleaned_df.shape}")
    return cleaned_df


# Example usage function
def process_file_complete(file_path: str) -> Dict[str, Any]:
    """
    Complete file processing pipeline
    Returns processed data and metadata
    """
    try:
        # Parse file
        df = parse_file(file_path)
        
        # Get info about raw data
        raw_info = get_dataframe_info(df)
        
        # Clean data
        cleaned_df = validate_and_clean_data(df)
        
        # Get info about cleaned data
        cleaned_info = get_dataframe_info(cleaned_df)
        
        return {
            'status': 'success',
            'file_path': file_path,
            'raw_data': df,
            'cleaned_data': cleaned_df,
            'raw_info': raw_info,
            'cleaned_info': cleaned_info,
            'processing_notes': f"Successfully processed {raw_info['shape'][0]} rows, {raw_info['shape'][1]} columns"
        }
        
    except Exception as e:
        logger.error(f"Failed to process file {file_path}: {str(e)}")
        return {
            'status': 'error',
            'error': str(e),
            'file_path': file_path
        }
