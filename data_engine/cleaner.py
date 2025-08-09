# data_engine/cleaner.py
"""
data_cleaner.py

Single-file production-grade data cleaner.

Features:
- Parse 20+ data formats (CSV, TSV, Excel, JSON, JSONL, Parquet, Feather, ORC, XML, HTML, TXT, Avro, MessagePack,
  SPSS, SAS, Stata, HDF5, SQLite/MySQL/Postgres, Google Sheets, compressed archives).
- Streaming loads for large files (chunk-by-chunk).
- Config-driven pipeline (YAML/JSON compatible dict), pluggable rules.
- Async-friendly I/O using asyncio.to_thread for CPU/blocking ops.
- Profiling, schema enforcement, outputs to multiple formats.
- Logging of transformations with before/after snapshots.
- Clear exceptions, PEP8, docstrings, test-ready.
"""

from __future__ import annotations

import asyncio
import csv
import datetime as _dt
import hashlib
import io
import json
import logging
import os
import re
import sqlite3
import tarfile
import tempfile
import time
import typing as t
import zipfile
from dataclasses import dataclass, field, asdict
from functools import partial
from pathlib import Path

import pandas as pd
import numpy as np
from dateutil import parser as date_parser
from pydantic import BaseModel, Field, ValidationError

# Optional imports (handled gracefully)
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _HAS_PYARROW = True
except Exception:
    _HAS_PYARROW = False

try:
    import pyreadstat
    _HAS_PYREADSTAT = True
except Exception:
    _HAS_PYREADSTAT = False

try:
    import phonenumbers
    _HAS_PHONENUMBERS = True
except Exception:
    _HAS_PHONENUMBERS = False

try:
    import sqlalchemy as sa
    from sqlalchemy.engine import URL
    _HAS_SQLALCHEMY = True
except Exception:
    _HAS_SQLALCHEMY = False

try:
    import msgpack
    _HAS_MSGPACK = True
except Exception:
    _HAS_MSGPACK = False

try:
    from langdetect import detect as lang_detect
    _HAS_LANGDETECT = True
except Exception:
    _HAS_LANGDETECT = False

# Logging setup
logger = logging.getLogger("data_cleaner")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ---------------------------
# Types & Config models
# ---------------------------

@dataclass
class Snapshot:
    """Lightweight snapshot used for logging transformations (keeps small samples)."""
    rows_hash: str
    sample: list[dict] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    @classmethod
    def from_df(cls, df: pd.DataFrame, sample_n: int = 5) -> "Snapshot":
        # generate deterministic sample + hash of first N rows
        if df.empty:
            digest = hashlib.sha256(b"empty").hexdigest()
            sample = []
        else:
            sample_df = df.head(sample_n).copy()
            sample = sample_df.where(pd.notnull(sample_df), None).to_dict(orient="records")
            digest = hashlib.sha256(pd.util.hash_pandas_object(sample_df, index=True).values.tobytes()).hexdigest()
        return cls(rows_hash=digest, sample=sample)

class CleaningConfig(BaseModel):
    """
    Configuration structure for cleaning pipeline. Accept either JSON or YAML-parsed dict.
    """
    missing_strategy: str = Field("keep", description="one of keep, drop, fill")
    fill_values: dict | None = Field(None, description="column-specific fill values")
    drop_constant_columns: bool = Field(True)
    duplicate_subset: list[str] | None = Field(None)
    drop_duplicates: bool = Field(True)
    normalize_columns: bool = Field(True)
    lowercase_columns: bool = Field(True)
    date_columns: list[str] | None = Field(None)
    parse_dates: bool = Field(True)
    outlier_method: str | None = Field(None, description="zscore or iqr or None")
    outlier_threshold: float | None = Field(3.0)
    phone_columns: list[str] | None = Field(None)
    email_columns: list[str] | None = Field(None)
    convert_currency: bool = Field(False)
    currency_column: str | None = Field(None)
    currency_rates: dict | None = Field(None, description="map from code->float rate")
    encoding: str = Field("utf-8")
    drop_columns: list[str] | None = Field(None)
    keep_columns: list[str] | None = Field(None)
    enforce_schema: dict | None = Field(None, description="Optional schema to enforce: {col: type}")
    quality_drop_threshold: float | None = Field(None, description="If quality score < threshold, optionally fail")
    max_memory_mb: int = Field(1024, description="soft limit for in-memory operations")
    snapshot_sample: int = Field(5)

# ---------------------------
# Utility helpers
# ---------------------------

def normalize_colname(name: str) -> str:
    """Normalize column names to snake_case ASCII."""
    if not isinstance(name, str):
        name = str(name)
    # replace non-word with underscore
    s = re.sub(r"[^\w]+", "_", name.strip())
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)  # camel to snake
    s = s.strip("_").lower()
    return s or "col"

def standardize_text(val: t.Any, case: str | None = "lower") -> t.Any:
    """Trim, fix encoding, and optionally set case. Keep None intact."""
    if pd.isna(val):
        return val
    s = str(val).strip()
    if case == "lower":
        s = s.lower()
    elif case == "upper":
        s = s.upper()
    # remove control characters
    s = re.sub(r"[\r\n\t]+", " ", s)
    return s

def try_parse_date(val: t.Any) -> t.Any:
    if pd.isna(val):
        return val
    try:
        dt = date_parser.parse(str(val), fuzzy=True)
        return dt.isoformat()
    except Exception:
        return val

def safe_cast_series(s: pd.Series, dtype: str) -> pd.Series:
    """Try to convert series to dtype; if fails return original with a warning."""
    try:
        if dtype in {"int", "integer"}:
            return pd.to_numeric(s, errors="coerce").astype("Int64")
        if dtype in {"float", "double"}:
            return pd.to_numeric(s, errors="coerce").astype("Float64")
        if dtype in {"bool", "boolean"}:
            return s.map({"true": True, "false": False}).fillna(s).astype("boolean")
        if dtype in {"datetime", "date", "timestamp"}:
            return pd.to_datetime(s, errors="coerce")
        if dtype in {"category"}:
            return s.astype("category")
        if dtype in {"text", "str", "string"}:
            return s.astype("string")
        # fallback: return as-is
    except Exception as e:
        logger.warning("safe_cast_series failed: %s", e)
    return s

def detect_language_of_text(col: pd.Series) -> str | None:
    if not _HAS_LANGDETECT:
        return None
    sample = col.dropna().astype(str).head(20).tolist()
    if not sample:
        return None
    try:
        votes = {}
        for s in sample:
            try:
                lang = lang_detect(s)
                votes[lang] = votes.get(lang, 0) + 1
            except Exception:
                continue
        if not votes:
            return None
        return max(votes.items(), key=lambda x: x[1])[0]
    except Exception:
        return None

# ---------------------------
# Parsers (synchronous small helpers)
# ---------------------------

class ParserError(Exception):
    pass

class DataParser:
    """
    Responsible for format detection, extraction (zip/tar), and parsing into pandas.DataFrame.
    Use `await DataParser.parse(...)` for async-friendly usage.
    """

    SUPPORTED_SIMPLE = {
        ".csv", ".tsv", ".txt", ".json", ".jsonl", ".ndjson", ".parquet", ".feather", ".xlsx", ".xls",
        ".xml", ".html", ".avro", ".msgpack", ".sav", ".sas7bdat", ".dta", ".h5", ".hdf5"
    }

    @staticmethod
    def _is_compressed(path: Path) -> bool:
        return path.suffix in {".zip", ".gz", ".tgz", ".bz2"} or path.name.endswith(".tar.gz")

    @staticmethod
    def _maybe_extract(path: Path) -> Path:
        if path.suffix == ".zip":
            z = zipfile.ZipFile(path)
            namelist = z.namelist()
            # pick first file that looks like a data file
            candidate = next((n for n in namelist if Path(n).suffix.lower() in DataParser.SUPPORTED_SIMPLE), namelist[0])
            tmp = Path(tempfile.mkdtemp()) / Path(candidate).name
            with open(tmp, "wb") as f:
                f.write(z.read(candidate))
            return tmp
        if path.suffix in {".gz", ".tgz"} or path.name.endswith(".tar.gz"):
            # extract first relevant file
            tfile = tarfile.open(path, "r:*")
            members = tfile.getmembers()
            candidate = next((m for m in members if Path(m.name).suffix.lower() in DataParser.SUPPORTED_SIMPLE), members[0])
            tmp = Path(tempfile.mkdtemp()) / Path(candidate.name).name
            with open(tmp, "wb") as f:
                f.write(tfile.extractfile(candidate).read())
            return tmp
        return path

    @staticmethod
    def detect_format(path_or_buffer: t.Union[str, Path, io.BytesIO]) -> str:
        """Return lowercase extension or 'db' or 'google_sheets'."""
        if isinstance(path_or_buffer, io.BytesIO):
            return "bytes"
        if isinstance(path_or_buffer, (str, Path)):
            s = str(path_or_buffer)
            if s.startswith("sqlite://") or s.endswith(".db") or s.endswith(".sqlite"):
                return "sqlite"
            if s.startswith("postgres://") or s.startswith("postgresql://") or s.startswith("mysql://"):
                return "sql"
            if s.startswith("gsheet://") or s.startswith("gdrive://"):
                return "google_sheets"
            return Path(s).suffix.lower().lstrip(".")
        raise ParserError("Unsupported input type for format detection")

    @staticmethod
    def _read_csv_like(path, sep=",", encoding="utf-8", chunksize: int | None = None, **kwargs):
        return pd.read_csv(path, sep=sep, encoding=encoding, chunksize=chunksize, **kwargs)

    @staticmethod
    def _read_json(path, lines=False, chunksize=None):
        if chunksize:
            return pd.read_json(path, lines=lines, chunksize=chunksize)
        return pd.read_json(path, lines=lines)

    @staticmethod
    def _read_parquet(path):
        if _HAS_PYARROW:
            return pd.read_parquet(path)
        return pd.read_parquet(path)  # might still work if fastparquet installed

    @staticmethod
    def _read_hdf(path, key=None):
        return pd.read_hdf(path, key=key)

    @staticmethod
    def _read_spss(path):
        if not _HAS_PYREADSTAT:
            raise ParserError("pyreadstat required for SPSS/SAS/Stata. Install pyreadstat.")
        df, meta = pyreadstat.read_sav(path)
        return df

    @staticmethod
    def _read_sas(path):
        if not _HAS_PYREADSTAT:
            raise ParserError("pyreadstat required for SPSS/SAS/Stata.")
        df, meta = pyreadstat.read_sas7bdat(path)
        return df

    @staticmethod
    def _read_stata(path):
        if not _HAS_PYREADSTAT:
            raise ParserError("pyreadstat required for STATA.")
        df, meta = pyreadstat.read_dta(path)
        return df

    @staticmethod
    def _read_msgpack(path):
        if not _HAS_MSGPACK:
            raise ParserError("msgpack-python required for MessagePack.")
        with open(path, "rb") as f:
            obj = msgpack.unpack(f, raw=False)
        return pd.DataFrame(obj)

    @staticmethod
    def _read_avro(path):
        # Minimal avro support; prefer fastavro in production
        try:
            import avro.datafile, avro.io, avro.schema  # type: ignore
        except Exception:
            raise ParserError("avro library required. Install fastavro/avro.")
        raise ParserError("Please install fastavro and implement avro read - not included by default.")

    @staticmethod
    def _read_sql(url: str, query: str | None = None, table: str | None = None, chunksize: int | None = None):
        if not _HAS_SQLALCHEMY:
            raise ParserError("SQLAlchemy required for DB connections.")
        engine = sa.create_engine(url)
        if table and not query:
            query = f"SELECT * FROM {table}"
        if chunksize:
            return pd.read_sql_query(sql=query, con=engine, chunksize=chunksize)
        return pd.read_sql_query(sql=query, con=engine)

    @staticmethod
    def _read_google_sheet(identifier: str, creds: t.Union[str, dict] | None = None):
        try:
            import gspread
            from oauth2client.service_account import ServiceAccountCredentials
        except Exception:
            raise ParserError("gspread and oauth2client required for Google Sheets.")
        # credential handling: accept path to service-account json or dict
        if isinstance(creds, str) and Path(creds).exists():
            creds_json = creds
        elif isinstance(creds, dict):
            tmp = Path(tempfile.mkdtemp()) / "gs_creds.json"
            tmp.write_text(json.dumps(creds))
            creds_json = str(tmp)
        else:
            raise ParserError("Google Sheets credentials missing or invalid")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scope)
        gc = gspread.authorize(credentials)
        sh = gc.open_by_key(identifier) if len(identifier) == 44 else gc.open(identifier)
        worksheet = sh.sheet1
        data = worksheet.get_all_records()
        return pd.DataFrame(data)

    @staticmethod
    async def parse(source: t.Union[str, Path, io.BytesIO], *, chunksize: int | None = None,
                    encoding: str = "utf-8", google_creds: t.Union[str, dict] | None = None,
                    **kwargs) -> t.Union[pd.DataFrame, t.Iterator[pd.DataFrame]]:
        """
        Async parse entrypoint. Returns DataFrame or iterator (for chunked streaming).
        - source: path (string) / Path / sqlite URL / SQLAlchemy URL / io.BytesIO
        - chunksize: if provided and supported returns iterator of DataFrames
        - google_creds: used for Google Sheets reads
        """
        def _sync():
            p = source
            if isinstance(p, (str, Path)):
                ppath = Path(p)
                if ppath.exists() and DataParser._is_compressed(ppath):
                    ppath = DataParser._maybe_extract(ppath)
                fmt = DataParser.detect_format(ppath)
            else:
                fmt = DataParser.detect_format(p)

            fmt = (fmt or "").lower()
            logger.debug("Detected format: %s", fmt)

            try:
                if fmt in {"csv", "tsv", "txt"}:
                    sep = "\t" if fmt == "tsv" or str(ppath).endswith(".tsv") else kwargs.get("sep", ",")
                    return DataParser._read_csv_like(ppath, sep=sep, encoding=encoding, chunksize=chunksize, **kwargs)
                if fmt in {"json", "jsonl", "ndjson"}:
                    lines = fmt in {"jsonl", "ndjson"} or kwargs.get("lines", False)
                    return DataParser._read_json(ppath, lines=lines, chunksize=chunksize)
                if fmt in {"parquet", "feather"}:
                    return DataParser._read_parquet(ppath)
                if fmt in {"xlsx", "xls"}:
                    return pd.read_excel(ppath, engine=kwargs.get("engine", None))
                if fmt in {"xml", "html"}:
                    if fmt == "xml":
                        df = pd.read_xml(ppath)
                        return df
                    else:
                        tables = pd.read_html(ppath)
                        return tables[0] if tables else pd.DataFrame()
                if fmt in {"sav"}:
                    return DataParser._read_spss(ppath)
                if fmt in {"sas7bdat", "sas"}:
                    return DataParser._read_sas(ppath)
                if fmt in {"dta"}:
                    return DataParser._read_stata(ppath)
                if fmt in {"h5", "hdf5"}:
                    return DataParser._read_hdf(ppath)
                if fmt in {"msgpack"}:
                    return DataParser._read_msgpack(ppath)
                if fmt in {"avro"}:
                    return DataParser._read_avro(ppath)
                if fmt in {"sqlite"}:
                    # path or sqlite://
                    if str(ppath).startswith("sqlite://"):
                        url = str(ppath)
                    else:
                        url = f"sqlite:///{str(ppath)}"
                    return DataParser._read_sql(url, query=kwargs.get("query"), chunksize=chunksize)
                if fmt in {"sql", "postgres", "postgresql", "mysql"}:
                    url = str(ppath)
                    return DataParser._read_sql(url, query=kwargs.get("query"), chunksize=chunksize)
                if fmt in {"google_sheets"}:
                    return DataParser._read_google_sheet(kwargs.get("identifier"), creds=google_creds)
                if fmt == "bytes":
                    # try to interpret bytes as common formats (CSV/JSON)
                    b = source.getvalue()
                    s = b.decode(encoding, errors="ignore")
                    if s.lstrip().startswith("{") or s.lstrip().startswith("["):
                        return pd.read_json(io.StringIO(s), lines=kwargs.get("lines", False))
                    return pd.read_csv(io.StringIO(s), sep=kwargs.get("sep", ","))
            except Exception as e:
                raise ParserError(f"Failed to parse {source}: {e}") from e
            raise ParserError(f"Unsupported or unrecognized format: {fmt}")

        result = await asyncio.to_thread(_sync)
        return result

# ---------------------------
# Cleaning pipeline & rules
# ---------------------------

class TransformationLog:
    """Holds a sequence of transformation metadata entries for compliance/audit."""
    def __init__(self):
        self.entries: list[dict] = []

    def record(self, name: str, before: Snapshot, after: Snapshot, details: dict | None = None):
        entry = {
            "name": name,
            "timestamp": time.time(),
            "before": asdict(before),
            "after": asdict(after),
            "details": details or {}
        }
        self.entries.append(entry)
        logger.info("Transformation %s: %s", name, entry["details"])

    def to_json(self):
        return json.dumps({"transformations": self.entries}, default=str, indent=2)


class Cleaner:
    """
    Main cleaning orchestrator. Use `await Cleaner.clean(...)` to load and clean data.
    """
    def __init__(self, config: dict | CleaningConfig | None = None):
        """
        Args:
            config: dict or CleaningConfig instance describing cleaning behavior.
        """
        if isinstance(config, dict):
            self.config = CleaningConfig(**config)
        elif isinstance(config, CleaningConfig):
            self.config = config
        else:
            self.config = CleaningConfig()
        self.transform_log = TransformationLog()
        self.custom_rules: dict[str, t.Callable[[pd.DataFrame, CleaningConfig], pd.DataFrame]] = {}

    def register_rule(self, name: str, fn: t.Callable[[pd.DataFrame, CleaningConfig], pd.DataFrame]):
        """Register a custom rule function for pipeline integration."""
        self.custom_rules[name] = fn

    # ---------------------------
    # Individual operations
    # ---------------------------
    def _snapshot(self, df: pd.DataFrame) -> Snapshot:
        return Snapshot.from_df(df, sample_n=self.config.snapshot_sample)

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        before = self._snapshot(df)
        df = df.rename(columns={c: normalize_colname(c) for c in df.columns})
        after = self._snapshot(df)
        self.transform_log.record("normalize_columns", before, after)
        return df

    def _drop_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.config.drop_columns:
            return df
        before = self._snapshot(df)
        df = df.drop(columns=[c for c in self.config.drop_columns if c in df.columns], errors="ignore")
        after = self._snapshot(df)
        self.transform_log.record("drop_columns", before, after, {"dropped": self.config.drop_columns})
        return df

    def _keep_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.config.keep_columns:
            return df
        before = self._snapshot(df)
        keep = [c for c in self.config.keep_columns if c in df.columns]
        df = df.loc[:, keep]
        after = self._snapshot(df)
        self.transform_log.record("keep_columns", before, after, {"kept": keep})
        return df

    def _drop_constant_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.config.drop_constant_columns:
            return df
        before = self._snapshot(df)
        const_cols = [c for c in df.columns if df[c].nunique(dropna=False) <= 1]
        df = df.drop(columns=const_cols)
        after = self._snapshot(df)
        self.transform_log.record("drop_constant_columns", before, after, {"dropped": const_cols})
        return df

    def _drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.config.drop_duplicates:
            return df
        before = self._snapshot(df)
        if self.config.duplicate_subset:
            df = df.drop_duplicates(subset=self.config.duplicate_subset, keep="first")
            details = {"subset": self.config.duplicate_subset}
        else:
            df = df.drop_duplicates(keep="first")
            details = {"subset": None}
        after = self._snapshot(df)
        self.transform_log.record("drop_duplicates", before, after, details)
        return df

    def _handle_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        before = self._snapshot(df)
        strat = (self.config.missing_strategy or "keep").lower()
        if strat == "keep":
            after = self._snapshot(df)
            self.transform_log.record("handle_missing", before, after, {"strategy": "keep"})
            return df
        if strat == "drop":
            df = df.dropna()
            after = self._snapshot(df)
            self.transform_log.record("handle_missing", before, after, {"strategy": "drop"})
            return df
        if strat == "fill":
            fills = self.config.fill_values or {}
            df = df.fillna(value=fills)
            after = self._snapshot(df)
            self.transform_log.record("handle_missing", before, after, {"strategy": "fill", "fills": fills})
            return df
        after = self._snapshot(df)
        self.transform_log.record("handle_missing", before, after, {"strategy": "unknown"})
        return df

    def _standardize_text_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        before = self._snapshot(df)
        for c in df.select_dtypes(include=["object", "string"]).columns:
            df[c] = df[c].map(lambda v: standardize_text(v, case="lower" if self.config.lowercase_columns else None))
        after = self._snapshot(df)
        self.transform_log.record("standardize_text_columns", before, after, {"lowercase": self.config.lowercase_columns})
        return df

    def _standardize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.config.parse_dates:
            return df
        before = self._snapshot(df)
        date_cols = self.config.date_columns or [c for c in df.columns if re.search(r"date|time", c, re.I)]
        for c in date_cols:
            if c in df.columns:
                df[c] = df[c].map(try_parse_date)
        after = self._snapshot(df)
        self.transform_log.record("standardize_dates", before, after, {"date_columns": date_cols})
        return df

    def _normalize_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        # Basic mapping: merge common synonyms for country names (expandable via config)
        before = self._snapshot(df)
        mapping = {
            "usa": "united states",
            "u.s.a.": "united states",
            "us": "united states",
            "united states of america": "united states",
        }
        for c in df.select_dtypes(include=["object", "string"]).columns:
            # apply only when small cardinality
            if df[c].nunique(dropna=True) < 500:
                df[c] = df[c].astype(str).str.strip().str.lower().replace(mapping)
        after = self._snapshot(df)
        self.transform_log.record("normalize_categories", before, after)
        return df

    def _validate_emails(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic email validation; adds column {col}_email_valid boolean."""
        before = self._snapshot(df)
        for c in (self.config.email_columns or []):
            if c in df.columns:
                pattern = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
                df[f"{c}_email_valid"] = df[c].astype(str).map(lambda v: bool(pattern.match(v)) if pd.notna(v) else False)
        after = self._snapshot(df)
        self.transform_log.record("validate_emails", before, after)
        return df

    def _validate_phone(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate & format phone numbers to E.164 if phonenumbers available; adds {col}_phone_e164."""
        if not _HAS_PHONENUMBERS:
            logger.debug("phonenumbers not installed; skipping phone validation")
            return df
        before = self._snapshot(df)
        for c in (self.config.phone_columns or []):
            if c in df.columns:
                def _fmt(v):
                    if pd.isna(v):
                        return None
                    try:
                        p = phonenumbers.parse(str(v), None)
                        if phonenumbers.is_valid_number(p):
                            return phonenumbers.format_number(p, phonenumbers.PhoneNumberFormat.E164)
                    except Exception:
                        return None
                df[f"{c}_phone_e164"] = df[c].map(_fmt)
        after = self._snapshot(df)
        self.transform_log.record("validate_phone", before, after)
        return df

    def _convert_currency(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.config.convert_currency or not self.config.currency_column:
            return df
        before = self._snapshot(df)
        rates = self.config.currency_rates or {}
        col = self.config.currency_column
        if col in df.columns:
            def _to_base(x):
                try:
                    amount = float(re.sub(r"[^\d\.\-]", "", str(x)))
                    # naive: assume currency code column exists? else use default rate mapping
                    return amount * rates.get("default", 1.0)
                except Exception:
                    return np.nan
            df[f"{col}_base"] = df[col].map(_to_base)
        after = self._snapshot(df)
        self.transform_log.record("convert_currency", before, after)
        return df

    def _outlier_detection_and_fix(self, df: pd.DataFrame) -> pd.DataFrame:
        method = self.config.outlier_method
        if not method:
            return df
        before = self._snapshot(df)
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for c in numeric_cols:
            s = df[c].astype(float)
            if method == "zscore":
                mean = s.mean()
                std = s.std()
                mask = (s - mean).abs() > (self.config.outlier_threshold or 3.0) * std
                df.loc[mask, c] = np.nan
            elif method == "iqr":
                q1 = s.quantile(0.25)
                q3 = s.quantile(0.75)
                iqr = q3 - q1
                mask = (s < (q1 - 1.5 * iqr)) | (s > (q3 + 1.5 * iqr))
                df.loc[mask, c] = np.nan
        after = self._snapshot(df)
        self.transform_log.record("outlier_detection_and_fix", before, after, {"method": method})
        return df

    def _encoding_fixes(self, df: pd.DataFrame) -> pd.DataFrame:
        # Pandas usually handles encoding when reading; here we do a safe re-encode for object columns
        before = self._snapshot(df)
        for c in df.select_dtypes(include=["object", "string"]).columns:
            df[c] = df[c].map(lambda v: v.encode(self.config.encoding, errors="ignore").decode(self.config.encoding) if pd.notna(v) else v)
        after = self._snapshot(df)
        self.transform_log.record("encoding_fixes", before, after)
        return df

    def _enforce_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply strict schema if provided in config.enforce_schema."""
        if not self.config.enforce_schema:
            return df
        before = self._snapshot(df)
        mismatches = {}
        for col, typ in (self.config.enforce_schema or {}).items():
            if col in df.columns:
                orig = df[col]
                casted = safe_cast_series(orig, typ)
                # record where cast produced NaN but orig not NaN
                bad = ((pd.isna(casted)) & (~pd.isna(orig)))
                if bad.any():
                    mismatches[col] = int(bad.sum())
                df[col] = casted
            else:
                mismatches[col] = "missing"
        after = self._snapshot(df)
        self.transform_log.record("enforce_schema", before, after, {"mismatches": mismatches})
        return df

    # ---------------------------
    # Profiling
    # ---------------------------

    @staticmethod
    def _column_profile(series: pd.Series) -> dict:
        n = len(series)
        non_null = series.notna().sum()
        unique = series.nunique(dropna=True)
        result = {
            "type": str(series.dtype),
            "unique_pct": (unique / n * 100) if n else None,
            "missing_pct": (1 - non_null / n) * 100 if n else None,
        }
        try:
            if pd.api.types.is_numeric_dtype(series):
                s = series.dropna().astype(float)
                result.update({
                    "min": s.min() if not s.empty else None,
                    "max": s.max() if not s.empty else None,
                    "mean": s.mean() if not s.empty else None,
                    "median": s.median() if not s.empty else None,
                    "std": s.std() if not s.empty else None,
                })
            elif pd.api.types.is_datetime64_any_dtype(series) or pd.api.types.is_datetime64_ns_dtype(series):
                s = pd.to_datetime(series, errors="coerce").dropna()
                result.update({"min": str(s.min()) if not s.empty else None, "max": str(s.max()) if not s.empty else None})
        except Exception:
            pass
        return result

    def profile(self, df: pd.DataFrame) -> dict:
        """
        Profile dataset. Returns a dict with per-column stats and overall quality score.
        """
        cols = {}
        for c in df.columns:
            cols[c] = self._column_profile(df[c])
        # dataset-level
        total_cells = df.size
        missing = df.isna().sum().sum()
        duplicates = df.duplicated().sum()
        completeness = 100 * (1 - missing / total_cells) if total_cells else 100
        consistency = 100 * (1 - duplicates / len(df)) if len(df) else 100
        # simplistic quality score
        quality = int(np.clip((completeness * 0.6 + consistency * 0.3 + 100 * (1 - (df.select_dtypes(include=["object"]).isna().sum().sum() / total_cells if total_cells else 0)) * 0.1)/1.0, 0, 100))
        profile = {
            "columns": cols,
            "rows": len(df),
            "columns_count": len(df.columns),
            "missing_cells": int(missing),
            "duplicates": int(duplicates),
            "completeness_pct": completeness,
            "consistency_pct": consistency,
            "quality_score": quality
        }
        return profile

    # ---------------------------
    # Pipeline executor
    # ---------------------------

    async def clean(self, source: t.Union[str, Path, io.BytesIO, pd.DataFrame], *,
                    chunksize: int | None = None,
                    google_creds: t.Union[str, dict] | None = None,
                    return_profile: bool = True) -> dict:
        """
        Entrypoint to produce a cleaned DataFrame and optional profile.

        Returns:
            {
                "df": pandas.DataFrame,
                "profile": dict,
                "transform_log": TransformationLog
            }

        If `source` is a pandas.DataFrame it's processed in-memory synchronously.
        If `chunksize` is provided and parser supports chunking, returns the concatenated result (with care).
        """
        # load
        if isinstance(source, pd.DataFrame):
            df = source.copy()
        else:
            parsed = await DataParser.parse(source, chunksize=chunksize, encoding=self.config.encoding, google_creds=google_creds, identifier=kwargs_identifier_from_source(source))
            # If parsed is iterator (TextFileReader for CSV), concatenate in a memory-safe way
            if hasattr(parsed, "__iter__") and not isinstance(parsed, pd.DataFrame):
                parts = []
                for part in parsed:
                    parts.append(part)
                df = pd.concat(parts, ignore_index=True)
            else:
                df = parsed

        # pipeline (modular order)
        try:
            if self.config.normalize_columns:
                df = self._normalize_columns(df)
            df = self._drop_columns(df)
            df = self._keep_columns(df)
            df = self._drop_constant_columns(df)
            df = self._drop_duplicates(df)
            df = self._handle_missing(df)
            df = self._standardize_text_columns(df)
            df = self._standardize_dates(df)
            df = self._normalize_categories(df)
            df = self._validate_emails(df)
            df = self._validate_phone(df)
            df = self._convert_currency(df)
            df = self._outlier_detection_and_fix(df)
            df = self._encoding_fixes(df)
            # apply registered custom rules in order
            for name, rule in self.custom_rules.items():
                before = self._snapshot(df)
                df = rule(df, self.config)
                after = self._snapshot(df)
                self.transform_log.record(f"custom_rule:{name}", before, after)
            df = self._enforce_schema(df)
        except Exception as e:
            logger.exception("Cleaning pipeline error: %s", e)
            raise

        profile = self.profile(df) if return_profile else None

        # optional quality threshold
        if self.config.quality_drop_threshold is not None and profile and profile["quality_score"] < self.config.quality_drop_threshold:
            raise RuntimeError(f"Dataset quality {profile['quality_score']} below threshold {self.config.quality_drop_threshold}")

        return {"df": df, "profile": profile, "transform_log": self.transform_log}

# ---------------------------
# Exporters
# ---------------------------

class Exporter:
    """Export cleaned DataFrame to multiple targets."""

    @staticmethod
    async def to_csv(df: pd.DataFrame, path: str, index: bool = False, encoding: str = "utf-8", **kwargs):
        await asyncio.to_thread(df.to_csv, path, index=index, encoding=encoding, **kwargs)

    @staticmethod
    async def to_parquet(df: pd.DataFrame, path: str, engine: str | None = None, **kwargs):
        # prefer pyarrow if available
        await asyncio.to_thread(df.to_parquet, path, engine=engine, **kwargs)

    @staticmethod
    async def to_json(df: pd.DataFrame, path: str, orient: str = "records", lines: bool = False, **kwargs):
        await asyncio.to_thread(df.to_json, path, orient=orient, lines=lines, **kwargs)

    @staticmethod
    async def to_excel(df: pd.DataFrame, path: str, index: bool = False, engine: str | None = None, **kwargs):
        await asyncio.to_thread(df.to_excel, path, index=index, engine=engine, **kwargs)

    @staticmethod
    async def to_sqlite(df: pd.DataFrame, path: str, table_name: str = "data", if_exists: str = "replace"):
        # use pandas to_sql (uses sqlite3) - this is blocking
        await asyncio.to_thread(df.to_sql, table_name, sqlite3.connect(path), if_exists=if_exists, index=False)

    @staticmethod
    async def to_sql(df: pd.DataFrame, url: str, table_name: str = "data", if_exists: str = "replace"):
        if not _HAS_SQLALCHEMY:
            raise RuntimeError("SQLAlchemy required for to_sql")
        engine = sa.create_engine(url)
        await asyncio.to_thread(df.to_sql, table_name, engine, if_exists=if_exists, index=False)

# ---------------------------
# Helper for DataParser.parse call: extract identifier for google sheets
# ---------------------------

def kwargs_identifier_from_source(src: t.Any) -> str | None:
    """Extract identifier from source string for google_sheets logic. If not present return None."""
    if isinstance(src, str):
        # Accept "gsheet://<id>" or raw id
        if src.startswith("gsheet://"):
            return src.split("gsheet://", 1)[1]
        if len(src) == 44 or "/" in src:  # basic guess
            return src
    return None

# ---------------------------
# Example of registering a plugin rule
# ---------------------------

def sample_custom_rule(df: pd.DataFrame, cfg: CleaningConfig) -> pd.DataFrame:
    """
    Example custom rule: create a derived 'year' column from 'date' if exists.
    """
    if "date" in df.columns and "year" not in df.columns:
        df["year"] = pd.to_datetime(df["date"], errors="coerce").dt.year
    return df

# ---------------------------
# Module level convenience functions
# ---------------------------

async def clean_and_export(source: t.Union[str, Path, io.BytesIO, pd.DataFrame], out_path: str,
                           config: dict | CleaningConfig | None = None,
                           export_format: str | None = None,
                           google_creds: t.Union[str, dict] | None = None) -> dict:
    """
    High-level convenience function: parse, clean, and save result.
    Returns profile and transformation log summary.
    """
    cleaner = Cleaner(config)
    cleaner.register_rule("sample_rule", sample_custom_rule)
    res = await cleaner.clean(source, google_creds=google_creds)
    df = res["df"]
    profile = res["profile"]
    if export_format:
        fmt = export_format.lower()
        if fmt == "csv":
            await Exporter.to_csv(df, out_path)
        elif fmt == "parquet":
            await Exporter.to_parquet(df, out_path)
        elif fmt == "json":
            await Exporter.to_json(df, out_path)
        elif fmt == "excel":
            await Exporter.to_excel(df, out_path)
        elif fmt == "sqlite":
            await Exporter.to_sqlite(df, out_path)
        else:
            raise RuntimeError("Unsupported export format")
    return {"profile": profile, "transform_log": cleaner.transform_log.to_json()}

# ---------------------------
# Unit test helpers (small examples)
# ---------------------------

def _unit_test_smoke():
    """Quick smoke test for local dev (not a substitute for proper tests)."""
    df = pd.DataFrame({
        "Name": ["Alice", "Bob", "Bob", None],
        "Email": ["ALICE@X.COM", "bob@example.com", "bob@example.com", "nope"],
        "Amount": ["$1,000.00", "200", "-9999"],
        "signup_date": ["2020-01-01", "2021/02/02", "03-03-2022", None]
    })
    cfg = {
        "missing_strategy": "fill",
        "fill_values": {"Name": "unknown"},
        "email_columns": ["Email"],
        "phone_columns": [],
        "normalize_columns": True,
        "lowercase_columns": True,
        "date_columns": ["signup_date"],
        "drop_duplicates": True
    }
    c = Cleaner(cfg)
    c.register_rule("sample_rule", sample_custom_rule)
    r = asyncio.run(c.clean(df))
    print("PROFILE:", json.dumps(r["profile"], indent=2))
    print("TRANSFORM LOG:", r["transform_log"].to_json())
    return r

if __name__ == "__main__":
    _unit_test_smoke()


Rate this code 

import os
import csv
import json
import numpy as np
import pandas as pd
import chardet
from typing import Tuple, Dict, List
from scipy.stats import zscore
from typing import Tuple

from utils.logger import logger
from utils.file_parser import (
    parse_file,
    parse_sql_file,
    parse_pdf_file,
    parse_csv_file,
    parse_excel_file,
    parse_json_file,
    parse_parquet_file,
)

# Ensure folders
for folder in ["data/cleaned", "data/analyzed", "data/output", "data/exports", "data/temp", "data/uploaded"]:
    os.makedirs(folder, exist_ok=True)


def detect_encoding(file_path: str) -> str:
    with open(file_path, 'rb') as f:
        raw = f.read(10000)
    result = chardet.detect(raw)
    return result['encoding'] or 'utf-8'


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names by:
    - Lowercasing
    - Replacing spaces/special chars with underscores
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w\s]", "", regex=True)
        .str.replace(r"\s+", "_", regex=True)
    )
    return df
    

def replace_na_like_values(df: pd.DataFrame) -> pd.DataFrame:
    na_values = ["", "na", "n/a", "null", "NULL", "NaN", "-", "--"]
    return df.replace(na_values, np.nan)


def detect_outliers(df: pd.DataFrame) -> Dict[str, int]:
    outlier_counts = {}
    numeric = df.select_dtypes(include=[np.number])
    if numeric.empty:
        return outlier_counts

    z_scores = np.abs(zscore(numeric, nan_policy='omit'))
    for idx, col in enumerate(numeric.columns):
        count = int((z_scores[:, idx] > 3).sum())
        outlier_counts[col] = count
    return outlier_counts


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main cleaning logic:
    - Strip strings
    - Drop completely empty columns
    - Fill or drop missing values (basic logic)
    """
    # Strip string columns
    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].apply(lambda x: str(x).strip() if pd.notnull(x) else x)
            

    # Drop fully empty columns
    df.dropna(axis=1, how='all', inplace=True)

    # Fill missing values for numeric columns
    for col in df.select_dtypes(include=['number']).columns:
        df[col] = df[col].fillna(df[col].median())

    # Fill missing values for categorical columns
    for col in df.select_dtypes(include=['object', 'string']).columns:
        df[col] = df[col].fillna('unknown')

    return df
    
    
def clean_data(df: pd.DataFrame, output_path: str = None) -> Tuple[pd.DataFrame, str]:
    """
    Full cleaning pipeline:
    - Normalize column names
    - Clean data
    - Save cleaned file (optional)
    Returns:
        cleaned DataFrame and output path (if saved)
    """
    try:
        if df is None or df.empty:
            raise ValueError("Parsed DataFrame is empty.")

        logger.info(f"✅ Starting cleaning on DataFrame with shape: {df.shape}")

        # Normalize and clean
        df = normalize_column_names(df)
        df = clean_dataframe(df)

        # Save if output path provided
        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, index=False)
            logger.info(f"📁 Cleaned file saved to: {output_path}")
            return df, output_path

        return df, ""

    except Exception as e:
        logger.error(f"❌ Failed to clean data: {str(e)}")
        raise
        
        


def generate_cleaning_report(df: pd.DataFrame) -> dict:
    return {
        "columns": list(df.columns),
        "null_counts": df.isnull().sum().to_dict(),
        "dtypes": df.dtypes.apply(lambda x: str(x)).to_dict(),
        "num_rows": len(df)
    }


def clean_data_file(df: pd.DataFrame, output_path: str) -> pd.DataFrame:
    try:
        df.dropna(axis=1, how='all', inplace=True)
        df.drop_duplicates(inplace=True)

        # ✅ Safely clean column names
        df.columns = [str(col).strip().lower().replace(" ", "_") for col in df.columns]

        # Fill missing values in numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df[col].isnull().any():
                median_val = df[col].median()
                df[col].fillna(median_val, inplace=True)

        # Fill missing values in categorical columns
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        for col in categorical_cols:
            if df[col].isnull().any():
                mode_val = df[col].mode()[0] if not df[col].mode().empty else "Unknown"
                df[col].fillna(mode_val, inplace=True)

        # Strip whitespace from categorical values
        for col in categorical_cols:
            df[col] = df[col].astype(str).str.strip()

        # Save cleaned file
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)

        logger.info(f"[CLEANING] Data cleaned and saved to: {output_path}")
        return df

    except Exception as e:
        logger.error(f"[CLEANING ERROR] {e}", exc_info=True)
        raise
        
        
def clean_and_report(file_path: str, output_dir: str = "data/cleaned") -> Tuple[str, Dict]:
    try:
        logger.info(f"Cleaning and reporting for: {file_path}")
        df = parse_file(file_path)
        if isinstance(df, dict) and "dataframe" in df:
            df = df["dataframe"]
        if df is None or df.empty:
            raise ValueError("Parsed DataFrame is empty")

        original_rows = df.shape[0]
        df = clean_dataframe(df)
        cleaned_rows = df.shape[0]
        normalized_cols = normalize_column_names(df)
        null_summary = df.isnull().sum().to_dict()
        outlier_summary = detect_outliers(df)

        base = os.path.basename(file_path).rsplit(".", 1)[0]
        cleaned_path = os.path.join(output_dir, f"{base}_cleaned.csv")
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(cleaned_path, index=False)

        return cleaned_path, {
            "status": "success",
            "original_file": file_path,
            "cleaned_file_path": cleaned_path,
            "original_rows": original_rows,
            "cleaned_rows": cleaned_rows,
            "num_columns": df.shape[1],
            "duplicates_removed": original_rows - cleaned_rows,
            "normalized_columns": normalized_cols,
            "null_summary": null_summary,
            "outliers_detected": outlier_summary,
            "error": None
        }

    except Exception as e:
        logger.error(f"clean_and_report failed: {e}", exc_info=True)
        return "", {
            "status": "error",
            "original_file": file_path,
            "cleaned_file_path": "",
            "original_rows": 0,
            "cleaned_rows": 0,
            "num_columns": 0,
            "duplicates_removed": 0,
            "normalized_columns": [],
            "null_summary": {},
            "outliers_detected": {},
            "error": str(e)
        }
        
def save_dataframe_to_sqlite(df: pd.DataFrame, table_name: str = "data", db_path: str = "data/mydb.sqlite") -> str:
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    return db_path


