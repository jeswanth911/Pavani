# data_engine/cleaner.py
# -------------------------------------------------------------------
# Universal Data Cleaning Engine (30+ modular transformations)
# - Safe defaults, configurable, auditable
# - Works on arbitrary tabular DataFrames
# - Minimal deps: pandas, numpy, scipy
# -------------------------------------------------------------------

from __future__ import annotations

import os
import re
import json
from __future__ import annotations
import asyncio
import csv
import datetime as _dt
import hashlib
import io
import json
import logging
import sqlite3
import tarfile
import tempfile
import time
import typing as t
import zipfile
from dataclasses import dataclass, field, asdict
from functools import partial
from pathlib import Path
from dateutil import parser as date_parser
from pydantic import BaseModel, Field, ValidationError
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Any, Callable

import numpy as np
import pandas as pd
from scipy.stats import zscore

from utils.logger import logger

# Ensure folders exist
for folder in ["data/cleaned", "data/analyzed", "data/output", "data/exports", "data/temp", "data/uploaded"]:
    os.makedirs(folder, exist_ok=True)


# -----------------------------
# Configuration
# -----------------------------
@dataclass
class CleanerConfig:
    # General thresholds
    drop_col_null_threshold: float = 0.97     # drop columns with >97% nulls
    winsorize_lower_q: float = 0.01           # lower quantile for winsorizing
    winsorize_upper_q: float = 0.99           # upper quantile for winsorizing
    clip_numeric_min: Optional[float] = None  # global min bound (optional)
    clip_numeric_max: Optional[float] = None  # global max bound (optional)

    # Imputation
    impute_numeric: bool = True
    impute_categorical: bool = True
    categorical_impute_value: str = "unknown"

    # Coercion behavior
    coerce_numeric: bool = True
    coerce_datetime: bool = True
    datetime_infer: bool = True

    # Boolean normalization
    normalize_booleans: bool = True
    true_set: set = field(default_factory=lambda: {"true", "yes", "y", "1", "t"})
    false_set: set = field(default_factory=lambda: {"false", "no", "n", "0", "f"})

    # Text normalization
    normalize_case_categoricals: str = "lower"  # "lower"|"upper"|"none"
    collapse_internal_whitespace: bool = True
    remove_invisible_chars: bool = True
    remove_non_ascii: bool = False

    # Percent & currency parsing
    parse_percentages: bool = True
    parse_currency: bool = True
    currency_symbols: str = r"[\$\€\£\₹]"

    # Column handling
    id_like_columns_first: bool = True         # reorder: id columns first
    id_keywords: List[str] = field(default_factory=lambda: ["id", "uuid", "guid"])
    drop_constant_columns: bool = True
    rename_map: Dict[str, str] = field(default_factory=dict)  # optional renames
    enforce_schema: Dict[str, str] = field(default_factory=dict)  # {"col": "int|float|str|datetime|bool|category"}

    # Semantics
    positive_only_columns: List[str] = field(default_factory=list)  # names that cannot be negative

    # Synonym normalization for categoricals
    categorical_synonyms: Dict[str, Dict[str, str]] = field(default_factory=dict)
    # Example:
    # {
    #   "country": {"usa": "united states", "u.s.a.": "united states"}
    # }

    # JSON expansion
    parse_json_columns: bool = True           # try expanding JSON-like string columns
    json_max_expand_cols: int = 30            # safety cap

    # Compound column splitting
    split_delimiters: List[str] = field(default_factory=lambda: ["|", ",", ";", " - "])
    max_splits_per_col: int = 5

    # Outlier handling
    mark_outliers_zscore: bool = True         # add *_is_outlier flags
    zscore_threshold: float = 3.0

    # Reporting
    keep_report: bool = True


# -----------------------------
# Utility helpers
# -----------------------------
def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        pd.Series(df.columns)
        .astype(str)
        .str.strip()
        .str.replace(r"[^\w\s]", "_", regex=True)
        .str.replace(r"\s+", "_", regex=True)
        .str.lower()
    ).tolist()
    return df


def _is_probably_json(s: Any) -> bool:
    if not isinstance(s, str):
        return False
    s_strip = s.strip()
    return (s_strip.startswith("{") and s_strip.endswith("}")) or (s_strip.startswith("[") and s_strip.endswith("]"))


def _coerce_numeric(series: pd.Series) -> pd.Series:
    # Remove thousands separators and stray spaces
    s = series.astype(str).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(s, errors="coerce")


def _coerce_percentage(series: pd.Series) -> pd.Series:
    # e.g., "12.5%" -> 0.125 ; "12,5%" -> 0.125
    s = series.astype(str).str.replace(",", ".", regex=False).str.strip()
    s = s.str.replace(r"%\s*$", "", regex=True)
    return pd.to_numeric(s, errors="coerce") / 100.0


def _coerce_currency(series: pd.Series, currency_symbols_pattern: str) -> pd.Series:
    s = series.astype(str)
    s = s.str.replace(currency_symbols_pattern, "", regex=True)
    s = s.str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(s, errors="coerce")


def _strip_invisible(s: pd.Series) -> pd.Series:
    # Remove BOM, zero-width etc.
    return s.astype(str).str.replace(r"[\u200B-\u200D\uFEFF]", "", regex=True)


def _collapse_spaces(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace(r"\s+", " ", regex=True).str.strip()


def _standardize_bool(series: pd.Series, true_set: set, false_set: set) -> pd.Series:
    def _map(v):
        if pd.isna(v):
            return np.nan
        s = str(v).strip().lower()
        if s in true_set:
            return True
        if s in false_set:
            return False
        return np.nan
    return series.map(_map)


def _safe_mode(series: pd.Series) -> Any:
    try:
        m = series.mode(dropna=True)
        if not m.empty:
            return m.iloc[0]
    except Exception:
        pass
    return np.nan


# -----------------------------
# Core cleaning engine
# -----------------------------
class DataCleaner:
    """
    Implements 30+ cleaning transformations.
    Each step returns (df, notes) and is registered in `self.pipeline`.
    """

    def __init__(self, config: Optional[CleanerConfig] = None):
        self.cfg = config or CleanerConfig()
        self.report: Dict[str, Any] = {"applied_steps": []}

        # Register pipeline (order matters!)
        self.pipeline: List[Tuple[str, Callable[[pd.DataFrame], pd.DataFrame]]] = [
            ("normalize_column_names", self.step_normalize_column_names),           # 1
            ("drop_empty_rows", self.step_drop_empty_rows),                         # 2
            ("drop_constant_columns", self.step_drop_constant_columns),             # 3
            ("drop_high_null_columns", self.step_drop_high_null_columns),           # 4
            ("strip_whitespace", self.step_strip_whitespace),                       # 5
            ("remove_invisible_chars", self.step_remove_invisible_chars),           # 6
            ("collapse_whitespace", self.step_collapse_whitespace),                 # 7
            ("replace_na_like_values", self.step_replace_na_like_values),           # 8
            ("remove_non_ascii", self.step_remove_non_ascii),                       # 9
            ("coerce_numeric_columns", self.step_coerce_numeric_columns),           # 10
            ("coerce_datetime_columns", self.step_coerce_datetime_columns),         # 11
            ("parse_percentages", self.step_parse_percentages),                     # 12
            ("parse_currency", self.step_parse_currency),                           # 13
            ("normalize_booleans", self.step_normalize_booleans),                   # 14
            ("standardize_categorical_case", self.step_standardize_categorical_case),# 15
            ("apply_categorical_synonyms", self.step_apply_categorical_synonyms),   # 16
            ("json_column_expansion", self.step_json_column_expansion),             # 17
            ("split_compound_columns", self.step_split_compound_columns),           # 18
            ("extract_numbers_from_text", self.step_extract_numbers_from_text),     # 19
            ("winsorize_numeric", self.step_winsorize_numeric),                     # 20
            ("clip_numeric_range", self.step_clip_numeric_range),                   # 21
            ("enforce_positive_only", self.step_enforce_positive_only),             # 22
            ("impute_numeric", self.step_impute_numeric),                           # 23
            ("impute_categorical", self.step_impute_categorical),                   # 24
            ("mark_outliers_zscore", self.step_mark_outliers_zscore),               # 25
            ("dedupe_rows", self.step_dedupe_rows),                                 # 26
            ("set_category_dtype", self.step_set_category_dtype),                   # 27
            ("rename_columns_map", self.step_rename_columns_map),                   # 28
            ("enforce_schema", self.step_enforce_schema),                           # 29
            ("reorder_columns", self.step_reorder_columns),                         # 30
        ]

    # --------- Step wrappers (record + log) ----------
    def _apply(self, name: str, func: Callable[[pd.DataFrame], pd.DataFrame], df: pd.DataFrame) -> pd.DataFrame:
        before_shape = df.shape
        try:
            df2 = func(df.copy())
            after_shape = df2.shape
            note = {"step": name, "before": before_shape, "after": after_shape}
            self.report["applied_steps"].append(note)
            logger.info(f"[CLEAN] {name}: {before_shape} -> {after_shape}")
            return df2
        except Exception as e:
            logger.warning(f"[CLEAN][SKIPPED] {name}: {e}")
            self.report["applied_steps"].append({"step": name, "skipped": True, "reason": str(e)})
            return df

    # -----------------------------
    # Steps (30)
    # -----------------------------
    def step_normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        return normalize_column_names(df)

    def step_drop_empty_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna(how="all")

    def step_drop_constant_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.cfg.drop_constant_columns:
            return df
        nunique = df.nunique(dropna=False)
        drop_cols = nunique[nunique <= 1].index.tolist()
        return df.drop(columns=drop_cols, errors="ignore")

    def step_drop_high_null_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        thresh = self.cfg.drop_col_null_threshold
        if thresh is None:
            return df
        ratio = df.isna().mean()
        drop_cols = ratio[ratio >= thresh].index.tolist()
        return df.drop(columns=drop_cols, errors="ignore")

    def step_strip_whitespace(self, df: pd.DataFrame) -> pd.DataFrame:
        obj_cols = df.select_dtypes(include=["object", "string"]).columns
        for c in obj_cols:
            df[c] = df[c].astype(str).str.strip()
        return df

    def step_remove_invisible_chars(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.cfg.remove_invisible_chars:
            return df
        obj_cols = df.select_dtypes(include=["object", "string"]).columns
        for c in obj_cols:
            df[c] = _strip_invisible(df[c])
        return df

    def step_collapse_whitespace(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.cfg.collapse_internal_whitespace:
            return df
        obj_cols = df.select_dtypes(include=["object", "string"]).columns
        for c in obj_cols:
            df[c] = _collapse_spaces(df[c])
        return df

    def step_replace_na_like_values(self, df: pd.DataFrame) -> pd.DataFrame:
        na_values = {"", "na", "n/a", "null", "nan", "-", "--", "none", "Nil", "NIL", "NULL", "NaN"}
        obj_cols = df.select_dtypes(include=["object", "string"]).columns
        for c in obj_cols:
            df[c] = df[c].replace(list(na_values), np.nan)
        return df

    def step_remove_non_ascii(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.cfg.remove_non_ascii:
            return df
        obj_cols = df.select_dtypes(include=["object", "string"]).columns
        for c in obj_cols:
            df[c] = df[c].astype(str).str.encode("ascii", errors="ignore").str.decode("ascii")
        return df

    def step_coerce_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.cfg.coerce_numeric:
            return df
        obj_cols = df.select_dtypes(include=["object", "string"]).columns
        for c in obj_cols:
            # Only coerce if majority looks numeric
            sample = df[c].dropna().astype(str).head(500)
            if len(sample) == 0:
                continue
            looks_numeric = (sample.str.replace(",", "", regex=False).str.replace(".", "", regex=False).str.fullmatch(r"-?\d+")).mean()
            if looks_numeric > 0.6:
                df[c] = _coerce_numeric(df[c])
        return df

    def step_coerce_datetime_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.cfg.coerce_datetime:
            return df
        obj_cols = df.select_dtypes(include=["object", "string"]).columns
        for c in obj_cols:
            sample = df[c].dropna().astype(str).head(200)
            if sample.empty:
                continue
            # Heuristic: presence of '-' or '/' or ':' often indicates date/time
            if sample.str.contains(r"[-/:\.]", regex=True).mean() > 0.5:
                df[c] = pd.to_datetime(df[c], errors="coerce", infer_datetime_format=self.cfg.datetime_infer)
        return df

    def step_parse_percentages(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.cfg.parse_percentages:
            return df
        obj_cols = df.select_dtypes(include=["object", "string"]).columns
        for c in obj_cols:
            sample = df[c].dropna().astype(str).head(200)
            if sample.str.contains(r"%", regex=True).mean() > 0.6:
                df[c] = _coerce_percentage(df[c])
        return df

    def step_parse_currency(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.cfg.parse_currency:
            return df
        pattern = self.cfg.currency_symbols
        obj_cols = df.select_dtypes(include=["object", "string"]).columns
        for c in obj_cols:
            sample = df[c].dropna().astype(str).head(200)
            if sample.str.contains(pattern, regex=True).mean() > 0.5:
                df[c] = _coerce_currency(df[c], pattern)
        return df

    def step_normalize_booleans(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.cfg.normalize_booleans:
            return df
        obj_cols = df.select_dtypes(include=["object", "string"]).columns
        for c in obj_cols:
            s = df[c].dropna().astype(str).str.lower()
            # If majority are in true/false sets, convert
            frac_bool_like = s.isin(self.cfg.true_set.union(self.cfg.false_set)).mean() if len(s) else 0
            if frac_bool_like > 0.6:
                df[c] = _standardize_bool(df[c], self.cfg.true_set, self.cfg.false_set)
        return df

    def step_standardize_categorical_case(self, df: pd.DataFrame) -> pd.DataFrame:
        mode = self.cfg.normalize_case_categoricals
        if mode not in ("lower", "upper"):
            return df
        obj_cols = df.select_dtypes(include=["object", "string"]).columns
        for c in obj_cols:
            if mode == "lower":
                df[c] = df[c].astype(str).str.lower()
            else:
                df[c] = df[c].astype(str).str.upper()
        return df

    def step_apply_categorical_synonyms(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.cfg.categorical_synonyms:
            return df
        for col, mapping in self.cfg.categorical_synonyms.items():
            if col in df.columns:
                df[col] = df[col].astype(str).str.lower().map(lambda x: mapping.get(x, x))
        return df

    def step_json_column_expansion(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.cfg.parse_json_columns:
            return df
        obj_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
        expand_count = 0
        for c in obj_cols:
            # Check if enough rows look like JSON
            sample = df[c].dropna().astype(str).head(100)
            if len(sample) and (sample.map(_is_probably_json).mean() > 0.5):
                try:
                    expanded = df[c].dropna().apply(lambda s: json.loads(s) if _is_probably_json(s) else {})
                    expanded_df = pd.json_normalize(expanded)
                    # Avoid explosion
                    if expanded_df.shape[1] <= self.cfg.json_max_expand_cols:
                        # Prefix to avoid collisions
                        expanded_df = expanded_df.add_prefix(f"{c}__")
                        df = df.drop(columns=[c]).reset_index(drop=True)
                        df = pd.concat([df, expanded_df.reindex(df.index)], axis=1)
                        expand_count += 1
                except Exception:
                    continue
        return df

    def step_split_compound_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        delims = [re.escape(d) for d in self.cfg.split_delimiters]
        pattern = r"|".join(delims)
        obj_cols = df.select_dtypes(include=["object", "string"]).columns
        for c in obj_cols:
            sample = df[c].dropna().astype(str).head(200)
            if sample.str.contains(pattern, regex=True).mean() > 0.7:
                try:
                    splits = df[c].astype(str).str.split(pattern, expand=True, n=self.cfg.max_splits_per_col)
                    # Name split columns
                    new_cols = [f"{c}_part{i+1}" for i in range(splits.shape[1])]
                    splits.columns = new_cols
                    df = df.drop(columns=[c]).join(splits)
                except Exception:
                    pass
        return df

    def step_extract_numbers_from_text(self, df: pd.DataFrame) -> pd.DataFrame:
        obj_cols = df.select_dtypes(include=["object", "string"]).columns
        for c in obj_cols:
            # If column is mostly text but contains numbers, create a numeric companion column
            s = df[c].astype(str)
            has_digits = s.str.contains(r"\d").mean()
            if has_digits > 0.5 and c + "_num" not in df.columns:
                df[c + "_num"] = pd.to_numeric(s.str.extract(r"([-+]?\d*\.?\d+)", expand=False), errors="coerce")
        return df

    def step_winsorize_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        ql, qu = self.cfg.winsorize_lower_q, self.cfg.winsorize_upper_q
        nums = df.select_dtypes(include=[np.number]).columns
        for c in nums:
            low, high = df[c].quantile(ql), df[c].quantile(qu)
            df[c] = df[c].clip(lower=low, upper=high)
        return df

    def step_clip_numeric_range(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.cfg.clip_numeric_min is None and self.cfg.clip_numeric_max is None:
            return df
        nums = df.select_dtypes(include=[np.number]).columns
        df[nums] = df[nums].clip(lower=self.cfg.clip_numeric_min, upper=self.cfg.clip_numeric_max)
        return df

    def step_enforce_positive_only(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.cfg.positive_only_columns:
            return df
        for col in self.cfg.positive_only_columns:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].clip(lower=0)
        return df

    def step_impute_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.cfg.impute_numeric:
            return df
        nums = df.select_dtypes(include=[np.number]).columns
        for c in nums:
            if df[c].isna().any():
                df[c] = df[c].fillna(df[c].median())
        return df

    def step_impute_categorical(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.cfg.impute_categorical:
            return df
        cats = df.select_dtypes(include=["object", "string", "category"]).columns
        for c in cats:
            if df[c].isna(
