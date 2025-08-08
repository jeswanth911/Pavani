import pandas as pd
from fastapi import APIRouter
import numpy as np
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Tuple, Dict
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import accuracy_score, f1_score, mean_squared_error
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
import shap
from utils.logger import logger

predict_router = APIRouter()

class PredictionInput(BaseModel):
    feature1: float
    feature2: float

def preprocess_data(df: pd.DataFrame, target_col: str) -> Tuple[pd.DataFrame, pd.Series, dict]:
    logger.info("Preprocessing data...")
    df = df.copy()

    df = df.dropna(subset=[target_col])
    y = df[target_col]
    X = df.drop(columns=[target_col])

    encoders = {}
    for col in X.select_dtypes(include=['object', 'category']).columns:
        le = LabelEncoder()
        X[col] = X[col].fillna('missing')
        X[col] = le.fit_transform(X[col])
        encoders[col] = le
        logger.debug(f"Encoded column: {col}")

    for col in X.select_dtypes(include=[np.number]).columns:
        X[col] = X[col].fillna(X[col].median())
        logger.debug(f"Imputed missing values in column: {col}")

    scaler = StandardScaler()
    X_scaled = pd.DataFrame(scaler.fit_transform(X), columns=X.columns)

    return X_scaled, y, encoders


def determine_problem_type(y: pd.Series) -> str:
    if y.dtype == 'object' or y.nunique() <= 20:
        return 'classification'
    return 'regression'


def train_model(X_train, y_train, problem_type: str):
    logger.info(f"Training {problem_type} model using RandomForest...")
    if problem_type == 'classification':
        model = RandomForestClassifier()
    else:
        model = RandomForestRegressor()
    model.fit(X_train, y_train)
    return model


def calculate_metrics(model, X_test, y_test, problem_type: str) -> Dict[str, float]:
    y_pred = model.predict(X_test)
    if problem_type == 'classification':
        return {
            "accuracy": accuracy_score(y_test, y_pred),
            "f1_score": f1_score(y_test, y_pred, average="weighted"),
        }
    else:
        return {
            "rmse": mean_squared_error(y_test, y_pred, squared=False)
        }


def explain_model(model, X_train, problem_type: str) -> Optional[pd.DataFrame]:
    try:
        logger.info("Generating model explainability using SHAP...")
        explainer = shap.Explainer(model.predict, X_train)
        shap_values = explainer(X_train[:50])
        shap.summary_plot(shap_values, X_train, show=False)
        import matplotlib.pyplot as plt
        plt.savefig("data/output/shap_summary_plot.png")
        plt.close()

        feature_importance = np.abs(shap_values.values).mean(axis=0)
        importance_df = pd.DataFrame({
            "feature": X_train.columns,
            "importance": feature_importance
        }).sort_values(by="importance", ascending=False)
        return importance_df
    except Exception as e:
        logger.warning(f"SHAP explainability failed: {e}")
        return None


def run_prediction_pipeline(df: pd.DataFrame, target_col: Optional[str] = None) -> Dict:
    logger.info("Starting prediction pipeline...")

    if target_col is None:
        target_col = df.columns[-1]
        logger.warning(f"No target_col provided. Auto-detected: '{target_col}'")

    X, y, encoders = preprocess_data(df, target_col)
    problem_type = determine_problem_type(y)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = train_model(X_train, y_train, problem_type)
    metrics = calculate_metrics(model, X_test, y_test, problem_type)

    predictions = model.predict(X)
    result_df = df.copy()
    result_df[f"{target_col}_prediction"] = predictions

    explanation = explain_model(model, X_train, problem_type)

    return {
        "status": "success",
        "problem_type": problem_type,
        "target_column": target_col,
        "metrics": metrics,
        "predictions": result_df,
        "model_summary": str(model),
        "explanation": explanation.to_dict(orient="records") if explanation is not None else None
    }


def predict_from_file(df: pd.DataFrame, target_col: Optional[str] = None) -> Tuple[pd.DataFrame, Dict]:
    result = run_prediction_pipeline(df, target_col)
    return result["predictions"], {
        "metrics": result["metrics"],
        "model_summary": result["model_summary"],
        "explanation": result["explanation"],
        "target_column": result["target_column"],
        "problem_type": result["problem_type"]
      }
  
@predict_router.post("/predict")
def predict_endpoint():
    return {"message": "Prediction endpoint working!"}
