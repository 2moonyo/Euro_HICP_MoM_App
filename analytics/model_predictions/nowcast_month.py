#!/usr/bin/env python3
"""
Nowcast HICP for a future month not in gold.gold_nowcast_input.

Appends synthetic future row, forward-fills features, predicts MoM, and converts
to index level.
"""

from __future__ import annotations

from pathlib import Path
from typing import Sequence, Tuple
import os
import duckdb
import numpy as np
import pandas as pd
import joblib
from typing import cast
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parents[2]))
DB_PATH = Path(os.getenv("DB_PATH", PROJECT_ROOT / "data" / "euro_nowcast.duckdb"))
MODEL_DIR = PROJECT_ROOT / "models"

# ------------------Helpers------------------

def load_gold() -> pd.DataFrame:
    """Load gold.gold_nowcast_input with month as datetime."""
    con = duckdb.connect(str(DB_PATH), read_only=True)  # ✅ Read-only mode
    try:
        df = con.execute(
            "SELECT * FROM gold.gold_nowcast_input ORDER BY month"
        ).df()
        df["month"] = pd.to_datetime(df["month"])
        return df
    finally:
        con.close()

def add_smoothing_features(
    df: pd.DataFrame,
    cols: Sequence[str],
    windows: Sequence[int] = (3, 6, 12),
) -> pd.DataFrame:
    """Add shifted rolling mean/std (shift(1) prevents leakage)."""
    out = df.copy()
    for col in cols:
        if col not in out.columns:
            continue
        s = out[col].shift(1)  # no leakage
        for w in windows:
            out[f"{col}_ma{w}"] = s.rolling(w, min_periods=1).mean()
            out[f"{col}_std{w}"] = s.rolling(w, min_periods=1).std().fillna(0.0)
    return out


def append_future_row(
    df_geo: pd.DataFrame,
    target_month: pd.Timestamp,
    geo: str,
) -> pd.DataFrame:
    """Append synthetic row for future month (indicators as NaN, forward-filled later)."""
    target_month = pd.Timestamp(target_month).normalize()

    # Check if it already exists
    mask_existing = (df_geo["month"] == target_month) & (df_geo["geo"] == geo)
    if mask_existing.any():
        # Row already present, just return original df
        return df_geo

    """Append a synthetic row for the future month with NaN for target_hicp_index."""
    new_row = {col: np.nan for col in df_geo.columns}  # type: ignore
    new_row["month"] = target_month # Set the month for the synthetic row
    new_row["geo"] = geo # Set the geo for the synthetic row
    
    return pd.concat([df_geo, pd.DataFrame([new_row])], ignore_index=True)

def align_features_to_model(X: pd.DataFrame, model) -> pd.DataFrame:
    """Align X to model's expected features (add missing as zeros, drop extras)."""
    if hasattr(model, 'feature_names_in_'):
        expected_features = list(model.feature_names_in_)
    else:
        expected_features = model.get_booster().feature_names
    
    # Add missing features (fill with 0)
    for feat in expected_features:
        if feat not in X.columns:
            X[feat] = 0.0
            print(f"  Added missing feature '{feat}' (filled with 0.0)")
    
    # Remove extra features
    extra = set(X.columns) - set(expected_features)
    if extra:
        print(f"  Removing extra features: {extra}")
    
    # Select and reorder
    X = X[expected_features]
    
    return X

def build_feature_matrix_for_inference(
    df: pd.DataFrame,
    level_col: str = "target_hicp_index",
    time_col: str = "month",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build feature matrix X for all rows (forward-fills missing values)."""
    df = df.sort_values(time_col).reset_index(drop=True)

    # Date features
    df["month_of_year"] = df[time_col].dt.month
    df["quarter"] = df[time_col].dt.quarter

    # Volatile columns present in gold_nowcast_input
    volatile_candidates = ["fx_usd_eur_avg", "policy_rate_monthly_avg", "policy_rate_eom"]
    volatile_cols = [c for c in volatile_candidates if c in df.columns]

    df = add_smoothing_features(df, volatile_cols, windows=(3, 6, 12))

    # Columns to drop from features to avoid leakage
    drop_cols = [
        level_col,    # target index level
        "hicp_mom",   # MoM target
        "hicp_yoy",   # YoY target
        time_col,
        "geo",
    ]
    drop_cols = [c for c in drop_cols if c in df.columns]

    X = df.drop(columns=drop_cols, errors="ignore")

    # Keep only numerics
    numeric_cols = X.select_dtypes(include=["number", "bool"]).columns.tolist()
    X = X[numeric_cols]

    # SHORTCUT: if there are missing values (e.g. future month),
    # forward fill and then backfill.
    X = X.ffill().bfill()

    return df, X


def load_trained_model(path: Path | None = None):
    """Load trained XGBoost model (defaults to last walk-forward model)."""
    if path is None:
        path = MODEL_DIR / "xgb_model_robust_last_wf.pkl"
    if not path.exists():
        raise FileNotFoundError(f"Model file not found at {path}")
    return joblib.load(path)


# ---------------------------------------------------------------------------
# 2) CORE NOWCAST FUNCTION
# ---------------------------------------------------------------------------

def nowcast_future_month(
    target_month: str | pd.Timestamp,
    geo: str = "EA20",
) -> dict:
    """Nowcast HICP for future month (returns month, geo, mom_pred, level_pred, last_known values)."""
    target_month = pd.Timestamp(target_month).normalize()

    # 1. Load history
    df = load_gold()
    df_geo = df[df["geo"] == geo].copy()
    if df_geo.empty:
        raise ValueError(f"No rows found for geo = {geo}")

    # 2. Append synthetic future row
    df_geo = append_future_row(df_geo, target_month=target_month, geo=geo)

    # 3. Build features for all months (including future)
    df_geo, X = build_feature_matrix_for_inference(df_geo)
    months = df_geo["month"]

    # 4. Find index of the future row
    mask_target = months == target_month
    if not mask_target.any():
        raise RuntimeError("Failed to create target month row for inference")
    idx_target = np.where(mask_target)[0][0]
    X_target = X.iloc[[idx_target]]  # keep as 2D

    # 5. Identify last realised level before target month
    mask_past = (months < target_month) & df_geo["target_hicp_index"].notna()
    if not mask_past.any():
        raise ValueError(
            f"No historical HICP index found before {target_month.date()} for geo = {geo}"
        )

    idx_last = np.where(mask_past)[0][-1]
    last_known_month = months.iloc[idx_last]
    last_known_level = float(cast(float, df_geo.loc[idx_last, "target_hicp_index"]))

    # 6. Predict MoM using trained model
    model = load_trained_model()
    X_target = align_features_to_model(X_target, model)
    mom_pred = float(model.predict(X_target)[0])

    # 7. Convert predicted MoM to level
    # ASSUMPTION: hicp_mom is a fractional change, e.g. 0.005 = 0.5%
    level_pred = last_known_level * (1.0 + mom_pred)

    return {
            "month": target_month,
            "geo": geo,
            "mom_pred": mom_pred,
            "level_pred": level_pred,
            "last_known_month": last_known_month,
            "last_known_level": last_known_level,
        }

def save_nowcast_to_db(result: dict) -> None:
    """Save nowcast prediction to gold.nowcast_output."""
    con = duckdb.connect(str(DB_PATH), read_only=False)
    try:
        # Table already exists from model_training5.py
        # Schema: month, geo, model_version, y_nowcast, y_actual, residual, created_at_utc
        
        # Delete existing prediction for this month/geo if present
        con.execute("""
            DELETE FROM gold.nowcast_output 
            WHERE month = ? AND geo = ?
        """, [result['month'], result['geo']])
        
        # Insert new prediction
        con.execute("""
            INSERT INTO gold.nowcast_output 
            (month, geo, model_version, y_nowcast, y_actual, residual, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [
            result['month'],
            result['geo'],
            'nowcast_future',  # model version identifier
            result['level_pred'],  # predicted HICP index
            None,  # y_actual is NULL for future predictions
            None,  # residual is NULL for future predictions
        ])
        
        print(f"Saved prediction to gold.nowcast_output")
        print(f"   Month: {result['month'].date()}, Predicted Index: {result['level_pred']:.2f}")
        
    finally:
        con.close()

def main():
    # Example: predict CPI for October 2025
    target_month = "2025-10-01"

    result = nowcast_future_month(target_month, geo="EA20")

    print("=== FUTURE HICP NOWCAST ===")
    print(f"Geo                : {result['geo']}")
    print(f"Target month       : {result['month'].date()}")
    print(f"Last known month   : {result['last_known_month'].date()}")
    print(f"Last known level   : {result['last_known_level']:.2f} (2015 = 100)")
    print(f"Predicted MoM      : {result['mom_pred']:+.4f}")
    print(f"Predicted HICP idx : {result['level_pred']:.2f} (2015 = 100)")

    save_nowcast_to_db(result)

if __name__ == "__main__":
    main()
