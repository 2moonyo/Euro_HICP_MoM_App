#!/usr/bin/env python3
"""
Train SARIMA nowcast model using walk-forward validation.

Pure time series approach using only target_hicp_index history.
No exogenous features - SARIMA captures autocorrelation and seasonality internally.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Tuple
import os
import duckdb
import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_absolute_error, r2_score, median_absolute_error, mean_squared_error
import joblib
from dotenv import load_dotenv

warnings.filterwarnings('ignore')

load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parents[2]))
DB_PATH = Path(os.getenv("DB_PATH", str(PROJECT_ROOT / "data" / "euro_nowcast.duckdb")))
MODEL_DIR = PROJECT_ROOT / "models" / "arima"  # Subfolder for ARIMA models
OUT_DIR = PROJECT_ROOT / "analytics" / "outputs"

MODEL_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Cutoff for initial training period
TRAIN_STOP = pd.Timestamp("2023-07-01")

# ----------------------Data Loading--------------------------

def load_target_series() -> pd.DataFrame:
    """Load only month and target_hicp_index from gold table."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        query = """
            SELECT 
                month,
                target_hicp_index
            FROM gold.gold_nowcast_input
            WHERE target_hicp_index IS NOT NULL
            ORDER BY month
        """
        df = con.execute(query).df()
        df["month"] = pd.to_datetime(df["month"])
        return df.set_index("month")
    finally:
        con.close()

# ------------------------Metrics--------------------------------------------

def calculate_robust_metrics(y_true, y_pred) -> dict:
    """Calculate same error metrics as XGBoost model for comparison."""
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)

    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    median_ae = median_absolute_error(y_true, y_pred)

    mask = y_true != 0
    mape = float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100) if mask.sum() > 0 else None

    huber_delta = 1.0
    residuals = np.abs(y_true - y_pred)
    huber = np.where(
        residuals <= huber_delta,
        0.5 * residuals**2,
        huber_delta * (residuals - 0.5 * huber_delta),
    ).mean()

    max_error = float(np.max(residuals))
    p90_error = float(np.percentile(residuals, 90))

    return {
        "mae": float(mae),
        "median_ae": float(median_ae),
        "rmse": float(rmse),
        "r2": float(r2),
        "mape": mape,
        "huber_loss": float(huber),
        "max_error": max_error,
        "p90_error": p90_error,
    }

# ------------------------Walk Forward Training & Evaluation------------------------

def walk_forward_sarima(
    y: pd.Series,
    start_after: str | pd.Timestamp = TRAIN_STOP,
    order: Tuple[int, int, int] = (1, 1, 1),
    seasonal_order: Tuple[int, int, int, int] = (1, 1, 1, 12),
    verbose: bool = True,
) -> Tuple[pd.DataFrame, Optional[SARIMAX]]:
    """
    Walk-forward validation with SARIMA.
    
    Args:
        y: Time series with datetime index
        start_after: Train on data up to this date, predict after
        order: (p,d,q) for ARIMA
        seasonal_order: (P,D,Q,s) for seasonal component
        verbose: Print progress
        
    Returns:
        DataFrame with predictions and actuals
        Last fitted model
    """
    start_after = pd.Timestamp(start_after)
    
    # Get indices to predict (everything after cutoff)
    test_indices = y.index[y.index > start_after]
    
    if len(test_indices) == 0:
        raise ValueError(f"No data after {start_after} to predict")
    
    rows = []
    last_model = None
    
    if verbose:
        print("\n=== SARIMA WALK-FORWARD START ===")
        print(f"Order: {order}, Seasonal: {seasonal_order}")
        print(f"Start after: {start_after.date()} ({len(test_indices)} steps ahead)\n")
    
    for i, predict_month in enumerate(test_indices, start=1):
        # Training data: everything up to (but not including) prediction month
        train_data = y[:predict_month].iloc[:-1]
        
        if len(train_data) < 24:  # Need at least 2 years for seasonal model
            if verbose:
                print(f"[Step {i:02d}] Skipping {predict_month.date()} - insufficient training data")
            continue
        
        if verbose:
            print(f"[Step {i:02d}] Training on {len(train_data)} months → Predict {predict_month.date()}")
        
        try:
            # Fit SARIMA model
            model = SARIMAX(
                train_data,
                order=order,
                seasonal_order=seasonal_order,
                enforce_stationarity=False,
                enforce_invertibility=False
            )
            
            fitted = model.fit(disp=False, maxiter=200)
            last_model = fitted
            
            # Forecast one step ahead
            forecast = fitted.forecast(steps=1)
            y_pred = float(forecast.iloc[0])
            y_true = float(y.loc[predict_month])
            
            residual = y_true - y_pred
            
            rows.append({
                "month": predict_month,
                "y_true": y_true,
                "y_pred": y_pred,
                "residual": residual,
                "abs_error": abs(residual),
            })
            
            if verbose:
                print(f"→ Pred: {y_pred:.2f}, Actual: {y_true:.2f}, Residual: {residual:+.2f}\n")
                
        except Exception as e:
            if verbose:
                print(f"→ ERROR: {str(e)}\n")
            continue
    
    if verbose:
        print(f"=== SARIMA WALK-FORWARD COMPLETE: {len(rows)} predictions made ===\n")
    
    wf_df = pd.DataFrame(rows)
    return wf_df, last_model

# ---------------------Main Execution---------------------------------------------------

def main() -> None:
    print("=" * 80)
    print("SARIMA MODEL TRAINING (Pure Time Series + Walk-Forward)")
    print("=" * 80)

    # 1. Load target series (only month and target_hicp_index)
    df = load_target_series()
    y = df["target_hicp_index"]
    
    print(f"\nLoaded {len(y)} months of HICP data")
    print(f"Period: {y.index.min().date()} → {y.index.max().date()}")
    
    # 2. Run walk-forward validation
    wf_df, last_model = walk_forward_sarima(
        y, 
        start_after=TRAIN_STOP,
        order=(1, 1, 1),  # ARIMA(1,1,1)
        seasonal_order=(1, 1, 1, 12),  # Monthly seasonality
        verbose=True
    )
    
    if wf_df.empty:
        raise ValueError("Walk-forward produced no rows. Check TRAIN_STOP date.")
    
    # 3. Calculate performance metrics
    metrics = calculate_robust_metrics(wf_df["y_true"].values, wf_df["y_pred"].values)
    
    print("\n" + "=" * 80)
    print("WALK-FORWARD PERFORMANCE (OUT-OF-SAMPLE)")
    print("=" * 80)
    for k, v in metrics.items():
        print(f"{k.upper():15s}: {('N/A' if v is None else f'{v:.4f}')}")
    
    # 4. Train final model on all pre-cutoff data for storage
    train_data = y[y.index <= TRAIN_STOP]
    print(f"\n Training final model on {len(train_data)} months (up to {TRAIN_STOP.date()})...")
    
    final_model = SARIMAX(
        train_data,
        order=(1, 1, 1),
        seasonal_order=(1, 1, 1, 12),
        enforce_stationarity=False,
        enforce_invertibility=False
    )
    final_fitted = final_model.fit(disp=False, maxiter=200)
    
    # 5. Save models to arima subfolder
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    base_path = MODEL_DIR / "sarima_model.pkl"
    joblib.dump(final_fitted, base_path)
    print(f"\nSaved base SARIMA model to {base_path}")
    
    if last_model is not None:
        last_model_path = MODEL_DIR / "sarima_model_last_wf.pkl"
        joblib.dump(last_model, last_model_path)
        print(f"Saved last walk-forward model to {last_model_path}")
    
    versioned_path = MODEL_DIR / f"sarima_model_{timestamp}.pkl"
    joblib.dump(final_fitted, versioned_path)
    print(f"Saved versioned model to {versioned_path}")
    
    # 6. Save predictions and metrics
    wf_df.to_csv(OUT_DIR / "sarima_predictions.csv", index=False)
    print(f"\nSaved predictions to {OUT_DIR / 'sarima_predictions.csv'}")
    
    metrics_df = pd.DataFrame([metrics])
    metrics_df.to_csv(OUT_DIR / "sarima_metrics.csv", index=False)
    print(f"Saved metrics to {OUT_DIR / 'sarima_metrics.csv'}")
    
    # 7. Summary
    print("\n" + "=" * 80)
    print("SARIMA WALK-FORWARD SUMMARY")
    print("=" * 80)
    print(f"Model type    : SARIMA(1,1,1)(1,1,1)[12]")
    print(f"Features      : target_hicp_index only (no exogenous)")
    print(f"WF window     : {wf_df['month'].min().date()} → {wf_df['month'].max().date()} ({len(wf_df)} months)")
    print(f"MAE / RMSE    : {metrics['mae']:.3f} / {metrics['rmse']:.3f}")
    print(f"Median AE     : {metrics['median_ae']:.3f}")
    print(f"P90 error     : {metrics['p90_error']:.3f}")
    print(f"R²            : {metrics['r2']:.3f}")
    if metrics['mape'] is not None:
        print(f"MAPE          : {metrics['mape']:.2f}%")
    print("\nTRAINING COMPLETE")

if __name__ == "__main__":
    main()