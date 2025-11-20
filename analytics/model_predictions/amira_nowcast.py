#!/usr/bin/env python3
"""
Nowcast HICP for a future month using SARIMA model.

Uses only historical target_hicp_index values to predict future values.
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
import os
import duckdb
import pandas as pd
import joblib
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parents[2]))
DB_PATH = Path(os.getenv("DB_PATH", PROJECT_ROOT / "data" / "euro_nowcast.duckdb"))
MODEL_DIR = PROJECT_ROOT / "models" / "arima"

# ------------------Helpers------------------

def load_historical_hicp(geo: str = "EA20") -> pd.Series:
    """Load historical target_hicp_index for given geography."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        query = """
            SELECT 
                month,
                target_hicp_index
            FROM gold.gold_nowcast_input
            WHERE geo = ?
              AND target_hicp_index IS NOT NULL
            ORDER BY month
        """
        df = con.execute(query, [geo]).df()
        df["month"] = pd.to_datetime(df["month"])
        return df.set_index("month")["target_hicp_index"]
    finally:
        con.close()

def load_sarima_model(path: Path | None = None):
    """Load trained SARIMA model."""
    if path is None:
        path = MODEL_DIR / "sarima_model.pkl"
    if not path.exists():
        raise FileNotFoundError(f"SARIMA model not found at {path}")
    return joblib.load(path)

def calculate_steps_ahead(historical_series: pd.Series, target_month: pd.Timestamp) -> int:
    """Calculate how many months ahead to forecast."""
    last_known_month = historical_series.index[-1]
    
    # Calculate month difference
    months_diff = (target_month.year - last_known_month.year) * 12 + \
                  (target_month.month - last_known_month.month)
    
    if months_diff <= 0:
        raise ValueError(
            f"Target month {target_month.date()} must be after last known month {last_known_month.date()}"
        )
    
    return months_diff

def save_nowcast_to_db(result: dict) -> None:
    """Save SARIMA nowcast prediction to gold.nowcast_output."""
    con = duckdb.connect(str(DB_PATH), read_only=False)
    try:
        # Delete existing prediction for this month/geo if present
        con.execute("""
            DELETE FROM gold.nowcast_output 
            WHERE month = ? AND geo = ? AND model_version = 'sarima_nowcast'
        """, [result['month'], result['geo']])
        
        # Insert new prediction
        con.execute("""
            INSERT INTO gold.nowcast_output 
            (month, geo, model_version, y_nowcast, y_actual, residual, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [
            result['month'],
            result['geo'],
            'sarima_nowcast',  # model version identifier
            result['level_pred'],  # predicted HICP index
            None,  # y_actual is NULL for future predictions
            None,  # residual is NULL for future predictions
        ])
        
        print(f"\n✅ Saved SARIMA prediction to gold.nowcast_output")
        print(f"   Month: {result['month'].date()}, Predicted Index: {result['level_pred']:.2f}")
        
    finally:
        con.close()

# ---------------------------------------------------------------------------
# CORE SARIMA NOWCAST FUNCTION
# ---------------------------------------------------------------------------

def nowcast_future_month_sarima(
    target_month: str | pd.Timestamp,
    geo: str = "EA20",
) -> dict:
    """
    Nowcast HICP for future month using SARIMA model.
    
    Args:
        target_month: Month to predict (e.g., "2025-10-01")
        geo: Geography code (default "EA20")
        
    Returns:
        Dictionary with prediction results
    """
    target_month = pd.Timestamp(target_month).normalize()
    
    print(f"\n{'='*60}")
    print(f"SARIMA NOWCAST FOR {target_month.date()}")
    print(f"{'='*60}")
    
    # 1. Load historical HICP series
    print(f"\n1. Loading historical HICP data for {geo}...")
    y_historical = load_historical_hicp(geo=geo)
    
    if y_historical.empty:
        raise ValueError(f"No historical data found for geo = {geo}")
    
    last_known_month = y_historical.index[-1]
    last_known_level = float(y_historical.iloc[-1])
    
    print(f"   ✓ Loaded {len(y_historical)} months")
    print(f"   ✓ Last known: {last_known_month.date()} = {last_known_level:.2f}")
    
    # 2. Calculate steps ahead
    steps_ahead = calculate_steps_ahead(y_historical, target_month)
    print(f"\n2. Forecasting {steps_ahead} month(s) ahead...")
    
    # 3. Load SARIMA model
    print(f"\n3. Loading SARIMA model...")
    model = load_sarima_model()
    print(f"   ✓ Model loaded: SARIMA{model.model_orders}")
    
    # 4. Forecast future values
    print(f"\n4. Generating forecast...")
    forecast = model.forecast(steps=steps_ahead)
    
    # Get the prediction for target month (last forecast value)
    level_pred = float(forecast.iloc[-1])
    
    # Calculate implied MoM change
    if steps_ahead == 1:
        mom_pred = (level_pred - last_known_level) / last_known_level
    else:
        # Multi-step forecast: calculate MoM from second-to-last to last
        prev_level = float(forecast.iloc[-2]) if steps_ahead > 1 else last_known_level
        mom_pred = (level_pred - prev_level) / prev_level
    
    print(f"   ✓ Predicted HICP index: {level_pred:.2f}")
    print(f"   ✓ Implied MoM change: {mom_pred:+.4f} ({mom_pred*100:+.2f}%)")
    
    # 5. Build result dictionary
    result = {
        "month": target_month,
        "geo": geo,
        "mom_pred": mom_pred,
        "level_pred": level_pred,
        "last_known_month": last_known_month,
        "last_known_level": last_known_level,
        "steps_ahead": steps_ahead,
        "model_type": "SARIMA",
    }
    
    return result

def main():
    """Example usage: Predict HICP for next month after latest data."""
    
    # First, check what the last known month is
    y_historical = load_historical_hicp(geo="EA20")
    last_known_month = y_historical.index[-1]
    
    # Predict next month
    next_month = last_known_month + pd.DateOffset(months=1)
    target_month = next_month.strftime("%Y-%m-01")
    
    print(f"Last known data: {last_known_month.date()}")
    print(f"Predicting for: {next_month.date()}")
    
    try:
        result = nowcast_future_month_sarima(target_month, geo="EA20")
        
        print(f"\n{'='*60}")
        print("SARIMA NOWCAST RESULTS")
        print(f"{'='*60}")
        print(f"Geography          : {result['geo']}")
        print(f"Target month       : {result['month'].date()}")
        print(f"Steps ahead        : {result['steps_ahead']} month(s)")
        print(f"Last known month   : {result['last_known_month'].date()}")
        print(f"Last known level   : {result['last_known_level']:.2f} (2015=100)")
        print(f"Predicted MoM      : {result['mom_pred']:+.4f} ({result['mom_pred']*100:+.2f}%)")
        print(f"Predicted HICP idx : {result['level_pred']:.2f} (2015=100)")
        print(f"{'='*60}\n")
        
        # Save to database
        save_nowcast_to_db(result)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        raise

if __name__ == "__main__":
    main()