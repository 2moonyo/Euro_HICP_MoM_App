#!/usr/bin/env python3
"""
Train XGBoost nowcast model using walk-forward validation.

Trains on month-over-month HICP changes, evaluates on index levels, and uses
incremental training with ensemble averaging.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Sequence, Tuple
import os
import duckdb
import numpy as np
import pandas as pd
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, r2_score, median_absolute_error, mean_squared_error
from scipy.stats import mstats
import joblib
from log_2_duckdb import gen_model_version, write_ml_artifacts_to_duckdb
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parents[2]))
DB_PATH = Path(os.getenv("DB_PATH", str(PROJECT_ROOT / "data" / "euro_nowcast.duckdb")))
MODEL_DIR = PROJECT_ROOT / "models"
OUT_DIR = PROJECT_ROOT / "analytics" / "outputs"

MODEL_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Cutoff for initial training period. All data after this date will be used for walk-forward validation.
TRAIN_STOP = pd.Timestamp("2023-07-01")  # inclusive

# ----------------------Data Loading & Preprocessing--------------------------

def load_gold() -> pd.DataFrame:
    """Load gold.gold_nowcast_input ordered by month."""
    con = duckdb.connect(str(DB_PATH))
    try:
        # Ensure data is time-ordered from the database
        df = con.execute("SELECT * FROM gold.gold_labour_nowcast_input ORDER BY month").df()
        # Convert month to datetime
        df["month"] = pd.to_datetime(df["month"])
        return df
    finally:
        con.close()

def add_smoothing_features(
    df: pd.DataFrame,
    cols: Sequence[str],
    windows: Sequence[int] = (3, 6, 12),
) -> pd.DataFrame:
    """Add shifted rolling mean/std to smooth volatile indicators (shift(1) prevents leakage)."""
    out = df.copy()
    for col in cols:
        if col not in out.columns:
            continue
        # IMPORTANT: shift(1) prevents leakage from the current month's value
        s = out[col].shift(1)
        for w in windows:
            out[f"{col}_ma{w}"] = s.rolling(w, min_periods=1).mean()
            out[f"{col}_std{w}"] = s.rolling(w, min_periods=1).std().fillna(0.0)
    return out

def winsorize_target(y: pd.Series, limits: Tuple[float, float] = (0.05, 0.05)) -> pd.Series:
    """Cap extreme target values to reduce outlier influence."""
    return pd.Series(mstats.winsorize(y, limits=limits), index=y.index, name=y.name)

def build_dataset(
    df: pd.DataFrame,
    target_col: str = "hicp_mom",
    level_col: str = "target_hicp_index",
    time_col: str = "month",
    winsorize: bool = True,
) -> Tuple[pd.DataFrame, pd.Series, pd.Series, pd.Series]:
    """Build feature matrix X plus aligned target/level/time series."""
    df = df.sort_values(time_col).reset_index(drop=True)

    # 1. Add date features for seasonality
    df['month_of_year'] = df[time_col].dt.month
    df['quarter'] = df[time_col].dt.quarter

    # 2. Add smoothing features for volatile columns
    volatile_cols = ["fx_usd_eur_avg", "policy_rate_monthly_avg", "policy_rate_eom"]
    df = add_smoothing_features(df, volatile_cols, windows=(3, 6, 12))

    # 3. Define all potential target/leakage cols to drop from features
    drop_cols = [
        level_col, "hicp_mom", "hicp_yoy",  # Drop all target types
        time_col, "geo", "energy_price_usd"   # Drop other non-features
    ]
    drop_cols = [c for c in drop_cols if c in df.columns]

    X = df.drop(columns=drop_cols, errors="ignore")
    y_target = df[target_col]
    y_levels = df[level_col]  
    months = df[time_col]

    # 4. Keep only numeric columns
    numeric_cols = X.select_dtypes(include=[np.number, "bool"]).columns.tolist()
    non_numeric = [c for c in X.columns if c not in numeric_cols]
    if non_numeric:
        warnings.warn(f"Dropping non-numeric columns: {non_numeric}")
    X = X[numeric_cols]

    # 5. Winsorize target 
    if winsorize:
        y_target = winsorize_target(y_target, limits=(0.05, 0.05))

    # 6. Drop rows with NaNs (in features or *this* target)
    mask = X.notna().all(axis=1) & y_target.notna() & y_levels.notna()
    X = X[mask].reset_index(drop=True)
    y_target = y_target[mask].reset_index(drop=True)
    y_levels = y_levels[mask].reset_index(drop=True)
    months = months[mask].reset_index(drop=True)

    return X, y_target, y_levels, months

def build_train_mask_until(months: pd.Series, end_inclusive: str | pd.Timestamp) -> np.ndarray:
    """Return boolean mask for months <= end_inclusive."""
    end_inclusive = pd.Timestamp(end_inclusive)
    m = pd.to_datetime(months)
    return (m <= end_inclusive).to_numpy()

def assert_finite(*arrays) -> None:
    """Raise ValueError if arrays contain NaN or inf."""
    for arr in arrays:
        a = np.asarray(arr)
        if not np.isfinite(a).all():
            raise ValueError("Non-finite values detected in array used for training or scoring")

# ------------------------Metrics--------------------------------------------

def calculate_robust_metrics(y_true, y_pred) -> dict:
    """Calculate error metrics (MAE, RMSE, Huber, percentiles)."""
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

def get_xgb(random_state: int = 42) -> XGBRegressor:
    """Return configured XGBRegressor for tabular data."""
    return XGBRegressor(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=3,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=0.1,
        reg_lambda=1.0,
        objective="reg:squarederror",
        random_state=random_state,
        tree_method="hist",
        eval_metric="mae",
    )

def walk_forward_nowcast(
    X: pd.DataFrame,
    y_mom: pd.Series,
    y_levels: pd.Series,
    months: pd.Series,
    geo: str = "EA20",
    start_after: str | pd.Timestamp = TRAIN_STOP,
    seeds: Sequence[int] = (42, 123, 456), 
    verbose: bool = True,
) -> Tuple[pd.DataFrame, Optional[XGBRegressor]]:
    start_after = pd.Timestamp(start_after)

    # Enforce chronological order (though load_gold should handle this)
    order = np.argsort(months.to_numpy())
    X, y_mom, y_levels, months = X.iloc[order], y_mom.iloc[order], y_levels.iloc[order], months.iloc[order]

    # Find all index points needed to predict (everything after the cutoff)
    idxs = np.where(months > start_after)[0]
    rows = []

    # 1. Store one model per seed for incremental training
    ensemble_models: dict[int, XGBRegressor | None] = {seed: None for seed in seeds}
    last_model: Optional[XGBRegressor] = None

    if verbose:
        print("\n=== WALK-FORWARD NOWCAST START ===")
        print(f"Start after: {start_after.date()} ({len(idxs)} steps ahead)\n")

    for i, idx in enumerate(idxs, start=1):
        if idx == 0: continue # Cannot predict the very first point
            
        predict_month = months.iloc[idx]

        # We train on all data up to the prediction month 
        X_tr, y_tr_mom = X.iloc[:idx], y_mom.iloc[:idx]
        # We predict for the single next month
        X_te = X.iloc[idx:idx + 1]

        # Get true values for this step
        y_true_mom = float(y_mom.iloc[idx])
        y_true_level = float(y_levels.iloc[idx])
        
        # Get *known* previous level to convert prediction
        lag_1_level = float(y_levels.iloc[idx - 1])

        if verbose:
            print(
                f"[Step {i:02d}] Training up to {months.iloc[idx-1].date()} "
                f"({len(X_tr)} rows) → Predict {predict_month.date()}"
            )

        preds_mom = []
        for sd in seeds:
            m = get_xgb(random_state=sd)
            prev_model = ensemble_models[sd]

            # 2. Incremental training
            m.fit(X_tr, y_tr_mom, verbose=False, xgb_model=prev_model)

            ensemble_models[sd] = m  # Save new model for next step
            preds_mom.append(float(m.predict(X_te)[0]))
            last_model = m

        # 3. Average MoM predictions
        y_hat_mom = float(np.mean(preds_mom))

        # 4. Convert MoM prediction back to level prediction
        # hicp_mom is fractional (e.g., 0.005 for 0.5%)
        y_hat_level = lag_1_level * (1 + y_hat_mom)
        

        residual = y_true_level - y_hat_level

        rows.append(
            {
                "month": predict_month,
                "geo": geo,
                "y_true_level": y_true_level,
                "y_pred_level": y_hat_level, 
                "y_true_mom": y_true_mom,
                "y_pred_mom": y_hat_mom,
                "residual": residual,
                "abs_error": abs(residual),
            }
        )

        if verbose:
            print(f"→ MoM Pred: {y_hat_mom:+.4f}, Actual: {y_true_mom:+.4f}")
            print(f"→ Level Pred: {y_hat_level:.2f}, Actual: {y_true_level:.2f}, Residual: {residual:+.2f}\n")

    if verbose:
        print(f"=== WALK-FORWARD COMPLETE: {len(rows)} predictions made ===\n")

    wf_df = pd.DataFrame(rows)
    return wf_df, last_model

def persist_nowcast_rows_to_gold(wf_df: pd.DataFrame, model_version: str) -> None:
    """Append walk-forward predictions to gold.nowcast_output."""
    if wf_df.empty:
        return
    
    # Ensure DataFrame has the columns 'y_true' and 'y_pred' for the SQL query
    if "y_true" not in wf_df.columns or "y_pred" not in wf_df.columns:
        raise KeyError("DataFrame must have 'y_true' and 'y_pred' columns for persisting.")

    con = duckdb.connect(str(DB_PATH))
    try:
        data = [
            [
                pd.to_datetime(r["month"]).date(),
                r["geo"],
                model_version,
                float(r["y_pred"]), 
                float(r["y_true"]), 
                float(r["residual"]),
                datetime.now(timezone.utc),
            ]
            for _, r in wf_df.iterrows()
        ]
        con.executemany(
            """
            INSERT INTO gold.nowcast_output
            (month, geo, model_version, y_nowcast, y_actual, residual, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            data,
        )
    finally:
        con.close()

# ---------------------Main Execution---------------------------------------------------

def main() -> None:
    print("=" * 80)
    print("ROBUST MODEL TRAINING (MoM Target + Walk-Forward)")
    print("=" * 80)

    # 1. Load and build dataset
    df = load_gold()
    print(f"\nLoaded {len(df)} rows from gold.gold_nowcast_input")

    # Target is 'hicp_mom', 'target_hicp_index' is used for level conversion
    X, y_mom, y_levels, months = build_dataset(
        df, 
        target_col="hicp_mom", 
        level_col="target_hicp_index", 
        winsorize=True
    )
    print(f"\nFeatures: {len(X.columns)} columns")
    print(f"Samples after cleaning: {len(X)}")
    
    assert_finite(X.to_numpy(), y_mom.to_numpy(), y_levels.to_numpy())

    # 2. Define initial train/test split (for feature importance snapshot)
    train_mask_until = build_train_mask_until(months, TRAIN_STOP)
    if train_mask_until.sum() < 24:
        raise ValueError("Too few training rows before the cutoff; check TRAIN_STOP or data.")

    X_train0, y_train0_mom = X[train_mask_until], y_mom[train_mask_until]
    X_test0, y_test0_mom = X[~train_mask_until], y_mom[~train_mask_until]
    m_train0, m_test0 = months[train_mask_until], months[~train_mask_until]

    print(f"\nInitial Train-until: {m_train0.min().date()} → {m_train0.max().date()}  ({len(X_train0)} samples)")
    print(f"Walk-forward window: {m_test0.min().date()} → {m_test0.max().date()}  ({len(X_test0)} samples)")

    # 3. Run walk-forward out-of-sample (retrain monthly, predict next)
    wf_df, last_model = walk_forward_nowcast(
        X, y_mom, y_levels, months, start_after=TRAIN_STOP
    )

    if wf_df.empty:
        raise ValueError("Walk-forward produced no rows. Is TRAIN_STOP beyond the last month?")

    # 4. Aggregate performance on the walk-forward period (using LEVELS)
    metrics = calculate_robust_metrics(wf_df["y_true_level"].values, wf_df["y_pred_level"].values)
    print("\n" + "=" * 80)
    print("WALK-FORWARD PERFORMANCE (OUT-OF-SAMPLE, ON LEVELS)")
    print("=" * 80)
    for k, v in metrics.items():
        print(f"{k.upper():15s}: {('N/A' if v is None else f'{v:.4f}')}")

    # 5. Feature importance snapshot (from model trained on the full pre-cutoff history)
    primary_model = get_xgb(random_state=42)
    primary_model.fit(X_train0, y_train0_mom, verbose=False) # Train on MoM

    fi = (
        pd.DataFrame({"feature": X.columns, "importance": primary_model.feature_importances_})
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )
    fi.to_csv(OUT_DIR / "feature_importance_robust.csv", index=False)
    print(f"\nTop 10 features (from model trained up to {TRAIN_STOP.date()}):")
    print(fi.head(10).to_string(index=False))

    # 6. Save models
    base_path = MODEL_DIR / "xgb_model_robust.pkl"
    joblib.dump(primary_model, base_path)
    print(f"\nSaved primary (pre-cutoff) model to {base_path}")
    if last_model is not None:
        last_model_path = MODEL_DIR / "xgb_model_robust_last_wf.pkl"
        joblib.dump(last_model, last_model_path)
        print(f"Saved last walk-forward model to {last_model_path}")

    # 7. Log run to DuckDB
    # Prep dataframe for logging (needs columns 'y_true', 'y_pred')
    test_pred_df_logs = wf_df.rename(
        columns={"y_true_level": "y_true", "y_pred_level": "y_pred"}
    )
    
    train_period = (m_train0.min(), m_train0.max())
    test_period = (wf_df["month"].min(), wf_df["month"].max())

    model_version = gen_model_version(primary_model, X.columns.tolist())
    model_versioned = MODEL_DIR / f"xgb_model_robust_{model_version}.pkl"
    joblib.dump(primary_model, model_versioned)

    info = write_ml_artifacts_to_duckdb(
        model=primary_model,
        X_cols=X.columns.tolist(),
        mae=metrics["mae"],
        r2=metrics["r2"],
        feature_importance=fi,
        test_frame=test_pred_df_logs[["month", "geo", "y_true", "y_pred", "residual"]],
        train_period=train_period,
        test_period=test_period,
        target_col="hicp_mom",  
        notes=(
            "Walk-forward (incremental train): train ≤ 2023-07, predict 2023-08→latest. "
            "Trains on hicp_mom, evaluates on levels. "
            "Date features, shifted rolling features, ensemble per step."
        ),
        artifact_path=model_versioned,
    )
    print("\nLogged to DuckDB:")
    print(f"  run_id:        {info['run_id']}")
    print(f"  model_version: {info['model_version']}")

    # 8. Store extra robust metrics
    con = duckdb.connect(str(DB_PATH))
    try:
        extra = {k: v for k, v in metrics.items() if k not in ("mae", "r2") and v is not None}
        for k, v in extra.items():
            con.execute("INSERT INTO ml.metrics VALUES (?, ?, ?)", [info['run_id'], k, float(v)])
        con.execute(
            "INSERT INTO ml.params VALUES (?, ?, ?)",
            [info['run_id'], "train_until", TRAIN_STOP.strftime("%Y-%m-%d")],
        )
    finally:
        con.close()

    # 9. Append all walk-forward months to gold.nowcast_output
    persist_nowcast_rows_to_gold(
        test_pred_df_logs.assign(geo="EA20"), model_version=info["model_version"]
    )
    print(f"\nAppended {len(wf_df)} rows to gold.nowcast_output")

    # 10. Quick summary
    print("\n" + "=" * 80)
    print("WALK-FORWARD SUMMARY (ON LEVELS)")
    print("=" * 80)
    print(f"WF window     : {wf_df['month'].min().date()} → {wf_df['month'].max().date()} ({len(wf_df)} months)")
    print(f"MAE / RMSE    : {metrics['mae']:.3f} / {metrics['rmse']:.3f}")
    print(f"Median AE     : {metrics['median_ae']:.3f}")
    print(f"P90 error     : {metrics['p90_error']:.3f}")
    print(f"R²            : {metrics['r2']:.3f}")
    print("\nTRAINING COMPLETE")

if __name__ == "__main__":
    main()