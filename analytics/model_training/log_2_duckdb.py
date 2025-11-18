"""Log ML params to DuckDB ml schema."""

from __future__ import annotations
import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).parents[2]
DB_PATH = PROJECT_ROOT / "data" / "euro_nowcast.duckdb"

def gen_model_version(model_obj, X_columns: list[str]) -> str:
    """Generate model version from timestamp and hyperparameter hash."""
    # Extract params dict safely
    try:
        params = model_obj.get_xgb_params()
    except Exception:
        try:
            params = model_obj.get_params()
        except Exception:
            params = {}
    payload = json.dumps({"params": params, "cols": X_columns}, sort_keys=True).encode()
    short = hashlib.sha1(payload).hexdigest()[:8]
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"xgb_{ts}_{short}"

def write_ml_artifacts_to_duckdb(
    *,
    model,
    X_cols: list[str],
    mae: float,
    r2: float,
    feature_importance: pd.DataFrame,   # columns: feature, importance
    test_frame: pd.DataFrame,           # columns: month, geo, y_true, y_pred, residual
    train_period: tuple[pd.Timestamp, pd.Timestamp],
    test_period: tuple[pd.Timestamp, pd.Timestamp],
    target_col: str,
    notes: str | None = None,
    artifact_path: Path,
) -> dict:
    """Write metrics, params, feature importance, and predictions to ml schema."""

    con = duckdb.connect(str(DB_PATH))

    # Ensure schema/tables exist 
    con.execute(open(PROJECT_ROOT / "analytics" / "model_training" / "sql" / "create_ml_schema.sql").read())

    started_at = datetime.now(timezone.utc)  # this function call time is fine
    model_version = gen_model_version(model, X_cols)

    # Generate run_id separate from model_version
    run_id = str(uuid.uuid4())

    # Runners
    features_csv = ",".join(X_cols)
    finished_at = datetime.now(timezone.utc)

    # Insert ml.runs
    con.execute(
        """
        INSERT INTO ml.runs
        (run_id, model_version, model_type, started_at_utc, finished_at_utc,
         train_start, train_end, test_start, test_end, target, features, artifact_path, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            model_version,
            "xgboost_regressor",
            started_at,
            finished_at,
            pd.to_datetime(train_period[0]).date() if train_period[0] is not None else None,
            pd.to_datetime(train_period[1]).date() if train_period[1] is not None else None,
            pd.to_datetime(test_period[0]).date() if test_period[0] is not None else None,
            pd.to_datetime(test_period[1]).date() if test_period[1] is not None else None,
            target_col,
            features_csv,
            str(artifact_path),
            notes or "",
        ],
    )

    # Insert ml.metrics
    con.execute("INSERT INTO ml.metrics VALUES (?, ?, ?)", [run_id, "MAE", float(mae)])
    con.execute("INSERT INTO ml.metrics VALUES (?, ?, ?)", [run_id, "R2", float(r2)])

    # Insert ml.params
    try:
        p = model.get_xgb_params()
    except Exception:
        p = model.get_params()
    params_rows = [(run_id, str(k), str(v)) for k, v in sorted(p.items(), key=lambda kv: kv[0])]
    if params_rows:
        con.register("tmp_params", pd.DataFrame(params_rows, columns=["run_id", "name", "value"]))
        con.execute("INSERT INTO ml.params SELECT * FROM tmp_params")
        con.unregister("tmp_params")

    # Insert ml.feature_importance
    fi = feature_importance.copy()
    fi["run_id"] = run_id
    fi = fi[["run_id", "feature", "importance"]]
    con.register("tmp_fi", fi)
    con.execute("INSERT INTO ml.feature_importance SELECT * FROM tmp_fi")
    con.unregister("tmp_fi")

    # Insert ml.predictions
    pred = test_frame.copy()
    pred["run_id"] = run_id
    pred = pred[["run_id", "month", "geo", "y_true", "y_pred", "residual"]]
    # Ensure dtypes are friendly
    pred["month"] = pd.to_datetime(pred["month"]).dt.date
    con.register("tmp_pred", pred)
    con.execute("INSERT INTO ml.predictions SELECT * FROM tmp_pred")
    con.unregister("tmp_pred")

    con.close()
    return {"run_id": run_id, "model_version": model_version}
