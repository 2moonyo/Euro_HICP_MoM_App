CREATE SCHEMA IF NOT EXISTS ml;

-- One row per training run
CREATE TABLE IF NOT EXISTS ml.runs (
  run_id           VARCHAR PRIMARY KEY,
  model_version    VARCHAR NOT NULL,
  model_type       VARCHAR NOT NULL,          -- 'xgboost_regressor'
  started_at_utc   TIMESTAMP NOT NULL,
  finished_at_utc  TIMESTAMP NOT NULL,
  train_start      DATE,
  train_end        DATE,
  test_start       DATE,
  test_end         DATE,
  target           VARCHAR NOT NULL,          -- 'target_hicp_index'
  features         VARCHAR NOT NULL,          -- comma-separated feature list for reproducibility
  artifact_path    VARCHAR NOT NULL,          -- models/xgb_model_<version>.pkl
  notes            VARCHAR                    -- free text
);

-- Overall scalar metrics per run
CREATE TABLE IF NOT EXISTS ml.metrics (
  run_id   VARCHAR NOT NULL,
  metric   VARCHAR NOT NULL,                  -- 'MAE', 'R2', etc
  value    DOUBLE  NOT NULL
);

-- Flat key=value hyperparameters
CREATE TABLE IF NOT EXISTS ml.params (
  run_id   VARCHAR NOT NULL,
  name     VARCHAR NOT NULL,
  value    VARCHAR NOT NULL
);

-- Permissive FI table
CREATE TABLE IF NOT EXISTS ml.feature_importance (
  run_id    VARCHAR NOT NULL,
  feature   VARCHAR NOT NULL,
  importance DOUBLE NOT NULL
);

-- Out-of-sample predictions (test set)
CREATE TABLE IF NOT EXISTS ml.predictions (
  run_id    VARCHAR NOT NULL,
  month     DATE    NOT NULL,
  geo       VARCHAR,
  y_true    DOUBLE,
  y_pred    DOUBLE,
  residual  DOUBLE
);

-- Latest nowcast for dashboard consumption (one row per month x geo x model_version)
CREATE SCHEMA IF NOT EXISTS gold;
CREATE TABLE IF NOT EXISTS gold.nowcast_output (
  month          DATE    NOT NULL,
  geo            VARCHAR NOT NULL,
  model_version  VARCHAR NOT NULL,
  y_nowcast      DOUBLE  NOT NULL,
  y_actual       DOUBLE,
  residual       DOUBLE,
  created_at_utc TIMESTAMP NOT NULL
);
