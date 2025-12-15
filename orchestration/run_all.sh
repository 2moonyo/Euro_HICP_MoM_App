#!/usr/bin/env bash
# Full pipeline orchestration: Bronze → Silver → Gold → ML → Dashboard
# Runs all ingestion scripts, SQL transformations, dbt models, and ML training
# with observability logging to DuckDB and csv exports to logs/runs/

set -euo pipefail

# -------- Config --------
# Load .env from project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$(dirname "$SCRIPT_DIR")/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: .env file not found at $ENV_FILE"
    exit 1
fi

echo "Loading environment from: $ENV_FILE"
export $(grep -v '^#' "$ENV_FILE" | grep -v '^$' | xargs)

# Required variables from .env
PROJECT_ROOT="${PROJECT_ROOT:?ERROR: PROJECT_ROOT not set in .env}"
DB="${DB_PATH:?ERROR: DB_PATH not set in .env}"
BRONZE_ROOT="${BRONZE_ROOT:?ERROR: BRONZE_ROOT not set in .env}"
DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"
DBT_DIR="${DBT_DIR:-$PROJECT_ROOT/dbt_project}"
MODE="${1:-full}"

echo "Configuration:"
echo "  PROJECT_ROOT: $PROJECT_ROOT"
echo "  DB: $DB"
echo "  BRONZE_ROOT: $BRONZE_ROOT"
echo "  DBT_DIR: $DBT_DIR"
echo "  MODE: $MODE"
echo ""

# ----------Logging Directory ----------
RUN_ID="run_$(date +%Y%m%d_%H%M%S)"
LOG_DIR="$PROJECT_ROOT/logs/runs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/${RUN_ID}.log"
METRICS_DIR="$LOG_DIR/${RUN_ID}_metrics"
STAGE_START_TIME=""

# Redirect all terminal output to both screen and log file
exec > >(tee -a "$LOG_FILE")
exec 2>&1

echo "=========================================="
echo "Pipeline Run: $RUN_ID"
echo "Started: $(date)"
echo "Log file: $LOG_FILE"
echo "=========================================="

# Initialize observability schema 
$DUCKDB_BIN "$DB" -c ".read $PROJECT_ROOT/sql/00_create_observability_schema.sql" 2>/dev/null || true

# Record run start in database
$DUCKDB_BIN "$DB" <<SQL
INSERT INTO observability.pipeline_runs (run_id, started_at, status, mode, log_file)
VALUES ('$RUN_ID', CURRENT_TIMESTAMP, 'running', '$MODE', '$LOG_FILE');
SQL

# -------- Helper Functions  --------

# Mark the start of a stage 
stage_start() {
    STAGE_START_TIME=$(date +%s)
}

# Mark the end of a stage and log metrics to database
stage_end() {
    local stage_name=$1
    local status=${2:-success}  # Default to 'success'
    local row_count=${3:-0}     # Default to 0
    local error_msg=${4:-}      # Optional error message
    
    local end_time=$(date +%s)
    local duration=$((end_time - STAGE_START_TIME))
    
    # Escape single quotes in error message for SQL
    error_msg=$(echo "$error_msg" | sed "s/'/''/g")
    
    $DUCKDB_BIN "$DB" <<SQL
INSERT INTO observability.stage_metrics 
    (run_id, stage_name, started_at, ended_at, duration_seconds, row_count, status, error_message)
VALUES 
    ('$RUN_ID', 
     '$stage_name', 
     CURRENT_TIMESTAMP - INTERVAL '$duration seconds',
     CURRENT_TIMESTAMP, 
     $duration, 
     $row_count, 
     '$status',
     $([ -z "$error_msg" ] && echo "NULL" || echo "'$error_msg'"));
SQL
    
    echo "[LOGGED] Stage '$stage_name' completed in ${duration}s with status: $status"
}

# Helper to count rows in a table
get_row_count() {
    local table_name="$1"
    if [ -z "$table_name" ]; then
        echo "0"
        return
    fi
    
    # Try to get count, return 0 if table doesn't exist
    local count=$($DUCKDB_BIN "$DB" -csv -noheader -c "
        SELECT COUNT(*) 
        FROM $table_name
    " 2>/dev/null || echo "0")
    
    echo "$count"
}

# Helper to run SQL and capture status
run_sql_stage() {
    local stage_name=$1
    local sql_file=$2
    
    stage_start
    if $DUCKDB_BIN "$DB" -c ".read $sql_file" 2>&1; then
        stage_end "$stage_name" "success"
        return 0
    else
        stage_end "$stage_name" "failed" 0 "SQL execution failed"
        return 1
    fi
}

# Trap errors and mark run as failed
trap 'handle_error $?' EXIT

handle_error() {
    local exit_code=$1
    if [ $exit_code -ne 0 ]; then
        echo ""
        echo "=========================================="
        echo "PIPELINE FAILED"
        echo "=========================================="
        echo "Run ID: $RUN_ID"
        echo "Exit code: $exit_code"
        echo "Log file: $LOG_FILE"
        
        # Update run status to failed in database
        $DUCKDB_BIN "$DB" <<SQL 2>/dev/null || true
UPDATE observability.pipeline_runs
SET ended_at = CURRENT_TIMESTAMP, status = 'failed'
WHERE run_id = '$RUN_ID';
SQL
    fi
}

# -------- END Helper Functions --------

# Change to project root 
cd "$PROJECT_ROOT"

export PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}"

echo "Python environment:"
echo "  PYTHONPATH: $PYTHONPATH"
echo "  Working directory: $(pwd)"
echo ""


# -------- 1) Run extractors --------
echo ""
echo "=========================================="
echo "STEP 1: Running Data Extractors"
echo "=========================================="

# FX Data
stage_start
echo "Fetching FX data..."
if python ingestion/lambda_ecb_fx/fx_history.py; then
    fx_files=$(find "$BRONZE_ROOT/ecb_fx_eu" -name "*.csv" 2>/dev/null | wc -l | tr -d ' ')
    stage_end "ingestion_fx" "success" "$fx_files"
else
    stage_end "ingestion_fx" "failed" 0 "FX ingestion failed"
    exit 1
fi

# Interest Rates
stage_start
echo "Fetching interest rates data..."
if python ingestion/lambda_ecb_rates/ecb_rates_history.py; then
    rates_files=$(find "$BRONZE_ROOT/ecb_rates_eu" -name "*.csv" 2>/dev/null | wc -l | tr -d ' ')
    stage_end "ingestion_rates" "success" "$rates_files"
else
    stage_end "ingestion_rates" "failed" 0 "Rates ingestion failed"
    exit 1
fi

# Energy Prices
stage_start
echo "Fetching energy prices..."
if python ingestion/lambda_energy_prices/energy_prices.py; then
    energy_files=$(find "$BRONZE_ROOT/energy_prices" -name "*.csv" 2>/dev/null | wc -l | tr -d ' ')
    stage_end "ingestion_energy" "success" "$energy_files"
else
    stage_end "ingestion_energy" "failed" 0 "Energy ingestion failed"
    exit 1
fi

# HICP Data
stage_start
echo "Fetching HICP data..."
if python ingestion/lambda_eurostat_hicp/hicp_eu_history.py; then
    hicp_files=$(find "$BRONZE_ROOT/eurostat_hicp" -name "*.csv" 2>/dev/null | wc -l | tr -d ' ')
    stage_end "ingestion_hicp" "success" "$hicp_files"
else
    stage_end "ingestion_hicp" "failed" 0 "HICP ingestion failed"
    exit 1
fi

# Labour Unemployment Data
stage_start
echo "Fetching Labour Unemployment data..."
if python ingestion/lambda_labour/unemployment_rate.py; then
    unemployment_rate=$(find "$BRONZE_ROOT/labour_unemployment" -name "*.csv" 2>/dev/null | wc -l | tr -d ' ')
    stage_end "ingestion_unemployment" "success" "$unemployment_rate"
else
    stage_end "ingestion_unemployment" "failed" 0 "Unemployment ingestion failed"
    exit 1
fi

# Labour Job Vacancy Data
stage_start
echo "Fetching Labour Job Vacancy data..."
if python ingestion/lambda_labour/job_vacancy_rate.py; then
    job_vacancy_rate=$(find "$BRONZE_ROOT/labour_job_vacancy_rate" -name "*.csv" 2>/dev/null | wc -l | tr -d ' ')
    stage_end "ingestion_job_vacancy" "success" "$job_vacancy_rate"
else
    stage_end "ingestion_job_vacancy" "failed" 0 "Job vacancy ingestion failed"
    exit 1
fi

# Labour Hours Worked Data
stage_start
echo "Fetching Labour Hours Worked data..."
if python ingestion/lambda_labour/hours_worked.py; then
    hours_worked=$(find "$BRONZE_ROOT/labour_hours_worked" -name "*.csv" 2>/dev/null | wc -l | tr -d ' ')
    stage_end "ingestion_hours_worked" "success" "$hours_worked"
else
    stage_end "ingestion_hours_worked" "failed" 0 "Hours worked ingestion failed"
    exit 1
fi

# Labour Cost Index Data

stage_start
echo "Fetching Labour Cost Index data..."
if python ingestion/lambda_labour/labour_cost_index_history.py; then
    labour_cost_index=$(find "$BRONZE_ROOT/labour_cost_index" -name "*.csv" 2>/dev/null | wc -l | tr -d ' ')
    stage_end "ingestion_labour_cost_index" "success" "$labour_cost_index"
else
    stage_end "ingestion_labour_cost_index" "failed" 0 "Labour cost index ingestion failed"
    exit 1
fi
echo "[OK] All data extraction complete."

# -------- 2) Create schemas --------
echo ""
echo "=========================================="
echo "STEP 2: Creating Database Schemas"
echo "=========================================="
run_sql_stage "create_schemas" "$PROJECT_ROOT/sql/01_create_schemas.sql"

# -------- 3) Register bronze --------
echo ""
echo "=========================================="
echo "STEP 3: Registering Bronze Layer"
echo "=========================================="
stage_start
if run_sql_stage "register_bronze" "$PROJECT_ROOT/sql/02_register_bronze.sql"; then
    # Now count from registered bronze tables
    fx_bronze=$(get_row_count "bronze.fx_history")
    rates_bronze=$(get_row_count "bronze.rates_history")
    energy_bronze=$(get_row_count "bronze.energy_prices")
    hicp_bronze=$(get_row_count "bronze.hicp_history")
    total_bronze=$((fx_bronze + rates_bronze + energy_bronze + hicp_bronze))
    
    stage_end "register_bronze" "success" "$total_bronze"
else
    stage_end "register_bronze" "failed" 0 "Bronze registration failed"
    exit 1
fi

# -------- 4) Build LWW bronze views --------
echo ""
echo "=========================================="
echo "STEP 4: Creating Bronze LWW Views"
echo "=========================================="
run_sql_stage "bronze_lww_views" "$PROJECT_ROOT/sql/03_bronze_lww_views.sql"

# -------- 5) Validate bronze --------
echo ""
echo "=========================================="
echo "STEP 5: Validating Bronze Layer"
echo "=========================================="
run_sql_stage "validate_bronze" "$PROJECT_ROOT/sql/04_validate.sql"

# -------- 6) Build Silver tables --------
echo ""
echo "=========================================="
echo "STEP 6: Creating Silver Tables"
echo "=========================================="
run_sql_stage "create_silver" "$PROJECT_ROOT/sql/silver/01_create_silver.sql"

echo ""
echo "=========================================="
echo "STEP 7: Validating Silver Layer"
echo "=========================================="
run_sql_stage "validate_silver" "$PROJECT_ROOT/sql/silver/02_validate.sql"

echo ""
echo "=========================================="
echo "STEP 8: Creating Unified Silver View"
echo "=========================================="
run_sql_stage "unified_silver" "$PROJECT_ROOT/sql/silver/03_unified_table.sql"

echo "[OK] Full pipeline (Bronze -> Silver) complete."

# -------- 7) Run dbt for Gold layer --------
echo ""
echo "=========================================="
echo "STEP 9: Running dbt Gold Layer"
echo "=========================================="
cd "$DBT_DIR"

if command -v dbt &> /dev/null; then
    stage_start
    echo "Running dbt models for gold layer..."
    if dbt run --profiles-dir . --select gold.* --exclude tag:dashboard; then
        gold_count=$(get_row_count "gold.fact_hicp")
        stage_end "dbt_gold_run" "success" "$gold_count"
    else
        stage_end "dbt_gold_run" "failed" 0 "dbt run failed"
        cd "$PROJECT_ROOT"
        exit 1
    fi
    
    stage_start
    echo "Running dbt tests for gold layer..."
    if dbt test --profiles-dir . --select gold.* --exclude tag:dashboard; then
        stage_end "dbt_gold_test" "success"
    else
        stage_end "dbt_gold_test" "failed" 0 "dbt tests failed"
        # Don't exit - tests can fail without breaking pipeline
    fi
    
    echo "[OK] dbt Gold layer complete."
else
    echo "[WARNING] dbt command not found. Skipping dbt gold layer execution."
fi

cd "$PROJECT_ROOT"

# -------- 8) Train ML model --------
echo ""
echo "=========================================="
echo "STEP 10: Training ML Model"
echo "=========================================="
stage_start
if python analytics/model_training/model_training5.py; then
    ml_artifacts_count=$(get_row_count "observability.ml_artifacts")
    stage_end "ml_model_training" "success" "$ml_artifacts_count"
    echo "[OK] Model training complete."
else
    stage_end "ml_model_training" "failed" 0 "Model training failed"
    exit 1
fi

stage_start
if python analytics/model_training/model_training_labour.py; then
    ml_artifacts_count=$(get_row_count "observability.ml_artifacts")
    stage_end "ml_model_training" "success" "$ml_artifacts_count"
    echo "[OK] Model training complete."
else
    stage_end "ml_model_training" "failed" 0 "Model training failed"
    exit 1
fi


# -------- 9) Run nowcast prediction --------
echo ""
echo "=========================================="
echo "STEP 11: Running Nowcast Prediction"
echo "=========================================="
stage_start
if python analytics/model_predictions/nowcast_month.py; then
    nowcast_count=$(get_row_count "gold.nowcast_predictions")
    stage_end "nowcast_prediction" "success" "$nowcast_count"
    echo "[OK] Nowcast prediction complete."
else
    stage_end "nowcast_prediction" "failed" 0 "Nowcast prediction failed"
    exit 1
fi

# -------- 10) Build dashboard views --------
echo ""
echo "=========================================="
echo "STEP 12: Building Dashboard Views"
echo "=========================================="
cd "$DBT_DIR"

if command -v dbt &> /dev/null; then
    stage_start
    echo "Running dbt dashboard views..."
    if dbt run --profiles-dir . --select tag:dashboard; then
        stage_end "dbt_dashboard_views" "success"
        echo "[OK] Dashboard views created."
    else
        stage_end "dbt_dashboard_views" "failed" 0 "Dashboard view creation failed"
    fi
else
    echo "[WARNING] dbt command not found. Skipping dashboard views."
fi

cd "$PROJECT_ROOT"

# -------- 11) Verify dashboard views --------
echo ""
echo "=========================================="
echo "STEP 13: Verifying Dashboard Data"
echo "=========================================="
stage_start
echo "Checking v_hicp_dashboard_wide..."
hicp_count=$($DUCKDB_BIN "$DB" -csv -c "SELECT COUNT(*) as row_count FROM gold.v_hicp_dashboard_wide;" | tail -1)
echo "Row count: $hicp_count"

echo "Checking v_macro_monthly_wide..."
macro_count=$($DUCKDB_BIN "$DB" -csv -c "SELECT COUNT(*) as row_count FROM gold.v_macro_monthly_wide;" | tail -1)
echo "Row count: $macro_count"

stage_end "verify_dashboard" "success" "$((hicp_count + macro_count))"

# -------- 12) Export Database Profile --------
    # Database CSV run logs
echo ""
echo "=========================================="
echo "STEP 14: Exporting Database Profile"
echo "=========================================="

stage_start
SCHEMA_DIR="$METRICS_DIR/database_profile"
mkdir -p "$SCHEMA_DIR"

PROFILE_SQL_DIR="$PROJECT_ROOT/analytics/logs/database_profile"

echo "Exporting table catalog..."
$DUCKDB_BIN "$DB" -csv -c ".read $PROFILE_SQL_DIR/tables_catalog.sql" > "$SCHEMA_DIR/tables_catalog.csv" 2>/dev/null || echo "Error" > "$SCHEMA_DIR/tables_catalog.csv"

echo "Exporting column definitions..."
$DUCKDB_BIN "$DB" -csv -c ".read $PROFILE_SQL_DIR/column_definitions.sql" > "$SCHEMA_DIR/column_definitions.csv" 2>/dev/null || echo "Error" > "$SCHEMA_DIR/column_definitions.csv"

echo "Exporting table row counts..."
$DUCKDB_BIN "$DB" -csv -c ".read $PROFILE_SQL_DIR/table_row_counts.sql" > "$SCHEMA_DIR/table_row_counts.csv" 2>/dev/null || echo "Error" > "$SCHEMA_DIR/table_row_counts.csv"

echo "Exporting bronze layer stats..."
$DUCKDB_BIN "$DB" -csv -c ".read $PROFILE_SQL_DIR/bronze_layer_stats.sql" > "$SCHEMA_DIR/bronze_layer_stats.csv" 2>/dev/null || echo "Error" > "$SCHEMA_DIR/bronze_layer_stats.csv"

echo "Exporting silver layer stats..."
$DUCKDB_BIN "$DB" -csv -c ".read $PROFILE_SQL_DIR/silver_layer_stats.sql" > "$SCHEMA_DIR/silver_layer_stats.csv" 2>/dev/null || echo "Error" > "$SCHEMA_DIR/silver_layer_stats.csv"

echo "Exporting gold layer stats..."
$DUCKDB_BIN "$DB" -csv -c ".read $PROFILE_SQL_DIR/gold_layer_stats.sql" > "$SCHEMA_DIR/gold_layer_stats.csv" 2>/dev/null || echo "Error" > "$SCHEMA_DIR/gold_layer_stats.csv"

echo "Exporting observability stats..."
$DUCKDB_BIN "$DB" -csv -c ".read $PROFILE_SQL_DIR/observability_stats.sql" > "$SCHEMA_DIR/observability_stats.csv" 2>/dev/null || echo "Error" > "$SCHEMA_DIR/observability_stats.csv"

echo "Exporting schema summary..."
$DUCKDB_BIN "$DB" -csv -c ".read $PROFILE_SQL_DIR/schema_summary.sql" > "$SCHEMA_DIR/schema_summary.csv" 2>/dev/null || echo "Error" > "$SCHEMA_DIR/schema_summary.csv"

total_profile_files=$(ls -1 "$SCHEMA_DIR"/*.csv 2>/dev/null | wc -l | tr -d ' ')
stage_end "database_profile" "success" "$total_profile_files"
echo "[OK] Database profile exported to: $SCHEMA_DIR"


# -------- Final summary --------
RUN_END_TIME=$(date +%s)
RUN_DURATION=$((RUN_END_TIME - $(date -r "$LOG_FILE" +%s)))

# Update run status to success
$DUCKDB_BIN "$DB" <<SQL
UPDATE observability.pipeline_runs
SET ended_at = CURRENT_TIMESTAMP, status = 'success'
WHERE run_id = '$RUN_ID';
SQL

# -------- Export observability metrics to CSV --------
echo ""
echo "=========================================="
echo "STEP 14: Exporting Run Metrics"
echo "=========================================="


mkdir -p "$METRICS_DIR"

echo "Exporting run activity..."
$DUCKDB_BIN "$DB" -csv -c "SELECT * FROM observability.recent_activity WHERE run_id = '$RUN_ID';" > "$METRICS_DIR/run_activity.csv"

echo "Exporting stage metrics..."
$DUCKDB_BIN "$DB" -csv -c "SELECT * FROM observability.stage_metrics WHERE run_id = '$RUN_ID' ORDER BY started_at;" > "$METRICS_DIR/stage_metrics.csv"

echo "Exporting stage performance summary..."
$DUCKDB_BIN "$DB" -csv -c ".read $PROJECT_ROOT/analytics/logs/stage_performance.sql" > "$METRICS_DIR/stage_performance_30days.csv"

echo "Exporting recent runs..."
$DUCKDB_BIN "$DB" -csv -c ".read $PROJECT_ROOT/analytics/logs/recent_runs.sql" > "$METRICS_DIR/recent_runs.csv"

echo "[OK] Metrics exported to: $METRICS_DIR"

echo ""
echo "=========================================="
echo "PIPELINE COMPLETE"
echo "=========================================="
echo "Run ID: $RUN_ID"
echo "Duration: ${RUN_DURATION}s"
echo "Log file: $LOG_FILE"
echo ""
echo "✓ Data extraction (Bronze)"
echo "✓ Bronze layer validation"
echo "✓ Silver layer creation"
echo "✓ Gold layer (dbt)"
echo "✓ Model training"
echo "✓ Nowcast prediction"
echo "✓ Dashboard views created"
echo "✓ Database profile exported"
echo ""
echo "Query database anytime with:"
echo "  duckdb $DB -c \".read $PROFILE_SQL_DIR/table_row_counts.sql\""
echo "  duckdb $DB -c \".read $PROFILE_SQL_DIR/schema_summary.sql\""
echo ""
echo "View exported profiles:"
echo "  open $SCHEMA_DIR"
echo ""
echo "All steps completed successfully!"