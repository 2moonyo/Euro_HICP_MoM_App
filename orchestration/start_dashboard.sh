#!/usr/bin/env bash
# Launch Dash dashboard on localhost:5000
# Requires DuckDB with populated gold views

set -euo pipefail

# Load .env from project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$(dirname "$SCRIPT_DIR")/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: .env file not found at $ENV_FILE"
    exit 1
fi

echo "Loading environment from: $ENV_FILE"
export $(grep -v '^#' "$ENV_FILE" | grep -v '^$' | xargs)

# Now use PROJECT_ROOT from .env
PROJECT_ROOT="${PROJECT_ROOT:?ERROR: PROJECT_ROOT not set in .env}"
DASHBOARD_DIR="$PROJECT_ROOT/dashboard/InflationNowcastingDash"

echo "=========================================="
echo "Starting HICP Nowcast Dashboard"
echo "=========================================="
echo ""
echo "Dashboard will be available at:"
echo "  http://localhost:5000"
echo ""
echo "Press Ctrl+C to stop"
echo ""

cd "$DASHBOARD_DIR"
python app.py