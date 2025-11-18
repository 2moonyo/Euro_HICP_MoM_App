"""Fetch ECB policy rates and write Bronze partitions.

Pulls MRR, DFR, MLF from SDMX Financial Markets dataset, normalises dates, and
writes Hive-style folders.
"""

import requests
import json
import os
import csv
from datetime import date, datetime, timezone
from typing import Dict, Any, List, Tuple, Iterable
from itertools import groupby
from ingestion.common.bronze_io import write_bronze_by_dt
from pathlib import Path
from dotenv import load_dotenv

# -------------------- IO CONFIG --------------------
load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parents[2]))
BRONZE_ROOT = os.getenv("BRONZE_ROOT", str(PROJECT_ROOT / "data" / "bronze"))
DATASET = os.getenv("DATASET", "ecb_rates_eu")  
SOURCE = os.getenv("SOURCE", "ECB")  

# -------------------- CONFIG --------------------
BASE_URL = "https://data-api.ecb.europa.eu/service"
DATAFLOW_ID = "FM"  # Financial Markets (ECB policy rates live here)

# Store ONLY the KEY part (everything after "FM.")
SERIES_MAP = {
    "MRR": "D.U2.EUR.4F.KR.MRR_FR.LEV",  # Main refinancing operations - fixed rate tenders (date of changes) - Level
    "DFR": "D.U2.EUR.4F.KR.DFR.LEV",     # Deposit facility - date of changes (raw data) - Level
    "MLF": "D.U2.EUR.4F.KR.MLFR.LEV",    # Marginal lending facility - date of changes (raw data) - Level
}

# -------------------- URL BUILDER --------------------
def build_rates_url(
    series_key: str,
    start: str,
    end: str,
    fmt: str = "jsondata",
) -> str:
    """Build SDMX URL for ECB policy rates with given parameters."""
    return (
        f"{BASE_URL}/data/{DATAFLOW_ID}/{series_key}"
        f"?startPeriod={start}"
        f"&endPeriod={end}"
        f"&format={fmt}"
    )
# -------------------- FETCH --------------------
def fetch_rates_series(url: str) -> Dict[str, Any]:
    """Fetch JSON from ECB SDMX endpoint."""
    resp = requests.get(url, headers={"Accept": "application/vnd.sdmx.data+json"})
    if resp.status_code != 200:
        ct = resp.headers.get("content-type", "")
        snippet = resp.text[:400]
        raise RuntimeError(f"ECB API HTTP {resp.status_code} (ct={ct}): {snippet}")
    try:
        return resp.json()
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Expected JSON but got ct={resp.headers.get('content-type')}; body[:200]={resp.text[:200]}"
        ) from e
# -------------------- EXTRACT --------------------
def extract_rows_rates(payload: Dict[str, Any], label: str) -> List[Tuple[str, str, float]]:
    """Parse SDMX JSON into (date, series_label, value) tuples, normalising time strings."""
    structure = payload["structure"]
    obs_dims = structure["dimensions"]["observation"]
    if not obs_dims:
        raise RuntimeError("Unexpected SDMX payload: missing observation dimensions")
    time_values = [v["id"] for v in obs_dims[0]["values"]]  # time axis

    data = payload["dataSets"][0]
    series_dict = data.get("series") or {}

    rows: List[Tuple[str, str, float]] = []

    # Typical FM payload exposes a single series; iterate defensively anyway
    if series_dict:
        for _, series_body in series_dict.items():
            observations = series_body.get("observations", {})
            for obs_index_str, obs_value_arr in observations.items():
                idx = int(obs_index_str)
                raw = time_values[idx]
                # Normalise date string
                if len(raw) == 10:        # YYYY-MM-DD
                    dt_str = raw
                elif len(raw) == 7:       # YYYY-MM
                    dt_str = f"{raw}-01"
                elif len(raw) == 4:       # YYYY
                    dt_str = f"{raw}-01-01"
                else:
                    dt_str = raw

                value = obs_value_arr[0]
                if value is None:
                    continue
                rows.append((dt_str, label, float(value)))
    else:
        # Some SDMX variants put "observations" at the top level
        observations = data.get("observations", {})
        for obs_index_str, obs_value_arr in observations.items():
            idx = int(obs_index_str)
            raw = time_values[idx]
            if len(raw) == 10:
                dt_str = raw
            elif len(raw) == 7:
                dt_str = f"{raw}-01"
            elif len(raw) == 4:
                dt_str = f"{raw}-01-01"
            else:
                dt_str = raw
            value = obs_value_arr[0]
            if value is None:
                continue
            rows.append((dt_str, label, float(value)))

    return rows

# -------------------- WRITE --------------------
def ensure_dir(path: str) -> None:
    """Create directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)

def write_csv_batch(rows: List[Tuple[str, str, float]], load_date: str, stamp: str) -> str:
    """Write aggregated CSV for analysts alongside Bronze partitions."""
    # Build full path to dataset directory
    dataset_dir = os.path.join(BRONZE_ROOT, DATASET)
    batch_dir = os.path.join(dataset_dir, "Batch")
    ensure_dir(batch_dir)
    out_path = os.path.join(batch_dir, f"ecb_rates_eu_{stamp}.csv")

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "series", "value", "load_ts"])
        for dt_str, series_label, val in rows:
            w.writerow([dt_str, series_label, val, stamp])
    
    return out_path

def write_bronze_rates(records):
    """Map parsed tuples onto Bronze schema and write per-date files."""
    bronze_rows = []
    for date_str, series_label, value in records:  # ✅ Unpack the tuple
        bronze_rows.append({
            "dt": date_str,
            "dataset": DATASET,
            "series_id": series_label,  # e.g. 'MRR', 'DFR', 'MLF'
            "geo": "EA20",
            "value": float(value) if value is not None else None,
            "source": SOURCE,
            "ingest_ts": None,  # Auto-filled by bronze_io
        })
    
    files_written = write_bronze_by_dt(bronze_rows, dataset=DATASET, root=BRONZE_ROOT)
    return files_written, len(bronze_rows)

# -------------------- MAIN --------------------
if __name__ == "__main__":
    # ---- CONFIG SECTION ----
    # Historical build to align with your FX:
    start_full = "2000-01-01"
    end_full = date.today().isoformat()

    series_map = SERIES_MAP.copy()

    # 1) Fetch + extract for each series, accumulate rows
    all_rows: List[Tuple[str, str, float]] = []
    for label, key in series_map.items():
        url = build_rates_url(series_key=key,
                              start=start_full,
                              end=end_full)
        print(f"[{label}] Requesting: {url}")
        payload = fetch_rates_series(url)
        rows = extract_rows_rates(payload, label)
        all_rows.extend(rows)

    # 2) Sort for reproducibility (date, series)
    all_rows.sort(key=lambda r: (r[0], r[1]))

    bronze_files, nrows = write_bronze_rates(all_rows)
    
    # 3) Write batch + partitions
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    load_date_iso = stamp[:4] + "-" + stamp[4:6] + "-" + stamp[6:8]
    batch_path = write_csv_batch(all_rows, load_date_iso, stamp)

    print(json.dumps({
        "dataset": DATASET,
        "rows": nrows,
        "bronze_files": bronze_files,
        "bronze_root": BRONZE_ROOT
    }, indent=2))
