"""Fetch ECB FX rates and write Bronze partitions.

Parses SDMX JSON, produces one row per date/currency, and writes S3-style
folders for Silver ingestion.
"""

import requests
import json
from datetime import date, datetime, timezone
from typing import Dict, Any, List, Tuple, Iterable
import csv
import os
from ingestion.common.bronze_io import write_bronze_by_dt
from pathlib import Path
from dotenv import load_dotenv

# -------------------- IO CONFIG --------------------
load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parents[2]))
BRONZE_ROOT = os.getenv("BRONZE_ROOT", str(PROJECT_ROOT / "data" / "bronze"))
DATASET = os.getenv("DATASET", "ecb_fx_eu")  
SOURCE = os.getenv("SOURCE", "ECB")  
# -------------------- CONFIG --------------------
BASE_URL = "https://data-api.ecb.europa.eu/service"
DATAFLOW_ID = "EXR"



# -------------------- URL BUILDER --------------------
def build_fx_url(
    quote_ccys: List[str],
    base_ccy: str,
    freq: str,
    rate_type: str,
    variation: str,
    start: str,
    end: str,
    fmt: str = "jsondata",
) -> str:
    """Build SDMX URL for ECB FX rates with given parameters."""
    quote_key = "+".join([q.upper() for q in quote_ccys])
    series_key = f"{freq}.{quote_key}.{base_ccy}.{rate_type}.{variation}"

    url = (
        f"{BASE_URL}/data/{DATAFLOW_ID}/{series_key}"
        f"?startPeriod={start}"
        f"&endPeriod={end}"
        f"&format={fmt}"
    )
    return url


def fetch_fx_series(url: str) -> Dict[str, Any]:
    """Fetch JSON from ECB SDMX endpoint."""
    resp = requests.get(url)
    if resp.status_code != 200:
        raise RuntimeError(f"ECB API HTTP {resp.status_code}: {resp.text[:300]}")
    try:
        return resp.json()
    except json.JSONDecodeError as e:
        # Helpful debugging if the ECB sends XML instead
        raise RuntimeError(
            f"Did not get JSON. content-type={resp.headers.get('content-type')}, body[:200]={resp.text[:200]}"
        ) from e


def extract_rows_final(payload: Dict[str, Any]) -> List[Tuple[str, str, str, float]]:
    """Parse SDMX JSON into (date, quote_ccy, base_ccy, rate) tuples."""

    # 1. Get the series-level dimensions (FREQ, CURRENCY, CURRENCY_DENOM, EXR_TYPE, EXR_SUFFIX)
    structure = payload["structure"]
    series_dims = structure["dimensions"]["series"]  # list, ordered
    obs_dims = structure["dimensions"]["observation"]  # list with TIME_PERIOD

    series_dims_meta = []
    for dim in series_dims:
        dim_id = dim["id"]  # e.g. "CURRENCY"
        codes = [v["id"] for v in dim["values"]]  # e.g. ["GBP","JPY","USD"]
        series_dims_meta.append({
            "id": dim_id,
            "codes": codes,
        })

    # Find which dimension index corresponds to quote and base currencies
    quote_dim_idx = None
    base_dim_idx = None
    for idx, meta in enumerate(series_dims_meta):
        if meta["id"] == "CURRENCY":
            quote_dim_idx = idx
        if meta["id"] == "CURRENCY_DENOM":
            base_dim_idx = idx

    if quote_dim_idx is None or base_dim_idx is None:
        raise RuntimeError("Could not find currency dimensions in structure.")

    # 2. Build the time axis list
    # obs_dims[0] should define TIME_PERIOD; .values is list of {id: "2025-09-29", ...}
    time_values = [v["id"] for v in obs_dims[0]["values"]]

    # 3. Iterate over each series in dataSets[0].series
    series_dict = payload["dataSets"][0]["series"]

    rows: List[Tuple[str, str, str, float]] = []

    for series_key, series_body in series_dict.items():
        # series_key looks like "0:0:0:0:0"
        parts = series_key.split(":")

        # decode quote_ccy and base_ccy using the dimension maps
        quote_ccy_code = series_dims_meta[quote_dim_idx]["codes"][int(parts[quote_dim_idx])]
        base_ccy_code = series_dims_meta[base_dim_idx]["codes"][int(parts[base_dim_idx])]

        observations = series_body["observations"]
        # observations is dict: obs_index -> [value, ...]
        for obs_index_str, obs_value_arr in observations.items():
            obs_index = int(obs_index_str)
            obs_date = time_values[obs_index]
            value = obs_value_arr[0]  # the first element is the actual rate

            if value is None:
                continue

            rows.append(
                (obs_date, quote_ccy_code, base_ccy_code, float(value))
            )

    return rows
# -------------------- WRITE CSV --------------------

def ensure_dir(path: str) -> None:
    """Create ``path`` if it does not exist."""
    os.makedirs(path, exist_ok=True)

def write_csv(rows: List[Tuple[str, str, str, float]], out_path: str):
    """Persist a human-readable CSV in addition to Bronze partitions."""
    # Generate timestamp
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    
    # Build full path to dataset directory
    dataset_dir = os.path.join(BRONZE_ROOT, DATASET)
    batch_dir = os.path.join(dataset_dir, "Batch")
    ensure_dir(batch_dir)
    full_path = os.path.join(batch_dir, f"ecb_fx_eu_{stamp}.csv")

    with open(full_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "quote_ccy", "base_ccy", "rate", "load_ts"])
        for date_str, quote_ccy, base_ccy, rate in rows:
            w.writerow([date_str, quote_ccy, base_ccy, rate, stamp])
    
    return full_path


if __name__ == "__main__":
    # ---- CONFIG SECTION ----
    # ASSUMPTION: We pull from 2000-01-01 to today for offline history build.
    start_full = "2000-01-01"
    end_full = date.today().isoformat()

    base_ccy = "EUR"
    quote_ccys = ["USD", "GBP", "JPY", "CNY"]
    freq = "D"          # daily
    rate_type = "SP00"  # spot/reference rate
    variation = "A"     # main published series

    url = build_fx_url(
        quote_ccys=quote_ccys,
        base_ccy=base_ccy,
        freq=freq,
        rate_type=rate_type,
        variation=variation,
        start=start_full,
        end=end_full,
    )

    print("Requesting:", url)
    payload = fetch_fx_series(url)

    rows = extract_rows_final(payload)
    # Sort rows just in case (by date then currency)
    rows.sort(key=lambda x: (x[0], x[1]))

    # ---- BRONZE WRITING SECTION ----
    bronze_rows = []
    for dt_str, quote_ccy, base_ccy, value in rows:
        bronze_rows.append({
            "dt": dt_str,
            "dataset": DATASET,
            "series_id": f"EXR.D.{quote_ccy}.{base_ccy}.{rate_type}.{variation}",
            "geo": "EA20",
            "value": float(value),
            "source": SOURCE,
            "ingest_ts": None,  # auto-filled by helper
        })

    # Use the shared writer to output under ./data/bronze/ecb_fx/dt=YYYY-MM-DD/
    # Choose the folder date (latest obs date for this run)
    bronze_files = write_bronze_by_dt(bronze_rows, dataset=DATASET, root=BRONZE_ROOT)

    # Keep legacy batch CSV (unchanged schema) for compatibility
    out_file = "fx_history.csv"
    reference_path = write_csv(rows, out_file)

    print(json.dumps({
        "dataset": DATASET,
        "rows": len(bronze_rows),
        "bronze_files": bronze_files,
        "reference_csv": reference_path,
        "bronze_root": BRONZE_ROOT
    }, indent=2))
