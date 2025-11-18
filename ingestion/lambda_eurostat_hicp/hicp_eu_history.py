"""Fetch Eurostat HICP monthly index and write Bronze partitions.

Builds SDMX CSV URLs dynamically to handle indicator name changes
(e.g., EA19 → EA20). Outputs S3-style folders for Silver ingestion.
"""

import os
import io
import csv
import json
import argparse
from datetime import date, datetime, timezone
from typing import List, Tuple, Dict
from itertools import groupby
from ingestion.common.bronze_io import write_bronze_by_dt
from pathlib import Path
from dotenv import load_dotenv

# -------------------- IO CONFIG --------------------
load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parents[2]))
BRONZE_ROOT = os.getenv("BRONZE_ROOT", str(PROJECT_ROOT / "data" / "bronze"))
DATASET = os.getenv("DATASET", "eurostat_hicp")  
SOURCE = os.getenv("SOURCE", "Eurostat")  

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"
DATAFLOW_ID = "prc_hicp_midx"  # HICP - monthly index

# SDMX series key order for this dataset: FREQ.UNIT.COICOP.GEO
DEFAULT_FREQ = "M"
DEFAULT_UNIT = "I15"     # Index, 2015 = 100
DEFAULT_COICOP = "CP00"  # All-items
DEFAULT_GEO = "EA20"     # Euro area (current composition)



# -------------------- URL BUILDER (CSV only) --------------------
def build_hicp_csv_url(freq: str, unit: str, coicop: str, geo: str, start: str, end: str) -> str:
    """Build SDMX URL for one HICP series with given parameters."""
    series_key = f"{freq}.{unit}.{coicop}.{geo}"
    return (
        f"{BASE_URL}/data/{DATAFLOW_ID}/{series_key}"
        f"?startPeriod={start}&endPeriod={end}&format=SDMX-CSV"
    )

# -------------------- FETCH (CSV only) --------------------------
def fetch_hicp_csv(url: str) -> str:
    """Fetch CSV from Eurostat SDMX endpoint."""
    import requests
    r = requests.get(url, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Eurostat CSV HTTP {r.status_code}: {r.text[:400]}")
    return r.text

# -------------------- EXTRACT (CSV) -----------------------------
def extract_rows_hicp_from_csv(csv_text: str) -> List[Tuple[str, str, str, str, float]]:
    """Parse SDMX CSV into (date, geo, coicop, unit, value) tuples."""
    rows: List[Tuple[str, str, str, str, float]] = []
    reader = csv.DictReader(io.StringIO(csv_text))
    for rec in reader:
        t = rec.get("TIME_PERIOD")
        if not t:
            continue
        # Normalise to YYYY-MM-01 for monthly; handle YYYY and YYYY-MM if ever present
        if len(t) == 7:
            dt_str = f"{t}-01"
        elif len(t) == 4:
            dt_str = f"{t}-01-01"
        else:
            dt_str = t

        val = rec.get("OBS_VALUE")
        if val in ("", None):
            continue
        try:
            value = float(val)
        except ValueError:
            continue

        rows.append((dt_str, rec.get("geo", ""), rec.get("coicop", ""), rec.get("unit", ""), value))
    return rows

# -------------------- WRITE --------------------
def ensure_dir(path: str) -> None:
    """Create directory if missing."""
    os.makedirs(path, exist_ok=True)

def write_csv_batch(rows: List[Tuple[str, str, str, str, float]], load_date: str, stamp: str) -> str:
    """Write consolidated CSV for analyst reference."""
    # Build full path to dataset directory
    dataset_dir = os.path.join(BRONZE_ROOT, DATASET)
    batch_dir = os.path.join(dataset_dir, "Batch")
    ensure_dir(batch_dir)
    out_path = os.path.join(batch_dir, f"eurostat_hicp_{stamp}.csv")

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "geo", "coicop", "unit", "value", "load_ts"])
        for dt_str, geo, coicop, unit, val in rows:
            w.writerow([dt_str, geo, coicop, unit, val, stamp])
    
    return out_path

def write_bronze_hicp(rows):
    """Map tuples to Bronze schema and write partitions."""
    bronze_rows = []
    for dt_str, geo, coicop, unit, value in rows:
        bronze_rows.append({
            "dt": dt_str,
            "dataset": DATASET,
            "series_id": coicop,
            "geo": geo or "EA20",
            "value": float(value) if value is not None else None,
            "source": SOURCE,
            "ingest_ts": None,  # Auto-filled by bronze_io.ensure_bronze_row()
        })
    files_written = write_bronze_by_dt(bronze_rows, dataset=DATASET, root=BRONZE_ROOT)
    return files_written, len(bronze_rows)

# -------------------- MAIN --------------------
def main():
    """CLI entry point for pulling HICP data."""
    ap = argparse.ArgumentParser(description="Eurostat HICP (prc_hicp_midx) → local CSV partitions (CSV-only).")
    ap.add_argument("--geo", default=DEFAULT_GEO, help="GEO code (e.g., EA20, BE, DE). Default EA20.")
    ap.add_argument("--unit", default=DEFAULT_UNIT, help="UNIT code (e.g., I15 for 2015=100). Default I15.")
    ap.add_argument("--coicop", default=DEFAULT_COICOP, help="COICOP code (e.g., CP00). Default CP00.")
    ap.add_argument("--freq", default=DEFAULT_FREQ, help="FREQ code (M monthly). Default M.")
    ap.add_argument("--start", default="2000-01-01", help="Start period (YYYY or YYYY-MM or YYYY-MM-DD).")
    ap.add_argument("--end", default=date.today().isoformat(), help="End period (YYYY or YYYY-MM or YYYY-MM-DD).")
    args = ap.parse_args()

    url = build_hicp_csv_url(args.freq, args.unit, args.coicop, args.geo, args.start, args.end)
    print(f"Requesting (CSV): {url}")

    csv_text = fetch_hicp_csv(url)
    rows = extract_rows_hicp_from_csv(csv_text)
    rows.sort(key=lambda r: (r[0], r[1]))  # sort by date, then geo

    bronze_files, nrows = write_bronze_hicp(rows)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    load_date_iso = f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}"

    batch_path = write_csv_batch(rows, load_date_iso, stamp)
    

    print(json.dumps({
        "dataset": DATASET,
        "rows": nrows,
        "bronze_root": BRONZE_ROOT,
        "bronze_files": bronze_files,
        # Optional:
        "batch_csv": batch_path
    }, indent=2))


if __name__ == "__main__":
    main()
