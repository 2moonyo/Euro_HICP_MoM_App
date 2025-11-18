#!/usr/bin/env python3
"""Fetch energy prices from FRED and write Bronze partitions.

Pulls World Bank commodity series (Brent, EU gas) from FRED, handles pagination,
and writes S3-style folders plus consolidated CSV.
"""

import os
import io
import csv
import json
import argparse
import datetime as dt
from datetime import date, datetime, timezone
from typing import TypedDict, List, Dict, Optional, Tuple
import urllib.parse
import urllib.request
from dotenv import load_dotenv  
from itertools import groupby
from ingestion.common.bronze_io import write_bronze_by_dt

from pathlib import Path
from dotenv import load_dotenv

# -------------------- IO CONFIG --------------------
load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parents[2]))
BRONZE_ROOT = os.getenv("BRONZE_ROOT", str(PROJECT_ROOT / "data" / "bronze"))
DATASET = os.getenv("DATASET", "energy_prices")  
SOURCE = os.getenv("SOURCE", "stlouisfed_fred")  

# -------------------- HARD-CODED CONFIG --------------------
START_DATE = "2000-01-01"
END_DATE   = date.today().isoformat()

# -------------------- SERIES CATALOGUE ---------------------
SERIES: List[Dict[str, str]] = [
    {
        "series_id": "POILBREUSDM",
        "name": "Global price of Brent Crude",
        "unit": "USD per Barrel",
        "frequency": "Monthly",
        "source": "FRED/World Bank (Primary Commodity Prices)"
    },
    {
        "series_id": "PNGASEUUSDM",
        "name": "Global price of Natural gas, EU",
        "unit": "USD per MMBtu",
        "frequency": "Monthly",
        "source": "FRED/World Bank (Primary Commodity Prices)"
    }
]

TIDY_FIELDS = ["date", "series_id", "value", "unit", "frequency", "source"]

# -------------------- FRED API HELPERS ---------------------
FRED_API_BASE = "https://api.stlouisfed.org/fred/series/observations"

class EnergyRecord(TypedDict):
    """Single FRED observation."""

    date: str
    series_id: str
    value: Optional[float]

def fred_observations(series_id: str, start_date: str, end_date: str, api_key: str,
                      limit: int = 1000) -> List[EnergyRecord]:
    """Fetch and paginate observations from FRED between given dates."""
    obs: List[EnergyRecord] = []
    offset = 0
    while True:
        params = {
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "observation_start": start_date,
            "observation_end": end_date,
            "limit": str(limit),
            "offset": str(offset),
        }
        url = FRED_API_BASE + "?" + urllib.parse.urlencode(params)
        with urllib.request.urlopen(url, timeout=30) as r:
            payload = json.loads(r.read().decode("utf-8"))

        items = payload.get("observations", [])
        if not items:
            break

        for it in items:
            date = it.get("date", "").strip()
            v = it.get("value", "").strip()
            
            # Parse value
            value: Optional[float] = None
            if v not in ("", ".", "NaN", "nan", None):
                try:
                    value = float(v)
                except ValueError:
                    value = None
            
            # Only append if date exists
            if date:
                obs.append({
                    "date": date,
                    "series_id": series_id,
                    "value": value
                })

        if len(items) < limit:
            break
        offset += limit

    return obs

# -------------------- WRITE LAYER (LOCAL FS) ----------------
def ensure_dir(path: str) -> None:
    """Create directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)

def write_batch_all_series(series_catalog: List[Dict[str, str]],
                           series_records: Dict[str, List[EnergyRecord]]) -> str:
    """Write consolidated CSV of all series for analyst inspection."""
    batch_dir = os.path.join(BRONZE_ROOT, "energy_prices", "Batch")
    ensure_dir(batch_dir)
    fname = os.path.join(batch_dir, "energy_prices_all_series.csv")

    meta_by_id = {m["series_id"]: m for m in series_catalog}

    rows = []
    for sid, recs in series_records.items():
        meta = meta_by_id[sid]
        for r in recs:
            date = r.get("date")
            if not date:
                continue
            rows.append({
                "date": date,
                "series_id": sid,
                "value": "" if r.get("value") is None else r["value"],
                "unit": meta["unit"],
                "frequency": meta["frequency"],
                "source": meta["source"]
            })

    rows.sort(key=lambda x: (x["date"], x["series_id"]))

    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=TIDY_FIELDS)
        w.writeheader()
        w.writerows(rows)

    return fname

def write_bronze_energy(series_catalog: List[Dict[str, str]], 
                        series_records: Dict[str, List[EnergyRecord]]):
    """Convert FRED records to Bronze rows and write per-date files."""
    bronze_rows = []
    meta_by_id = {m["series_id"]: m for m in series_catalog}
    
    for sid, recs in series_records.items():
        meta = meta_by_id[sid]
        for rec in recs:
            date = rec.get("date")
            if not date:
                continue
                
            bronze_rows.append({
                "dt": date,
                "dataset": DATASET,
                "series_id": sid,
                "geo": "NA",  # Energy prices not geo-specific
                "value": float(rec["value"]) if rec["value"] is not None else None,
                "source": SOURCE,
                "ingest_ts": None,
            })
    
    files_written = write_bronze_by_dt(bronze_rows, dataset=DATASET, root=BRONZE_ROOT)
    return files_written, len(bronze_rows)

# -------------------- ORCHESTRATION -------------------------
def main():
    api_key = os.getenv("FRED_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing FRED_API_KEY. Please set it in your .env")

    series_records: Dict[str, List[EnergyRecord]] = {}
    for meta in SERIES:
        sid = meta["series_id"]
        series_records[sid] = fred_observations(sid, START_DATE, END_DATE, api_key=api_key, limit=1000)

    bronze_files, nrows = write_bronze_energy(SERIES, series_records)
    batch_path = write_batch_all_series(SERIES, series_records)
    
    print(json.dumps({
        "status": "ok",
        "run_date_local": dt.datetime.now().isoformat(timespec="seconds"),
        "rows": nrows,
        "bronze_files": bronze_files,
        "bronze_root": BRONZE_ROOT,
        "batch_csv": batch_path,
        "start_date": START_DATE,
        "end_date": END_DATE
    }, indent=2))


if __name__ == "__main__":
    main()