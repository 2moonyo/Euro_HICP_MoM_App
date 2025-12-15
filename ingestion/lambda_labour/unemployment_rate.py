#!/usr/bin/env python3
"""
Fetch Eurostat monthly unemployment (une_rt_m) via SDMX 3.0
and write Bronze-layer partitions for the Euro CPI Nowcast pipeline.

POC scope:
- Geo: EA20 (euro area 20 countries)
- Sex: T (Total)
- Age: Y15-74
- Seasonal adjustment: SA
- Unit: PC_ACT (percent of active population)
- Frequency: monthly

Output Bronze schema (required by bronze_io.BRONZE_COLUMNS):
    dt         : 'YYYY-MM-DD' (month-end represented as first of month)
    dataset    : logical dataset name, here 'labour_unemployment'
    series_id  : identifier for this time series (includes Eurostat dims)
    geo        : geo code (EA20)
    value      : unemployment rate (% of active population)
    source     : 'eurostat_sdmx3_une_rt_m'
    ingest_ts  : ISO 8601 UTC timestamp of ingestion
"""

import os
import csv
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Iterable
from pathlib import Path

import requests
from dotenv import load_dotenv

from ingestion.common.bronze_io import write_bronze_by_dt


# ------------------------
# Constants / configuration
# ------------------------

# Logical name you will refer to in Silver/Gold
DATASET = "labour_unemployment"

# Eurostat SDMX 3.0 endpoint for unemployment by sex & age - monthly
# See: API - Detailed guidelines - SDMX3.0 API - data query :contentReference[oaicite:2]{index=2}
EUROSTAT_SDMX3_BASE = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0"
)
UNE_RT_M_PATH = "data/dataflow/ESTAT/une_rt_m/1.0"

# ASSUMPTION: POC fixed dimensions for "headline" Euro area unemployment
GEO_CODE = "EA20"      # euro area 20 (matches HICP geo in your model)
SEX_CODE = "T"         # Total
# AGE_CODE = "TOTAL"    # 15–64 years
S_ADJ_CODE = "SA"      # Seasonally adjusted
UNIT_CODE = "PC_ACT"   # Percent of active population

START_YEAR = 2000     # earliest year to request


# ------------------------
# Helpers
# ------------------------

def _build_time_filter(start_year: int) -> str:
    """
    Build SDMX 3.0 TIME_PERIOD constraint for monthly data.

    For Eurostat SDMX 3.0 the TIME_PERIOD format for monthly data is YYYY-MM,
    not YYYY-MM-DD.

    Example output for start_year=2000:
        'ge:2000-01'

    This means "all months from Jan 2000 onwards".
    """
    # If you really want an explicit upper bound, you can do:
    # now = datetime.now(timezone.utc)
    # end_year = now.year
    # end_month = now.month
    # return f"ge:{start_year}-01+le:{end_year}-{end_month:02d}"

    return f"ge:{start_year}-01"



def fetch_unemployment_tsv(start_year: int = START_YEAR) -> str:
    """
    Call Eurostat SDMX 3.0 API and return the raw TSV payload as a string.

    Uses:
      - context=dataflow
      - resourceID=une_rt_m
      - filters via c[...] parameters for geo, age, sex, s_adj, unit, time
    """
    url = f"{EUROSTAT_SDMX3_BASE}/{UNE_RT_M_PATH}"

    time_filter = _build_time_filter(start_year)

    params = {
        "format": "TSV",
        "compress": "false",
        "c[geo]": GEO_CODE,
        "c[sex]": SEX_CODE,
        # age left unconstrained to avoid empty TSV
        "c[s_adj]": S_ADJ_CODE,
        "c[unit]": UNIT_CODE,
        "c[TIME_PERIOD]": time_filter,
        "attributes": "none",
        "measures": "all",
    }

    # Tell Eurostat we want TSV, not XML
    headers = {
        "Accept": "text/tab-separated-values, */*;q=0.1"
    }

    resp = requests.get(url, params=params, headers=headers, timeout=60)
    if resp.status_code != 200:
        print("Eurostat error payload:\n", resp.text[:1000])
    else:
        print("Eurostat TSV preview:\n", resp.text.splitlines()[:3])
    resp.raise_for_status()
    return resp.text


def _iter_tsv_rows(tsv_text: str) -> Iterable[str]:
    """
    Yield non-comment, non-empty lines for csv.DictReader.

    Eurostat TSV often includes leading comment lines starting with '#'.
    """
    for line in tsv_text.splitlines():
        if not line.strip():
            continue
        if line.startswith("#"):
            continue
        yield line


def parse_unemployment_tsv_to_bronze(
    tsv_text: str,
    dataset: str = DATASET,
    source: str = "eurostat_sdmx3_une_rt_m",
) -> List[Dict[str, Any]]:
    """
    Parse Eurostat *time series TSV* into Bronze rows.

    The TSV structure is:

        Header:
            freq,s_adj,age,unit,sex,geo\\TIME_PERIOD\t2000-01\t2000-02\t...

        Rows:
            M,SA,TOTAL,PC_ACT,T,EA20\t9.4\t9.4\t...

    So:
      - column 0 is the key "freq,s_adj,age,unit,sex,geo"
      - columns 1..N are monthly time periods
      - each cell (row, col) is an observation

    We normalise this into the standard Bronze schema:
        dt, dataset, series_id, geo, value, source, ingest_ts
    """

    # Use plain reader because this is not SDMX-CSV "long" form
    reader = csv.reader(_iter_tsv_rows(tsv_text), delimiter="\t")

    try:
        header = next(reader)
    except StopIteration:
        # Empty TSV
        return []

    # First column is the key; the rest are time period labels
    time_labels = [h.strip() for h in header[1:]]

    ingest_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows: List[Dict[str, Any]] = []

    for row in reader:
        if not row:
            continue

        # First field: freq,s_adj,age,unit,sex,geo
        key = row[0].strip()
        if not key:
            continue

        try:
            freq, s_adj, age, unit, sex, geo = [part.strip() for part in key.split(",")]
        except ValueError:
            # Unexpected key format; skip defensively
            continue

        # Remaining fields are values per time period
        values = row[1:]

        for time_label, value_str in zip(time_labels, values):
            value_str = value_str.strip()
            if value_str in ("", ".", ":"):
                # Missing / confidential / not available
                continue

            # TIME_PERIOD is YYYY-MM, convert to YYYY-MM-01
            # (matches your monthly dt convention elsewhere)
            time_label = time_label.strip()
            if len(time_label) == 7:
                dt_str = f"{time_label}-01"
            else:
                dt_str = time_label  # fallback, do not crash

            try:
                value = float(value_str)
            except ValueError:
                # Non-numeric junk, skip
                continue

            # Deterministic series_id encoding the Eurostat dimensions
            series_id = f"une_rt_m.{geo}.{age}.{sex}.{s_adj}.{unit}"

            rows.append(
                {
                    "dt": dt_str,
                    "dataset": dataset,
                    "series_id": series_id,
                    "geo": geo,
                    "value": value,
                    "source": source,
                    "ingest_ts": ingest_ts,
                }
            )

    return rows


def write_reference_csv(rows: List[Dict[str, Any]], path: Path) -> str:
    """
    SHORTCUT: simple batch CSV export for manual inspection / debugging.

    This does NOT have to be used by Silver/Gold; it is purely a convenience
    (mirrors your fx_history 'reference CSV' idea).
    """
    if not rows:
        return str(path)

    fieldnames = list(rows[0].keys())
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return str(path)

def write_batch_csv(rows: List[Dict[str, Any]], dataset: str, bronze_root: str) -> str:
    """
    Write a single batch CSV containing ALL Bronze rows for this run.
    Output filename example:
        batch/labour_unemployment_batch_20241121_153000.csv
    """
    if not rows:
        return ""

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{dataset}_batch_{ts}.csv"
    path = Path(bronze_root) / "batch"  / filename  

    fieldnames = list(rows[0].keys())
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return path

def main() -> None:
    """
    Entry point: fetch unemployment history and write Bronze partitions.
    """
    load_dotenv()

    bronze_root = os.getenv("BRONZE_ROOT", "data/bronze")
    bronze_root = str(Path(bronze_root))

    # 1) Fetch raw TSV from Eurostat
    tsv_text = fetch_unemployment_tsv(start_year=START_YEAR)

    # 2) Parse into Bronze rows
    bronze_rows = parse_unemployment_tsv_to_bronze(tsv_text)

    # 3) Write Bronze partitions by dt using your shared helper
    bronze_files = write_bronze_by_dt(
        bronze_rows,
        dataset=DATASET,
        root=bronze_root,
    )

    # 4) Optional reference CSV for manual inspection
    reference_csv = write_reference_csv(
        bronze_rows,
        path=Path(bronze_root) / "labour_unemployment" / "batch" /"unemployment_history.csv",
    )

    #batch_csv = write_batch_csv(bronze_rows, DATASET,bronze_root)
    # 5) Emit a compact JSON summary (same pattern as fx_history/ecb_rates_history)
    summary = {
        "dataset": DATASET,
        "rows": len(bronze_rows),
        "bronze_files": bronze_files,
        "reference_csv": reference_csv,
        #"batch_csv": batch_csv,
        "bronze_root": bronze_root,
        "geo": GEO_CODE,
        #"age": AGE_CODE,
        "sex": SEX_CODE,
        "s_adj": S_ADJ_CODE,
        "unit": UNIT_CODE,
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
