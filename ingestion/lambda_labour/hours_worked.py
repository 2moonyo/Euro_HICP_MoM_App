#!/usr/bin/env python3
"""
Fetch Eurostat employment / hours worked (namq_10_a10_e) via SDMX 3.0
and write Bronze-layer partitions for the Euro CPI Nowcast pipeline.

POC scope:
- Geo: EA20 (euro area 20 countries)
- Frequency: quarterly

ASSUMPTION:
- We use Eurostat SDMX 3.0 "dataflow" endpoint:
    https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/namq_10_a10_e/1.0
- Dataset namq_10_a10_e already contains employment metrics with:
    - geo (EA, countries)
    - na_item (EMP, EMP_DC, etc)
    - unit (e.g. THS_HW, THS_PER, I15, I10)
    - s_adj (working-day/seasonal adjustment flags)
    - nace_r2 (industry breakdowns)

We deliberately DO NOT filter by na_item / unit / s_adj / NACE at the API level.
We only constrain:
    - geo = EA20
    - TIME_PERIOD >= START_YEAR
All other dimensions remain in the first column key and are encoded in series_id.
Silver/dbt can later filter to:
    - desired na_item (e.g. EMP_DC)
    - unit (e.g. THS_HW or employment index)
    - specific NACE groupings
    - desired seasonal adjustment

Output Bronze schema (compatible with write_bronze_by_dt):
    dt         : 'YYYY-MM-DD' (quarter represented as first month of quarter)
    dataset    : 'labour_hours_worked'
    series_id  : identifier for this time series (encodes Eurostat dims)
    geo        : geo code (EA20)
    value      : employment / hours worked value (depends on unit)
    source     : 'eurostat_sdmx3_namq_10_a10_e'
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

# Logical dataset name used in your pipeline
DATASET = "labour_hours_worked"

# Eurostat SDMX 3.0 endpoint base
EUROSTAT_SDMX3_BASE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0"
# National accounts employment by main industry (quarterly)
NAMQ_PATH = "data/dataflow/ESTAT/namq_10_a10_e/1.0"

# POC: headline euro area
GEO_CODE = "EA20"

# Practical lower bound for quarterly NA (you can tighten later if needed)
START_YEAR = 2000


# ------------------------
# Helpers
# ------------------------

def _build_time_filter(start_year: int) -> str:
    """
    Build SDMX 3.0 TIME_PERIOD constraint for quarterly data.

    For quarterly series, 'ge:YYYY' means "all quarters from YYYY onwards".
    """
    return f"ge:{start_year}"


def fetch_hours_worked_tsv(start_year: int = START_YEAR) -> str:
    """
    Call Eurostat SDMX 3.0 API and return the raw TSV payload
    for employment / hours worked (namq_10_a10_e).

    We constrain:
        - geo = EA20
        - TIME_PERIOD >= start_year

    All other dimensions (na_item, unit, s_adj, nace_r2) are left
    unconstrained and appear in the first TSV column.
    """
    url = f"{EUROSTAT_SDMX3_BASE}/{NAMQ_PATH}"

    time_filter = _build_time_filter(start_year)

    params = {
        "format": "TSV",
        "compress": "false",
        "c[geo]": GEO_CODE,
        "c[TIME_PERIOD]": time_filter,
        "attributes": "none",
        "measures": "all",
    }

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
    Yield non-comment, non-empty lines for csv.reader.

    Eurostat TSV often includes leading comment lines starting with '#'.
    """
    for line in tsv_text.splitlines():
        if not line.strip():
            continue
        if line.startswith("#"):
            continue
        yield line


def parse_hours_worked_tsv_to_bronze(
    tsv_text: str,
    dataset: str = DATASET,
    source: str = "eurostat_sdmx3_namq_10_a10_e",
) -> List[Dict[str, Any]]:
    """
    Parse Eurostat national accounts employment TSV into Bronze rows.

    Expected TSV structure is similar to other Eurostat time series:

        Header (example):
          freq,na_item,unit,s_adj,nace_r2,geo\\TIME_PERIOD\t2000-Q1\t2000-Q2\t...

        Row (example):
          Q,EMP_DC,THS_HW,SCA,TOTAL,EA20\t12345.6\t12378.9\t...

    We do NOT hard-code the dimension list. Instead we:
      - split the first column key by comma
      - treat the last part as geo
      - build series_id from the full key

    We convert the quarterly TIME_PERIOD label (YYYY-Qx) to
    the first month of the quarter:
        Q1 -> YYYY-01-01
        Q2 -> YYYY-04-01
        Q3 -> YYYY-07-01
        Q4 -> YYYY-10-01
    """

    reader = csv.reader(_iter_tsv_rows(tsv_text), delimiter="\t")
    try:
        header = next(reader)
    except StopIteration:
        return []

    # First column is the composite key; remaining are time periods
    time_labels = [h.strip() for h in header[1:]]

    ingest_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows: List[Dict[str, Any]] = []

    for row in reader:
        if not row:
            continue

        key = row[0].strip()
        if not key:
            continue

        parts = [p.strip() for p in key.split(",")]
        if len(parts) < 2:
            # Unexpected key format, skip defensively
            continue

        # Last part is assumed to be geo
        geo = parts[-1]

        # Example series_id:
        #   namq_10_a10_e.Q.EMP_DC.THS_HW.SCA.TOTAL.EA20
        series_id = "namq_10_a10_e." + ".".join(parts)

        values = row[1:]

        for time_label, value_str in zip(time_labels, values):
            value_str = value_str.strip()
            if value_str in ("", ".", ":"):
                # Missing or confidential data
                continue

            # Convert YYYY-Qx to YYYY-MM-01
            tl = time_label.strip()
            if len(tl) == 6 and "-Q" in tl:
                year, q = tl.split("-Q")
                month = {"1": "01", "2": "04", "3": "07", "4": "10"}.get(q, "01")
                dt_str = f"{year}-{month}-01"
            else:
                # Fallback if Eurostat ever changes label format
                dt_str = tl

            try:
                value = float(value_str)
            except ValueError:
                # Non numeric value, skip
                continue

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
    Optional "reference" CSV for manual inspection and debugging.

    This is not used directly by Silver or Gold models, it is just
    a convenient history file similar to your other ingestion scripts.
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

    This mirrors the LCI / JVS script pattern for easy re-runs and backfills.
    """
    if not rows:
        return ""

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{dataset}_batch_{ts}.csv"
    path = Path(bronze_root) / "batch" / filename

    fieldnames = list(rows[0].keys())
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return str(path)


def main() -> None:
    load_dotenv()

    bronze_root = os.getenv("BRONZE_ROOT", "data/bronze")
    bronze_root = str(Path(bronze_root))

    # 1) Fetch raw TSV from Eurostat
    tsv_text = fetch_hours_worked_tsv(start_year=START_YEAR)

    # 2) Parse into Bronze rows
    bronze_rows = parse_hours_worked_tsv_to_bronze(tsv_text)

    # 3) Write Bronze partitions by dt using your shared helper
    bronze_files = write_bronze_by_dt(
        bronze_rows,
        dataset=DATASET,
        root=bronze_root,
    )

    # 4) Optional reference CSV
    reference_csv = write_reference_csv(
        bronze_rows,
        path=Path(bronze_root)
        / DATASET
        / "batch"
        / "hours_worked_history.csv",
    )

    # 5) Optional single batch CSV for the whole run
    batch_csv = write_batch_csv(bronze_rows, DATASET, bronze_root)

    summary = {
        "dataset": DATASET,
        "rows": len(bronze_rows),
        "bronze_files": bronze_files,
        "reference_csv": str(reference_csv),
        "batch_csv": str(batch_csv),
        "bronze_root": bronze_root,
        "geo": GEO_CODE,
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
