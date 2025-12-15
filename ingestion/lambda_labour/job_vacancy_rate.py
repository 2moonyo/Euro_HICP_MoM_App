#!/usr/bin/env python3
"""
Fetch Eurostat job vacancy rate (jvs_q_nace2) via SDMX 3.0
and write Bronze-layer partitions for the Euro CPI Nowcast pipeline.

POC scope:
- Geo: EA20 (euro area 20 countries)
- Frequency: quarterly

ASSUMPTION:
- We use Eurostat SDMX 3.0 "dataflow" endpoint:
    https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/jvs_q_nace2/1.0
- Dataset jvs_q_nace2 already contains job vacancy rates with:
    - geo (EA, countries)
    - nace_r2
    - unit including PC (% of total posts)
    - seasonal adjustment and other flags

We deliberately DO NOT filter by NACE / unit / s_adj at the API level.
We only constrain:
    - geo = EA20
    - TIME_PERIOD >= 2001
All other dimensions remain in the first column key and are encoded in series_id.
Silver/dbt can later filter to:
    - unit = PC
    - specific NACE groupings
    - desired seasonal adjustment

Output Bronze schema (bronze_io.BRONZE_COLUMNS):
    dt         : 'YYYY-MM-DD' (quarter represented as first month of quarter)
    dataset    : 'labour_job_vacancy_rate'
    series_id  : identifier for this time series (encodes Eurostat dims)
    geo        : geo code (EA20)
    value      : job vacancy rate (percent of total posts)
    source     : 'eurostat_sdmx3_jvs_q_nace2'
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
DATASET = "labour_job_vacancy_rate"

# Eurostat SDMX 3.0 endpoint
EUROSTAT_SDMX3_BASE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0"
JVS_PATH = "data/dataflow/ESTAT/jvs_q_nace2/1.0"

# POC: headline euro area
GEO_CODE = "EA20"

# Official Eurostat note: quarterly JVS starts in 2001
START_YEAR = 2000


# ------------------------
# Helpers
# ------------------------

def _build_time_filter(start_year: int) -> str:
    """
    Build SDMX 3.0 TIME_PERIOD constraint for quarterly data.

    For quarterly series, 'ge:2001' means "all quarters from 2001 onwards".
    """
    return f"ge:{start_year}"


def fetch_jvr_tsv(start_year: int = START_YEAR) -> str:
    """
    Call Eurostat SDMX 3.0 API and return the raw TSV payload
    for job vacancy statistics (jvs_q_nace2).

    We constrain:
        - geo = EA20
        - TIME_PERIOD >= start_year

    All other dimensions (NACE, size class, unit, s_adj, etc)
    are left unconstrained and appear in the first TSV column.
    """
    url = f"{EUROSTAT_SDMX3_BASE}/{JVS_PATH}"

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


def parse_jvr_tsv_to_bronze(
    tsv_text: str,
    dataset: str = DATASET,
    source: str = "eurostat_sdmx3_jvs_q_nace2",
) -> List[Dict[str, Any]]:
    """
    Parse Eurostat JVS time series TSV into Bronze rows.

    Expected TSV structure is similar to other Eurostat time series:

        Header (example):
          freq,s_adj,unit,nace_r2,sizeclas,geo\\TIME_PERIOD\t2001-Q1\t2001-Q2\t...

        Row (example):
          Q,SA,PC,B-S,X,EA20\t2.0\t2.1\t...

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
        #   jvs_q_nace2.Q.SA.PC.B-S.X.EA20
        series_id = "jvs_q_nace2." + ".".join(parts)

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

    This mirrors the LCI script pattern for easy re runs and backfills.
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
    tsv_text = fetch_jvr_tsv(start_year=START_YEAR)

    # 2) Parse into Bronze rows
    bronze_rows = parse_jvr_tsv_to_bronze(tsv_text)

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
        / "job_vacancy_rate_history.csv",
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
