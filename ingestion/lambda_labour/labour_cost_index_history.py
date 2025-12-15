#!/usr/bin/env python3
"""
Fetch Eurostat labour cost index (ei_lmlc_q) via SDMX 3.0
and write Bronze-layer partitions for the Euro CPI Nowcast pipeline.

POC scope:
- Geo: EA20 (euro area 20 countries)
- Frequency: quarterly

We deliberately DO NOT filter by NACE / unit / seasonal adjustment at the API
level, because the SDMX 3.0 dataflow has a slightly different dimensional
structure. Instead, we ingest all series for EA20 and filter in Silver/dbt.

Output Bronze schema (bronze_io.BRONZE_COLUMNS):
    dt         : 'YYYY-MM-DD' (quarter represented as first month of quarter)
    dataset    : 'labour_cost_index'
    series_id  : identifier for this time series (encodes Eurostat dims)
    geo        : geo code (EA20)
    value      : labour cost index (index, 2020=100 or similar)
    source     : 'eurostat_sdmx3_ei_lmlc_q'
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

DATASET = "labour_cost_index"

EUROSTAT_SDMX3_BASE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0"
LCI_PATH = "data/dataflow/ESTAT/ei_lmlc_q/1.0"

GEO_CODE = "EA20"
START_YEAR = 2000  # earliest year to request


# ------------------------
# Helpers
# ------------------------

def _build_time_filter(start_year: int) -> str:
    """
    Build SDMX 3.0 TIME_PERIOD constraint for quarterly data.

    For quarterly series, 'ge:2000' means "all periods from 2000 onwards".
    """
    return f"ge:{start_year}"


def fetch_lci_tsv(start_year: int = START_YEAR) -> str:
    """
    Call Eurostat SDMX 3.0 API and return the raw TSV payload for LCI.

    NOTE: We only filter on:
      - geo = EA20
      - TIME_PERIOD >= 2000

    All other dimensions (NACE, cost component, unit, s_adj, etc.)
    are left unconstrained and will be visible in the first column of the TSV.
    """
    url = f"{EUROSTAT_SDMX3_BASE}/{LCI_PATH}"

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
    Yield non-comment, non-empty lines for csv reader.
    """
    for line in tsv_text.splitlines():
        if not line.strip():
            continue
        if line.startswith("#"):
            continue
        yield line


def parse_lci_tsv_to_bronze(
    tsv_text: str,
    dataset: str = DATASET,
    source: str = "eurostat_sdmx3_ei_lmlc_q",
) -> List[Dict[str, Any]]:
    """
    Parse Eurostat LCI time series TSV into Bronze rows.

    Expected structure (similar to other Eurostat time series TSVs):

        Header:
          freq,s_adj,nace_r2,cost_component,unit,geo\\TIME_PERIOD\t2000-Q1\t...

        Rows:
          Q,SA,A-S,TOT,I10,EA20\t95.0\t...

    We do NOT hard-code the list of dimensions; we:
      - split the first column by comma => dim parts
      - assume the last part is geo
      - build series_id from the whole key
    """
    reader = csv.reader(_iter_tsv_rows(tsv_text), delimiter="\t")
    try:
        header = next(reader)
    except StopIteration:
        return []

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
            # Unexpected, skip defensively
            continue

        geo = parts[-1]
        other_dims = parts[:-1]
        # Example series_id: ei_lmlc_q.Q.SA.A-S.TOT.I10.EA20
        series_id = f"ei_lmlc_q." + ".".join(parts)

        values = row[1:]
        for time_label, value_str in zip(time_labels, values):
            value_str = value_str.strip()
            if value_str in ("", ".", ":"):
                continue

            # Convert YYYY-Qx to YYYY-MM-01 (Q1=01, Q2=04, Q3=07, Q4=10)
            if len(time_label) == 6 and "-Q" in time_label:
                year, q = time_label.split("-Q")
                month = {"1": "01", "2": "04", "3": "07", "4": "10"}.get(q, "01")
                dt_str = f"{year}-{month}-01"
            else:
                dt_str = time_label

            try:
                value = float(value_str)
            except ValueError:
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
    tsv_text = fetch_lci_tsv(start_year=START_YEAR)

    # 2) Parse into Bronze rows
    bronze_rows = parse_lci_tsv_to_bronze(tsv_text)

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
        / "labour_cost_index"
        / "batch"
        / "labour_cost_index_history.csv",
    )

    # 5) Optional single batch CSV
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
