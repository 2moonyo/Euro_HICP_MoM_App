"""Write Bronze-layer CSV partitions in a consistent format.

All ingestion scripts use these helpers to produce S3-style folders
(dt=YYYY-MM-DD) and enforce a standard schema so Silver/Gold SQL
remains simple.
"""

import os
import csv
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Iterable, Dict, Any, List


# Required Bronze columns, in order. All rows are coerced to this shape before
# writing so DuckDB/Glue readers see a consistent schema.
BRONZE_COLUMNS = [
    "dt", "dataset", "series_id", "geo", "value", "source", "ingest_ts"
]

def ensure_bronze_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Fill missing columns and normalise dates for the Bronze schema.

    Args:
        row: Dict from ingestion script (may have missing keys).

    Returns:
        Dict with all Bronze columns present (dt, dataset, series_id, geo,
        value, source, ingest_ts). Dates normalised to YYYY-MM-DD.
    """
    out = {}
    for k in BRONZE_COLUMNS:
        out[k] = row.get(k, None)
    # coerce dt to YYYY-MM-DD
    if isinstance(out["dt"], (datetime, date)):
        out["dt"] = out["dt"].strftime("%Y-%m-%d")
    # ingest_ts default now (UTC ISO)
    if not out["ingest_ts"]:
        out["ingest_ts"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    # defaults
    if not out["geo"]:
        out["geo"] = "NA"
    return out

def bronze_path(root: str, dataset: str, dt_str: str) -> Path:
    """Return S3 partitioned style folder path.

    Example: data/bronze/ecb_fx/dt=2024-10-31
    """
    return Path(root) / dataset / f"dt={dt_str}"

def write_bronze_csv(rows: Iterable[Dict[str, Any]], *, dataset: str, dt_str: str, root: str) -> str:
    """Write rows to a single date partition as CSV.

    Args:
        rows: Records to write (each passed through ensure_bronze_row).
        dataset: Dataset name (e.g., "ecb_fx").
        dt_str: Partition date (YYYY-MM-DD).
        root: Bronze root folder.

    Returns:
        File path of written CSV.
    """
    target_dir = bronze_path(root, dataset, dt_str)
    target_dir.mkdir(parents=True, exist_ok=True)
    part_name = f"part-{datetime.now(timezone.utc).strftime('%H%M%S%f')}.csv"
    out_path = target_dir / part_name

    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=BRONZE_COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow(ensure_bronze_row(r))

    return str(out_path)

def write_bronze_by_dt(rows: List[Dict[str, Any]], *, dataset: str, root: str) -> List[str]:
    """Group rows by date and write one CSV per partition.

    Args:
        rows: List of records (each must have 'dt' key).
        dataset: Dataset name.
        root: Bronze root folder.

    Returns:
        List of file paths created.
    """
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        dt_str = r["dt"]
        buckets.setdefault(dt_str, []).append(r)

    written: List[str] = []
    for dt_str, group in buckets.items():
        written.append(write_bronze_csv(group, dataset=dataset, dt_str=dt_str, root=root))
    return written
