"""
CLI options for ETL of public vehicle sensors (comma2k19).
    python -m etl_comma2k19.cli extract --limit 2000
    python -m etl_comma2k19.cli transform --raw data/raw/comma2k19_raw.jsonl
    python -m etl_comma2k19.cli metrics
    python -m etl_comma2k19.cli all --limit 2000
"""

from __future__ import annotations
import argparse
from pathlib import Path
from typing import Optional
from tqdm import tqdm
import pandas as pd

from .config import init_config, ensure_dirs
from .api import stream_records
from .raw import write_jsonl
from .transform import records_to_modalities, write_silver
from .metrics import kpi_speed_summary, kpi_gnss_quality, kpi_accel_events, write_gold


def cmd_extract(limit: Optional[int] = None, dataset: Optional[str] = None, split: Optional[str] = None):
    ensure_dirs()
    limit = init_config.EXTRACT_LIMIT if limit is None else int(limit)
    dataset = init_config.DATASET_NAME if dataset is None else dataset
    split = init_config.SPLIT if split is None else split

    print(f"[extract] dataset={dataset} split={split} limit={limit}")
    it = stream_records(dataset_name=dataset, split=split, limit=limit)

    # Stream -> RAW JSONL with a progress bar
    path = init_config.RAW_PATH
    count = 0
    with path.open("w", encoding="utf-8") as f:
        for rec in tqdm(it, total=limit or None, unit="rec"):
            f.write((str(rec).replace("'", '"')) + "\n")
            count += 1
    print(f"[extract] wrote {count} records -> {path}")


def cmd_transform(raw_path: Optional[str] = None):
    ensure_dirs()
    raw = Path(raw_path) if raw_path else init_config.RAW_PATH
    print(f"[transform] RAW -> {raw}")
    dfs = records_to_modalities(raw)
    paths = write_silver(dfs, init_config.SILVER_DIR)
    for k, p in paths.items():
        print(f"[transform] {k}: {p}")


def cmd_metrics():
    ensure_dirs()
    # Load SILVER CSVs (if present)
    silver = init_config.SILVER_DIR
    print(f"[metrics] reading SILVER from {silver}")

    def _safe_read(name: str):
        p = silver / f"{name}.csv"
        return pd.read_csv(p) if p.exists() else pd.DataFrame()

    can_df = _safe_read("can")
    gnss_df = _safe_read("gnss")
    imu_df = _safe_read("imu")

    speed = kpi_speed_summary(can_df)
    gnssq = kpi_gnss_quality(gnss_df)
    accel = kpi_accel_events(imu_df)

    out_paths = write_gold(
        {"vw_speed_summary": speed, "vw_gnss_quality": gnssq, "vw_accel_events": accel},
        init_config.GOLD_DIR,
    )
    for k, p in out_paths.items():
        print(f"[metrics] {k}: {p}")


def cmd_all(limit: Optional[int] = None, dataset: Optional[str] = None, split: Optional[str] = None):
    cmd_extract(limit=limit, dataset=dataset, split=split)
    cmd_transform()
    cmd_metrics()


def main():
    parser = argparse.ArgumentParser(description="ETL for public vehicle sensors (comma2k19).")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_ext = sub.add_parser("extract", help="Stream public data to RAW JSONL")
    p_ext.add_argument("--limit", type=int, default=None)
    p_ext.add_argument("--dataset", type=str, default=None)
    p_ext.add_argument("--split", type=str, default=None)

    p_tr = sub.add_parser("transform", help="RAW JSONL -> SILVER CSVs")
    p_tr.add_argument("--raw", type=str, default=None, help="Path to raw JSONL")

    sub.add_parser("metrics", help="Compute GOLD KPIs")

    p_all = sub.add_parser("all", help="Run extract -> transform -> metrics")
    p_all.add_argument("--limit", type=int, default=None)
    p_all.add_argument("--dataset", type=str, default=None)
    p_all.add_argument("--split", type=str, default=None)

    args = parser.parse_args()
    if args.cmd == "extract":
        cmd_extract(limit=args.limit, dataset=args.dataset, split=args.split)
    elif args.cmd == "transform":
        cmd_transform(raw_path=args.raw)
    elif args.cmd == "metrics":
        cmd_metrics()
    elif args.cmd == "all":
        cmd_all(limit=args.limit, dataset=args.dataset, split=args.split)


if __name__ == "__main__":
    main()
