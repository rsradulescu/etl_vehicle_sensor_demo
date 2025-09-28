"""
Transform RAW JSONL into tidy DataFrames per modality (gnss, imu, can, pose),
then write in processed CSVs.
"""

from __future__ import annotations
from typing import Dict, Any, Iterable
from pathlib import Path
import pandas as pd
import numpy as np
from .raw import read_jsonl


def _coalesce(d: Dict[str, Any], *names: str, default=None):
    for n in names:
        if n in d and d[n] is not None:
            return d[n]
    return default


def _parse_time_col(df: pd.DataFrame, col: str = "time") -> pd.DataFrame:
    if col in df:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    return df


def records_to_modalities(raw_path: Path) -> dict[str, pd.DataFrame]:
    """
    Load RAW JSONL and split into modality DataFrames.
    """
    gnss_rows, imu_rows, can_rows, pose_rows = [], [], [], []

    for r in read_jsonl(raw_path):
        # Some datasets wrap signals inside top-level keys; this keeps it flexible
        gnss = r.get("gnss") or r.get("GNSS") or {}
        imu  = r.get("imu")  or r.get("IMU")  or {}
        can  = r.get("can")  or r.get("CAN")  or {}
        pose = r.get("pose") or r.get("POSE") or {}

        if gnss:
            gnss_rows.append({
                "time": _coalesce(gnss, "time", "timestamp"),
                "lat":  _coalesce(gnss, "lat", "latitude"),
                "lon":  _coalesce(gnss, "lon", "longitude"),
                "alt":  gnss.get("alt"),
                "speed_mps": _coalesce(gnss, "speed_mps", "speed"),
                "fix":  gnss.get("fix"),
                "num_sats": _coalesce(gnss, "num_sats", "satellites"),
            })

        if imu:
            imu_rows.append({
                "time": _coalesce(imu, "time", "timestamp"),
                "ax": imu.get("ax"), "ay": imu.get("ay"), "az": imu.get("az"),
                "gx": imu.get("gx"), "gy": imu.get("gy"), "gz": imu.get("gz"),
            })

        if can:
            can_rows.append({
                "time": _coalesce(can, "time", "timestamp"),
                "vehicle_speed_kph": _coalesce(can, "vehicle_speed_kph", "speed_kph"),
                "steering_angle_deg": _coalesce(can, "steering_angle_deg", "steer_angle_deg"),
                "throttle_pct": can.get("throttle_pct"),
                "brake_pct": can.get("brake_pct"),
            })

        if pose:
            pose_rows.append({
                "time": _coalesce(pose, "time", "timestamp"),
                "x": pose.get("x"), "y": pose.get("y"), "z": pose.get("z"),
                "roll": pose.get("roll"), "pitch": pose.get("pitch"), "yaw": pose.get("yaw"),
            })

    dfs: dict[str, pd.DataFrame] = {}

    if gnss_rows:
        g = pd.DataFrame(gnss_rows)
        g = _parse_time_col(g)
        if "speed_mps" in g:
            g["speed_kph"] = g["speed_mps"] * 3.6
        dfs["gnss"] = g.dropna(how="all")
    else:
        dfs["gnss"] = pd.DataFrame(columns=["time","lat","lon","alt","speed_mps","fix","num_sats","speed_kph"])

    if imu_rows:
        i = pd.DataFrame(imu_rows)
        i = _parse_time_col(i)
        dfs["imu"] = i.dropna(how="all")
    else:
        dfs["imu"] = pd.DataFrame(columns=["time","ax","ay","az","gx","gy","gz"])

    if can_rows:
        c = pd.DataFrame(can_rows)
        c = _parse_time_col(c)
        dfs["can"] = c.dropna(how="all")
    else:
        dfs["can"] = pd.DataFrame(columns=["time","vehicle_speed_kph","steering_angle_deg","throttle_pct","brake_pct"])

    if pose_rows:
        p = pd.DataFrame(pose_rows)
        p = _parse_time_col(p)
        dfs["pose"] = p.dropna(how="all")
    else:
        dfs["pose"] = pd.DataFrame(columns=["time","x","y","z","roll","pitch","yaw"])

    return dfs


def write_silver(dfs: dict[str, pd.DataFrame], silver_dir: Path) -> dict[str, Path]:
    """
    Write modality DataFrames to CSV files in SILVER.
    Returns a dict of modality -> file path.
    """
    silver_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}

    for name, df in dfs.items():
        out = silver_dir / f"{name}.csv"
        # Ensure ISO 8601 strings for timestamps for portability
        if "time" in df.columns:
            df = df.copy()
            df["time"] = df["time"].astype("datetime64[ns, UTC]").astype(str)
        df.to_csv(out, index=False)
        paths[name] = out

    return paths
