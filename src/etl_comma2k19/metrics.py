"""
Compute KPIs and write processed CSVs.
"""

from __future__ import annotations
from typing import Dict
from pathlib import Path
import pandas as pd
import numpy as np


def kpi_speed_summary(can_df: pd.DataFrame) -> pd.DataFrame:
    """Average & max speed per day from CAN (vehicle_speed_kph)."""
    if can_df is None or can_df.empty or "vehicle_speed_kph" not in can_df:
        return pd.DataFrame(columns=["ts_date","avg_speed_kph","max_speed_kph"])

    df = can_df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)
    df["ts_date"] = df["time"].dt.date
    out = (df.groupby("ts_date", as_index=False)
             .agg(avg_speed_kph=("vehicle_speed_kph","mean"),
                  max_speed_kph=("vehicle_speed_kph","max")))
    return out


def kpi_gnss_quality(gnss_df: pd.DataFrame) -> pd.DataFrame:
    """Average satellites and % 3D fix per day from GNSS."""
    if gnss_df is None or gnss_df.empty or "num_sats" not in gnss_df:
        return pd.DataFrame(columns=["ts_date","avg_sats","pct_3d_fix"])

    df = gnss_df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)
    df["ts_date"] = df["time"].dt.date
    if "fix" in df.columns:
        df["is_3d"] = (pd.to_numeric(df["fix"], errors="coerce") >= 3).astype(float)
        out = (df.groupby("ts_date", as_index=False)
                 .agg(avg_sats=("num_sats","mean"),
                      pct_3d_fix=("is_3d","mean")))
    else:
        out = (df.groupby("ts_date", as_index=False)
                 .agg(avg_sats=("num_sats","mean")))
        out["pct_3d_fix"] = np.nan
    return out


def kpi_accel_events(imu_df: pd.DataFrame, threshold: float = 1.5) -> pd.DataFrame:
    """Count hard accel/brake events per day from longitudinal acceleration (ax)."""
    if imu_df is None or imu_df.empty or "ax" not in imu_df:
        return pd.DataFrame(columns=["ts_date","hard_accel_events","hard_brake_events"])

    df = imu_df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)
    df["ts_date"] = df["time"].dt.date
    out = (df.groupby("ts_date", as_index=False)
             .agg(
                 hard_accel_events=("ax", lambda s: (pd.to_numeric(s, errors="coerce") >  threshold).sum()),
                 hard_brake_events=("ax", lambda s: (pd.to_numeric(s, errors="coerce") < -threshold).sum()),
             ))
    return out


def write_gold(kpis: Dict[str, pd.DataFrame], gold_dir: Path) -> Dict[str, Path]:
    gold_dir.mkdir(parents=True, exist_ok=True)
    paths: Dict[str, Path] = {}
    for name, df in kpis.items():
        out = gold_dir / f"{name}.csv"
        df.to_csv(out, index=False)
        paths[name] = out
    return paths
