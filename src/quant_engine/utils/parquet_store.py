from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import os
import pandas as pd

from quant_engine.utils.paths import data_root_from_file, resolve_under_root

DATA_ROOT = data_root_from_file(__file__, levels_up=3)


def _snapshot_path(
    *,
    root: Path,
    stage: str | None,
    domain: str | None,
    asset: str,
    interval: str,
    data_ts: int,
) -> Path:
    dt = datetime.fromtimestamp(int(data_ts) / 1000.0, tz=timezone.utc)
    year = dt.strftime("%Y")
    ymd = dt.strftime("%Y_%m_%d")
    base = resolve_under_root(DATA_ROOT, root, strip_prefix="data")

    if stage and stage not in base.parts:
        base = base / stage
    if domain and domain not in base.parts:
        base = base / domain

    return base / asset / interval / year / ymd / f"{int(data_ts)}.parquet"


def write_raw_snapshot_append_only(
    *,
    root: Path,
    domain: str | None,
    asset: str,
    interval: str,
    data_ts: int,
    df: pd.DataFrame,
    sort_cols: list[str] | None = None,
    stage: str | None = "raw",
) -> Path:
    """Append-only parquet writer using one file per snapshot.

    Path layout (default stage/domain):
        data/<stage>/<domain>/<asset>/<interval>/<YYYY>/<YYYY_MM_DD>/<data_ts>.parquet
    """
    path = _snapshot_path(
        root=root,
        stage=stage,
        domain=domain,
        asset=str(asset),
        interval=str(interval),
        data_ts=int(data_ts),
    )

    if path.exists():
        return path

    path.parent.mkdir(parents=True, exist_ok=True)

    if df is None:
        df = pd.DataFrame()

    out = df.copy()
    if "data_ts" not in out.columns:
        out["data_ts"] = int(data_ts)
    if sort_cols:
        cols = [c for c in sort_cols if c in out.columns]
        if cols:
            out = out.sort_values(cols, kind="stable")

    tmp = path.with_suffix(path.suffix + ".tmp")
    out.to_parquet(tmp, index=False)
    os.replace(tmp, path)
    return path
