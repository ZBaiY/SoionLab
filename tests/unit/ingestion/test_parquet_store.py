from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

from quant_engine.utils.paths import data_root_from_file
from quant_engine.utils.parquet_store import write_raw_snapshot_append_only
from ingestion.option_chain.source import OptionChainFileSource


def test_append_only_snapshots_and_replay_order() -> None:
    data_root = data_root_from_file(__file__, levels_up=3)
    base_root = data_root / "_test_raw_snapshots"
    asset = "TEST"
    interval = "1m"
    domain = "option_chain"

    if base_root.exists():
        shutil.rmtree(base_root)

    try:
        ts_list = [1700000000000, 1700000001000, 1700000002000]
        for ts in ts_list:
            df = pd.DataFrame([{"data_ts": ts, "value": ts}])
            write_raw_snapshot_append_only(
                root=base_root,
                domain=domain,
                asset=asset,
                interval=interval,
                data_ts=ts,
                df=df,
                sort_cols=["data_ts"],
                stage="raw",
            )

        files = list((base_root / "raw" / domain / asset / interval).rglob("*.parquet"))
        assert len(files) == 3

        # idempotent re-run
        for ts in ts_list:
            df = pd.DataFrame([{"data_ts": ts, "value": ts}])
            write_raw_snapshot_append_only(
                root=base_root,
                domain=domain,
                asset=asset,
                interval=interval,
                data_ts=ts,
                df=df,
                sort_cols=["data_ts"],
                stage="raw",
            )

        files = list((base_root / "raw" / domain / asset / interval).rglob("*.parquet"))
        assert len(files) == 3

        source = OptionChainFileSource(root=base_root / "raw" / domain, asset=asset, interval=interval)
        got = [int(snap["data_ts"]) for snap in source]
        assert got == ts_list
    finally:
        if base_root.exists():
            shutil.rmtree(base_root)
