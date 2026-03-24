from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import load_table, write_table


def run(input_path: Path, output_path: Path, *, horizon_bars: int, target_kind: str) -> pd.DataFrame:
    df = load_table(input_path).copy()
    if "timestamp" in df.columns:
        df = df.sort_values(["symbol", "timestamp"] if "symbol" in df.columns else ["timestamp"], kind="mergesort")
    if "close" not in df.columns:
        raise ValueError("Target construction requires a close column.")

    group_keys = ["symbol"] if "symbol" in df.columns else []
    grouped = df.groupby(group_keys, group_keys=False) if group_keys else [(None, df)]

    frames: list[pd.DataFrame] = []
    for _, group in grouped:
        group = group.copy()
        future_close = group["close"].shift(-horizon_bars)
        if target_kind == "forward_log_return":
            target = np.log(future_close / group["close"])
        elif target_kind == "forward_direction":
            target = np.sign(future_close - group["close"])
        else:
            target = (future_close - group["close"]) / group["close"]
        group[f"target_{target_kind}_{horizon_bars}"] = target
        frames.append(group)

    out = pd.concat(frames, ignore_index=True)
    write_table(out, output_path)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--horizon-bars", type=int, default=1)
    parser.add_argument("--target-kind", default="forward_return")
    args = parser.parse_args()
    run(Path(args.input), Path(args.output), horizon_bars=int(args.horizon_bars), target_kind=args.target_kind)


if __name__ == "__main__":
    main()
