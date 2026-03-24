from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import dump_json, load_json, load_table, parse_features_arg
from evaluation_utils import compute_ic


def slice_by_ts(df: pd.DataFrame, start_ts: int, end_ts: int) -> pd.DataFrame:
    return df[(df["timestamp"] >= start_ts) & (df["timestamp"] <= end_ts)].copy()


def run(input_path: Path, splits_path: Path, output_path: Path, *, features: str | list[str] | None, target_column: str, ic_kind: str) -> dict:
    df = load_table(input_path)
    splits_payload = load_json(splits_path)
    feature_cols = parse_features_arg(features, df)
    results = []
    for idx, split in enumerate(splits_payload["splits"], start=1):
        train_df = slice_by_ts(df, split["train"]["start_ts"], split["train"]["end_ts"])
        test_df = slice_by_ts(df, split["test"]["start_ts"], split["test"]["end_ts"])
        train_ic = compute_ic(train_df, features=feature_cols, target_column=target_column, ic_kind=ic_kind)
        test_ic = compute_ic(test_df, features=feature_cols, target_column=target_column, ic_kind=ic_kind)
        results.append({
            "split_id": idx,
            "train": split["train"],
            "test": split["test"],
            "train_ic": train_ic,
            "test_ic": test_ic,
        })
    payload = {
        "target_column": target_column,
        "features": feature_cols,
        "split_count": len(results),
        "results": results,
    }
    dump_json(output_path, payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--splits", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--features")
    parser.add_argument("--target-column", required=True)
    parser.add_argument("--ic-kind", default="auto")
    args = parser.parse_args()
    run(Path(args.input), Path(args.splits), Path(args.output), features=args.features, target_column=args.target_column, ic_kind=args.ic_kind)


if __name__ == "__main__":
    main()
