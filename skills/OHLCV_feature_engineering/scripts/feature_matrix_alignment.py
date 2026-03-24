from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import dump_json, load_table, parse_features_arg, write_table


def run(input_path: Path, output_path: Path, *, features: str | list[str] | None, target_column: str, summary_output: Path | None = None) -> pd.DataFrame:
    df = load_table(input_path).copy()
    feature_cols = parse_features_arg(features, df)
    required = [column for column in ["timestamp", "symbol"] if column in df.columns] + feature_cols + [target_column]
    aligned = df[required].dropna().copy()
    if "timestamp" in aligned.columns:
        aligned = aligned.sort_values(["timestamp", "symbol"] if "symbol" in aligned.columns else ["timestamp"], kind="mergesort")
    write_table(aligned, output_path)
    if summary_output is not None:
        dump_json(summary_output, {
            "row_count": int(aligned.shape[0]),
            "feature_count": len(feature_cols),
            "target_column": target_column,
            "features": feature_cols,
        })
    return aligned


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--features")
    parser.add_argument("--target-column", required=True)
    parser.add_argument("--summary-output")
    args = parser.parse_args()
    run(
        Path(args.input),
        Path(args.output),
        features=args.features,
        target_column=args.target_column,
        summary_output=Path(args.summary_output) if args.summary_output else None,
    )


if __name__ == "__main__":
    main()
