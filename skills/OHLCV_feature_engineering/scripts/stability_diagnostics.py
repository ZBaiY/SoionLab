from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import dump_json, load_table, parse_features_arg


def run(input_path: Path, output_path: Path, *, features: str | list[str] | None, target_column: str, rolling_window: int = 8) -> dict:
    df = load_table(input_path)
    feature_cols = parse_features_arg(features, df)
    diagnostics: list[dict] = []
    for feature in feature_cols:
        frame = df[[feature, target_column]].dropna().copy()
        if frame.empty:
            diagnostics.append({
                "feature": feature,
                "coverage": 0.0,
                "value_std": None,
                "target_std": None,
                "rolling_ic_mean": None,
                "rolling_ic_std": None,
                "sign_flip_count": None,
            })
            continue
        rolling_ic = frame[feature].rolling(rolling_window).corr(frame[target_column])
        rolling_ic = rolling_ic.dropna()
        sign_flips = int((rolling_ic.fillna(0).apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0).diff().abs() > 0).sum()) if not rolling_ic.empty else 0
        diagnostics.append({
            "feature": feature,
            "coverage": float(frame.shape[0] / max(len(df), 1)),
            "value_std": float(frame[feature].std()) if frame[feature].nunique() > 1 else 0.0,
            "target_std": float(frame[target_column].std()) if frame[target_column].nunique() > 1 else 0.0,
            "rolling_ic_mean": float(rolling_ic.mean()) if not rolling_ic.empty else None,
            "rolling_ic_std": float(rolling_ic.std()) if not rolling_ic.empty else None,
            "sign_flip_count": sign_flips,
        })
    result = {
        "target_column": target_column,
        "rolling_window": rolling_window,
        "diagnostics": diagnostics,
    }
    dump_json(output_path, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--features")
    parser.add_argument("--target-column", required=True)
    parser.add_argument("--rolling-window", type=int, default=8)
    args = parser.parse_args()
    run(Path(args.input), Path(args.output), features=args.features, target_column=args.target_column, rolling_window=int(args.rolling_window))


if __name__ == "__main__":
    main()
