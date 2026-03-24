from __future__ import annotations

import argparse
from pathlib import Path

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import dump_json, load_table, parse_features_arg
from evaluation_utils import compute_ic


def run(input_path: Path, output_path: Path, *, features: str | list[str] | None, target_column: str, ic_kind: str, method: str = "spearman") -> dict:
    df = load_table(input_path)
    feature_cols = parse_features_arg(features, df)
    result = compute_ic(df, features=feature_cols, target_column=target_column, ic_kind=ic_kind, method=method)
    result["target_column"] = target_column
    result["features"] = feature_cols
    dump_json(output_path, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--features")
    parser.add_argument("--target-column", required=True)
    parser.add_argument("--ic-kind", default="auto")
    parser.add_argument("--method", default="spearman")
    args = parser.parse_args()
    run(Path(args.input), Path(args.output), features=args.features, target_column=args.target_column, ic_kind=args.ic_kind, method=args.method)


if __name__ == "__main__":
    main()
