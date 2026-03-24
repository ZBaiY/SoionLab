from __future__ import annotations

import argparse
from pathlib import Path

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import dump_json
from feature_matrix_builder import run as run_feature_matrix_builder
from target_construction import run as run_target_construction
from feature_matrix_alignment import run as run_feature_matrix_alignment
from ic_analysis import run as run_ic_analysis
from stability_diagnostics import run as run_stability_diagnostics
from split_generation import run as run_split_generation
from walk_forward_validation import run as run_walk_forward_validation
from redundancy_analysis import run as run_redundancy_analysis
from report_synthesis import run as run_report_synthesis


def run_pipeline(
    input_path: Path,
    output_dir: Path,
    *,
    features: str | None,
    horizon_bars: int,
    target_kind: str,
    ic_kind: str,
    train_bars: int,
    test_bars: int,
    step_bars: int,
    family: str | None = None,
    symbol: str = "BTCUSDT",
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    matrix_source = input_path
    built_matrix_path = output_dir / "feature_matrix.csv"
    if family is not None:
        run_feature_matrix_builder(input_path, built_matrix_path, family=family, symbol=symbol, features=features)
        matrix_source = built_matrix_path

    target_path = output_dir / "target_matrix.csv"
    aligned_path = output_dir / "aligned_feature_matrix.csv"
    align_summary_path = output_dir / "alignment_summary.json"
    target_column = f"target_{target_kind}_{horizon_bars}"
    ic_path = output_dir / "ic_analysis.json"
    stability_path = output_dir / "stability_diagnostics.json"
    splits_path = output_dir / "splits.json"
    walk_path = output_dir / "walk_forward_validation.json"
    redundancy_path = output_dir / "redundancy_analysis.json"
    report_path = output_dir / "evaluation_quality_report.md"

    run_target_construction(matrix_source, target_path, horizon_bars=horizon_bars, target_kind=target_kind)
    run_feature_matrix_alignment(target_path, aligned_path, features=features, target_column=target_column, summary_output=align_summary_path)
    run_ic_analysis(aligned_path, ic_path, features=features, target_column=target_column, ic_kind=ic_kind)
    run_stability_diagnostics(aligned_path, stability_path, features=features, target_column=target_column)
    run_split_generation(aligned_path, splits_path, train_bars=train_bars, test_bars=test_bars, step_bars=step_bars, mode="rolling")
    run_walk_forward_validation(aligned_path, splits_path, walk_path, features=features, target_column=target_column, ic_kind=ic_kind)
    run_redundancy_analysis(aligned_path, redundancy_path, features=features)
    run_report_synthesis(ic_path, stability_path, walk_path, redundancy_path, report_path)

    manifest = {
        "target_matrix.csv": str(target_path),
        "aligned_feature_matrix.csv": str(aligned_path),
        "ic_analysis.json": str(ic_path),
        "stability_diagnostics.json": str(stability_path),
        "splits.json": str(splits_path),
        "walk_forward_validation.json": str(walk_path),
        "redundancy_analysis.json": str(redundancy_path),
        "evaluation_quality_report.md": str(report_path),
    }
    dump_json(output_dir / "evaluation_manifest.json", manifest)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--features")
    parser.add_argument("--horizon-bars", type=int, default=1)
    parser.add_argument("--target-kind", default="forward_return")
    parser.add_argument("--ic-kind", default="auto")
    parser.add_argument("--train-bars", type=int, default=8)
    parser.add_argument("--test-bars", type=int, default=4)
    parser.add_argument("--step-bars", type=int, default=4)
    parser.add_argument("--family")
    parser.add_argument("--symbol", default="BTCUSDT")
    args = parser.parse_args()
    run_pipeline(
        Path(args.input),
        Path(args.output_dir),
        features=args.features,
        horizon_bars=int(args.horizon_bars),
        target_kind=args.target_kind,
        ic_kind=args.ic_kind,
        train_bars=int(args.train_bars),
        test_bars=int(args.test_bars),
        step_bars=int(args.step_bars),
        family=args.family,
        symbol=args.symbol,
    )


if __name__ == "__main__":
    main()
