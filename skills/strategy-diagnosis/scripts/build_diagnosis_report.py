from __future__ import annotations

import argparse
import csv
from pathlib import Path


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fp:
        return list(csv.DictReader(fp))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a simple markdown diagnosis report from prior CSV outputs.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--title", default="Strategy Diagnosis")
    parser.add_argument("--window-csv", required=True)
    parser.add_argument("--behavior-csv", required=True)
    parser.add_argument("--regime-matrix-csv", default=None)
    parser.add_argument("--drift-csv", default=None)
    parser.add_argument("--variant-csv", default=None)
    parser.add_argument("--clustering-snippet-md", default=None)
    parser.add_argument("--out-md", required=True)
    args = parser.parse_args()

    window_rows = load_csv(Path(args.window_csv))
    behavior_rows = load_csv(Path(args.behavior_csv))
    regime_rows = load_csv(Path(args.regime_matrix_csv)) if args.regime_matrix_csv else []
    drift_rows = load_csv(Path(args.drift_csv)) if args.drift_csv else []
    variant_rows = load_csv(Path(args.variant_csv)) if args.variant_csv else []
    clustering_snippet = ""
    if args.clustering_snippet_md:
        snippet_path = Path(args.clustering_snippet_md)
        if snippet_path.exists():
            clustering_snippet = snippet_path.read_text(encoding="utf-8").strip()

    lines = [
        f"# {args.title}",
        "",
        f"- Run: `{args.run_id}`",
        "",
        "## Window Summary",
        "",
    ]
    if window_rows:
        for row in window_rows:
            lines.append(
                f"- `{row['label']}`: return `{100*float(row['total_return']):.2f}%`, "
                f"Sharpe `{float(row['sharpe']):.3f}`, max DD `{100*float(row['max_drawdown']):.2f}%`"
            )
    else:
        lines.append("- No window slices available.")

    lines.extend(["", "## Behavior Summary", ""])
    if behavior_rows:
        row = behavior_rows[0]
        lines.append(
            f"- entries `{row['entry_count']}`, time in market `{100*float(row['time_in_market_ratio']):.2f}%`, "
            f"mean holding steps `{float(row['mean_holding_steps']):.2f}`"
        )
    else:
        lines.append("- No behavior summary available.")

    lines.extend(["", "## Regime Matrix", ""])
    if regime_rows:
        for row in regime_rows:
            lines.append(
                f"- `{row['regime']}`: steps `{row['steps']}`, mean step return `{100*float(row['mean_step_return']):.4f}%`, "
                f"positive-step ratio `{100*float(row['positive_step_ratio']):.2f}%`"
            )
    else:
        lines.append("- No regime matrix provided.")

    lines.extend(["", "## Decision Drift", ""])
    if drift_rows:
        by_event: dict[str, list[float]] = {}
        for row in drift_rows:
            by_event.setdefault(row["event_type"], []).append(float(row["forward_return"]))
        for event_type, values in sorted(by_event.items()):
            mean_value = sum(values) / len(values) if values else 0.0
            lines.append(f"- `{event_type}`: samples `{len(values)}`, mean forward return `{100*mean_value:.4f}%`")
    else:
        lines.append("- No decision drift provided.")

    lines.extend(["", "## Variant Deltas", ""])
    if variant_rows:
        lines.append(f"- differing decision steps: `{len(variant_rows)}`")
    else:
        lines.append("- No variant comparison provided.")

    if clustering_snippet:
        lines.extend(["", clustering_snippet])

    Path(args.out_md).write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
