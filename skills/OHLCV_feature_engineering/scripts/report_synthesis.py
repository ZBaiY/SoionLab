from __future__ import annotations

import argparse
from pathlib import Path

if __package__ in {None, ""}:
    import sys
    sys.path.append(str(Path(__file__).resolve().parent))

from common import load_json, read_template, write_text


def run(ic_path: Path, stability_path: Path, walk_forward_path: Path, redundancy_path: Path, output_path: Path) -> str:
    ic = load_json(ic_path)
    stability = load_json(stability_path)
    walk = load_json(walk_forward_path)
    redundancy = load_json(redundancy_path)

    ic_lines = []
    for item in ic["results"]:
        summary_value = item.get("overall_ic", item.get("mean_ic", item.get("mean_symbol_ic")))
        ic_lines.append(f"- `{item['feature']}`: `{summary_value}` via `{item['ic_kind']}` `{item['method']}`")

    stability_lines = []
    for item in stability["diagnostics"]:
        stability_lines.append(
            f"- `{item['feature']}`: coverage={item['coverage']:.3f}, rolling_ic_mean={item['rolling_ic_mean']}, sign_flips={item['sign_flip_count']}"
        )

    walk_lines = []
    for item in walk["results"]:
        walk_lines.append(
            f"- Split `{item['split_id']}`: train={item['train']}, test={item['test']}"
        )

    redundancy_lines = [
        f"- `{pair['left']}` vs `{pair['right']}`: corr={pair['spearman_corr']:.4f}"
        for pair in redundancy["high_corr_pairs"]
    ] or ["- No feature pairs exceeded the redundancy threshold."]

    template = read_template("evaluation_report_template.md")
    rendered = template.format(
        ic_kind=ic["ic_kind"],
        target_column=ic["target_column"],
        ic_lines="\n".join(ic_lines) or "- No IC results.",
        stability_lines="\n".join(stability_lines) or "- No stability diagnostics.",
        walk_lines="\n".join(walk_lines) or "- No walk-forward splits.",
        redundancy_lines="\n".join(redundancy_lines),
    )
    write_text(output_path, rendered)
    return rendered


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ic", required=True)
    parser.add_argument("--stability", required=True)
    parser.add_argument("--walk-forward", required=True)
    parser.add_argument("--redundancy", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    run(Path(args.ic), Path(args.stability), Path(args.walk_forward), Path(args.redundancy), Path(args.output))


if __name__ == "__main__":
    main()
