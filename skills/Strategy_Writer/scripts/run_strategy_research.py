#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import itertools
import json
from pathlib import Path
from typing import Any

from common_research import (
    aggregate_rows,
    head_to_head,
    run_strategy_once,
    score_rows,
    serialize_metrics,
    write_scan_csv,
)


def load_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def windows_from_config(items: list[dict[str, Any]]) -> list[tuple[str, int, int]]:
    return [(str(item["label"]), int(item["start_ts"]), int(item["end_ts"])) for item in items]


def parameter_combos(grid: dict[str, list[Any]]) -> list[dict[str, Any]]:
    if not grid:
        return [{}]
    keys = sorted(grid.keys())
    values = [list(grid[key]) for key in keys]
    return [dict(zip(keys, combo)) for combo in itertools.product(*values)]


def bind_with_params(base_bind: dict[str, str], params: dict[str, Any]) -> dict[str, str]:
    bind = dict(base_bind)
    for key, value in params.items():
        bind[str(key)] = str(value)
    return bind


def report_markdown(
    *,
    title: str,
    scan_strategy: str,
    best_scan: dict[str, Any] | None,
    scan_rows: list[dict[str, Any]],
    primary_oos: list[dict[str, Any]],
    references: dict[str, list[dict[str, Any]]],
    comparisons: dict[str, dict[str, float]],
) -> str:
    lines = [
        f"# {title}",
        "",
        f"Primary strategy: `{scan_strategy}`",
        "",
        "## Best Scan",
        "",
        json.dumps(best_scan or {}, indent=2, sort_keys=True),
        "",
        "## Scan Rows",
        "",
        f"Total scan rows: {len(scan_rows)}",
        "",
        "## Primary OOS",
        "",
        json.dumps(primary_oos, indent=2, sort_keys=True),
        "",
        "## Reference OOS",
        "",
        json.dumps(references, indent=2, sort_keys=True),
        "",
        "## Head To Head",
        "",
        json.dumps(comparisons, indent=2, sort_keys=True),
        "",
    ]
    return "\n".join(lines) + "\n"


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run a generic strategy research workflow from JSON config.")
    parser.add_argument("--config", required=True, help="Path to strategy research config JSON.")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    cfg = load_config(config_path)
    output_dir = Path(str(cfg["output_dir"])).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    scan_strategy = str(cfg["scan_strategy"])
    base_bind = {str(k): str(v) for k, v in dict(cfg["base_bind"]).items()}
    is_windows = windows_from_config(list(cfg["is_windows"]))
    oos_windows = windows_from_config(list(cfg["oos_windows"]))
    references = list(cfg.get("references") or [])
    report_title = str(cfg.get("report_title") or f"{scan_strategy} Research Report")

    scan_rows: list[dict[str, Any]] = []
    combo_runs: dict[tuple[tuple[str, Any], ...], list[Any]] = {}
    best_scan: dict[str, Any] | None = None

    for params in parameter_combos(dict(cfg.get("parameter_grid") or {})):
        bind = bind_with_params(base_bind, params)
        rows = []
        for label, start_ts, end_ts in is_windows:
            run_id_parts = [scan_strategy, label] + [f"{k}-{bind[str(k)]}" for k in sorted(params.keys())]
            run_id = "IS_" + "_".join(part.replace(" ", "-") for part in run_id_parts)
            metric = await run_strategy_once(
                run_id=run_id,
                strategy_name=scan_strategy,
                bind_symbols=bind,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            metric.label = label
            rows.append(metric)
        key = tuple(sorted(params.items()))
        combo_runs[key] = rows
        row = {"strategy_name": scan_strategy, **params, **score_rows(rows)}
        scan_rows.append(row)
        if best_scan is None or float(row["objective"]) > float(best_scan["objective"]):
            best_scan = row

    best_params = {
        key: value
        for key, value in (best_scan or {}).items()
        if key not in {"strategy_name", "clean_ratio", "median_sharpe", "mean_return", "median_trades", "objective"}
    }
    primary_bind = bind_with_params(base_bind, best_params)

    primary_oos_metrics = []
    for label, start_ts, end_ts in oos_windows:
        run_id = "OOS_" + "_".join([scan_strategy, label])
        metric = await run_strategy_once(
            run_id=run_id,
            strategy_name=scan_strategy,
            bind_symbols=primary_bind,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        metric.label = label
        primary_oos_metrics.append(metric)

    reference_metrics: dict[str, list[Any]] = {}
    comparisons: dict[str, dict[str, float]] = {}
    for ref in references:
        ref_label = str(ref["label"])
        ref_strategy = str(ref["strategy_name"])
        ref_bind = dict(base_bind)
        ref_bind.update({str(k): str(v) for k, v in dict(ref.get("bind_overrides") or {}).items()})
        rows = []
        for label, start_ts, end_ts in oos_windows:
            run_id = "OOS_" + "_".join([ref_strategy, ref_label, label])
            metric = await run_strategy_once(
                run_id=run_id,
                strategy_name=ref_strategy,
                bind_symbols=ref_bind,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            metric.label = label
            rows.append(metric)
        reference_metrics[ref_label] = rows
        comparisons[ref_label] = head_to_head(primary_oos_metrics, rows)

    payload = {
        "scan_strategy": scan_strategy,
        "base_bind": base_bind,
        "best_scan": best_scan,
        "scan_rows": scan_rows,
        "primary_oos": serialize_metrics(primary_oos_metrics),
        "primary_oos_aggregate": aggregate_rows(primary_oos_metrics),
        "references": {label: serialize_metrics(rows) for label, rows in reference_metrics.items()},
        "reference_aggregates": {label: aggregate_rows(rows) for label, rows in reference_metrics.items()},
        "comparisons": comparisons,
    }

    (output_dir / "research_results.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    write_scan_csv(output_dir / "parameter_scan.csv", scan_rows)
    (output_dir / "research_report.md").write_text(
        report_markdown(
            title=report_title,
            scan_strategy=scan_strategy,
            best_scan=best_scan,
            scan_rows=scan_rows,
            primary_oos=serialize_metrics(primary_oos_metrics),
            references={label: serialize_metrics(rows) for label, rows in reference_metrics.items()},
            comparisons=comparisons,
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    asyncio.run(main())

