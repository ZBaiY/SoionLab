from __future__ import annotations

import argparse
import csv
import json
from bisect import bisect_right
from pathlib import Path

from common import load_report, safe_float, write_csv


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fp:
        return list(csv.DictReader(fp))


def interval_to_ms(raw: str) -> int:
    value = str(raw).strip().lower()
    if not value:
        raise ValueError("interval must be non-empty")
    unit = value[-1]
    amount = int(value[:-1])
    if unit == "m":
        return amount * 60_000
    if unit == "h":
        return amount * 3_600_000
    if unit == "d":
        return amount * 86_400_000
    raise ValueError(f"unsupported interval: {raw!r}")


def parse_gap_edges(raw: str) -> list[int]:
    edges = sorted({int(part.strip()) for part in str(raw).split(",") if part.strip()})
    if not edges:
        raise ValueError("gap bucket edges must be non-empty")
    return edges


def classify_gap_bucket(gap_bars: int | None, edges: list[int]) -> str:
    if gap_bars is None:
        return "first_trade"
    lower = 0
    for edge in edges:
        if gap_bars <= edge:
            return f"{lower}-{edge}"
        lower = edge + 1
    return f"{edges[-1] + 1}+"


def rank_bucket(rank: int) -> str:
    if rank <= 1:
        return "first_after_flat"
    if rank == 2:
        return "second_within_window"
    return "third_or_later_within_window"


def nearest_regime(ts_ms: int, label_ts: list[int], by_ts: dict[int, str]) -> str:
    idx = bisect_right(label_ts, ts_ms) - 1
    if idx < 0:
        return "unclassified"
    return by_ts.get(label_ts[idx], "unclassified")


def summarize_group(rows: list[dict[str, object]], *, group_key: str) -> list[dict[str, object]]:
    grouped: dict[str, dict[str, float]] = {}
    for row in rows:
        key = str(row[group_key])
        agg = grouped.setdefault(
            key,
            {
                "count": 0.0,
                "net_pnl": 0.0,
                "wins": 0.0,
                "sum_return_pct": 0.0,
                "sum_holding_steps": 0.0,
            },
        )
        pnl = safe_float(row.get("realized_pnl"))
        agg["count"] += 1.0
        agg["net_pnl"] += pnl
        agg["sum_return_pct"] += safe_float(row.get("return_pct"))
        agg["sum_holding_steps"] += safe_float(row.get("holding_steps"))
        if pnl > 0.0:
            agg["wins"] += 1.0

    out: list[dict[str, object]] = []
    for key, agg in grouped.items():
        count = agg["count"]
        out.append(
            {
                group_key: key,
                "count": int(count),
                "net_pnl": agg["net_pnl"],
                "avg_pnl": agg["net_pnl"] / count if count else 0.0,
                "win_rate": agg["wins"] / count if count else 0.0,
                "avg_return_pct": agg["sum_return_pct"] / count if count else 0.0,
                "avg_holding_steps": agg["sum_holding_steps"] / count if count else 0.0,
            }
        )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze trade clustering and rapid re-entry from completed round trips.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--regime-csv", required=True)
    parser.add_argument("--target-regime", default="choppy_high_vol")
    parser.add_argument("--comparison-regime", default="sideways_low_vol")
    parser.add_argument("--cluster-window-bars", type=int, default=20)
    parser.add_argument("--gap-bucket-edges", default="5,10,20")
    parser.add_argument("--out-trades-csv", required=True)
    parser.add_argument("--out-rank-summary-csv", required=True)
    parser.add_argument("--out-gap-summary-csv", required=True)
    parser.add_argument("--out-summary-json", required=True)
    parser.add_argument("--out-snippet-md", required=True)
    args = parser.parse_args()

    repo_root = Path(args.repo_root)
    report = load_report(repo_root, args.run_id)
    interval_ms = interval_to_ms(str((report.get("run_metadata") or {}).get("interval") or "15m"))
    gap_edges = parse_gap_edges(args.gap_bucket_edges)

    label_rows = load_csv(Path(args.regime_csv))
    label_ts = sorted(int(row["ts_ms"]) for row in label_rows)
    regime_by_ts = {int(row["ts_ms"]): row["regime"] for row in label_rows}

    trades = sorted(list(report.get("trade_round_trips") or []), key=lambda trade: int(trade.get("entry_time_ms") or 0))
    annotated_rows: list[dict[str, object]] = []
    prev_exit_ms: int | None = None
    current_cluster_rank = 0
    for trade in trades:
        entry_ms = int(trade.get("entry_time_ms") or 0)
        exit_ms = int(trade.get("exit_time_ms") or 0)
        gap_bars: int | None = None
        if prev_exit_ms is not None:
            gap_bars = max(0, int((entry_ms - prev_exit_ms) // interval_ms))
        if gap_bars is None or gap_bars > int(args.cluster_window_bars):
            current_cluster_rank = 1
        else:
            current_cluster_rank += 1

        annotated_rows.append(
            {
                "run_id": args.run_id,
                "trade_id": int(trade.get("trade_id") or 0),
                "entry_time_ms": entry_ms,
                "exit_time_ms": exit_ms,
                "entry_regime": nearest_regime(entry_ms, label_ts, regime_by_ts),
                "holding_steps": int(trade.get("holding_steps") or 0),
                "quantity": safe_float(trade.get("quantity")),
                "realized_pnl": safe_float(trade.get("realized_pnl")),
                "net_pnl": safe_float(trade.get("net_pnl")),
                "return_pct": safe_float(trade.get("return_pct")),
                "gap_since_previous_exit_bars": gap_bars if gap_bars is not None else "",
                "is_first_after_flat": 1 if current_cluster_rank == 1 else 0,
                "reentry_rank_within_window": current_cluster_rank,
                "reentry_rank_bucket": rank_bucket(current_cluster_rank),
                "gap_bucket": classify_gap_bucket(gap_bars, gap_edges),
            }
        )
        prev_exit_ms = exit_ms

    write_csv(Path(args.out_trades_csv), annotated_rows)

    target_rows = [row for row in annotated_rows if row["entry_regime"] == args.target_regime]
    comparison_rows = [row for row in annotated_rows if row["entry_regime"] == args.comparison_regime]

    rank_summary_rows: list[dict[str, object]] = []
    gap_summary_rows: list[dict[str, object]] = []
    for regime_name, regime_rows in ((args.target_regime, target_rows), (args.comparison_regime, comparison_rows)):
        if not regime_rows:
            continue
        for row in summarize_group(regime_rows, group_key="reentry_rank_bucket"):
            row["regime"] = regime_name
            rank_summary_rows.append(row)
        for row in summarize_group(regime_rows, group_key="gap_bucket"):
            row["regime"] = regime_name
            gap_summary_rows.append(row)

    write_csv(Path(args.out_rank_summary_csv), rank_summary_rows)
    write_csv(Path(args.out_gap_summary_csv), gap_summary_rows)

    def lookup(rows: list[dict[str, object]], regime: str, key_name: str, key_value: str) -> dict[str, object] | None:
        for row in rows:
            if row.get("regime") == regime and row.get(key_name) == key_value:
                return row
        return None

    first_rank = lookup(rank_summary_rows, args.target_regime, "reentry_rank_bucket", "first_after_flat")
    second_rank = lookup(rank_summary_rows, args.target_regime, "reentry_rank_bucket", "second_within_window")
    third_rank = lookup(rank_summary_rows, args.target_regime, "reentry_rank_bucket", "third_or_later_within_window")

    short_gap_rows = [
        row for row in gap_summary_rows
        if row.get("regime") == args.target_regime and row.get("gap_bucket") in {"0-5", "6-10", "11-20"}
    ]
    long_gap = lookup(gap_summary_rows, args.target_regime, "gap_bucket", f"{gap_edges[-1] + 1}+")
    short_gap_count = sum(int(row.get("count") or 0) for row in short_gap_rows)
    short_gap_net_pnl = sum(safe_float(row.get("net_pnl")) for row in short_gap_rows)
    short_gap_avg_pnl = short_gap_net_pnl / short_gap_count if short_gap_count else 0.0
    long_gap_avg_pnl = safe_float(long_gap.get("avg_pnl")) if long_gap else 0.0

    cooldown_support = "mixed"
    if first_rank and second_rank:
        later_rows = [row for row in (second_rank, third_rank) if row is not None]
        later_count = sum(int(row.get("count") or 0) for row in later_rows)
        later_net_pnl = sum(safe_float(row.get("net_pnl")) for row in later_rows)
        first_avg = safe_float(first_rank.get("avg_pnl"))
        later_avg = later_net_pnl / later_count if later_count else 0.0
        if later_count > 0 and later_avg < first_avg and short_gap_avg_pnl < long_gap_avg_pnl:
            cooldown_support = "supports_choppy_cooldown"
        elif later_count > 0 and later_avg >= first_avg and short_gap_avg_pnl >= long_gap_avg_pnl:
            cooldown_support = "does_not_support_choppy_cooldown"

    summary = {
        "run_id": args.run_id,
        "target_regime": args.target_regime,
        "comparison_regime": args.comparison_regime,
        "cluster_window_bars": int(args.cluster_window_bars),
        "gap_bucket_edges": gap_edges,
        "target_trade_count": len(target_rows),
        "comparison_trade_count": len(comparison_rows),
        "rank_summary": [row for row in rank_summary_rows if row.get("regime") == args.target_regime],
        "gap_summary": [row for row in gap_summary_rows if row.get("regime") == args.target_regime],
        "short_gap_avg_pnl": short_gap_avg_pnl,
        "long_gap_avg_pnl": long_gap_avg_pnl,
        "cooldown_support": cooldown_support,
    }
    Path(args.out_summary_json).write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        "## Trade Clustering",
        "",
        f"- target regime `{args.target_regime}` with cluster window `{int(args.cluster_window_bars)}` bars",
        f"- target trades `{len(target_rows)}`, comparison trades `{len(comparison_rows)}` in `{args.comparison_regime}`",
    ]
    if first_rank:
        lines.append(
            f"- first-after-flat: count `{int(first_rank['count'])}`, avg PnL `{safe_float(first_rank['avg_pnl']):.2f}`, "
            f"win rate `{100*safe_float(first_rank['win_rate']):.2f}%`"
        )
    if second_rank:
        lines.append(
            f"- second-within-window: count `{int(second_rank['count'])}`, avg PnL `{safe_float(second_rank['avg_pnl']):.2f}`, "
            f"win rate `{100*safe_float(second_rank['win_rate']):.2f}%`"
        )
    if third_rank:
        lines.append(
            f"- third-or-later: count `{int(third_rank['count'])}`, avg PnL `{safe_float(third_rank['avg_pnl']):.2f}`, "
            f"win rate `{100*safe_float(third_rank['win_rate']):.2f}%`"
        )
    lines.append(f"- short-gap avg PnL `{short_gap_avg_pnl:.2f}` vs long-gap avg PnL `{long_gap_avg_pnl:.2f}`")
    lines.append(f"- cooldown evidence: `{cooldown_support}`")
    Path(args.out_snippet_md).write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
