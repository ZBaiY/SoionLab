from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from statistics import median


ADX_THRESHOLD = 30.0
VARIANCE_FACTOR = 1.8
MAE = 0.0
PATH_HORIZON_BARS = 24
DRIFT_HORIZONS = (3, 6, 12, 24)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze trace-level trade lifecycle diagnostics")
    parser.add_argument("--repo-root", default=".", help="repository root")
    parser.add_argument("--run-id", required=True, help="completed run id")
    parser.add_argument("--symbol", default="BTCUSDT", help="symbol to analyze")
    parser.add_argument("--out-dir", required=True, help="output directory")
    return parser


def _classify_regime(*, adx: float, di_plus: float, di_minus: float, rsi: float) -> str:
    if adx >= ADX_THRESHOLD and di_plus > di_minus:
        return "trend_up"
    if adx >= ADX_THRESHOLD and di_minus > di_plus:
        return "trend_down"
    if adx < 20.0 and 30.0 < rsi < 70.0:
        return "sideways_low_vol"
    if adx < ADX_THRESHOLD:
        return "choppy_high_vol"
    return "unclassified"


def _exit_reason(*, rsi: float, rsi_mean: float, rsi_std: float, adx: float, di_plus: float, di_minus: float) -> str:
    dynamic_upper = rsi_mean + VARIANCE_FACTOR * rsi_std
    dynamic_lower = rsi_mean - VARIANCE_FACTOR * rsi_std
    if rsi >= (dynamic_upper - MAE):
        return "upper_band_exit"
    if adx >= ADX_THRESHOLD and di_minus > di_plus and rsi <= (dynamic_lower + MAE):
        return "adx_bearish_flush"
    return "other"


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _bucket_gap(value: int) -> str:
    if value <= 5:
        return "0-5"
    if value <= 10:
        return "6-10"
    if value <= 20:
        return "11-20"
    return "21+"


def _path_archetype(path: list[float]) -> str:
    if not path:
        return "insufficient"
    min_ret = min(path)
    max_ret = max(path)
    end_ret = path[-1]
    peak_idx = max(range(len(path)), key=lambda idx: path[idx])
    trough_idx = min(range(len(path)), key=lambda idx: path[idx])
    sign_flips = 0
    prev_sign = 0
    for value in path:
        sign = 1 if value > 0 else (-1 if value < 0 else 0)
        if sign != 0 and prev_sign != 0 and sign != prev_sign:
            sign_flips += 1
        if sign != 0:
            prev_sign = sign
    if max_ret > 0.0 and min_ret >= -0.002 and peak_idx <= 6:
        return "immediate_rebound"
    if trough_idx < peak_idx and min_ret <= -0.01 and max_ret >= 0.01:
        return "dip_then_rebound"
    if sign_flips >= 2 and max_ret >= 0.003 and min_ret <= -0.003:
        return "noisy_oscillation"
    if end_ret < 0.0 and max_ret <= 0.003:
        return "immediate_failure"
    return "mixed"


def _load_trace(trace_path: Path, symbol: str) -> tuple[dict[int, dict[str, float]], list[int]]:
    steps: dict[int, dict[str, float]] = {}
    ordered_ts: list[int] = []
    with trace_path.open() as fh:
        for line in fh:
            obj = json.loads(line)
            if obj.get("event") != "engine.step.trace":
                continue
            if obj.get("symbol") != symbol:
                continue
            ts_ms = int(obj["ts_ms"])
            features = obj.get("features") or {}
            market = ((obj.get("market_snapshots") or {}).get("ohlcv") or {}).get(symbol) or {}
            numeric = market.get("numeric") or {}
            close = numeric.get("close")
            if close is None:
                continue
            steps[ts_ms] = {
                "close": float(close),
                "rsi": float(features.get(f"RSI_DECISION_{symbol}", 0.0)),
                "adx": float(features.get(f"ADX_DECISION_{symbol}", 0.0)),
                "di_plus": float(features.get(f"ADX-DI-PLUS_DECISION_{symbol}", 0.0)),
                "di_minus": float(features.get(f"ADX-DI-MINUS_DECISION_{symbol}", 0.0)),
                "rsi_mean": float(features.get(f"RSI-MEAN_DECISION_{symbol}", 0.0)),
                "rsi_std": float(features.get(f"RSI-STD_DECISION_{symbol}", 0.0)),
            }
            ordered_ts.append(ts_ms)
    ordered_ts.sort()
    return steps, ordered_ts


def _forward_return(ordered_ts: list[int], ts_to_idx: dict[int, int], steps: dict[int, dict[str, float]], anchor_ts: int, horizon: int) -> float | None:
    idx = ts_to_idx.get(anchor_ts)
    if idx is None:
        return None
    future_idx = idx + horizon
    if future_idx >= len(ordered_ts):
        return None
    start_px = steps[anchor_ts]["close"]
    future_ts = ordered_ts[future_idx]
    future_px = steps[future_ts]["close"]
    return future_px / start_px - 1.0


def main() -> None:
    args = _build_parser().parse_args()
    repo_root = Path(str(args.repo_root)).resolve()
    run_id = str(args.run_id)
    symbol = str(args.symbol)
    out_dir = (repo_root / str(args.out_dir)).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    run_dir = repo_root / "artifacts" / "runs" / run_id
    report = json.loads((run_dir / "report" / "report.json").read_text())
    steps, ordered_ts = _load_trace(run_dir / "logs" / "trace.jsonl", symbol)
    ts_to_idx = {ts: idx for idx, ts in enumerate(ordered_ts)}

    exit_drift_rows: list[dict[str, object]] = []
    mfe_rows: list[dict[str, object]] = []
    archetype_rows: list[dict[str, object]] = []
    archetype_examples: dict[str, list[dict[str, object]]] = {}
    regime_exit_summary: dict[tuple[str, str, int], list[float]] = {}
    regime_mfe_summary: dict[str, list[dict[str, float]]] = {}
    exit_reason_trade_stats: dict[str, list[float]] = {}

    for trade in report["trade_round_trips"]:
        entry_ts = int(trade["entry_time_ms"])
        exit_ts = int(trade["exit_time_ms"])
        entry_step = steps.get(entry_ts)
        exit_step = steps.get(exit_ts)
        if entry_step is None or exit_step is None:
            continue

        entry_regime = _classify_regime(
            adx=entry_step["adx"],
            di_plus=entry_step["di_plus"],
            di_minus=entry_step["di_minus"],
            rsi=entry_step["rsi"],
        )
        exit_reason = _exit_reason(
            rsi=exit_step["rsi"],
            rsi_mean=exit_step["rsi_mean"],
            rsi_std=exit_step["rsi_std"],
            adx=exit_step["adx"],
            di_plus=exit_step["di_plus"],
            di_minus=exit_step["di_minus"],
        )

        exit_row: dict[str, object] = {
            "trade_id": int(trade["trade_id"]),
            "entry_ts": entry_ts,
            "exit_ts": exit_ts,
            "entry_regime": entry_regime,
            "exit_reason": exit_reason,
            "net_pnl": float(trade["net_pnl"]),
            "holding_steps": int(trade["holding_steps"]),
        }
        for horizon in DRIFT_HORIZONS:
            forward_ret = _forward_return(ordered_ts, ts_to_idx, steps, exit_ts, horizon)
            exit_row[f"forward_return_{horizon}"] = forward_ret
            if forward_ret is not None:
                regime_exit_summary.setdefault((entry_regime, exit_reason, horizon), []).append(forward_ret)
                exit_reason_trade_stats.setdefault(exit_reason, []).append(forward_ret)
        exit_drift_rows.append(exit_row)

        entry_idx = ts_to_idx[entry_ts]
        exit_idx = ts_to_idx[exit_ts]
        entry_close = entry_step["close"]
        path_limit = min(entry_idx + PATH_HORIZON_BARS, len(ordered_ts) - 1)
        path_returns: list[float] = []
        for idx in range(entry_idx, path_limit + 1):
            step_ts = ordered_ts[idx]
            path_returns.append(steps[step_ts]["close"] / entry_close - 1.0)

        path_archetype = _path_archetype(path_returns[1:])
        for horizon in range(1, min(PATH_HORIZON_BARS, len(path_returns) - 1) + 1):
            archetype_rows.append(
                {
                    "trade_id": int(trade["trade_id"]),
                    "entry_regime": entry_regime,
                    "outcome": str(trade["outcome"]).lower(),
                    "path_archetype": path_archetype,
                    "horizon_bar": horizon,
                    "price_return": path_returns[horizon],
                }
            )
        example = {
            "trade_id": int(trade["trade_id"]),
            "entry_regime": entry_regime,
            "outcome": str(trade["outcome"]).lower(),
            "entry_ts": entry_ts,
            "exit_ts": exit_ts,
            "path_returns_first_24": path_returns[1:],
        }
        examples = archetype_examples.setdefault(path_archetype, [])
        if len(examples) < 5:
            examples.append(example)

        favorable: list[tuple[int, float]] = []
        adverse: list[tuple[int, float]] = []
        for idx in range(entry_idx, exit_idx + 1):
            step_ts = ordered_ts[idx]
            price_ret = steps[step_ts]["close"] / entry_close - 1.0
            favorable.append((idx - entry_idx, price_ret))
            adverse.append((idx - entry_idx, price_ret))
        max_favorable_offset, mfe = max(favorable, key=lambda item: item[1])
        max_adverse_offset, mae = min(adverse, key=lambda item: item[1])
        exit_gap = int(trade["holding_steps"]) - int(max_favorable_offset)
        mfe_rows.append(
            {
                "trade_id": int(trade["trade_id"]),
                "entry_ts": entry_ts,
                "exit_ts": exit_ts,
                "entry_regime": entry_regime,
                "holding_steps": int(trade["holding_steps"]),
                "net_pnl": float(trade["net_pnl"]),
                "mfe": mfe,
                "mae": mae,
                "time_to_mfe": int(max_favorable_offset),
                "time_to_mae": int(max_adverse_offset),
                "exit_time_vs_mfe_gap": exit_gap,
            }
        )
        regime_mfe_summary.setdefault(entry_regime, []).append(
            {
                "mfe": mfe,
                "mae": mae,
                "exit_gap": float(exit_gap),
                "time_to_mfe": float(max_favorable_offset),
                "holding_steps": float(trade["holding_steps"]),
                "win_flag": 1.0 if float(trade["net_pnl"]) > 0 else 0.0,
            }
        )

    with (out_dir / "exit_reason_forward_drift.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(exit_drift_rows[0].keys()))
        writer.writeheader()
        writer.writerows(exit_drift_rows)

    with (out_dir / "trade_path_archetypes.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(archetype_rows[0].keys()))
        writer.writeheader()
        writer.writerows(archetype_rows)

    with (out_dir / "trade_path_examples.json").open("w") as fh:
        json.dump(archetype_examples, fh, indent=2)

    with (out_dir / "mfe_analysis.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(mfe_rows[0].keys()))
        writer.writeheader()
        writer.writerows(mfe_rows)

    regime_exit_rows: list[dict[str, object]] = []
    for regime in ("choppy_high_vol", "sideways_low_vol"):
        for exit_reason in ("upper_band_exit", "adx_bearish_flush", "other"):
            row: dict[str, object] = {"entry_regime": regime, "exit_reason": exit_reason}
            for horizon in DRIFT_HORIZONS:
                values = regime_exit_summary.get((regime, exit_reason, horizon), [])
                row[f"avg_forward_return_{horizon}"] = _avg(values) if values else None
                row[f"median_forward_return_{horizon}"] = median(values) if values else None
                row[f"count_{horizon}"] = len(values)
            regime_exit_rows.append(row)
    with (out_dir / "regime_exit_drift_comparison.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(regime_exit_rows[0].keys()))
        writer.writeheader()
        writer.writerows(regime_exit_rows)

    regime_mfe_rows: list[dict[str, object]] = []
    for regime in ("choppy_high_vol", "sideways_low_vol"):
        values = regime_mfe_summary.get(regime, [])
        regime_mfe_rows.append(
            {
                "entry_regime": regime,
                "trades": len(values),
                "avg_mfe": _avg([item["mfe"] for item in values]) if values else 0.0,
                "avg_mae": _avg([item["mae"] for item in values]) if values else 0.0,
                "avg_time_to_mfe": _avg([item["time_to_mfe"] for item in values]) if values else 0.0,
                "avg_exit_time_vs_mfe_gap": _avg([item["exit_gap"] for item in values]) if values else 0.0,
                "median_exit_time_vs_mfe_gap": median([item["exit_gap"] for item in values]) if values else 0.0,
                "avg_holding_steps": _avg([item["holding_steps"] for item in values]) if values else 0.0,
                "win_rate_pct": _avg([item["win_flag"] for item in values]) * 100.0 if values else 0.0,
            }
        )
    with (out_dir / "regime_mfe_comparison.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(regime_mfe_rows[0].keys()))
        writer.writeheader()
        writer.writerows(regime_mfe_rows)

    exit_reason_summary: dict[str, dict[str, float]] = {}
    for reason in ("upper_band_exit", "adx_bearish_flush", "other"):
        subset = [row for row in exit_drift_rows if row["exit_reason"] == reason]
        exit_reason_summary[reason] = {
            "trades": len(subset),
            "avg_forward_return_12": _avg([float(row["forward_return_12"]) for row in subset if row["forward_return_12"] is not None]),
            "avg_forward_return_24": _avg([float(row["forward_return_24"]) for row in subset if row["forward_return_24"] is not None]),
        }

    archetype_summary: dict[str, dict[str, float]] = {}
    for archetype, examples in archetype_examples.items():
        trade_ids = {int(item["trade_id"]) for item in examples}
        archetype_summary[archetype] = {"example_count": len(examples), "example_trade_ids": sorted(trade_ids)}

    choppy_exit = next(row for row in regime_mfe_rows if row["entry_regime"] == "choppy_high_vol")
    sideways_exit = next(row for row in regime_mfe_rows if row["entry_regime"] == "sideways_low_vol")
    choppy_win_values = [
        item for item in regime_mfe_summary.get("choppy_high_vol", []) if item["win_flag"] > 0.5
    ]
    sideways_win_values = [
        item for item in regime_mfe_summary.get("sideways_low_vol", []) if item["win_flag"] > 0.5
    ]
    choppy_win_peak_share = (
        _avg([item["time_to_mfe"] / item["holding_steps"] for item in choppy_win_values if item["holding_steps"] > 0])
        if choppy_win_values
        else 0.0
    )
    sideways_win_peak_share = (
        _avg([item["time_to_mfe"] / item["holding_steps"] for item in sideways_win_values if item["holding_steps"] > 0])
        if sideways_win_values
        else 0.0
    )

    summary = {
        "run_id": run_id,
        "trades_analyzed": len(mfe_rows),
        "exit_reason_summary": exit_reason_summary,
        "regime_mfe_comparison": regime_mfe_rows,
        "archetype_examples": archetype_summary,
        "conclusion": {
            "exits_look_early": exit_reason_summary["upper_band_exit"]["avg_forward_return_12"] > 0.0,
            "winning_trades_show_delayed_peaks": choppy_win_peak_share > 0.6,
            "exit_design_is_next_lever": (
                exit_reason_summary["upper_band_exit"]["avg_forward_return_24"] > 0.0
                or exit_reason_summary["adx_bearish_flush"]["avg_forward_return_24"] > 0.0
            ),
            "path_dependence_is_material": choppy_exit["avg_mfe"] > abs(choppy_exit["avg_mae"]),
            "choppy_winner_peak_share": choppy_win_peak_share,
            "sideways_winner_peak_share": sideways_win_peak_share,
            "notes": [
                "Positive forward drift after exit means some profitable motion continues after the strategy is flat.",
                "Large exit_time_vs_MFE_gap indicates the strategy usually exits after the local peak, so the problem is not classic pre-peak selling.",
                "Compare choppy_high_vol vs sideways_low_vol to separate noisy path dependence from a regime-specific exit issue.",
            ],
        },
    }
    with (out_dir / "trace_diagnosis_summary.json").open("w") as fh:
        json.dump(summary, fh, indent=2)

    report_lines = [
        f"# Trace Diagnosis Report: {run_id}",
        "",
        f"- Trades analyzed: `{len(mfe_rows)}`",
        f"- Upper-band exit avg forward return at 12 bars: `{exit_reason_summary['upper_band_exit']['avg_forward_return_12']:.4%}`",
        f"- Upper-band exit avg forward return at 24 bars: `{exit_reason_summary['upper_band_exit']['avg_forward_return_24']:.4%}`",
        f"- Choppy avg MFE: `{choppy_exit['avg_mfe']:.4%}` vs avg MAE: `{choppy_exit['avg_mae']:.4%}`",
        f"- Choppy avg time_to_MFE: `{choppy_exit['avg_time_to_mfe']:.2f}` bars of `{choppy_exit['avg_holding_steps']:.2f}`",
        f"- Choppy avg exit_time_vs_MFE_gap: `{choppy_exit['avg_exit_time_vs_mfe_gap']:.2f}` bars",
        f"- Sideways avg exit_time_vs_MFE_gap: `{sideways_exit['avg_exit_time_vs_mfe_gap']:.2f}` bars",
        f"- Choppy winner peak share: `{choppy_win_peak_share:.2%}` of holding window",
        f"- Sideways winner peak share: `{sideways_win_peak_share:.2%}` of holding window",
        "",
        "## Interpretation",
        "",
        "- Positive forward drift after `upper_band_exit` supports some post-exit continuation, but it does not by itself prove systematically early exits.",
        "- Positive `exit_time_vs_MFE_gap` means the strategy usually exits after the local peak rather than before it.",
        "- Winner peak share near the back half of the holding window means many winning trades need time to mature before peaking.",
        "- If choppy trades have stronger MFE than MAE, path-dependent noise is present but entries still have directional edge.",
        "",
        "## Preliminary Verdict",
        "",
        f"- `exits_look_early`: `{summary['conclusion']['exits_look_early']}`",
        f"- `winning_trades_show_delayed_peaks`: `{summary['conclusion']['winning_trades_show_delayed_peaks']}`",
        f"- `path_dependence_is_material`: `{summary['conclusion']['path_dependence_is_material']}`",
        f"- `exit_design_is_next_lever`: `{summary['conclusion']['exit_design_is_next_lever']}`",
    ]
    (out_dir / "trace_diagnosis_report.md").write_text("\n".join(report_lines))


if __name__ == "__main__":
    main()
