from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ingestion.option_chain.source import OptionChainFileSource
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.derivatives.option_chain.helpers import _annotate_snapshot_rows, _infer_data_ts, _tick_from_payload

TAU_TARGETS_MS: dict[str, int] = {
    "7d": 7 * 24 * 60 * 60 * 1000,
    "30d": 30 * 24 * 60 * 60 * 1000,
    "90d": 90 * 24 * 60 * 60 * 1000,
}
X_MAXS: tuple[float, ...] = (0.05, 0.10)
EXCEED_THRESHOLDS: tuple[float, ...] = (0.10, 0.20, 0.30)
MIN_NS: tuple[int, ...] = (6, 4)


@dataclass
class GroupAgg:
    snapshot_count: int = 0
    n_total_in_slice: int = 0
    n_eligible_in_slice: int = 0
    n_corridor_rows: int = 0
    spreads: list[np.ndarray] | None = None


def _utc_day(ms: int) -> str:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def _discover_files(base_root: Path, *, symbol: str, interval: str, year: int, month: int | None, max_days: int | None) -> list[Path]:
    year_dir = base_root / symbol / interval / f"{year:04d}"
    if not year_dir.exists():
        return []
    files = sorted(year_dir.glob("*.parquet"))
    if month is not None:
        prefix = f"{year:04d}_{month:02d}_"
        files = [p for p in files if p.name.startswith(prefix)]
    if max_days is not None and max_days > 0:
        files = files[: int(max_days)]
    return files


def _handler_for(symbol: str, interval: str) -> OptionChainDataHandler:
    return OptionChainDataHandler(
        symbol=symbol,
        preset="option_chain",
        interval=interval,
        source="DERIBIT",
        mode="backtest",
        quality_mode="TRADING",
    )


def _spread_ratio(df: pd.DataFrame, *, eps: float) -> np.ndarray:
    if "bid_price" not in df.columns or "ask_price" not in df.columns:
        return np.array([], dtype=float)
    bid = pd.to_numeric(df["bid_price"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    ask = pd.to_numeric(df["ask_price"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)

    if "mid_price" in df.columns:
        mid = pd.to_numeric(df["mid_price"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    elif "mark_price" in df.columns:
        mid = pd.to_numeric(df["mark_price"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    else:
        mid = (bid + ask) / 2.0

    ok = (~np.isnan(bid)) & (~np.isnan(ask)) & (~np.isnan(mid))
    if not bool(np.any(ok)):
        return np.array([], dtype=float)
    denom = np.maximum(np.abs(mid[ok]), float(eps))
    ratio = (ask[ok] - bid[ok]) / denom
    ratio = ratio[np.isfinite(ratio)]
    return ratio.astype(float, copy=False)


def _concat_spreads(parts: list[np.ndarray] | None) -> np.ndarray:
    if not parts:
        return np.array([], dtype=float)
    if len(parts) == 1:
        return parts[0]
    return np.concatenate(parts)


def _quantile_or_nan(x: np.ndarray, q: float) -> float:
    if x.size == 0:
        return float("nan")
    return float(np.quantile(x, q))


def _build_markdown_table(df: pd.DataFrame, cols: list[str]) -> str:
    if df.empty:
        return "(no rows)"
    out = df[cols].copy()
    return out.to_markdown(index=False)


def run_calibration(
    *,
    symbols: list[str],
    year: int,
    month: int | None,
    interval: str,
    max_days: int | None,
    output_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    data_root = Path("data/cleaned/option_chain")

    group_aggs: dict[tuple[str, str, str, float], GroupAgg] = {}
    snapshot_rows: list[dict[str, Any]] = []
    dataset_rows: list[dict[str, Any]] = []

    for symbol in symbols:
        files = _discover_files(data_root, symbol=symbol, interval=interval, year=year, month=month, max_days=max_days)
        if len(files) < 3:
            continue

        source = OptionChainFileSource(
            root=data_root,
            asset=symbol,
            interval=interval,
            paths=[p.resolve() for p in files],
        )
        handler = _handler_for(symbol, interval)
        source_id = getattr(handler, "source_id", None)
        display_symbol = str(getattr(handler, "display_symbol", symbol))
        eps = float(handler.quality_cfg.get("eps", 1e-12))
        row_policy_cfg = dict(getattr(handler, "row_policy_cfg", {}))

        loaded = 0
        first_ts: int | None = None
        last_ts: int | None = None

        for raw_payload in source:
            ts = int(_infer_data_ts(raw_payload))
            tick = _tick_from_payload(raw_payload, symbol=display_symbol, source_id=source_id)
            handler.align_to(ts)
            handler.on_new_tick(tick)
            loaded += 1
            first_ts = ts if first_ts is None else min(first_ts, ts)
            last_ts = ts if last_ts is None else max(last_ts, ts)

            day = _utc_day(ts)
            for tau_label, tau_target_ms in TAU_TARGETS_MS.items():
                tau_df, _ = handler.select_tau(ts=ts, tau_ms=int(tau_target_ms), method="nearest_bucket", quality_mode="TRADING")
                n_total_slice = int(len(tau_df))

                if n_total_slice > 0:
                    _, row_mask = _annotate_snapshot_rows(tau_df, row_policy_cfg=row_policy_cfg)
                    eligible_df = tau_df.loc[row_mask].copy()
                else:
                    eligible_df = pd.DataFrame()

                n_eligible = int(len(eligible_df))

                for x_max in X_MAXS:
                    if n_eligible > 0 and "x" in eligible_df.columns:
                        x = pd.to_numeric(eligible_df["x"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
                        x_mask = np.isfinite(x) & (np.abs(x) <= float(x_max))
                        corridor_df = eligible_df.loc[x_mask].copy()
                    else:
                        corridor_df = pd.DataFrame()

                    spreads = _spread_ratio(corridor_df, eps=eps)
                    n_corridor = int(len(spreads))
                    mask_cov = (float(n_corridor) / float(n_total_slice)) if n_total_slice > 0 else 0.0

                    snapshot_rows.append(
                        {
                            "symbol": symbol,
                            "day": day,
                            "snapshot_ts": ts,
                            "tau_target": tau_label,
                            "x_max": float(x_max),
                            "n_total_in_slice": n_total_slice,
                            "n_eligible_in_slice": n_eligible,
                            "n_corridor_rows": n_corridor,
                            "mask_coverage_corridor": mask_cov,
                            "p75": _quantile_or_nan(spreads, 0.75),
                            "p90": _quantile_or_nan(spreads, 0.90),
                        }
                    )

                    key = (symbol, day, tau_label, float(x_max))
                    agg = group_aggs.get(key)
                    if agg is None:
                        agg = GroupAgg(spreads=[])
                        group_aggs[key] = agg
                    agg.snapshot_count += 1
                    agg.n_total_in_slice += n_total_slice
                    agg.n_eligible_in_slice += n_eligible
                    agg.n_corridor_rows += n_corridor
                    if n_corridor > 0:
                        assert agg.spreads is not None
                        agg.spreads.append(spreads)

        dataset_rows.append(
            {
                "symbol": symbol,
                "files": ", ".join(p.name for p in files),
                "n_files": len(files),
                "first_day": files[0].stem.replace("_", "-"),
                "last_day": files[-1].stem.replace("_", "-"),
                "snapshot_count": loaded,
                "first_ts": first_ts,
                "last_ts": last_ts,
            }
        )

    metrics_rows: list[dict[str, Any]] = []
    for (symbol, day, tau_label, x_max), agg in sorted(group_aggs.items()):
        spreads = _concat_spreads(agg.spreads)
        row = {
            "symbol": symbol,
            "day": day,
            "tau_target": tau_label,
            "x_max": x_max,
            "snapshot_count": int(agg.snapshot_count),
            "n_total_in_slice": int(agg.n_total_in_slice),
            "n_eligible_in_slice": int(agg.n_eligible_in_slice),
            "n_corridor_rows": int(agg.n_corridor_rows),
            "mask_coverage_corridor": (float(agg.n_corridor_rows) / float(agg.n_total_in_slice)) if agg.n_total_in_slice > 0 else 0.0,
            "p50": _quantile_or_nan(spreads, 0.50),
            "p75": _quantile_or_nan(spreads, 0.75),
            "p90": _quantile_or_nan(spreads, 0.90),
            "p95": _quantile_or_nan(spreads, 0.95),
        }
        for t in EXCEED_THRESHOLDS:
            if spreads.size == 0:
                row[f"exceed_gt_{t:.2f}"] = float("nan")
            else:
                row[f"exceed_gt_{t:.2f}"] = float(np.mean(spreads > float(t)))
        metrics_rows.append(row)

    metrics_df = pd.DataFrame(metrics_rows)
    snapshots_df = pd.DataFrame(snapshot_rows)
    dataset_df = pd.DataFrame(dataset_rows)

    if not snapshots_df.empty:
        sens_rows: list[dict[str, Any]] = []
        for (symbol, day, tau_label, x_max), g in snapshots_df.groupby(["symbol", "day", "tau_target", "x_max"], sort=True):
            row = {
                "symbol": symbol,
                "day": day,
                "tau_target": tau_label,
                "x_max": float(x_max), # type: ignore
                "snapshot_count": int(len(g)),
            }
            for n in MIN_NS:
                row[f"pass_min_n_{n}"] = float((g["n_corridor_rows"] >= int(n)).mean())
            sens_rows.append(row)
        sensitivity_df = pd.DataFrame(sens_rows)
    else:
        sensitivity_df = pd.DataFrame()

    output_dir.mkdir(parents=True, exist_ok=True)
    dataset_df.to_csv(output_dir / "spread_liquidity_dataset_summary.csv", index=False)
    metrics_df.to_csv(output_dir / "spread_liquidity_metrics_by_day.csv", index=False)
    snapshots_df.to_csv(output_dir / "spread_liquidity_snapshot_metrics.csv", index=False)
    sensitivity_df.to_csv(output_dir / "spread_liquidity_min_n_sensitivity.csv", index=False)

    return dataset_df, metrics_df, snapshots_df, sensitivity_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Option-chain spread liquidity calibration (slice-level).")
    parser.add_argument("--symbol", action="append", default=[], help="Symbol(s), repeatable. Default: BTC and ETH")
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--month", type=int, default=None)
    parser.add_argument("--interval", type=str, default="5m")
    parser.add_argument("--max-days", type=int, default=None)
    parser.add_argument("--output-dir", type=Path, default=Path("docs/agents/option_chain_qc"))
    args = parser.parse_args()

    symbols = [s.upper() for s in args.symbol] if args.symbol else ["BTC", "ETH"]

    dataset_df, metrics_df, snapshots_df, sensitivity_df = run_calibration(
        symbols=symbols,
        year=int(args.year),
        month=args.month,
        interval=str(args.interval),
        max_days=args.max_days,
        output_dir=args.output_dir,
    )

    print("datasets:", len(dataset_df))
    print("metric rows:", len(metrics_df))
    print("snapshot metric rows:", len(snapshots_df))
    print("sensitivity rows:", len(sensitivity_df))


if __name__ == "__main__":
    main()
