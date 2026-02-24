from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ingestion.option_chain.source import OptionChainFileSource
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.derivatives.option_chain.helpers import _infer_data_ts, _tick_from_payload

DAY_MS = 86_400_000
TAU_TARGETS = {
    "7d": 7 * DAY_MS,
    "30d": 30 * DAY_MS,
    "90d": 90 * DAY_MS,
}

LIQ_DIAG_XS = (0.10, 0.05)


@dataclass(frozen=True)
class DatasetDay:
    symbol: str
    interval: str
    date: str  # YYYY-MM-DD
    parquet_path: Path


def _day_bounds_ms(day: str) -> tuple[int, int]:
    d0 = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    d1 = d0 + timedelta(days=1) - timedelta(milliseconds=1)
    return int(d0.timestamp() * 1000), int(d1.timestamp() * 1000)


def discover_days(root: Path, symbol: str, interval: str, year: str = "2026") -> list[DatasetDay]:
    d = root / symbol / interval / year
    if not d.exists():
        return []
    out: list[DatasetDay] = []
    for fp in sorted(d.glob("*.parquet")):
        stem = fp.stem  # YYYY_MM_DD
        parts = stem.split("_")
        if len(parts) != 3:
            continue
        try:
            date = f"{int(parts[0]):04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
        except ValueError:
            continue
        out.append(DatasetDay(symbol=symbol, interval=interval, date=date, parquet_path=fp))
    return out


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        v = float(x)
        if np.isnan(v) or np.isinf(v):
            return None
        return v
    except Exception:
        return None


def _liquidity_diag_from_slice(
    handler: OptionChainDataHandler,
    *,
    slice_df: pd.DataFrame,
    snapshot_data_ts: int,
    x_max: float,
) -> dict[str, Any]:
    n_rows = int(len(slice_df))
    if n_rows == 0:
        return {
            "x_max": float(x_max),
            "n_corridor": 0,
            "corridor_coverage": 0.0,
            "p75": None,
            "p90": None,
        }

    eligible = np.ones(n_rows, dtype=bool)
    snap = handler.get_snapshot(snapshot_data_ts)
    snap_mask = getattr(snap, "row_mask", None) if snap is not None else None
    if (
        isinstance(snap_mask, np.ndarray)
        and snap_mask.ndim == 1
        and snap_mask.dtype == np.dtype("bool")
        and np.issubdtype(slice_df.index.to_numpy().dtype, np.integer)
    ):
        idx = slice_df.index.to_numpy(dtype=np.int64, copy=False)
        if idx.size > 0 and int(idx.min()) >= 0 and int(idx.max()) < int(len(snap_mask)):
            aligned = True
            if snap is not None and "instrument_name" in slice_df.columns and "instrument_name" in snap.chain_frame.columns:
                snap_names = snap.chain_frame["instrument_name"].iloc[idx].to_numpy(dtype=object, copy=False)
                slice_names = slice_df["instrument_name"].to_numpy(dtype=object, copy=False)
                aligned = np.array_equal(snap_names, slice_names)
            if aligned:
                eligible = snap_mask[idx]

    x_vals = (
        pd.to_numeric(slice_df["x"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        if "x" in slice_df.columns
        else np.full(n_rows, np.nan, dtype=float)
    )
    corridor = eligible & np.isfinite(x_vals) & (np.abs(x_vals) <= float(x_max))
    n_corridor = int(corridor.sum())
    coverage = float(n_corridor) / float(max(1, n_rows))

    spread = np.array([], dtype=float)
    if "bid_price" in slice_df.columns and "ask_price" in slice_df.columns:
        bid = pd.to_numeric(slice_df["bid_price"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        ask = pd.to_numeric(slice_df["ask_price"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        if "mid_price" in slice_df.columns:
            mid = pd.to_numeric(slice_df["mid_price"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        elif "mark_price" in slice_df.columns:
            mid = pd.to_numeric(slice_df["mark_price"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        else:
            mid = (bid + ask) / 2.0
        mask = corridor & np.isfinite(bid) & np.isfinite(ask) & np.isfinite(mid)
        if bool(np.any(mask)):
            eps = float(handler.quality_cfg.get("eps", 1e-12))
            denom = np.maximum(np.abs(mid[mask]), eps)
            spread = ((ask[mask] - bid[mask]) / denom).astype(float, copy=False)
            spread = spread[np.isfinite(spread)]

    p75 = float(np.quantile(spread, 0.75)) if spread.size > 0 else None
    p90 = float(np.quantile(spread, 0.90)) if spread.size > 0 else None

    return {
        "x_max": float(x_max),
        "n_corridor": n_corridor,
        "corridor_coverage": float(coverage),
        "p75": _safe_float(p75),
        "p90": _safe_float(p90),
    }


def replay_day(
    *,
    data_root: Path,
    day: DatasetDay,
    quality_mode: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    start_ts, end_ts = _day_bounds_ms(day.date)
    source = OptionChainFileSource(
        root=data_root,
        asset=day.symbol,
        interval=day.interval,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    handler = OptionChainDataHandler(symbol=day.symbol, interval=day.interval, preset="option_chain")

    snapshot_rows: list[dict[str, Any]] = []
    reason_rows: list[dict[str, Any]] = []
    selection_rows: list[dict[str, Any]] = []

    for raw_payload in source:
        ts = int(_infer_data_ts(raw_payload))
        tick = _tick_from_payload(raw_payload, symbol=handler.display_symbol, source_id=handler.source_id)
        handler.align_to(ts)
        handler.on_new_tick(tick)

        report = handler.qc_report(ts=ts, quality_mode=quality_mode)
        summary = dict(report.get("summary") or {})
        reasons = list(report.get("reasons") or [])

        chain_state = str(summary.get("state") or "")
        tradable = summary.get("tradable")
        qc_scope = summary.get("qc_scope")

        snapshot_row = {
            "symbol": day.symbol,
            "interval": day.interval,
            "date": day.date,
            "snapshot_data_ts": int(report.get("snapshot_data_ts") or ts),
            "qc_scope": qc_scope,
            "row_policy_hash": report.get("row_policy_hash"),
            "summary_ok": bool(summary.get("ok")),
            "summary_state": chain_state,
            "summary_tradable": tradable,
            "n_total_rows": int(summary.get("n_rows") or 0),
            "n_masked_rows": int(summary.get("n_eligible_rows") or 0),
            "mask_coverage": _safe_float(summary.get("mask_coverage")),
            "n_valid_x": int(summary.get("n_valid_x") or 0),
            "n_valid_tau": int(summary.get("n_valid_tau") or 0),
            "n_quotes": int(summary.get("n_quotes") or 0),
            "chain_reason_count": len(reasons),
            "chain_reasons_json": json.dumps(reasons, ensure_ascii=False),
            "chain_reason_codes": "|".join(sorted({str(r.get("code") or "") for r in reasons if r.get("code")})),
        }

        # Parameter metrics used for preset-friendliness audit
        snap = handler.get_snapshot(ts)
        if snap is not None:
            # Offline audit path: use merged snapshot frame so quote fields are present.
            df = snap.frame
            mask = getattr(snap, "row_mask", None)
            use_mask = isinstance(mask, np.ndarray) and mask.dtype == np.dtype("bool") and len(mask) == len(df) and bool(mask.any())
            df_qc = df[mask] if use_mask else df

            # Chain spread max ratio metric
            max_spread_ratio = None
            bid_col = "bid_price" if "bid_price" in df_qc.columns else ("bid" if "bid" in df_qc.columns else None)
            ask_col = "ask_price" if "ask_price" in df_qc.columns else ("ask" if "ask" in df_qc.columns else None)
            mid_col = "mid_price" if "mid_price" in df_qc.columns else ("mid" if "mid" in df_qc.columns else None)
            mark_col = "mark_price" if "mark_price" in df_qc.columns else ("mark" if "mark" in df_qc.columns else None)
            oi_col = "open_interest" if "open_interest" in df_qc.columns else ("oi" if "oi" in df_qc.columns else None)

            if bid_col is not None and ask_col is not None and len(df_qc) > 0:
                bid = pd.to_numeric(df_qc[bid_col], errors="coerce")
                ask = pd.to_numeric(df_qc[ask_col], errors="coerce")
                valid = bid.notna() & ask.notna()
                if bool(valid.any()):
                    if mid_col is not None:
                        mid_like = pd.to_numeric(df_qc[mid_col], errors="coerce")
                    elif mark_col is not None:
                        mid_like = pd.to_numeric(df_qc[mark_col], errors="coerce")
                    else:
                        mid_like = (bid + ask) / 2.0
                    eps = float(handler.quality_cfg.get("eps", 1e-12))
                    denom = mid_like.abs().clip(lower=eps)
                    sr = ((ask - bid) / denom).replace([np.inf, -np.inf], np.nan).dropna()
                    if not sr.empty:
                        max_spread_ratio = float(sr.max())

            # OI zero ratio metric
            oi_zero_ratio_obs = None
            if (oi_col is not None) and len(df_qc) > 0:
                oi = pd.to_numeric(df_qc[oi_col], errors="coerce").dropna()
                if not oi.empty:
                    oi_zero_ratio_obs = float((oi <= 0).sum()) / float(len(oi))

            snapshot_row["obs_max_spread_ratio"] = _safe_float(max_spread_ratio)
            snapshot_row["obs_oi_zero_ratio"] = _safe_float(oi_zero_ratio_obs)
        else:
            snapshot_row["obs_max_spread_ratio"] = None
            snapshot_row["obs_oi_zero_ratio"] = None

        snapshot_rows.append(snapshot_row)

        for r in reasons:
            reason_rows.append(
                {
                    "symbol": day.symbol,
                    "interval": day.interval,
                    "date": day.date,
                    "snapshot_data_ts": snapshot_row["snapshot_data_ts"],
                    "domain": "chain",
                    "state": chain_state,
                    "reason_code": str(r.get("code") or ""),
                    "severity": str(r.get("severity") or ""),
                    "ctx_json": json.dumps(r.get("ctx") or {}, ensure_ascii=False),
                }
            )

        # Selection-level liquidity diagnostics: 7d / 30d / 90d
        for tau_name, tau_ms in TAU_TARGETS.items():
            slice_df, meta_sel = handler.select_tau(
                ts=ts,
                tau_ms=int(tau_ms),
                method="nearest_bucket",
                quality_mode=quality_mode,
            )
            liq = dict(meta_sel.get("liquidity") or {})
            sel_reasons = list(meta_sel.get("reasons") or [])
            sel_state = str(meta_sel.get("state") or "")

            d010 = _liquidity_diag_from_slice(
                handler,
                slice_df=slice_df,
                snapshot_data_ts=int(snapshot_row["snapshot_data_ts"]),
                x_max=0.10,
            )
            d005 = _liquidity_diag_from_slice(
                handler,
                slice_df=slice_df,
                snapshot_data_ts=int(snapshot_row["snapshot_data_ts"]),
                x_max=0.05,
            )

            n_slice_rows = int(len(slice_df))
            tau_actual_abs_err_ms = None
            if n_slice_rows > 0 and "tau_ms" in slice_df.columns:
                tau_vals = pd.to_numeric(slice_df["tau_ms"], errors="coerce").dropna()
                if not tau_vals.empty:
                    tau_actual_abs_err_ms = float(np.abs(float(tau_vals.median()) - float(tau_ms)))

            row = {
                "symbol": day.symbol,
                "interval": day.interval,
                "date": day.date,
                "snapshot_data_ts": snapshot_row["snapshot_data_ts"],
                "tau_target": tau_name,
                "tau_target_ms": int(tau_ms),
                "selection_state": sel_state,
                "selection_tradable": meta_sel.get("tradable"),
                "n_slice_rows": n_slice_rows,
                "tau_actual_abs_err_ms": _safe_float(tau_actual_abs_err_ms),
                "liquidity_enabled": bool(liq.get("enabled", False)),
                "liquidity_state": liq.get("state"),
                "liquidity_ok": liq.get("ok"),
                "liq_n_corridor_x010": d010.get("n_corridor"),
                "liq_cov_x010": d010.get("corridor_coverage"),
                "liq_p75_x010": d010.get("p75"),
                "liq_p90_x010": d010.get("p90"),
                "liq_n_corridor_x005": d005.get("n_corridor"),
                "liq_cov_x005": d005.get("corridor_coverage"),
                "liq_p75_x005": d005.get("p75"),
                "liq_p90_x005": d005.get("p90"),
                "liquidity_meta_json": json.dumps(liq, ensure_ascii=False),
                "selection_reasons_json": json.dumps(sel_reasons, ensure_ascii=False),
                "selection_reason_codes": "|".join(sorted({str(r.get("reason_code") or "") for r in sel_reasons if r.get("reason_code")})),
            }
            selection_rows.append(row)

            for r in sel_reasons:
                code = str(r.get("reason_code") or "")
                if not code:
                    continue
                reason_rows.append(
                    {
                        "symbol": day.symbol,
                        "interval": day.interval,
                        "date": day.date,
                        "snapshot_data_ts": snapshot_row["snapshot_data_ts"],
                        "domain": "selection",
                        "state": sel_state,
                        "reason_code": code,
                        "severity": str(r.get("severity") or ""),
                        "ctx_json": json.dumps(r.get("details") or {}, ensure_ascii=False),
                    }
                )

    return snapshot_rows, reason_rows, selection_rows


def _parse_ctx_numeric(ctx_json: str) -> dict[str, float]:
    try:
        obj = json.loads(ctx_json) if ctx_json else {}
    except Exception:
        return {}
    out: dict[str, float] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            try:
                fv = float(v)
            except Exception:
                continue
            if np.isfinite(fv):
                out[str(k)] = float(fv)
    return out


def build_reason_leaderboard(reason_df: pd.DataFrame) -> pd.DataFrame:
    # Chain soft-degrade reasons leaderboard + payload stats
    soft_chain = reason_df[(reason_df["domain"] == "chain") & (reason_df["state"] == "SOFT_DEGRADED")].copy()
    if soft_chain.empty:
        return pd.DataFrame(columns=[
            "symbol", "date", "reason_code", "count", "share_of_soft", "severity_mode", "typical_payload_ranges"
        ])

    total_soft = (
        soft_chain.groupby(["symbol", "date", "snapshot_data_ts"], as_index=False)
        .size()
        .groupby(["symbol", "date"], as_index=False)["snapshot_data_ts"].nunique()
        .rename(columns={"snapshot_data_ts": "n_soft_snapshots"})
    ) # type: ignore # type

    grp = (
        soft_chain.groupby(["symbol", "date", "reason_code"], as_index=False)
        .agg(count=("snapshot_data_ts", "count"), severity_mode=("severity", lambda s: s.mode().iloc[0] if not s.mode().empty else ""))
    )
    grp = grp.merge(total_soft, on=["symbol", "date"], how="left")
    grp["share_of_soft"] = grp["count"] / grp["n_soft_snapshots"].clip(lower=1)

    payload_stats: list[dict[str, Any]] = []
    for (sym, date, reason), part in soft_chain.groupby(["symbol", "date", "reason_code"]):
        nums: dict[str, list[float]] = {}
        for ctx in part["ctx_json"].astype(str):
            for k, v in _parse_ctx_numeric(ctx).items():
                nums.setdefault(k, []).append(v)
        summary: dict[str, Any] = {}
        for k, arr in nums.items():
            a = np.asarray(arr, dtype=float)
            if a.size == 0:
                continue
            summary[k] = {
                "p10": float(np.quantile(a, 0.10)),
                "p50": float(np.quantile(a, 0.50)),
                "p90": float(np.quantile(a, 0.90)),
            }
        payload_stats.append(
            {
                "symbol": sym,
                "date": date,
                "reason_code": reason,
                "typical_payload_ranges": json.dumps(summary, ensure_ascii=False),
            }
        )

    out = grp.merge(pd.DataFrame(payload_stats), on=["symbol", "date", "reason_code"], how="left")
    out = out.sort_values(["symbol", "date", "count"], ascending=[True, True, False]).reset_index(drop=True)
    return out[["symbol", "date", "reason_code", "count", "share_of_soft", "severity_mode", "typical_payload_ranges"]]


def build_by_day(snapshot_df: pd.DataFrame, selection_df: pd.DataFrame) -> pd.DataFrame:
    chain = (
        snapshot_df.groupby(["symbol", "date"], as_index=False)
        .agg(
            snapshots=("snapshot_data_ts", "nunique"),
            chain_ok=("summary_state", lambda s: int((s == "OK").sum())),
            chain_soft=("summary_state", lambda s: int((s == "SOFT_DEGRADED").sum())),
            chain_hard=("summary_state", lambda s: int((s == "HARD_FAIL").sum())),
            avg_mask_coverage=("mask_coverage", "mean"),
            p50_n_rows=("n_total_rows", lambda s: float(np.quantile(np.asarray(s, dtype=float), 0.50)) if len(s) else np.nan),
        )
    )

    sel = (
        selection_df.groupby(["symbol", "date"], as_index=False)
        .agg(
            selection_ok=("liquidity_state", lambda s: int((s == "OK").sum())),
            selection_soft=("liquidity_state", lambda s: int((s == "SOFT_DEGRADED").sum())),
            selection_hard=("liquidity_state", lambda s: int((s == "HARD_FAIL").sum())),
            selection_rows=("snapshot_data_ts", "count"),
            p50_liq_p75_x010=("liq_p75_x010", "median"),
            p90_liq_p90_x010=("liq_p90_x010", lambda s: float(np.nanquantile(np.asarray(s, dtype=float), 0.90)) if len(s) else np.nan),
        )
    )
    out = chain.merge(sel, on=["symbol", "date"], how="left")
    out["chain_soft_rate"] = out["chain_soft"] / out["snapshots"].clip(lower=1)
    out["chain_hard_rate"] = out["chain_hard"] / out["snapshots"].clip(lower=1)
    out["selection_soft_rate"] = out["selection_soft"] / out["selection_rows"].clip(lower=1)
    out["selection_hard_rate"] = out["selection_hard"] / out["selection_rows"].clip(lower=1)
    return out.sort_values(["symbol", "date"]).reset_index(drop=True)


def build_snapshot_sample(snapshot_df: pd.DataFrame, selection_df: pd.DataFrame, n: int = 200) -> pd.DataFrame:
    key_cols = ["symbol", "date", "snapshot_data_ts"]
    sel_agg = (
        selection_df.groupby(key_cols, as_index=False)
        .agg(
            selection_reason_codes=("selection_reason_codes", lambda s: "|".join(sorted({x for x in s if isinstance(x, str) and x}))),
            any_liq_soft=("liquidity_state", lambda s: bool((s == "SOFT_DEGRADED").any())),
            any_liq_hard=("liquidity_state", lambda s: bool((s == "HARD_FAIL").any())),
            p50_liq_p75_x010=("liq_p75_x010", "median"),
            p50_liq_p90_x010=("liq_p90_x010", "median"),
            min_n_corridor_x010=("liq_n_corridor_x010", "min"),
        )
    )
    merged = snapshot_df.merge(sel_agg, on=key_cols, how="left")

    severe = merged[
        (merged["summary_state"] != "OK")
        | (merged["any_liq_soft"] == True)
        | (merged["any_liq_hard"] == True)
    ].copy()

    if len(severe) >= n:
        return severe.sort_values(key_cols).sample(n=n, random_state=42).sort_values(key_cols).reset_index(drop=True)

    remain = n - len(severe)
    others = merged.drop(severe.index)
    if len(others) > 0 and remain > 0:
        add = others.sort_values(key_cols).sample(n=min(remain, len(others)), random_state=42)
        severe = pd.concat([severe, add], ignore_index=True)

    return severe.sort_values(key_cols).reset_index(drop=True)


def classify_reason(code: str) -> str:
    structural = {
        "MISSING_FRAME", "MISSING_MARKET_TS", "EMPTY_CHAIN", "MISSING_UNDERLYING_REF",
        "STALE_UNDERLYING", "COVERAGE_LOW", "EXPIRY_SELECTION_AMBIGUOUS",
    }
    market_state = {"OI_ZERO", "ZOMBIE_QUOTE", "WIDE_SPREAD", "NO_QUOTES", "LIQUIDITY_COVERAGE_LOW", "LIQUIDITY_SPREAD_WIDE"}
    if code in structural:
        return "structural"
    if code in market_state:
        return "market_state"
    return "other"


def summarize_preset_friendliness(snapshot_df: pd.DataFrame, selection_df: pd.DataFrame, handler_ref: OptionChainDataHandler) -> dict[str, Any]:
    out: dict[str, Any] = {}

    spread_series = pd.to_numeric(snapshot_df["obs_max_spread_ratio"], errors="coerce").dropna()
    if len(spread_series) > 0:
        thresholds = [0.05, 0.10, 0.20, 0.30]
        out["quality.spread_max"] = {
            "quantiles": {
                "p50": float(spread_series.quantile(0.50)),
                "p75": float(spread_series.quantile(0.75)),
                "p90": float(spread_series.quantile(0.90)),
                "p95": float(spread_series.quantile(0.95)),
            },
            "pass_rates": {str(t): float((spread_series <= t).mean()) for t in thresholds},
            "current": float(handler_ref.quality_cfg.get("spread_max", np.nan)),
        }

    oi_series = pd.to_numeric(snapshot_df["obs_oi_zero_ratio"], errors="coerce").dropna()
    if len(oi_series) > 0:
        thresholds = [0.80, 0.90, 0.95, 0.98]
        out["quality.oi_zero_ratio"] = {
            "quantiles": {
                "p50": float(oi_series.quantile(0.50)),
                "p75": float(oi_series.quantile(0.75)),
                "p90": float(oi_series.quantile(0.90)),
                "p95": float(oi_series.quantile(0.95)),
            },
            "pass_rates": {str(t): float((oi_series <= t).mean()) for t in thresholds},
            "current": float(handler_ref.quality_cfg.get("oi_zero_ratio", np.nan)),
        }

    min_n_series = pd.to_numeric(selection_df["n_slice_rows"], errors="coerce").dropna()
    if len(min_n_series) > 0:
        thresholds = [6, 10, 20, 30]
        out["quality.min_n_per_slice"] = {
            "quantiles": {
                "p10": float(min_n_series.quantile(0.10)),
                "p50": float(min_n_series.quantile(0.50)),
                "p90": float(min_n_series.quantile(0.90)),
            },
            "pass_rates": {str(t): float((min_n_series >= t).mean()) for t in thresholds},
            "current": int(handler_ref.quality_cfg.get("min_n_per_slice", 0)),
        }

    tau_err = pd.to_numeric(selection_df["tau_actual_abs_err_ms"], errors="coerce").dropna()
    if len(tau_err) > 0:
        curr = float(handler_ref.quality_cfg.get("max_tau_error_ms", np.nan))
        candidates = [float(curr), 0.5 * DAY_MS, 1.0 * DAY_MS, 2.0 * DAY_MS]
        candidates = sorted({round(x, 2) for x in candidates if np.isfinite(x) and x > 0})
        out["quality.max_tau_error_ms_factor"] = {
            "quantiles": {
                "p50": float(tau_err.quantile(0.50)),
                "p75": float(tau_err.quantile(0.75)),
                "p90": float(tau_err.quantile(0.90)),
                "p95": float(tau_err.quantile(0.95)),
            },
            "pass_rates": {str(t): float((tau_err <= t).mean()) for t in candidates},
            "current_max_tau_error_ms": curr,
        }

    liq = selection_df.copy()
    liq_p75 = pd.to_numeric(liq["liq_p75_x010"], errors="coerce")
    liq_p90 = pd.to_numeric(liq["liq_p90_x010"], errors="coerce")
    liq_n = pd.to_numeric(liq["liq_n_corridor_x010"], errors="coerce")

    def _pass_rate(min_n: int, p75_max: float, p90_max: float) -> float:
        mask = (liq_n >= min_n) & liq_p75.notna() & liq_p90.notna() & (liq_p75 <= p75_max) & (liq_p90 <= p90_max)
        valid = liq_n.notna()
        return float(mask.sum()) / float(max(1, valid.sum()))

    out["liquidity_gate"] = {
        "x_max": {
            "0.10": {
                "n_corridor_p50": _safe_float(np.nanquantile(pd.to_numeric(liq["liq_n_corridor_x010"], errors="coerce"), 0.50)),
                "n_corridor_p10": _safe_float(np.nanquantile(pd.to_numeric(liq["liq_n_corridor_x010"], errors="coerce"), 0.10)),
            },
            "0.05": {
                "n_corridor_p50": _safe_float(np.nanquantile(pd.to_numeric(liq["liq_n_corridor_x005"], errors="coerce"), 0.50)),
                "n_corridor_p10": _safe_float(np.nanquantile(pd.to_numeric(liq["liq_n_corridor_x005"], errors="coerce"), 0.10)),
            },
        },
        "configs": {
            "conservative": {
                "min_n": 6,
                "p75_max": 0.20,
                "p90_max": 0.35,
                "pass_rate": _pass_rate(6, 0.20, 0.35),
            },
            "balanced": {
                "min_n": 5,
                "p75_max": 0.23,
                "p90_max": 0.40,
                "pass_rate": _pass_rate(5, 0.23, 0.40),
            },
            "lenient": {
                "min_n": 4,
                "p75_max": 0.25,
                "p90_max": 0.45,
                "pass_rate": _pass_rate(4, 0.25, 0.45),
            },
        },
    }

    return out


def write_report(
    out_md: Path,
    *,
    dataset_days: list[DatasetDay],
    snapshot_df: pd.DataFrame,
    reason_df: pd.DataFrame,
    selection_df: pd.DataFrame,
    leaderboard_df: pd.DataFrame,
    by_day_df: pd.DataFrame,
    preset_stats: dict[str, Any],
) -> None:
    out_md.parent.mkdir(parents=True, exist_ok=True)

    total_snaps = int(snapshot_df["snapshot_data_ts"].nunique()) if not snapshot_df.empty else 0
    chain_state_counts = snapshot_df["summary_state"].value_counts(dropna=False).to_dict()
    sel_state_counts = selection_df["liquidity_state"].value_counts(dropna=False).to_dict()

    soft_chain_reasons = reason_df[(reason_df["domain"] == "chain") & (reason_df["state"] == "SOFT_DEGRADED")].copy()
    if not soft_chain_reasons.empty:
        soft_chain_reasons["class"] = soft_chain_reasons["reason_code"].map(classify_reason)
        class_counts = soft_chain_reasons["class"].value_counts().to_dict()
    else:
        class_counts = {}

    lines: list[str] = []
    lines.append("# Soft Degrade Root-Cause Audit (5m, 2026)")
    lines.append("")
    lines.append(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("")
    lines.append("## Dataset Coverage")
    lines.append(f"- Root: `data/cleaned/option_chain`")
    lines.append(f"- Files replayed: {len(dataset_days)}")
    for dd in dataset_days:
        lines.append(f"- {dd.symbol} {dd.interval} {dd.date}: `{dd.parquet_path}`")
    lines.append(f"- Unique snapshots: {total_snaps}")
    lines.append("")

    lines.append("## Outcome Overview")
    lines.append(f"- Chain QC states: `{json.dumps(chain_state_counts)}`")
    lines.append(f"- Selection liquidity states (all tau targets): `{json.dumps(sel_state_counts)}`")
    lines.append(f"- Chain SOFT reason class split: `{json.dumps(class_counts)}`")
    lines.append("")

    lines.append("## SOFT Reason Leaderboard (Chain, ranked)")
    if leaderboard_df.empty:
        lines.append("- No chain SOFT reasons observed.")
    else:
        top = leaderboard_df.sort_values(["count"], ascending=False).head(20)
        lines.append("```text")
        lines.append(top.to_string(index=False))
        lines.append("```")
    lines.append("")

    lines.append("## SOFT Root Cause Assessment")
    lines.append("- Structural pipeline issues: reasons like `MISSING_MARKET_TS`, `STALE_UNDERLYING`, `EXPIRY_SELECTION_AMBIGUOUS`, `COVERAGE_LOW`.")
    lines.append("- Market-state issues: reasons like `OI_ZERO`, `NO_QUOTES`, `ZOMBIE_QUOTE`, and selection liquidity reasons.")
    lines.append("- Preset strictness issues: observed when reason payloads cluster close to configured cutoffs and pass rates are low.")
    lines.append("")

    top_reasons = (
        leaderboard_df.groupby("reason_code", as_index=False)["count"].sum().sort_values("count", ascending=False).head(8) # type: ignore
        if not leaderboard_df.empty else pd.DataFrame(columns=["reason_code", "count"])
    )
    if not top_reasons.empty:
        lines.append("### Top-Reason Recommendations")
        for _, rr in top_reasons.iterrows():
            rc = str(rr["reason_code"])
            proposal = "remain gate"
            note = ""
            if rc in {"WIDE_SPREAD"}:
                proposal = "downgrade to metrics-only at chain level"
                note = "Use selection-level liquidity gate as execution readiness signal."
            elif rc in {"OI_ZERO"}:
                proposal = "keep SOFT gate, consider threshold relaxation in low-liquidity regimes"
                note = "Check symbol/day distribution before raising `oi_zero_ratio`."
            elif rc in {"COVERAGE_LOW", "EXPIRY_SELECTION_AMBIGUOUS"}:
                proposal = "keep gate"
                note = "Likely structural or tenor-target mismatch; fix selection/tau policy first."
            elif rc in {"MISSING_MARKET_TS", "STALE_UNDERLYING"}:
                proposal = "keep gate"
                note = "Data integrity issue; not tradability noise."
            elif rc in {"NO_QUOTES", "ZOMBIE_QUOTE"}:
                proposal = "keep SOFT gate"
                note = "Market-state degradation; useful to protect execution quality."
            lines.append(f"- `{rc}`: {proposal}. {note}")
        lines.append("")

    lines.append("## Preset Friendliness Audit")
    lines.append("```json")
    lines.append(json.dumps(preset_stats, indent=2, ensure_ascii=False))
    lines.append("```")
    lines.append("")

    lines.append("### Suggested Defaults (Report-Only, no code change)")
    lines.append("- Conservative: `x_max=0.10`, `min_n=6`, `p75_max=0.20`, `p90_max=0.35`.")
    lines.append("- Balanced: `x_max=0.10`, `min_n=5`, `p75_max=0.23`, `p90_max=0.40`.")
    lines.append("- Lenient: `x_max=0.10`, `min_n=4`, `p75_max=0.25`, `p90_max=0.45`.")
    lines.append("- Keep `x_max=0.05` as diagnostic only (coverage stress-test), not default gating corridor.")
    lines.append("")

    lines.append("## Config Diff Proposals (Not Applied)")
    lines.append("```yaml")
    lines.append("option_chain:")
    lines.append("  quality:")
    lines.append("    # Candidate remap only if SOFT is dominated by benign spread tails:")
    lines.append("    reason_severity:")
    lines.append("      WIDE_SPREAD:")
    lines.append("        TRADING: SOFT   # or metrics-only in chain QC policy if supported")
    lines.append("    liquidity_gate:")
    lines.append("      enabled: true")
    lines.append("      x_axis: log_moneyness")
    lines.append("      x_max: 0.10")
    lines.append("      min_n: 6")
    lines.append("      limits:")
    lines.append("        p75_max: 0.20")
    lines.append("        p90_max: 0.35")
    lines.append("      diagnostics:")
    lines.append("        x_max_strict: 0.05")
    lines.append("```")
    lines.append("")

    lines.append("## Separation Check")
    lines.append("- Chain QC findings above are structural/coverage-centric.")
    lines.append("- Liquidity gate findings are selection-level (`select_tau`) near-ATM corridor metrics.")

    out_md.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit root causes of SOFT degrade after QC/liquidity-gate upgrade.")
    ap.add_argument("--data-root", default="data/cleaned/option_chain", help="Root folder for cleaned option_chain data")
    ap.add_argument("--year", default="2026")
    ap.add_argument("--interval", default="5m")
    ap.add_argument("--symbols", default="BTC,ETH")
    ap.add_argument("--quality-mode", default="TRADING")
    ap.add_argument("--out-dir", default="docs/agents/option_chain_qc")
    args = ap.parse_args()

    data_root = Path(args.data_root)
    out_dir = Path(args.out_dir)
    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]

    dataset_days: list[DatasetDay] = []
    for sym in symbols:
        dataset_days.extend(discover_days(data_root, sym, args.interval, args.year))

    if not dataset_days:
        raise FileNotFoundError(f"No parquet days found under {data_root} for symbols={symbols}, interval={args.interval}, year={args.year}")

    snapshot_rows: list[dict[str, Any]] = []
    reason_rows: list[dict[str, Any]] = []
    selection_rows: list[dict[str, Any]] = []

    for dd in dataset_days:
        srows, rrows, lrows = replay_day(data_root=data_root, day=dd, quality_mode=args.quality_mode)
        snapshot_rows.extend(srows)
        reason_rows.extend(rrows)
        selection_rows.extend(lrows)

    snapshot_df = pd.DataFrame(snapshot_rows)
    reason_df = pd.DataFrame(reason_rows)
    selection_df = pd.DataFrame(selection_rows)

    leaderboard_df = build_reason_leaderboard(reason_df)
    by_day_df = build_by_day(snapshot_df, selection_df)
    sample_df = build_snapshot_sample(snapshot_df, selection_df, n=200)

    # Handler reference for current preset defaults
    handler_ref = OptionChainDataHandler(symbol="BTC", interval=args.interval, preset="option_chain")
    preset_stats = summarize_preset_friendliness(snapshot_df, selection_df, handler_ref)

    out_dir.mkdir(parents=True, exist_ok=True)
    leaderboard_df.to_csv(out_dir / "soft_degrade_reason_leaderboard.csv", index=False)
    by_day_df.to_csv(out_dir / "soft_degrade_by_day.csv", index=False)
    sample_df.to_csv(out_dir / "soft_degrade_snapshot_sample.csv", index=False)

    report_path = out_dir / "soft_degrade_root_cause_audit_2026-02-24.md"
    write_report(
        report_path,
        dataset_days=dataset_days,
        snapshot_df=snapshot_df,
        reason_df=reason_df,
        selection_df=selection_df,
        leaderboard_df=leaderboard_df,
        by_day_df=by_day_df,
        preset_stats=preset_stats,
    )

    print("Wrote:")
    print(" -", report_path)
    print(" -", out_dir / "soft_degrade_reason_leaderboard.csv")
    print(" -", out_dir / "soft_degrade_by_day.csv")
    print(" -", out_dir / "soft_degrade_snapshot_sample.csv")
    print("Snapshots:", len(snapshot_df), "Selection rows:", len(selection_df), "Reason rows:", len(reason_df))


if __name__ == "__main__":
    main()
