from __future__ import annotations

from typing import Any

import pandas as pd


def choose_ic_kind(df: pd.DataFrame, requested: str) -> str:
    if requested != "auto":
        return requested
    if "symbol" in df.columns and "timestamp" in df.columns:
        counts = df.groupby("timestamp")["symbol"].nunique()
        if not counts.empty and int(counts.max()) > 1:
            return "cross_sectional"
    return "time_series"


def _safe_corr(a: pd.Series, b: pd.Series, method: str) -> float | None:
    frame = pd.concat([a, b], axis=1).dropna()
    if len(frame) < 3:
        return None
    if frame.iloc[:, 0].nunique() < 2 or frame.iloc[:, 1].nunique() < 2:
        return None
    value = frame.iloc[:, 0].corr(frame.iloc[:, 1], method=method)
    if pd.isna(value):
        return None
    return float(value)


def compute_ic(df: pd.DataFrame, *, features: list[str], target_column: str, ic_kind: str, method: str = "spearman") -> dict[str, Any]:
    resolved_kind = choose_ic_kind(df, ic_kind)
    results: list[dict[str, Any]] = []
    if resolved_kind == "cross_sectional":
        if "timestamp" not in df.columns or "symbol" not in df.columns:
            raise ValueError("Cross-sectional IC requires timestamp and symbol columns.")
        for feature in features:
            per_timestamp: list[float] = []
            for _, group in df.groupby("timestamp"):
                corr = _safe_corr(group[feature], group[target_column], method)
                if corr is not None:
                    per_timestamp.append(corr)
            results.append({
                "feature": feature,
                "ic_kind": "cross_sectional",
                "method": method,
                "point_count": len(per_timestamp),
                "mean_ic": float(pd.Series(per_timestamp).mean()) if per_timestamp else None,
                "median_ic": float(pd.Series(per_timestamp).median()) if per_timestamp else None,
                "positive_share": float((pd.Series(per_timestamp) > 0).mean()) if per_timestamp else None,
            })
        return {"ic_kind": "cross_sectional", "method": method, "results": results}

    for feature in features:
        if "symbol" in df.columns:
            per_symbol: list[float] = []
            for _, group in df.groupby("symbol"):
                corr = _safe_corr(group[feature], group[target_column], method)
                if corr is not None:
                    per_symbol.append(corr)
            overall = _safe_corr(df[feature], df[target_column], method)
            results.append({
                "feature": feature,
                "ic_kind": "time_series",
                "method": method,
                "symbol_count": int(df["symbol"].nunique()),
                "mean_symbol_ic": float(pd.Series(per_symbol).mean()) if per_symbol else None,
                "median_symbol_ic": float(pd.Series(per_symbol).median()) if per_symbol else None,
                "overall_ic": overall,
                "positive_share": float((pd.Series(per_symbol) > 0).mean()) if per_symbol else None,
            })
        else:
            overall = _safe_corr(df[feature], df[target_column], method)
            results.append({
                "feature": feature,
                "ic_kind": "time_series",
                "method": method,
                "overall_ic": overall,
                "point_count": int(df[[feature, target_column]].dropna().shape[0]),
            })
    return {"ic_kind": "time_series", "method": method, "results": results}
