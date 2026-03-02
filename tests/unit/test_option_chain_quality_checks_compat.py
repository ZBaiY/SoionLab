from __future__ import annotations  # +
  # +
import copy  # +
  # +
import numpy as np  # +
import pandas as pd  # +
  # +
from quant_engine.data.derivatives.option_chain.helpers import _apply_quality_checks  # +
  # +
  # +
class _DummyHandler:  # +
    def __init__(self) -> None:  # +
        self.quality_cfg = {"oi_zero_ratio": 0.4, "mid_eps": 1e-6, "oi_eps": 1e-6, "stale_ms": 10}  # +
  # +
  # +
def _severity_for(reason_code: str, mode: str, reason_severity: dict[str, dict[str, str]]) -> str:  # +
    return str(reason_severity[str(reason_code)][str(mode).upper()])  # +
  # +
  # +
def _add_reason(meta: dict[str, object], reason_code: str, severity: str, details: dict[str, object]) -> None:  # +
    meta.setdefault("reasons", [])  # +
    meta["reasons"].append({"reason_code": reason_code, "severity": severity, "details": dict(details)})  # type: ignore # 
  # +
  # +
def _apply_quality_checks_reference(handler: _DummyHandler, df: pd.DataFrame, meta: dict[str, object], quality_mode: str, reason_severity: dict[str, dict[str, str]], *, qc_mask: np.ndarray | None = None, qc_scope: str = "full_rows") -> None:  # +
    n_rows = int(len(df))  # +
    n_valid_x = 0  # +
    n_valid_tau = 0  # +
    price_fields = [c for c in ("bid_price", "ask_price", "mid_price", "mark_price") if c in df.columns]  # +
    n_quotes = int(df[price_fields].notna().any(axis=1).sum()) if price_fields else 0  # +
    if (not price_fields) or n_quotes == 0:  # +
        _add_reason(meta, "NO_QUOTES", _severity_for("NO_QUOTES", quality_mode, reason_severity), {"fields": price_fields})  # +
    coverage: dict[str, object] = {"n_rows": n_rows, "n_valid_x": n_valid_x, "n_valid_tau": n_valid_tau, "n_quotes": n_quotes, "qc_scope": qc_scope}  # +
    if qc_mask is not None:  # +
        n_eligible = int(qc_mask.sum())  # +
        coverage["n_eligible_rows"] = n_eligible  # +
        coverage["mask_coverage"] = n_eligible / max(1, n_rows)  # +
    meta["coverage"] = coverage  # +
    if meta.get("atm_ref") is None:  # +
        _add_reason(meta, "MISSING_UNDERLYING_REF", _severity_for("MISSING_UNDERLYING_REF", quality_mode, reason_severity), {})  # +
    if meta.get("snapshot_data_ts") is not None and meta.get("snapshot_market_ts") is not None:  # +
        staleness_ms = int(meta["snapshot_data_ts"]) - int(meta["snapshot_market_ts"])  # type: ignore 
        meta["staleness"] = {"staleness_ms": staleness_ms, "stale_ms": int(handler.quality_cfg["stale_ms"])}  # +
        if staleness_ms > int(handler.quality_cfg["stale_ms"]):  # +
            _add_reason(meta, "STALE_UNDERLYING", _severity_for("STALE_UNDERLYING", quality_mode, reason_severity), {"staleness_ms": staleness_ms})  # +
    df_qc = df[qc_mask] if qc_mask is not None and bool(qc_mask.any()) else df  # +
    if "open_interest" in df_qc.columns:  # +
        oi = pd.to_numeric(df_qc["open_interest"], errors="coerce")  # +
        valid = oi.dropna()  # +
        if not valid.empty:  # +
            ratio_zero = float((valid <= 0).sum()) / float(len(valid))  # +
            if ratio_zero > float(handler.quality_cfg["oi_zero_ratio"]):  # +
                _add_reason(meta, "OI_ZERO", _severity_for("OI_ZERO", quality_mode, reason_severity), {"oi_zero_ratio": float(handler.quality_cfg["oi_zero_ratio"]), "ratio_zero": ratio_zero})  # +
    if "bid_price" in df_qc.columns and "ask_price" in df_qc.columns:  # +
        mid_series = pd.to_numeric(df_qc["mid_price"], errors="coerce") if "mid_price" in df_qc.columns else None  # +
        mark_series = pd.to_numeric(df_qc["mark_price"], errors="coerce") if "mark_price" in df_qc.columns else None  # +
        mid_like = mid_series if mid_series is not None else mark_series  # +
        if mid_like is not None:  # +
            bid = pd.to_numeric(df_qc["bid_price"], errors="coerce")  # +
            ask = pd.to_numeric(df_qc["ask_price"], errors="coerce")  # +
            oi = pd.to_numeric(df_qc["open_interest"], errors="coerce") if "open_interest" in df_qc.columns else None  # +
            mask = bid.notna() & ask.notna() & mid_like.notna()  # +
            if oi is not None:  # +
                mask = mask & oi.notna()  # +
            if bool(mask.any()):  # +
                zombie = (mid_like.abs() <= float(handler.quality_cfg["mid_eps"])) & ((oi.abs() <= float(handler.quality_cfg["oi_eps"])) if oi is not None else True)  # +
                if bool(zombie.any()):  # +
                    _add_reason(meta, "ZOMBIE_QUOTE", _severity_for("ZOMBIE_QUOTE", quality_mode, reason_severity), {"mid_eps": float(handler.quality_cfg["mid_eps"]), "oi_eps": float(handler.quality_cfg["oi_eps"])})  # +
  # +
  # +
def test_apply_quality_checks_matches_reference_behavior() -> None:  # +
    handler = _DummyHandler()  # +
    reason_severity = {code: {"TRADING": "SOFT"} for code in ("NO_QUOTES", "MISSING_UNDERLYING_REF", "STALE_UNDERLYING", "OI_ZERO", "ZOMBIE_QUOTE")}  # +
    df = pd.DataFrame([{"bid_price": 1.0, "ask_price": 1.1, "mid_price": 1.05, "mark_price": 1.04, "open_interest": 10.0}, {"bid_price": 0.5, "ask_price": 0.6, "mid_price": 0.0, "mark_price": 0.0, "open_interest": 0.0}, {"bid_price": 0.9, "ask_price": 1.0, "mid_price": 0.95, "mark_price": 0.94, "open_interest": 0.0}])  # +
    qc_mask = np.array([True, False, True], dtype=bool)  # +
    base_meta: dict[str, object] = {"snapshot_data_ts": 100, "snapshot_market_ts": 80, "atm_ref": None, "x_axis": "log_moneyness", "tau_anchor_ts": 100, "coverage": {}, "staleness": {}, "reasons": []}  # +
    meta_ref = copy.deepcopy(base_meta)  # +
    meta_new = copy.deepcopy(base_meta)  # +
    _apply_quality_checks_reference(handler, df, meta_ref, "TRADING", reason_severity, qc_mask=qc_mask, qc_scope="eligible_rows")  # +
    _apply_quality_checks(handler, df, meta_new, "TRADING", reason_severity, qc_mask=qc_mask, qc_scope="eligible_rows")  # type: ignore
    assert meta_new == meta_ref  # +
