from __future__ import annotations

import copy
import math
from typing import Any, Iterable, Mapping, Hashable, cast, TYPE_CHECKING

import numpy as np
import pandas as pd

from ingestion.contracts.tick import IngestionTick, _coerce_epoch_ms
from quant_engine.data.contracts.snapshot import MarketSpec
from quant_engine.runtime.modes import EngineMode

from .snapshot import OptionChainSnapshot

if TYPE_CHECKING:
    from .chain_handler import OptionChainDataHandler


def _tick_from_payload(payload: Mapping[str, Any], *, symbol: str, source_id: str | None = None) -> IngestionTick:
    data_ts = _infer_data_ts(payload)
    return IngestionTick(
        timestamp=int(data_ts),
        data_ts=int(data_ts),
        domain="option_chain",
        symbol=symbol,
        payload=payload,
        source_id=source_id,
    )


def _infer_data_ts(payload: Mapping[str, Any]) -> int:
    ts_any = payload.get("data_ts")
    if ts_any is None:
        raise ValueError("Option chain payload missing required data_ts (arrival-time authority)")
    return _coerce_epoch_ms(ts_any)


def _build_snapshot_from_payload(payload: Mapping[str, Any], *, symbol: str, market: MarketSpec) -> OptionChainSnapshot | None:
    d = {str(k): v for k, v in payload.items()}

    ts_any = d.get("data_ts")
    if ts_any is None:
        return None
    ts = int(ts_any)

    chain = d.get("chain")
    if chain is None:
        chain = d.get("frame")
    if chain is None:
        chain = d.get("records")

    if isinstance(chain, list):
        chain = pd.DataFrame(chain)

    if not isinstance(chain, pd.DataFrame):
        return None

    try:
        return OptionChainSnapshot.from_chain_aligned(
            data_ts=ts,
            chain=chain,
            symbol=symbol,
            market=market,
            schema_version=int(d.get("schema_version") or 3),
        )
    except Exception:
        return None


def _coerce_lookback_ms(lookback: Any, interval_ms: int | None) -> int | None:
    if lookback is None:
        return None
    if isinstance(lookback, dict):
        window_ms = lookback.get("window_ms")
        if window_ms is not None:
            return int(window_ms)
        bars = lookback.get("bars")
        if bars is not None and interval_ms is not None:
            return int(float(bars) * int(interval_ms))
        return None
    if isinstance(lookback, (int, float)):
        if interval_ms is not None:
            return int(float(lookback) * int(interval_ms))
        return int(float(lookback))
    if isinstance(lookback, str):
        from quant_engine.data.contracts.protocol_realtime import to_interval_ms

        ms = to_interval_ms(lookback)
        return int(ms) if ms is not None else None
    return None


def _coerce_lookback_bars(lookback: Any, interval_ms: int | None, max_bars: int | None) -> int | None:
    if interval_ms is None or interval_ms <= 0:
        return None
    window_ms = _coerce_lookback_ms(lookback, interval_ms)
    if window_ms is None:
        return None
    bars = max(1, int(math.ceil(int(window_ms) / int(interval_ms))))
    if max_bars is not None:
        bars = min(bars, int(max_bars))
    return bars


def _coerce_engine_mode(mode: Any) -> EngineMode | None:
    if isinstance(mode, EngineMode):
        return mode
    if isinstance(mode, str):
        try:
            return EngineMode(mode)
        except Exception:
            return None
    return None


def _resolve_source_id(
    *,
    source_id: Any | None,
    mode: EngineMode | None,
    data_root: Any | None,
    source: Any | None,
) -> str | None:
    if source_id is not None:
        return str(source_id)
    if mode == EngineMode.BACKTEST and data_root is not None:
        return str(data_root)
    if source is not None:
        return str(source)
    return None


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(cast(dict[str, Any], out[k]), v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _resolve_option_chain_config(
    *,
    preset: Any | None,
    config: Any | None,
    override: dict[str, Any],
    interval_ms: int | None,
) -> dict[str, Any]:
    base: dict[str, Any] = {}
    if preset is not None:
        if isinstance(preset, str):
            from quant_engine.strategy.base import get_global_presets

            presets = get_global_presets()
            if preset not in presets:
                raise KeyError(f"Unknown option_chain preset: {preset}")
            base = cast(dict[str, Any], presets[preset])
        elif isinstance(preset, dict):
            base = copy.deepcopy(preset)
        else:
            raise TypeError("option_chain preset must be a dict or preset name")
    if not base:
        from quant_engine.strategy.base import get_global_presets

        base = cast(dict[str, Any], get_global_presets().get("option_chain") or {})
    merged = _deep_merge(base, config if isinstance(config, dict) else {})
    merged = _deep_merge(merged, override or {})
    _validate_option_chain_config(merged, interval_ms=interval_ms)
    return merged


def _validate_option_chain_config(cfg: dict[str, Any], *, interval_ms: int | None) -> None:
    if "cache" not in cfg or not isinstance(cfg.get("cache"), dict):  # require cache block (kind/maxlen/windows/term_bucket_ms)
        raise ValueError("option_chain config missing cache")
    if "coords" not in cfg or not isinstance(cfg.get("coords"), dict):  # require coords block (tau/x/ATM mapping + price_field)
        raise ValueError("option_chain config missing coords")
    if "selection" not in cfg or not isinstance(cfg.get("selection"), dict):  # require selection block (tau bracketing + x interpolation policy)
        raise ValueError("option_chain config missing selection")
    if "quality" not in cfg or not isinstance(cfg.get("quality"), dict):  # require quality block (QC thresholds + reason severities)
        raise ValueError("option_chain config missing quality")

    cache = cast(dict[str, Any], cfg["cache"])  # cache config dict (validated/mutated in-place)
    coords = cast(dict[str, Any], cfg["coords"])  # coords config dict (validated)
    selection = cast(dict[str, Any], cfg["selection"])  # selection config dict (validated)
    quality = cast(dict[str, Any], cfg["quality"])  # quality/QC config dict (validated + derived fields injected)

    term_bucket_ms = cfg.get("term_bucket_ms") or cache.get("term_bucket_ms")  # canonical term bucket size (handler-level + cache must agree)
    if term_bucket_ms is None:
        raise ValueError("option_chain config missing term_bucket_ms")
    cfg["term_bucket_ms"] = int(term_bucket_ms)  # normalize to int ms (top-level)
    cache.setdefault("term_bucket_ms", cfg["term_bucket_ms"])  # default cache.term_bucket_ms from top-level
    if int(cache["term_bucket_ms"]) != int(cfg["term_bucket_ms"]):  # disallow split-brain bucket definitions
        raise ValueError("option_chain cache.term_bucket_ms must match term_bucket_ms")
    if int(cfg["term_bucket_ms"]) <= 0:  # bucket size must be positive
        raise ValueError("option_chain term_bucket_ms must be > 0")

    for k in ("maxlen", "default_term_window", "default_expiry_window"):  # required cache sizing/window defaults
        if k not in cache:
            raise ValueError(f"option_chain cache missing {k}")
    if int(cache["maxlen"]) <= 0:  # snapshot cache length > 0
        raise ValueError("option_chain cache.maxlen must be > 0")
    if int(cache["default_term_window"]) <= 0:  # default term window > 0
        raise ValueError("option_chain cache.default_term_window must be > 0")
    if int(cache["default_expiry_window"]) <= 0:  # default expiry window > 0
        raise ValueError("option_chain cache.default_expiry_window must be > 0")

    if "kind" not in cache:  # require cache strategy selector
        raise ValueError("option_chain cache missing kind")
    kind = str(cache.get("kind") or "").lower()  # normalize cache kind string
    if kind not in {"simple", "deque", "expiry", "term", "term_bucket", "bucketed"}:  # supported cache kinds only
        raise ValueError(f"option_chain cache.kind unsupported: {kind}")
    cache["kind"] = kind  # store normalized kind back into config

    tau_def = str(coords.get("tau_def") or "")  # tau anchor: market_ts (quote time) or data_ts (arrival/ingest time)
    if tau_def not in {"market_ts", "data_ts"}:
        raise ValueError("option_chain coords.tau_def must be 'market_ts' or 'data_ts'")
    x_axis = str(coords.get("x_axis") or "")  # x coordinate definition: log_moneyness or simple moneyness
    if x_axis not in {"log_moneyness", "moneyness"}:
        raise ValueError("option_chain coords.x_axis must be 'log_moneyness' or 'moneyness'")
    atm_def = str(coords.get("atm_def") or "")  # ATM reference definition (which underlying field drives x mapping)
    if atm_def not in {"underlying_price", "underlying_index", "mid_underlying"}:
        raise ValueError("option_chain coords.atm_def unsupported")
    cp_policy = str(coords.get("cp_policy") or "")  # call/put constraint when selecting points (same cp vs allow either)
    if cp_policy not in {"same", "either"}:
        raise ValueError("option_chain coords.cp_policy must be 'same' or 'either'")
    if not isinstance(coords.get("price_field"), str) or not coords.get("price_field"):  # which price field IV-layer may derive from (if needed)
        raise ValueError("option_chain coords.price_field must be a non-empty string")

    method = str(selection.get("method") or "")  # tau selection method: nearest_bucket or bracket expiries
    if method not in {"nearest_bucket", "bracket"}:
        raise ValueError("option_chain selection.method unsupported")
    interp = str(selection.get("interp") or "")  # x interpolation method within selected slice (nearest vs linear in x)
    if interp not in {"nearest", "linear_x"}:
        raise ValueError("option_chain selection.interp unsupported")

    quality_mode = str(cfg.get("quality_mode") or "")  # quality strictness mode (STRICT/TRADING/RESEARCH)
    if quality_mode.upper() not in {"STRICT", "TRADING", "RESEARCH"}:
        raise ValueError("option_chain quality_mode unsupported")
    cfg["quality_mode"] = quality_mode.upper()  # normalize to uppercase canonical enum

    if not isinstance(quality.get("policy_id"), str) or not str(quality.get("policy_id") or "").strip():
        raise ValueError("option_chain quality.policy_id must be a non-empty string")
    quality["policy_id"] = str(quality.get("policy_id")).strip()
    qc_debug = quality.get("qc_debug")
    quality["qc_debug"] = bool(qc_debug) if qc_debug is not None else False

    for key in ("spread_max", "min_n_per_slice", "oi_zero_ratio", "eps", "mid_eps", "oi_eps", "max_bucket_hops"):  # mandatory QC knobs
        if key not in quality:
            raise ValueError(f"option_chain quality missing {key}")
    if float(quality["spread_max"]) <= 0:  # max relative spread threshold (>0)
        raise ValueError("option_chain quality.spread_max must be > 0")
    if int(quality["min_n_per_slice"]) <= 0:  # minimum rows required for a slice to be considered usable
        raise ValueError("option_chain quality.min_n_per_slice must be > 0")
    if not (0.0 <= float(quality["oi_zero_ratio"]) <= 1.0):  # tolerance for OI==0 prevalence in a slice
        raise ValueError("option_chain quality.oi_zero_ratio must be within [0,1]")
    if float(quality["eps"]) <= 0 or float(quality["mid_eps"]) <= 0 or float(quality["oi_eps"]) <= 0:  # numeric epsilons for zombie/spread guards
        raise ValueError("option_chain quality eps values must be > 0")
    if int(quality["max_bucket_hops"]) < 0:  # how far tau selection may search across neighbor buckets
        raise ValueError("option_chain quality.max_bucket_hops must be >= 0")

    if "stale_ms" not in quality:  # derive stale_ms from interval if not explicitly provided
        if "stale_ms_factor" not in quality:
            raise ValueError("option_chain quality missing stale_ms or stale_ms_factor")
        if interval_ms is None:  # if interval unknown, cannot derive stale_ms deterministically
            quality["stale_ms"] = None
        else:
            quality["stale_ms"] = int(float(quality["stale_ms_factor"]) * int(interval_ms))  # staleness threshold in ms
    if "max_tau_error_ms" not in quality:  # derive max tau mismatch tolerance from term bucket size if not explicitly provided
        if "max_tau_error_ms_factor" not in quality:
            raise ValueError("option_chain quality missing max_tau_error_ms or max_tau_error_ms_factor")
        quality["max_tau_error_ms"] = int(float(quality["max_tau_error_ms_factor"]) * int(cfg["term_bucket_ms"]))  # allowable |tau_actual - tau_target|

    reason_severity = quality.get("reason_severity")  # reason_code -> severity mapping used to escalate OK/SOFT/HARD
    if not isinstance(reason_severity, dict):
        raise ValueError("option_chain quality.reason_severity must be a dict")
    quality["reason_severity"] = reason_severity  # normalize back to dict (even if empty)

    market_ts_ref_method = str(cfg.get("market_ts_ref_method") or "")  # how to compress per-row market_ts into snapshot_market_ts
    if market_ts_ref_method not in {"median"}:
        raise ValueError("option_chain market_ts_ref_method unsupported")
    cfg["market_ts_ref_method"] = market_ts_ref_method  # store normalized ref method


def _coerce_cp(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip().upper()
    if s in {"C", "CALL"}:
        return "C"
    if s in {"P", "PUT"}:
        return "P"
    return None


def _coerce_atm_ref(x: Any) -> float | None:
    try:
        val = float(x)
    except Exception:
        return None
    if pd.isna(val) or val == 0:
        return None
    return float(val)


def _compute_tau_series(df: pd.DataFrame, *, tau_anchor_ts: int | None) -> pd.Series:
    if tau_anchor_ts is None or "expiry_ts" not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    tau = pd.to_numeric(df["expiry_ts"], errors="coerce") - int(tau_anchor_ts)
    return tau.clip(lower=0)


def _compute_x_series(df: pd.DataFrame, *, atm_ref: Any, x_axis: str | None) -> pd.Series:
    if "strike" not in df.columns or x_axis is None:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    atm_val = _coerce_atm_ref(atm_ref)
    if atm_val is None:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    strike = pd.to_numeric(df["strike"], errors="coerce")
    if x_axis == "moneyness":
        return (strike / float(atm_val) - 1.0).astype("float64")
    return pd.Series(np.log(strike / float(atm_val)), index=strike.index, dtype="float64")


def _market_ts_ref(df: pd.DataFrame, snap: OptionChainSnapshot) -> int | None:
    ref, _ = _market_ts_ref_info(df, snap, method="median")
    return ref


def _market_ts_ref_info(df: pd.DataFrame, snap: OptionChainSnapshot, *, method: str) -> tuple[int | None, str]:
    if method != "median":
        return None, "missing"
    if df is not None and "market_ts" in df.columns:
        xs = pd.to_numeric(df["market_ts"], errors="coerce").dropna()
        if not xs.empty:
            return int(xs.median()), "frame_median"
    quote = getattr(snap, "quote_frame", None)
    if quote is not None and "market_ts" in quote.columns:
        xs = pd.to_numeric(quote["market_ts"], errors="coerce").dropna()
        if not xs.empty:
            return int(xs.median()), "quote_median"
    return None, "missing"


def _resolve_underlying(df: pd.DataFrame, atm_def: str) -> float | None:
    field = "underlying_price"
    if atm_def == "underlying_index":
        field = "underlying_index"
    if field not in df.columns and "underlying_price" in df.columns:
        field = "underlying_price"
    if field not in df.columns:
        return None
    xs = pd.to_numeric(df[field], errors="coerce").dropna()
    if xs.empty:
        return None
    return float(xs.median())


def _apply_quality_checks(
    handler: "OptionChainDataHandler",
    df: pd.DataFrame,
    meta: dict[str, Any],
    quality_mode: str,
    reason_severity: dict[str, dict[str, str]],
) -> None:
    n_rows = int(len(df))
    series_x = _compute_x_series(df, atm_ref=meta.get("atm_ref"), x_axis=meta.get("x_axis"))
    series_tau = _compute_tau_series(df, tau_anchor_ts=meta.get("tau_anchor_ts"))
    n_valid_x = int(series_x.notna().sum()) if n_rows > 0 else 0
    n_valid_tau = int(series_tau.notna().sum()) if n_rows > 0 else 0
    price_fields = [c for c in ("bid_price", "ask_price", "mid_price", "mark_price") if c in df.columns]
    n_quotes = 0
    if price_fields:
        n_quotes = int(df[price_fields].notna().any(axis=1).sum())
        if n_quotes == 0:
            _add_reason(meta, "NO_QUOTES", _severity_for("NO_QUOTES", quality_mode, reason_severity), {"fields": price_fields})
    else:
        _add_reason(meta, "NO_QUOTES", _severity_for("NO_QUOTES", quality_mode, reason_severity), {"fields": []})

    meta["coverage"] = {
        "n_rows": n_rows,
        "n_valid_x": n_valid_x,
        "n_valid_tau": n_valid_tau,
        "n_quotes": n_quotes,
    }

    atm_ref = _coerce_atm_ref(meta.get("atm_ref"))
    missing_atm = atm_ref is None
    if missing_atm:
        _add_reason(meta, "MISSING_UNDERLYING_REF", _severity_for("MISSING_UNDERLYING_REF", quality_mode, reason_severity), {})

    snapshot_data_ts = meta.get("snapshot_data_ts")
    snapshot_market_ts = meta.get("snapshot_market_ts")
    stale_ms = handler.quality_cfg.get("stale_ms")
    if stale_ms is not None and snapshot_data_ts is not None and snapshot_market_ts is not None:
        staleness_ms = int(snapshot_data_ts) - int(snapshot_market_ts)
        meta["staleness"] = {"staleness_ms": staleness_ms, "stale_ms": int(stale_ms)}
        if staleness_ms > int(stale_ms):
            _add_reason(
                meta,
                "STALE_UNDERLYING",
                _severity_for("STALE_UNDERLYING", quality_mode, reason_severity),
                {"staleness_ms": staleness_ms},
            )

    spread_max = float(handler.quality_cfg["spread_max"])
    eps = float(handler.quality_cfg["eps"])
    if "bid_price" in df.columns and "ask_price" in df.columns:
        bid = pd.to_numeric(df["bid_price"], errors="coerce")
        ask = pd.to_numeric(df["ask_price"], errors="coerce")
        mask = bid.notna() & ask.notna()
        if bool(mask.any()):
            if "mid_price" in df.columns:
                mid = pd.to_numeric(df["mid_price"], errors="coerce").where(mask)
            elif "mark_price" in df.columns:
                mid = pd.to_numeric(df["mark_price"], errors="coerce").where(mask)
            else:
                mid = (bid + ask) / 2.0
            denom = mid.abs().clip(lower=eps)
            spread_ratio = (ask - bid) / denom
            max_ratio = float(spread_ratio.max()) if not spread_ratio.empty else 0.0
            if max_ratio > spread_max:
                _add_reason(
                    meta,
                    "WIDE_SPREAD",
                    _severity_for("WIDE_SPREAD", quality_mode, reason_severity),
                    {"spread_max": spread_max, "max_spread_ratio": max_ratio},
                )

    if "open_interest" in df.columns:
        oi = pd.to_numeric(df["open_interest"], errors="coerce")
        valid = oi.dropna()
        if not valid.empty:
            ratio_zero = float((valid <= 0).sum()) / float(len(valid))
            oi_zero_ratio = float(handler.quality_cfg["oi_zero_ratio"])
            if ratio_zero > oi_zero_ratio:
                _add_reason(
                    meta,
                    "OI_ZERO",
                    _severity_for("OI_ZERO", quality_mode, reason_severity),
                    {"oi_zero_ratio": oi_zero_ratio, "ratio_zero": ratio_zero},
                )

    if "bid_price" in df.columns and "ask_price" in df.columns:
        mid_eps = float(handler.quality_cfg["mid_eps"])
        oi_eps = float(handler.quality_cfg["oi_eps"])
        mid_series = pd.to_numeric(df["mid_price"], errors="coerce") if "mid_price" in df.columns else None
        mark_series = pd.to_numeric(df["mark_price"], errors="coerce") if "mark_price" in df.columns else None
        mid_like = mid_series if mid_series is not None else mark_series
        if mid_like is not None:
            bid = pd.to_numeric(df["bid_price"], errors="coerce")
            ask = pd.to_numeric(df["ask_price"], errors="coerce")
            oi = pd.to_numeric(df["open_interest"], errors="coerce") if "open_interest" in df.columns else None
            mask = bid.notna() & ask.notna() & mid_like.notna()
            if oi is not None:
                mask = mask & oi.notna()
            if bool(mask.any()):
                zombie = (mid_like.abs() <= mid_eps) & ((oi.abs() <= oi_eps) if oi is not None else True)
                if bool(zombie.any()):
                    _add_reason(
                        meta,
                        "ZOMBIE_QUOTE",
                        _severity_for("ZOMBIE_QUOTE", quality_mode, reason_severity),
                        {"mid_eps": mid_eps, "oi_eps": oi_eps},
                    )


def _coerce_quality_mode(mode: Any) -> str:
    if mode is None:
        raise ValueError("quality_mode must be provided")
    val = str(mode).strip().upper()
    if val in {"STRICT", "TRADING", "RESEARCH"}:
        return val
    raise ValueError(f"Unsupported quality_mode: {mode}")


def _severity_for(reason_code: str, mode: str, reason_severity: dict[str, dict[str, str]]) -> str:
    mapping = reason_severity.get(str(reason_code)) or {}
    key = str(mode).upper()
    if key in mapping:
        return str(mapping[key])
    if "DEFAULT" in mapping:
        return str(mapping["DEFAULT"])
    raise ValueError(f"Missing severity mapping for reason_code={reason_code} mode={mode}")


def _empty_meta(snapshot_data_ts: int | None, snapshot_market_ts: int | None, quality_mode: str) -> dict[str, Any]:
    snapshot_data_ts_i = int(snapshot_data_ts) if snapshot_data_ts is not None else None
    snapshot_market_ts_i = int(snapshot_market_ts) if snapshot_market_ts is not None else None
    return {
        "snapshot_data_ts": snapshot_data_ts_i,
        "snapshot_market_ts": snapshot_market_ts_i,
        "market_ts_ref": snapshot_market_ts_i,
        "tau_anchor_ts": None,
        "tau_def": None,
        "market_ts_ref_method": None,
        "quality_mode": quality_mode,
        "selection": {},
        "selection_context": {},
        "coverage": {},
        "staleness": {},
        "reasons": [],
        "state": "OK",
        "tradable": True,
    }


def _clone_meta(meta: dict[str, Any]) -> dict[str, Any]:
    out = dict(meta)
    out["selection"] = dict(meta.get("selection") or {})
    out["selection_context"] = dict(meta.get("selection_context") or {})
    out["coverage"] = dict(meta.get("coverage") or {})
    out["staleness"] = dict(meta.get("staleness") or {})
    out["reasons"] = list(meta.get("reasons") or [])
    return out


def _merge_meta(base: dict[str, Any], other: dict[str, Any]) -> dict[str, Any]:
    out = _clone_meta(base)
    for key in ("selection", "selection_context", "coverage", "staleness"):
        merged = dict(out.get(key) or {})
        merged.update(other.get(key) or {})
        out[key] = merged
    if other.get("reasons"):
        out["reasons"].extend(other["reasons"])
    if "snapshot_data_ts" in other and other["snapshot_data_ts"] is not None:
        out["snapshot_data_ts"] = int(other["snapshot_data_ts"])
    if "snapshot_market_ts" in other and other["snapshot_market_ts"] is not None:
        out["snapshot_market_ts"] = int(other["snapshot_market_ts"])
        out["market_ts_ref"] = int(other["snapshot_market_ts"])
    return out


def _add_reason(meta: dict[str, Any], reason_code: str, severity: str, details: dict[str, Any]) -> None:
    meta.setdefault("reasons", [])
    meta["reasons"].append(
        {"reason_code": str(reason_code), "severity": str(severity), "details": dict(details)}
    )


def _finalize_meta(meta: dict[str, Any], quality_mode: str) -> None:
    reasons = meta.get("reasons") or []
    state = "OK"
    if any(r.get("severity") == "HARD" for r in reasons):
        state = "HARD_FAIL"
    elif any(r.get("severity") == "SOFT" for r in reasons):
        state = "SOFT_DEGRADED"
    meta["state"] = state
    meta["quality_mode"] = quality_mode
    tradable = state == "OK"
    for r in reasons:
        if r.get("reason_code") in {"WIDE_SPREAD", "ZOMBIE_QUOTE", "NO_QUOTES"}:
            tradable = False
    meta["tradable"] = tradable


def _set_selection_context(meta: dict[str, Any], **kwargs: Any) -> None:
    ctx = dict(meta.get("selection_context") or {})
    ctx.update({k: v for k, v in kwargs.items() if v is not None})
    meta["selection_context"] = ctx


def _qc_report_from_meta(
    meta: dict[str, Any],
    *,
    policy_id: str,
    debug: bool = False,
) -> dict[str, Any]:
    reasons = meta.get("reasons") or []
    counters: dict[str, int] = {}
    mapped: list[dict[str, Any]] = []
    for r in reasons:
        code = str(r.get("reason_code") or "")
        severity = str(r.get("severity") or "")
        ctx = dict(r.get("details") or {})
        mapped.append({"code": code, "severity": severity, "ctx": ctx})
        counters[code] = counters.get(code, 0) + 1

    coverage = meta.get("coverage") or {}
    staleness = meta.get("staleness") or {}
    # Downstream should use tradable as the boolean decision; state is descriptive.
    summary = {
        "ok": bool(meta.get("tradable")),
        "state": meta.get("state"),
        "tradable": meta.get("tradable"),
        "n_rows": int(coverage.get("n_rows") or 0),
        "n_valid_tau": int(coverage.get("n_valid_tau") or 0),
        "n_valid_x": int(coverage.get("n_valid_x") or 0),
        "n_quotes": int(coverage.get("n_quotes") or 0),
        "staleness_ms": int(staleness["staleness_ms"]) if staleness.get("staleness_ms") is not None else None,
    }
    artifacts: dict[str, Any] = {}
    if debug:
        artifacts = {
            "coverage": dict(coverage),
            "staleness": dict(staleness),
        }

    return {
        "snapshot_data_ts": int(meta["snapshot_data_ts"]) if meta.get("snapshot_data_ts") is not None else None,
        "snapshot_market_ts": int(meta["snapshot_market_ts"]) if meta.get("snapshot_market_ts") is not None else None,
        "quality_mode": str(meta.get("quality_mode") or ""),
        "policy_id": str(policy_id),
        "summary": summary,
        "reasons": mapped,
        "counters": counters,
        "artifacts": artifacts,
    }


def _apply_selection_slice(
    coords_df: pd.DataFrame,
    meta: dict[str, Any],
    selection: dict[str, Any],
    quality_mode: str,
    *,
    min_n_per_slice: int,
    reason_severity: dict[str, dict[str, str]],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    selected_expiries = selection.get("selected_expiries") or []
    slice_df = coords_df.loc[coords_df["expiry_ts"].isin(selected_expiries)] if selected_expiries else pd.DataFrame()
    if slice_df is None or slice_df.empty:
        _add_reason(
            meta,
            "COVERAGE_LOW",
            _severity_for("COVERAGE_LOW", quality_mode, reason_severity),
            {"min_n": min_n_per_slice},
        )
        _finalize_meta(meta, quality_mode)
        return pd.DataFrame(), meta
    if len(slice_df) < int(min_n_per_slice):
        _add_reason(
            meta,
            "COVERAGE_LOW",
            _severity_for("COVERAGE_LOW", quality_mode, reason_severity),
            {"min_n": int(min_n_per_slice), "n_rows": int(len(slice_df))},
        )
    slice_df = slice_df.copy()
    meta["selection"] = selection
    _finalize_meta(meta, quality_mode)
    return slice_df, meta


def _select_point_from_slice(
    slice_df: pd.DataFrame,
    *,
    x: float,
    x_axis: str,
    interp: str,
    price_field: str,
    cp_policy: str,
    snapshot_data_ts: int,
    tau_target_ms: int,
    tau_anchor_ts: int,
    atm_ref: Any,
    quality_mode: str | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    snapshot_data_ts = int(snapshot_data_ts)
    tau_target_ms = int(tau_target_ms)
    tau_anchor_ts = int(tau_anchor_ts)
    mode = str(quality_mode) if quality_mode is not None else "TRADING"
    meta = _empty_meta(
        snapshot_data_ts=snapshot_data_ts,
        snapshot_market_ts=None,
        quality_mode=mode,
    )
    if slice_df is None or slice_df.empty:
        _add_reason(meta, "COVERAGE_LOW", "SOFT", {"min_n": None})
        return None, meta
    series_x = _compute_x_series(slice_df, atm_ref=atm_ref, x_axis=x_axis)
    valid = slice_df.loc[series_x.notna()].copy()
    if valid.empty:
        _add_reason(meta, "COVERAGE_LOW", "SOFT", {"min_n": None})
        return None, meta
    series_tau = _compute_tau_series(slice_df, tau_anchor_ts=tau_anchor_ts)
    target_x = float(x)
    if interp == "linear_x":
        if cp_policy == "same" and "cp" in valid.columns:
            best = None
            for cp_val in valid["cp"].dropna().unique():
                group = valid.loc[valid["cp"] == cp_val]
                group_x = series_x.loc[group.index]
                left = group_x.loc[group_x <= target_x]
                right = group_x.loc[group_x >= target_x]
                if left.empty or right.empty:
                    continue
                lo_idx = (target_x - left).abs().idxmin()
                hi_idx = (right - target_x).abs().idxmin()
                span = abs(float(group_x.loc[hi_idx]) - float(group_x.loc[lo_idx]))
                dist = abs(target_x - float(group_x.loc[lo_idx])) + abs(float(group_x.loc[hi_idx]) - target_x)
                if best is None or (span, dist) < best[0]:
                    best = ((span, dist), lo_idx, hi_idx)
            if best is not None:
                lo_idx = best[1]
                hi_idx = best[2]
            else:
                lo_idx = None
                hi_idx = None
        else:
            all_x = series_x.loc[valid.index]
            left = all_x.loc[all_x <= target_x]
            right = all_x.loc[all_x >= target_x]
            lo_idx = (target_x - left).abs().idxmin() if not left.empty else None
            hi_idx = (right - target_x).abs().idxmin() if not right.empty else None
        if lo_idx is None or hi_idx is None:
            interp = "nearest"
        elif str(valid.loc[lo_idx].get("instrument_name")) == str(valid.loc[hi_idx].get("instrument_name")):
            interp = "nearest"
        else:
            x0 = _to_float_scalar(series_x.loc[lo_idx])
            x1 = _to_float_scalar(series_x.loc[hi_idx])
            v0 = _to_float_scalar(valid.loc[lo_idx].get(price_field))
            v1 = _to_float_scalar(valid.loc[hi_idx].get(price_field))
            if v0 is None or v1 is None or x0 is None or x1 is None or x1 == x0:
                interp = "nearest"
            else:
                weight = (target_x - x0) / (x1 - x0)
                value = float(v0) + weight * (float(v1) - float(v0))
                point = {
                    "ts": int(snapshot_data_ts),
                    "expiry_ts": _to_int_scalar(valid.loc[lo_idx].get("expiry_ts")),
                    "tau_target_ms": int(tau_target_ms),
                    "tau_realized_ms": _to_int_scalar(series_tau.loc[lo_idx]),
                    "x": float(target_x),
                    "x_axis": str(x_axis),
                    "value_fields": {str(price_field): value},
                }
                return point, meta
    diffs = (series_x.loc[valid.index] - target_x).abs()
    nearest = valid.loc[diffs.idxmin()]
    nearest_idx = cast(Hashable, nearest.name)
    tau_val = series_tau.get(nearest_idx, np.nan)
    x_val = series_x.get(nearest_idx, np.nan)
    point = {
        "ts": int(snapshot_data_ts),
        "expiry_ts": _to_int_scalar(nearest.get("expiry_ts")),
        "tau_target_ms": int(tau_target_ms),
        "tau_realized_ms": _to_int_scalar(tau_val),
        "x": float(_to_float_scalar(x_val) or 0.0),
        "x_axis": str(x_axis),
        "value_fields": {str(price_field): nearest.get(price_field)},
    }
    return point, meta


def _iter_ts(ts_start: int, ts_end: int, step_ms: int) -> Iterable[int]:
    if step_ms <= 0:
        return []
    t = int(ts_start)
    end = int(ts_end)
    while t <= end:
        yield t
        t += int(step_ms)


def _empty_df_like(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    try:
        return frame.iloc[0:0].copy()
    except Exception:
        return pd.DataFrame(columns=list(frame.columns))


def _to_float_scalar(x: Any) -> float | None:
    try:
        series = pd.to_numeric([x], errors="coerce")
        val = float(series[0]) if len(series) > 0 else float("nan")
    except Exception:
        return None
    if pd.isna(val):
        return None
    return float(val)


def _to_int_scalar(x: Any) -> int:
    val = _to_float_scalar(x)
    return int(val) if val is not None else 0
