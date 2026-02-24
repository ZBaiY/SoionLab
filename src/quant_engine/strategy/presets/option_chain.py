"""
Global option-chain presets.
"""

from __future__ import annotations

from typing import Any, Dict

OPTION_CHAIN_PRESETS: Dict[str, Any] = {
    "option_chain": {
        "term_bucket_ms": 86_400_000,
        "coords": {
            "tau_def": "market_ts",
            "x_axis": "log_moneyness",
            "atm_def": "underlying_price",
            "underlying_field": "underlying_price",
            "price_field": "mark_price",
            "cp_policy": "same",
        },
        "selection": {
            "method": "nearest_bucket",
            "interp": "nearest",
        },
        "quality_mode": "TRADING",
        "quality": {
            "policy_id": "v1",
            "spread_max": 0.05,
            "min_n_per_slice": 20,
            "oi_zero_ratio": 0.95,
            "stale_ms_factor": 2.0,
            "max_tau_error_ms_factor": 2.0,
            "eps": 1e-12,
            "mid_eps": 1e-12,
            "oi_eps": 1e-12,
            "max_bucket_hops": 2,
            "qc_debug": False,
            "reason_severity": {
                "MISSING_FRAME": {"STRICT": "HARD", "TRADING": "HARD", "RESEARCH": "HARD"},
                "MISSING_MARKET_TS": {"STRICT": "HARD", "TRADING": "SOFT", "RESEARCH": "SOFT"},
                "EMPTY_CHAIN": {"STRICT": "SOFT", "TRADING": "SOFT", "RESEARCH": "SOFT"},
                "MISSING_UNDERLYING_REF": {"STRICT": "HARD", "TRADING": "HARD", "RESEARCH": "SOFT"},
                "NO_QUOTES": {"STRICT": "HARD", "TRADING": "HARD", "RESEARCH": "SOFT"},
                "WIDE_SPREAD": {"STRICT": "SOFT", "TRADING": "SOFT", "RESEARCH": "SOFT"},
                "OI_ZERO": {"STRICT": "SOFT", "TRADING": "SOFT", "RESEARCH": "SOFT"},
                "STALE_UNDERLYING": {"STRICT": "HARD", "TRADING": "SOFT", "RESEARCH": "SOFT"},
                "ZOMBIE_QUOTE": {"STRICT": "HARD", "TRADING": "SOFT", "RESEARCH": "SOFT"},
                "COVERAGE_LOW": {"STRICT": "HARD", "TRADING": "SOFT", "RESEARCH": "SOFT"},
                "EXPIRY_SELECTION_AMBIGUOUS": {"STRICT": "HARD", "TRADING": "SOFT", "RESEARCH": "SOFT"},
                "LIQUIDITY_COVERAGE_LOW": {"STRICT": "HARD", "TRADING": "SOFT", "RESEARCH": "SOFT"},
                "LIQUIDITY_SPREAD_WIDE": {"STRICT": "HARD", "TRADING": "SOFT", "RESEARCH": "SOFT"},
            },
            "row_policy": {
                "id": "default",
                "version": "v1",
                "two_sided_required": True,
                "min_open_interest": 0.0,
                "min_mark_notional": None,
                "min_tau_ms": 0,
                "max_tau_ms": None,
                "max_abs_x": None,
            },
            "liquidity_gate": {
                "enabled": True,
                "x_axis": "log_moneyness",
                "x_max": 0.10,
                "min_n": 6,
                "quantiles": {"p75": 0.75, "p90": 0.90},
                "limits": {"p75_max": 0.20, "p90_max": 0.35},
                "diagnostics": {"x_max_strict": 0.05},
            },
        },
        "market_ts_ref_method": "median",
        "cache": {
            "kind": "term",
            "maxlen": 512,
            "term_bucket_ms": 86_400_000,
            "default_term_window": 5,
            "default_expiry_window": 5,
        },
    },
    "OPTION_CHAIN_5M": {
        "$ref": "option_chain",
        "interval": "5m",
        "bootstrap": {"lookback": "30d"},
    },
    "OPTION_CHAIN_1M": {
        "$ref": "option_chain",
        "interval": "1m",
        "bootstrap": {"lookback": "30d"},
        "cache": {"maxlen": 1024},
    },
}

