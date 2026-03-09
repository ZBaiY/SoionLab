# src/quant_engine/features/options/iv.py
# NOTE: These IV features use OHLCV-derived IV columns.
# Option-chain–based IV surface features live in iv_surface.py.
from __future__ import annotations

import math
from typing import Any, cast

from quant_engine.contracts.feature import FeatureChannelBase
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.features.registry import register_feature

# v4 Feature Module:
# - Feature identity (name) is injected by Strategy and treated as immutable.
# - This module performs pure feature computation only.
# - NOTE: These IV features are OHLCV-derived; option-chain IV surface features live elsewhere.

@register_feature("IV30")
class IV30Feature(FeatureChannelBase):
    def __init__(self, *, name: str, symbol: str, **kwargs):
        super().__init__(name=name, symbol=symbol)
        self._iv30: float | None = None

    def initialize(self, context, warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="options")

    def update(self, context):
        snapshot = self.get_snapshot(context, "options", symbol=self.symbol)
        if snapshot is None:
            return
        df = snapshot.get_attr("frame")
        if df is None:
            return
        if "iv_30d" not in df:
            return

        self._iv30 = float(df["iv_30d"].iloc[-1])

    def output(self):
        return self._iv30


@register_feature("IV-SKEW")
class IVSkewFeature(FeatureChannelBase):
    def __init__(self, *, name: str, symbol: str, **kwargs):
        super().__init__(name=name, symbol=symbol)
        self._skew: float | None = None

    def initialize(self, context, warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="options")

    def update(self, context):
        snapshot = self.get_snapshot(context, "options", symbol=self.symbol)
        if snapshot is None:
            return
        df = snapshot.get_attr("frame")
        if df is None:
            return
        if "iv_25d_call" not in df or "iv_25d_put" not in df:
            return

        call_iv = df["iv_25d_call"].iloc[-1]
        put_iv = df["iv_25d_put"].iloc[-1]
        self._skew = float(call_iv - put_iv)

    def output(self):
        return self._skew


@register_feature("OPTION-MARK-IV")
class OptionChainMarkIVFeature(FeatureChannelBase):
    """Fetch one option-chain point IV (`mark_iv`) at configured (tau_ms, x)."""

    def __init__(self, *, name: str, symbol: str, **kwargs: Any):
        super().__init__(name=name, symbol=symbol)

        x_raw = kwargs.get("x", kwargs.get("moneyness_target", 0.0))
        self.x = float(x_raw)

        tau_ms_raw = kwargs.get("tau_ms", kwargs.get("maturity_target_ms"))
        if tau_ms_raw is None:
            tau_days_raw = kwargs.get("tau_days", kwargs.get("maturity_target_days", 30))
            tau_ms_raw = float(tau_days_raw) * 86_400_000.0
        self.tau_ms = int(float(tau_ms_raw))

        freshness_raw = kwargs.get("freshness_ms")
        self.freshness_ms = int(float(freshness_raw)) if freshness_raw is not None else None

        self.method = kwargs.get("method")
        self.interp = kwargs.get("interp")
        self.cp_policy = kwargs.get("cp_policy")
        self.quality_mode = kwargs.get("quality_mode")

        self._iv: float | None = None

    def required_window(self) -> dict[str, int]:
        return {"option_chain": 1}

    def initialize(self, context, warmup_window=None):
        self._warmup_by_update(context, warmup_window, data_type="option_chain")

    def _resolve_freshness_ms(self, context: dict[str, Any], handler: Any) -> int:
        if isinstance(self.freshness_ms, int) and self.freshness_ms > 0:
            return int(self.freshness_ms)
        quality_cfg = getattr(handler, "quality_cfg", None)
        if isinstance(quality_cfg, dict):
            stale_ms = quality_cfg.get("stale_ms")
            if stale_ms is not None:
                try:
                    val = int(stale_ms)
                    if val > 0:
                        return val
                except Exception:
                    pass
        interval_ms = context.get("engine_interval_ms")
        if interval_ms is None:
            interval_ms = self.interval_ms
        try:
            val = cast(int, interval_ms)
            if val > 0:
                return max(1, val * 2)
        except Exception:
            pass
        return 900_000

    @staticmethod
    def _coerce_mark_iv(point: Any) -> float | None:
        if not isinstance(point, dict):
            return None
        fields = point.get("value_fields")
        if not isinstance(fields, dict):
            return None
        raw = fields.get("mark_iv")
        if raw is None:
            return None
        try:
            val = float(raw)
        except Exception:
            return None
        if not math.isfinite(val):
            return None
        return val

    def update(self, context):
        ts = int(context.get("timestamp", 0))
        try:
            handler = self._get_handler(context, "option_chain", self.symbol)
        except Exception:
            self._iv = None
            return

        freshness_ms = self._resolve_freshness_ms(context, handler)
        select_kwargs: dict[str, Any] = {
            "tau_ms": int(self.tau_ms),
            "x": float(self.x),
            "price_field": "mark_iv",
        }
        if self.method is not None:
            select_kwargs["method"] = self.method
        if self.interp is not None:
            select_kwargs["interp"] = self.interp
        if self.cp_policy is not None:
            select_kwargs["cp_policy"] = self.cp_policy
        if self.quality_mode is not None:
            select_kwargs["quality_mode"] = self.quality_mode

        try:
            point, meta = cast(OptionChainDataHandler, handler).select_point(ts=ts, **select_kwargs)
        except Exception:
            self._iv = None
            return

        snapshot_data_ts = meta.get("snapshot_data_ts") if isinstance(meta, dict) else None
        source_ts: int | None
        try:
            source_ts = int(snapshot_data_ts) if snapshot_data_ts is not None else None
        except Exception:
            source_ts = None

        # Defensive lookahead guard: ignore any accidental future snapshot.
        if source_ts is not None and source_ts > ts:
            self._iv = None
            return

        iv_val = self._coerce_mark_iv(point)
        if iv_val is not None:
            resolved_source_ts = int(source_ts) if source_ts is not None else int(ts)
            age = int(ts) - int(resolved_source_ts)
            if age <= int(freshness_ms):
                self._iv = float(iv_val)
                return

        # Strict v1: missing/invalid/stale IV => neutral path (no RSI-only fallback).
        self._iv = None

    def output(self):
        return self._iv
