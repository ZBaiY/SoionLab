# decision/threshold.py
import math
from typing import Any

from quant_engine.contracts.decision import DecisionBase
from .registry import register_decision


DAY_MS = 86_400_000


def _resolve_option_tau_ms(kwargs: dict[str, Any]) -> int:
    tau_ms_raw = kwargs.get("tau_ms", kwargs.get("iv_tau_ms"))
    if tau_ms_raw is None:
        tau_days_raw = kwargs.get("tau_days", kwargs.get("iv_tau_days", 30))
        tau_ms_raw = float(tau_days_raw) * float(DAY_MS)
    return int(float(tau_ms_raw))


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


def _resolve_option_chain_iv_candidate(
    context: dict[str, Any],
    *,
    tau_ms: int,
    x: float,
) -> float | None:
    ctx = context.get("option_chain_decision_ctx")
    if not isinstance(ctx, dict) or not bool(ctx.get("admitted")):
        return None

    handler = ctx.get("handler")
    if handler is None:
        return None

    step_ts_raw = context.get("timestamp")
    if step_ts_raw is None:
        return None
    step_ts = int(step_ts_raw)

    horizon_raw = ctx.get("candidate_horizon_ms")
    candidate_horizon_ms = int(horizon_raw) if isinstance(horizon_raw, (int, float)) and int(horizon_raw) > 0 else None

    try:
        snapshots = list(handler.window(ts=step_ts, n=3))
    except Exception:
        return None

    for snap in reversed(snapshots):
        candidate_ts_raw = getattr(snap, "data_ts", None)
        if candidate_ts_raw is None:
            continue
        try:
            candidate_ts = int(candidate_ts_raw)
        except Exception:
            continue
        if candidate_ts > step_ts:
            continue
        if candidate_horizon_ms is not None and (step_ts - candidate_ts) > candidate_horizon_ms:
            continue
        try:
            point, meta = handler.select_point(
                ts=candidate_ts,
                tau_ms=int(tau_ms),
                x=float(x),
                price_field="mark_iv",
            )
        except Exception:
            continue
        snapshot_data_ts = meta.get("snapshot_data_ts") if isinstance(meta, dict) else None
        if snapshot_data_ts is not None:
            try:
                if int(snapshot_data_ts) > step_ts:
                    continue
            except Exception:
                continue
        iv = _coerce_mark_iv(point)
        if iv is not None:
            return float(iv)
    return None


def _resolve_option_chain_iv_history(
    context: dict[str, Any],
    *,
    tau_ms: int,
    x: float,
    n: int,
) -> list[float]:
    ctx = context.get("option_chain_decision_ctx")
    if not isinstance(ctx, dict) or not bool(ctx.get("admitted")):
        return []

    handler = ctx.get("handler")
    if handler is None:
        return []

    step_ts_raw = context.get("timestamp")
    if step_ts_raw is None:
        return []
    step_ts = int(step_ts_raw)

    try:
        snapshots = list(handler.window(ts=step_ts, n=max(1, int(n))))
    except Exception:
        return []

    out: list[float] = []
    for snap in snapshots:
        candidate_ts_raw = getattr(snap, "data_ts", None)
        if candidate_ts_raw is None:
            continue
        try:
            candidate_ts = int(candidate_ts_raw)
        except Exception:
            continue
        if candidate_ts > step_ts:
            continue
        try:
            point, meta = handler.select_point(
                ts=candidate_ts,
                tau_ms=int(tau_ms),
                x=float(x),
                price_field="mark_iv",
            )
        except Exception:
            continue
        snapshot_data_ts = meta.get("snapshot_data_ts") if isinstance(meta, dict) else None
        if snapshot_data_ts is not None:
            try:
                if int(snapshot_data_ts) > step_ts:
                    continue
            except Exception:
                continue
        iv = _coerce_mark_iv(point)
        if iv is not None:
            out.append(float(iv))
    return out


def _median(xs: list[float]) -> float | None:
    vals = [float(x) for x in xs if math.isfinite(float(x))]
    if not vals:
        return None
    vals.sort()
    mid = len(vals) // 2
    if len(vals) % 2 == 1:
        return float(vals[mid])
    return float((vals[mid - 1] + vals[mid]) / 2.0)


@register_decision("THRESHOLD")
class ThresholdDecision(DecisionBase):
    def __init__(
        self,
        symbol: str | None = None,
        threshold: float = 0.0,
        score_key: str = "main",
        **kwargs,
    ):
        super().__init__(symbol=symbol, **kwargs)
        self.threshold = float(threshold)
        self.score_key = str(score_key)

    def decide(self, context: dict) -> float:
        # Prefer model outputs (v4 context shape). Fallback to legacy flat keys.
        models = context.get("models")
        if not isinstance(models, dict):
            models = {}
        score = models.get(self.score_key)
        if score is None:
            score = context.get("score", 0.0)
        x = float(score)
        if x > self.threshold:
            return 1.0
        if x < -self.threshold:
            return -1.0
        return 0.0
    
@register_decision("ZSCORE-THRESHOLD")
class ZScoreThresholdDecision(DecisionBase):
    """
    V4 Z-score threshold decision with hysteresis.

    Contracts:
    - symbol-agnostic
    - operates primarily on ZSCORE feature from context['features'] (fallback: model outputs)
    - decide(context) -> float in {-1.0, 0.0, +1.0}
    """
    # design-time capability requirement
    required_feature_types = {"ZSCORE"}

    def __init__(
        self,
        symbol: str | None = None,  
        **kwargs,
    ):
        super().__init__(symbol=symbol, **kwargs)
        self.enter = float(kwargs.get("enter", 2.0))
        self.exit = float(kwargs.get("exit", 0.5))
        self.purpose = str(kwargs.get("purpose", "DECISION"))
        self._signal_state = 0.0  # internal state: -1, 0, +1

    def decide(self, context: dict) -> float:
        # Prefer ZSCORE feature (explicit feature dependency). Fallback to model outputs.
        score: float
        features = context.get("features")
        if isinstance(features, dict):
            try:
                # DecisionBase.fget requires symbol; prefer bound self.symbol.
                if self.symbol is None:
                    raise ValueError("ZScoreThresholdDecision requires symbol=... to resolve ZSCORE feature")
                score = float(self.fget(features, ftype="ZSCORE", purpose=self.purpose, symbol=self.symbol))
            except Exception:
                score = 0.0
        else:
            score = 0.0

        if score == 0.0:
            models = context.get("models")
            if isinstance(models, dict):
                # tolerate multiple key conventions
                v = models.get("zscore")
                if v is None:
                    v = models.get("main")
                if v is not None:
                    score = float(v)
            if score == 0.0:
                score = float(context.get("model_score", 0.0))

        # enter logic
        if self._signal_state == 0.0:
            if score >= self.enter:
                self._signal_state = -1.0
            elif score <= -self.enter:
                self._signal_state = 1.0

        # exit logic
        elif self._signal_state > 0 and score >= -self.exit:
            self._signal_state = 0.0
        elif self._signal_state < 0 and score <= self.exit:
            self._signal_state = 0.0

        return float(self._signal_state)


# New Decision: RSIDynamicBandDecision
@register_decision("RSI-DYNAMIC-BAND")
class RSIDynamicBandDecision(DecisionBase):
    """V4 dynamic RSI band decision (sideways-only) with ref-free features.

    Intended semantics (matches the user's vectorized prototype):
    - Compute dynamic bands: upper = rsi_mean + variance_factor * rsi_std,
      lower = rsi_mean - variance_factor * rsi_std.
    - Only *enter* when ADX < adx_threshold (sideways market).
    - Enter long when RSI <= lower + mae.
    - Exit (flatten) when RSI >= upper - mae.

    Notes:
    - This is long/flat (no short) to mirror the prototype.
    - When ADX >= adx_threshold, we do not open new positions; an existing
      position is kept until an exit condition occurs.
    """

    # design-time capability requirement
    required_feature_types = {"RSI", "ADX", "RSI-MEAN", "RSI-STD"}

    def __init__(self, symbol: str | None = None, **kwargs):
        super().__init__(symbol=symbol, **kwargs)
        self.adx_threshold = float(kwargs.get("adx_threshold", 25.0))
        self.variance_factor = float(kwargs.get("variance_factor", 1.8))
        self.mae = float(kwargs.get("mae", 0.0))

        # Feature resolution:
        # - Prefer explicit feature names passed via cfg (after binding/standardize).
        # - Otherwise resolve semantically via fget(ftype,purpose,symbol).
        self.purpose = str(kwargs.get("purpose", "MODEL"))
        self.rsi_name = kwargs.get("rsi")
        self.rsi_mean_name = kwargs.get("rsi_mean")
        self.rsi_std_name = kwargs.get("rsi_std")
        self.adx_name = kwargs.get("adx")

        self._in_position = False

    def _get_feature(self, features: dict, *, name: str | None, ftype: str) -> float:
        if name:
            v = features.get(str(name))
            if v is not None:
                return float(v)
        if self.symbol is None:
            raise ValueError("RSIDynamicBandDecision requires symbol=... to resolve features")
        return float(self.fget(features, ftype=ftype, purpose=self.purpose, symbol=self.symbol))

    def decide(self, context: dict) -> float:
        features = context.get("features")
        portfolio = context.get("portfolio", {})
        self._in_position = portfolio.get('positions', {}).get(self.symbol, {}).get('qty', 0.0) > 1e-8
        if not isinstance(features, dict):
            return 0.0

        try:
            rsi = self._get_feature(features, name=self.rsi_name, ftype="RSI")
            adx = self._get_feature(features, name=self.adx_name, ftype="ADX")
            rsi_mean = self._get_feature(features, name=self.rsi_mean_name, ftype="RSI_ROLLING_MEAN")
            rsi_std = self._get_feature(features, name=self.rsi_std_name, ftype="RSI_ROLLING_STD")
        except Exception:
            # Fail-soft: if features not available yet, keep current position.
            return 0.0

        dynamic_upper = rsi_mean + self.variance_factor * rsi_std
        dynamic_lower = rsi_mean - self.variance_factor * rsi_std

        sideways = adx < self.adx_threshold

        # Enter: only in sideways markets
        if not self._in_position and sideways:
            if rsi <= (dynamic_lower + self.mae):
                self._in_position = True
                return 1.0

        # Exit: allowed regardless of sideways/trending to avoid trapping positions
        if self._in_position:
            if rsi >= (dynamic_upper - self.mae):
                self._in_position = False
                return -1.0

        return 0.0


@register_decision("RSI-IV-DYNAMICAL-ADX-SIDEWAY")
class RSIIVDynamicalADXSidewayDecision(DecisionBase):
    """RSI dynamic-band decision gated by ADX, with IV-driven band width.

    Semantics:
    - dynamic_upper = rsi_mean + (iv * beta + alpha)
    - dynamic_lower = rsi_mean - (iv * beta + alpha)
    - Enter only when ADX < adx_threshold and RSI <= dynamic_lower + mae
    - Exit when RSI >= dynamic_upper - mae
    """

    required_feature_types = {"RSI", "ADX", "RSI-MEAN"}

    def __init__(self, symbol: str | None = None, **kwargs):
        super().__init__(symbol=symbol, **kwargs)
        self.adx_threshold = float(kwargs.get("adx_threshold", 25.0))
        self.alpha = float(kwargs.get("alpha", 0.0))
        self.beta = float(kwargs.get("beta", 0.1))
        self.mae = float(kwargs.get("mae", 0.0))
        self.purpose = str(kwargs.get("purpose", "DECISION"))
        self.rsi_name = kwargs.get("rsi")
        self.rsi_mean_name = kwargs.get("rsi_mean")
        self.adx_name = kwargs.get("adx")
        self.option_x = float(kwargs.get("x", kwargs.get("iv_x_target", 0.0)))
        self.option_tau_ms = _resolve_option_tau_ms(kwargs)
        self._in_position = False

    def _get_feature(self, features: dict, *, name: str | None, ftype: str) -> float:
        if name:
            v = features.get(str(name))
            if v is not None:
                return float(v)
        if self.symbol is None:
            raise ValueError("RSIIVDynamicalADXSidewayDecision requires symbol=... to resolve features")
        return float(self.fget(features, ftype=ftype, purpose=self.purpose, symbol=self.symbol))

    def decide(self, context: dict) -> float:
        features = context.get("features")
        if not isinstance(features, dict):
            return 0.0

        portfolio = context.get("portfolio", {})
        positions = portfolio.get("positions", {}) if isinstance(portfolio, dict) else {}
        sym_pos = positions.get(self.symbol, {}) if isinstance(positions, dict) else {}
        qty = sym_pos.get("qty", 0.0) if isinstance(sym_pos, dict) else 0.0
        self._in_position = float(qty or 0.0) > 1e-8

        try:
            rsi = self._get_feature(features, name=self.rsi_name, ftype="RSI")
            adx = self._get_feature(features, name=self.adx_name, ftype="ADX")
            rsi_mean = self._get_feature(features, name=self.rsi_mean_name, ftype="RSI_ROLLING_MEAN")
        except Exception:
            return 0.0
        iv = _resolve_option_chain_iv_candidate(
            context,
            tau_ms=self.option_tau_ms,
            x=self.option_x,
        )
        if iv is None:
            return 0.0

        width = float(iv) * float(self.beta) + float(self.alpha)
        dynamic_upper = float(rsi_mean) + width
        dynamic_lower = float(rsi_mean) - width
        sideways = float(adx) < float(self.adx_threshold)

        if not self._in_position and sideways and float(rsi) <= (dynamic_lower + self.mae):
            self._in_position = True
            return 1.0

        if self._in_position and float(rsi) >= (dynamic_upper - self.mae):
            self._in_position = False
            return -1.0

        return 0.0


@register_decision("RSI-IV-LINEAR")
class RSIIVLinearDecision(DecisionBase):
    """Linear score from RSI and option-chain mark_iv feature."""

    required_feature_types = {"RSI", "OPTION-MARK-IV"}

    def __init__(self, symbol: str | None = None, **kwargs):
        super().__init__(symbol=symbol, **kwargs)
        self.alpha = float(kwargs.get("alpha", -0.01))
        self.purpose = str(kwargs.get("purpose", "DECISION"))
        self.rsi_name = kwargs.get("rsi")
        self.iv_name = kwargs.get("iv")

    def _get_feature(self, features: dict, *, name: str | None, ftype: str) -> float:
        if name:
            v = features.get(str(name))
            if v is not None:
                return float(v)
        if self.symbol is None:
            raise ValueError("RSIIVLinearDecision requires symbol=... to resolve features")
        return float(self.fget(features, ftype=ftype, purpose=self.purpose, symbol=self.symbol))

    def decide(self, context: dict) -> float:
        features = context.get("features")
        if not isinstance(features, dict):
            return 0.0
        try:
            rsi = self._get_feature(features, name=self.rsi_name, ftype="RSI")
            iv = self._get_feature(features, name=self.iv_name, ftype="OPTION-MARK-IV")
        except Exception:
            return 0.0

        rsi_component = (float(rsi) - 50.0) / 50.0
        iv_component = float(iv)
        score = float(rsi_component + self.alpha * iv_component)
        return max(-1.0, min(1.0, score))


@register_decision("RSI-IV-DYNAMICAL-BAND")
class RSIIVDynamicalBandDecision(DecisionBase):
    """Long/flat RSI band decision with IV-driven dynamic width around RSI mean.

    Semantics:
    - dynamic_upper = rsi_mean + (iv * beta + alpha)
    - dynamic_lower = rsi_mean - (iv * beta + alpha)
    - Enter long when RSI <= dynamic_lower + mae
    - Exit (flatten) when RSI >= dynamic_upper - mae
    """

    required_feature_types = {"RSI", "RSI-MEAN"}

    def __init__(self, symbol: str | None = None, **kwargs):
        super().__init__(symbol=symbol, **kwargs)
        self.alpha = float(kwargs.get("alpha", 0.0))
        self.beta = float(kwargs.get("beta", 0.1))
        self.mae = float(kwargs.get("mae", 0.0))
        self.purpose = str(kwargs.get("purpose", "DECISION"))
        self.rsi_name = kwargs.get("rsi")
        self.rsi_mean_name = kwargs.get("rsi_mean")
        self.option_x = float(kwargs.get("x", kwargs.get("iv_x_target", 0.0)))
        self.option_tau_ms = _resolve_option_tau_ms(kwargs)
        self._in_position = False

    def _get_feature(self, features: dict, *, name: str | None, ftype: str) -> float:
        if name:
            v = features.get(str(name))
            if v is not None:
                return float(v)
        if self.symbol is None:
            raise ValueError("RSIIVDynamicalBandDecision requires symbol=... to resolve features")
        return float(self.fget(features, ftype=ftype, purpose=self.purpose, symbol=self.symbol))

    def decide(self, context: dict) -> float:
        features = context.get("features")
        if not isinstance(features, dict):
            return 0.0

        portfolio = context.get("portfolio", {})
        positions = portfolio.get("positions", {}) if isinstance(portfolio, dict) else {}
        sym_pos = positions.get(self.symbol, {}) if isinstance(positions, dict) else {}
        qty = sym_pos.get("qty", 0.0) if isinstance(sym_pos, dict) else 0.0
        self._in_position = float(qty or 0.0) > 1e-8

        try:
            rsi = self._get_feature(features, name=self.rsi_name, ftype="RSI")
            rsi_mean = self._get_feature(features, name=self.rsi_mean_name, ftype="RSI_ROLLING_MEAN")
        except Exception:
            return 0.0
        iv = _resolve_option_chain_iv_candidate(
            context,
            tau_ms=self.option_tau_ms,
            x=self.option_x,
        )
        if iv is None:
            return 0.0

        width = float(iv) * float(self.beta) + float(self.alpha)
        center = float(rsi_mean)
        dynamic_upper = center + width
        dynamic_lower = center - width

        if not self._in_position and rsi <= (dynamic_lower + self.mae):
            self._in_position = True
            return 1.0

        if self._in_position and rsi >= (dynamic_upper - self.mae):
            self._in_position = False
            return -1.0

        return 0.0


@register_decision("RSI-IV-RELSTATE-ADX-SIDEWAY")
class RSIIVRelativeStateADXSidewayDecision(DecisionBase):
    """ADX-gated RSI dynamic band with bounded relative-IV width scaling.

    Semantics:
    - base_width = variance_factor * rsi_std
    - relative_iv = current_iv / median(recent_iv_history)
    - iv_scale = clip(relative_iv, iv_scale_low, iv_scale_high)
    - dynamic_upper = rsi_mean + base_width * iv_scale
    - dynamic_lower = rsi_mean - base_width * iv_scale
    """

    required_feature_types = {"RSI", "ADX", "RSI-MEAN", "RSI-STD"}

    def __init__(self, symbol: str | None = None, **kwargs):
        super().__init__(symbol=symbol, **kwargs)
        self.adx_threshold = float(kwargs.get("adx_threshold", 25.0))
        self.variance_factor = float(kwargs.get("variance_factor", 1.8))
        self.mae = float(kwargs.get("mae", 0.0))
        self.iv_scale_low = float(kwargs.get("iv_scale_low", 0.75))
        self.iv_scale_high = float(kwargs.get("iv_scale_high", 1.25))
        self.iv_background_points = int(float(kwargs.get("iv_background_points", 24)))
        self.purpose = str(kwargs.get("purpose", "DECISION"))
        self.rsi_name = kwargs.get("rsi")
        self.rsi_mean_name = kwargs.get("rsi_mean")
        self.rsi_std_name = kwargs.get("rsi_std")
        self.adx_name = kwargs.get("adx")
        self.option_x = float(kwargs.get("x", kwargs.get("iv_x_target", 0.0)))
        self.option_tau_ms = _resolve_option_tau_ms(kwargs)
        self._in_position = False

    def _get_feature(self, features: dict, *, name: str | None, ftype: str) -> float:
        if name:
            v = features.get(str(name))
            if v is not None:
                return float(v)
        if self.symbol is None:
            raise ValueError("RSIIVRelativeStateADXSidewayDecision requires symbol=... to resolve features")
        return float(self.fget(features, ftype=ftype, purpose=self.purpose, symbol=self.symbol))

    def decide(self, context: dict) -> float:
        features = context.get("features")
        if not isinstance(features, dict):
            return 0.0

        portfolio = context.get("portfolio", {})
        positions = portfolio.get("positions", {}) if isinstance(portfolio, dict) else {}
        sym_pos = positions.get(self.symbol, {}) if isinstance(positions, dict) else {}
        qty = sym_pos.get("qty", 0.0) if isinstance(sym_pos, dict) else 0.0
        self._in_position = float(qty or 0.0) > 1e-8

        try:
            rsi = self._get_feature(features, name=self.rsi_name, ftype="RSI")
            adx = self._get_feature(features, name=self.adx_name, ftype="ADX")
            rsi_mean = self._get_feature(features, name=self.rsi_mean_name, ftype="RSI_ROLLING_MEAN")
            rsi_std = self._get_feature(features, name=self.rsi_std_name, ftype="RSI_ROLLING_STD")
        except Exception:
            return 0.0

        iv_history = _resolve_option_chain_iv_history(
            context,
            tau_ms=self.option_tau_ms,
            x=self.option_x,
            n=self.iv_background_points,
        )
        if len(iv_history) < 3:
            return 0.0
        current_iv = float(iv_history[-1])
        background_iv = _median(iv_history[:-1])
        if background_iv is None or background_iv <= 0.0:
            return 0.0

        relative_iv = float(current_iv) / float(background_iv)
        iv_scale = max(float(self.iv_scale_low), min(float(self.iv_scale_high), float(relative_iv)))
        base_width = float(self.variance_factor) * float(rsi_std)
        width = float(base_width) * float(iv_scale)
        dynamic_upper = float(rsi_mean) + float(width)
        dynamic_lower = float(rsi_mean) - float(width)
        sideways = float(adx) < float(self.adx_threshold)

        if not self._in_position and sideways and float(rsi) <= (dynamic_lower + self.mae):
            self._in_position = True
            return 1.0

        if self._in_position and float(rsi) >= (dynamic_upper - self.mae):
            self._in_position = False
            return -1.0

        return 0.0
