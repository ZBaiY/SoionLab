# decision/threshold.py
from quant_engine.contracts.decision import DecisionBase
from .registry import register_decision


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
        return 1.0 if x > self.threshold else -1.0
    
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
        self._position = 0.0  # internal state: -1, 0, +1

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
        if self._position == 0.0:
            if score >= self.enter:
                self._position = -1.0
            elif score <= -self.enter:
                self._position = 1.0

        # exit logic
        elif self._position > 0 and score >= -self.exit:
            self._position = 0.0
        elif self._position < 0 and score <= self.exit:
            self._position = 0.0

        return float(self._position)


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

        self._position = 0.0  # 0.0 (flat) or 1.0 (long)

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
        if not isinstance(features, dict):
            return float(self._position)

        try:
            rsi = self._get_feature(features, name=self.rsi_name, ftype="RSI")
            adx = self._get_feature(features, name=self.adx_name, ftype="ADX")
            rsi_mean = self._get_feature(features, name=self.rsi_mean_name, ftype="RSI_ROLLING_MEAN")
            rsi_std = self._get_feature(features, name=self.rsi_std_name, ftype="RSI_ROLLING_STD")
        except Exception:
            # Fail-soft: if features not available yet, keep current position.
            return float(self._position)

        dynamic_upper = rsi_mean + self.variance_factor * rsi_std
        dynamic_lower = rsi_mean - self.variance_factor * rsi_std

        sideways = adx < self.adx_threshold

        # Enter: only in sideways markets
        if self._position == 0.0 and sideways:
            if rsi <= (dynamic_lower + self.mae):
                self._position = 1.0

        # Exit: allowed regardless of sideways/trending to avoid trapping positions
        elif self._position > 0.0:
            if rsi >= (dynamic_upper - self.mae):
                self._position = 0.0

        return float(self._position)