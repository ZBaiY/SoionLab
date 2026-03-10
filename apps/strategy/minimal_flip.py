"""
Minimal live execution plumbing strategy.

This strategy exists only to validate live order flow:
- close any existing position
- if flat, target full long
"""

from __future__ import annotations

from quant_engine.contracts.decision import DecisionBase
from quant_engine.contracts.feature import FeatureChannelBase
from quant_engine.decision.registry import register_decision
from quant_engine.features.registry import register_feature
from quant_engine.strategy.base import StrategyBase
from quant_engine.strategy.registry import register_strategy


@register_feature("LIVE-TEST-ANCHOR")
class LiveTestAnchorFeature(FeatureChannelBase):
    """Minimal OHLCV touchpoint to satisfy engine preload/warmup contracts."""

    def __init__(self, *, name: str, symbol: str | None = None, **kwargs):
        super().__init__(name=name, symbol=symbol, **kwargs)
        self._value: float | None = None

    def required_window(self) -> dict[str, int]:
        return {"ohlcv": 1}

    def initialize(self, context: dict, warmup_window: int | None = None) -> None:
        self._warmup_by_update(context, warmup_window, data_type="ohlcv")

    def update(self, context: dict) -> None:
        snap = self.get_snapshot(context, "ohlcv")
        if snap is None:
            return
        close = snap.get_attr("close")
        if close is None:
            return
        self._value = float(close)

    def output(self) -> float | None:
        return self._value


@register_decision("LIVE-TEST-FLIP-DECISION")
class LiveTestFlipDecision(DecisionBase):
    """Emit direct target-position intent from current portfolio state."""

    def __init__(self, symbol: str | None = None, position_epsilon: float = 1e-6, **kwargs):
        super().__init__(symbol=symbol, **kwargs)
        self.position_epsilon = float(position_epsilon)

    def decide(self, context: dict) -> float:
        portfolio = context.get("portfolio", {})
        qty = float(portfolio.get("position_qty", portfolio.get("position", 0.0)) or 0.0)
        if abs(qty) > self.position_epsilon:
            return 0.0
        return 1.0


@register_strategy("LIVE-TEST-FLIP")
class LiveTestFlipStrategy(StrategyBase):
    STRATEGY_NAME = "LIVE-TEST-FLIP"
    INTERVAL = "15m"
    UNIVERSE_TEMPLATE = {
        "primary": "{A}",
        "soft_readiness": {
            "enabled": False,
            "domains": ["orderbook", "option_chain", "iv_surface", "sentiment"],
            "max_staleness_ms": 300000,
        },
    }

    DATA = {
        "primary": {
            "ohlcv": {"$ref": "OHLCV_15M_180D"},
        }
    }

    REQUIRED_DATA = {"ohlcv"}
    FEATURES_USER = [
        {
            "name": "LIVE-TEST-ANCHOR_MODEL_{A}",
            "type": "LIVE-TEST-ANCHOR",
            "symbol": "{A}",
        }
    ]
    MODEL_CFG = None
    DECISION_CFG = {
        "type": "LIVE-TEST-FLIP-DECISION",
        "params": {"position_epsilon": 1e-9},
    }
    RISK_CFG = {
        "shortable": False,
        "mapping": {"name": "identity"},
        "rules": {
            "FRACTIONAL-CASH-CONSTRAINT": {
                "params": {
                    "fee_rate": 0.001,
                    "slippage_bound_bps": 10,
                    "min_notional": 10.0,
                },
            },
        },
    }
    EXECUTION_CFG = {
        "policy": {"type": "IMMEDIATE"},
        "router": {"type": "SIMPLE"},
        "slippage": {"type": "LINEAR"},
        "matching": {"type": "SIMULATED"},
    }
    PORTFOLIO_CFG = {
        "type": "FRACTIONAL",
        "params": {"initial_capital": 1000000, "step_size": 0.001},
    }
