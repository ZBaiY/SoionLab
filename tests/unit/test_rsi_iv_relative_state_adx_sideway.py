from __future__ import annotations

from typing import Any

import pytest

from apps.strategy.RSI_ranges import RSIIVRelativeStateADXSidewayStrategyFractional
from quant_engine.decision.threshold import RSIIVRelativeStateADXSidewayDecision


class _FakeSnapshot:
    def __init__(self, data_ts: int):
        self.data_ts = int(data_ts)


class _FakeOptionChainHandler:
    def __init__(self, responses_by_ts: dict[int, tuple[dict[str, Any] | None, dict[str, Any]]]):
        self.responses_by_ts = {int(k): v for k, v in responses_by_ts.items()}

    def window(self, ts: int | None = None, n: int = 1) -> list[_FakeSnapshot]:
        cutoff = int(ts) if ts is not None else max(self.responses_by_ts.keys(), default=0)
        eligible = sorted(k for k in self.responses_by_ts.keys() if int(k) <= cutoff)
        return [_FakeSnapshot(k) for k in eligible[-int(n):]]

    def select_point(self, ts: int | None = None, **kwargs: Any) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        if ts is None:
            return None, {"snapshot_data_ts": None}
        return self.responses_by_ts.get(int(ts), (None, {"snapshot_data_ts": int(ts)}))


def _ctx(
    *,
    ts: int,
    handler: Any,
    features: dict[str, Any],
    qty: float = 0.0,
    admitted: bool = True,
) -> dict[str, Any]:
    return {
        "timestamp": int(ts),
        "features": dict(features),
        "portfolio": {"positions": {"BTCUSDT": {"qty": float(qty)}}},
        "option_chain_decision_ctx": {
            "admitted": bool(admitted),
            "handler": handler,
            "candidate_horizon_ms": 900_000,
        },
    }


def test_relative_state_strategy_fractional_standardize_bindings() -> None:
    cfg = RSIIVRelativeStateADXSidewayStrategyFractional.standardize(
        symbols={
            "A": "BTCUSDT",
            "window_RSI": "14",
            "window_ADX": "14",
            "window_RSI_rolling": "5",
            "iv_x_target": "0.0",
            "iv_tau_days": "7",
        }
    )
    d = cfg.to_dict()
    assert d["decision"]["type"] == "RSI-IV-RELSTATE-ADX-SIDEWAY"
    assert d["decision"]["params"]["tau_days"] == "7"
    assert d["decision"]["params"]["iv_background_points"] == 24
    assert d["risk"]["rules"].get("FRACTIONAL-CASH-CONSTRAINT") is not None


def test_relative_state_decision_uses_clipped_multiplier_for_entry_and_exit() -> None:
    handler = _FakeOptionChainHandler(
        {
            1_000: ({"value_fields": {"mark_iv": 10.0}}, {"snapshot_data_ts": 1_000}),
            2_000: ({"value_fields": {"mark_iv": 10.0}}, {"snapshot_data_ts": 2_000}),
            3_000: ({"value_fields": {"mark_iv": 10.0}}, {"snapshot_data_ts": 3_000}),
            4_000: ({"value_fields": {"mark_iv": 20.0}}, {"snapshot_data_ts": 4_000}),
        }
    )
    decision = RSIIVRelativeStateADXSidewayDecision(
        symbol="BTCUSDT",
        rsi="RSI_DECISION_BTCUSDT",
        rsi_mean="RSI-MEAN_DECISION_BTCUSDT",
        rsi_std="RSI-STD_DECISION_BTCUSDT",
        adx="ADX_DECISION_BTCUSDT",
        adx_threshold=25.0,
        variance_factor=2.0,
        iv_scale_low=0.75,
        iv_scale_high=1.25,
        iv_background_points=4,
        x=0.0,
        tau_days=7,
    )

    # current_iv / median(history) = 20 / 10 = 2.0 -> clipped to 1.25
    # base_width = 2 * 4 = 8 => width = 10
    enter = _ctx(
        ts=4_000,
        handler=handler,
        features={
            "RSI_DECISION_BTCUSDT": 39.5,
            "RSI-MEAN_DECISION_BTCUSDT": 50.0,
            "RSI-STD_DECISION_BTCUSDT": 4.0,
            "ADX_DECISION_BTCUSDT": 20.0,
        },
    )
    assert decision.decide(enter) == pytest.approx(1.0)

    exit_ctx = _ctx(
        ts=4_000,
        handler=handler,
        features={
            "RSI_DECISION_BTCUSDT": 60.5,
            "RSI-MEAN_DECISION_BTCUSDT": 50.0,
            "RSI-STD_DECISION_BTCUSDT": 4.0,
            "ADX_DECISION_BTCUSDT": 35.0,
        },
        qty=1.0,
    )
    assert decision.decide(exit_ctx) == pytest.approx(-1.0)


def test_relative_state_decision_neutral_when_history_missing() -> None:
    handler = _FakeOptionChainHandler(
        {
            1_000: ({"value_fields": {"mark_iv": 10.0}}, {"snapshot_data_ts": 1_000}),
            2_000: ({"value_fields": {"mark_iv": 11.0}}, {"snapshot_data_ts": 2_000}),
        }
    )
    decision = RSIIVRelativeStateADXSidewayDecision(
        symbol="BTCUSDT",
        rsi="RSI_DECISION_BTCUSDT",
        rsi_mean="RSI-MEAN_DECISION_BTCUSDT",
        rsi_std="RSI-STD_DECISION_BTCUSDT",
        adx="ADX_DECISION_BTCUSDT",
        variance_factor=1.8,
        iv_background_points=4,
        x=0.0,
        tau_days=7,
    )
    context = _ctx(
        ts=2_000,
        handler=handler,
        features={
            "RSI_DECISION_BTCUSDT": 40.0,
            "RSI-MEAN_DECISION_BTCUSDT": 50.0,
            "RSI-STD_DECISION_BTCUSDT": 4.0,
            "ADX_DECISION_BTCUSDT": 20.0,
        },
    )
    assert decision.decide(context) == 0.0
