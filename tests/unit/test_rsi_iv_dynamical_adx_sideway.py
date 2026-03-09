from __future__ import annotations

from typing import Any

import pytest

from apps.strategy.RSI_ranges import (
    RSIIVDynamicalADXSidewayStrategy,
    RSIIVDynamicalADXSidewayStrategyFractional,
)
from quant_engine.decision.threshold import RSIIVDynamicalADXSidewayDecision


class _FakeDecisionSnapshot:
    def __init__(self, data_ts: int):
        self.data_ts = int(data_ts)


class _FakeDecisionOptionChainHandler:
    def __init__(self, responses_by_ts: dict[int, tuple[dict[str, Any] | None, dict[str, Any]]]):
        self.responses_by_ts = {int(k): v for k, v in responses_by_ts.items()}

    def window(self, ts: int | None = None, n: int = 1) -> list[_FakeDecisionSnapshot]:
        cutoff = int(ts) if ts is not None else max(self.responses_by_ts.keys(), default=0)
        eligible = sorted(k for k in self.responses_by_ts.keys() if int(k) <= cutoff)
        return [_FakeDecisionSnapshot(k) for k in eligible[-int(n):]]

    def select_point(self, ts: int | None = None, **kwargs: Any) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        if ts is None:
            return None, {"snapshot_data_ts": None}
        return self.responses_by_ts.get(int(ts), (None, {"snapshot_data_ts": int(ts)}))


def _decision_ctx(
    *,
    ts: int,
    handler: Any,
    features: dict[str, Any],
    qty: float = 0.0,
    admitted: bool = True,
    horizon_ms: int = 900_000,
) -> dict[str, Any]:
    return {
        "timestamp": int(ts),
        "features": dict(features),
        "portfolio": {"positions": {"BTCUSDT": {"qty": float(qty)}}},
        "option_chain_decision_ctx": {
            "admitted": bool(admitted),
            "handler": handler,
            "candidate_horizon_ms": int(horizon_ms),
        },
    }


def test_rsi_iv_dynamical_adx_sideway_standardize_bindings() -> None:
    cfg = RSIIVDynamicalADXSidewayStrategy.standardize(
        symbols={
            "A": "BTCUSDT",
            "alpha": "0.1",
            "beta": "0.1",
            "window_RSI": "14",
            "window_ADX": "14",
            "window_RSI_rolling": "5",
            "iv_x_target": "0.0",
            "iv_tau_days": "30",
            "iv_freshness_ms": "600000",
        }
    )
    d = cfg.to_dict()
    assert set(d["required_data"]) == {"ohlcv", "option_chain"}
    assert d["decision"]["type"] == "RSI-IV-DYNAMICAL-ADX-SIDEWAY"
    assert d["decision"]["params"]["alpha"] == "0.1"
    assert d["decision"]["params"]["beta"] == "0.1"
    assert d["decision"]["params"]["x"] == "0.0"
    assert d["decision"]["params"]["tau_days"] == "30"
    names = [f["name"] for f in d["features_user"]]
    assert "RSI_DECISION_BTCUSDT" in names
    assert "ADX_DECISION_BTCUSDT" in names
    assert "RSI-MEAN_DECISION_BTCUSDT" in names
    assert "OPTION-MARK-IV_DECISION_BTCUSDT" not in names


def test_rsi_iv_dynamical_adx_sideway_fractional_standardize_bindings() -> None:
    cfg = RSIIVDynamicalADXSidewayStrategyFractional.standardize(
        symbols={
            "A": "BTCUSDT",
            "alpha": "0.1",
            "beta": "0.1",
            "window_RSI": "14",
            "window_ADX": "14",
            "window_RSI_rolling": "5",
            "iv_x_target": "0.0",
            "iv_tau_days": "30",
            "iv_freshness_ms": "600000",
        }
    )
    d = cfg.to_dict()
    assert d["decision"]["type"] == "RSI-IV-DYNAMICAL-ADX-SIDEWAY"
    assert d["risk"]["rules"].get("FRACTIONAL-CASH-CONSTRAINT") is not None
    assert d["portfolio"]["type"] == "FRACTIONAL"


def test_decision_entry_exit_and_sideway_gate() -> None:
    handler = _FakeDecisionOptionChainHandler(
        {
            1_000: (
                {"value_fields": {"mark_iv": 10.0}},
                {"snapshot_data_ts": 1_000},
            )
        }
    )
    decision = RSIIVDynamicalADXSidewayDecision(
        symbol="BTCUSDT",
        rsi="RSI_DECISION_BTCUSDT",
        rsi_mean="RSI-MEAN_DECISION_BTCUSDT",
        adx="ADX_DECISION_BTCUSDT",
        adx_threshold=25.0,
        alpha=0.0,
        beta=0.5,
        mae=0.0,
        x=0.0,
        tau_days=30,
    )

    # rsi_mean=50, iv=10, beta=0.5, alpha=0 => lower=45 upper=55
    enter_ctx = _decision_ctx(
        ts=1_000,
        handler=handler,
        features={
            "RSI_DECISION_BTCUSDT": 44.0,
            "RSI-MEAN_DECISION_BTCUSDT": 50.0,
            "ADX_DECISION_BTCUSDT": 20.0,
        },
    )
    assert decision.decide(enter_ctx) == pytest.approx(1.0)

    # Exit is allowed even when ADX is above threshold.
    exit_ctx = _decision_ctx(
        ts=1_000,
        handler=handler,
        features={
            "RSI_DECISION_BTCUSDT": 56.0,
            "RSI-MEAN_DECISION_BTCUSDT": 50.0,
            "ADX_DECISION_BTCUSDT": 35.0,
        },
        qty=1.0,
    )
    assert decision.decide(exit_ctx) == pytest.approx(-1.0)

    # No new entry when not sideway.
    no_entry_ctx = _decision_ctx(
        ts=1_000,
        handler=handler,
        features={
            "RSI_DECISION_BTCUSDT": 44.0,
            "RSI-MEAN_DECISION_BTCUSDT": 50.0,
            "ADX_DECISION_BTCUSDT": 30.0,
        },
    )
    assert decision.decide(no_entry_ctx) == 0.0


def test_decision_neutral_when_option_chain_not_admitted() -> None:
    handler = _FakeDecisionOptionChainHandler(
        {
            1_000: (
                {"value_fields": {"mark_iv": 10.0}},
                {"snapshot_data_ts": 1_000},
            )
        }
    )
    decision = RSIIVDynamicalADXSidewayDecision(
        symbol="BTCUSDT",
        rsi="RSI_DECISION_BTCUSDT",
        rsi_mean="RSI-MEAN_DECISION_BTCUSDT",
        adx="ADX_DECISION_BTCUSDT",
        alpha=0.0,
        beta=0.5,
        x=0.0,
        tau_days=30,
    )
    context = _decision_ctx(
        ts=1_000,
        handler=handler,
        features={
            "RSI_DECISION_BTCUSDT": 40.0,
            "RSI-MEAN_DECISION_BTCUSDT": 50.0,
            "ADX_DECISION_BTCUSDT": 20.0,
        },
        admitted=False,
    )
    assert decision.decide(context) == 0.0
