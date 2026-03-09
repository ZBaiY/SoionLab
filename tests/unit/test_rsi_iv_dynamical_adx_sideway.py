from __future__ import annotations

import pytest

from apps.strategy.RSI_ranges import (
    RSIIVDynamicalADXSidewayStrategy,
    RSIIVDynamicalADXSidewayStrategyFractional,
)
from quant_engine.decision.threshold import RSIIVDynamicalADXSidewayDecision


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
    names = [f["name"] for f in d["features_user"]]
    assert "RSI_DECISION_BTCUSDT" in names
    assert "ADX_DECISION_BTCUSDT" in names
    assert "RSI-MEAN_DECISION_BTCUSDT" in names
    assert "OPTION-MARK-IV_DECISION_BTCUSDT" in names


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
    decision = RSIIVDynamicalADXSidewayDecision(
        symbol="BTCUSDT",
        rsi="RSI_DECISION_BTCUSDT",
        rsi_mean="RSI-MEAN_DECISION_BTCUSDT",
        adx="ADX_DECISION_BTCUSDT",
        iv="OPTION-MARK-IV_DECISION_BTCUSDT",
        adx_threshold=25.0,
        alpha=0.0,
        beta=0.5,
        mae=0.0,
    )

    # rsi_mean=50, iv=10, beta=0.5, alpha=0 => lower=45 upper=55
    enter_ctx = {
        "features": {
            "RSI_DECISION_BTCUSDT": 44.0,
            "RSI-MEAN_DECISION_BTCUSDT": 50.0,
            "ADX_DECISION_BTCUSDT": 20.0,
            "OPTION-MARK-IV_DECISION_BTCUSDT": 10.0,
        },
        "portfolio": {"positions": {"BTCUSDT": {"qty": 0.0}}},
    }
    assert decision.decide(enter_ctx) == pytest.approx(1.0)

    # Exit is allowed even when ADX is above threshold.
    exit_ctx = {
        "features": {
            "RSI_DECISION_BTCUSDT": 56.0,
            "RSI-MEAN_DECISION_BTCUSDT": 50.0,
            "ADX_DECISION_BTCUSDT": 35.0,
            "OPTION-MARK-IV_DECISION_BTCUSDT": 10.0,
        },
        "portfolio": {"positions": {"BTCUSDT": {"qty": 1.0}}},
    }
    assert decision.decide(exit_ctx) == pytest.approx(-1.0)

    # No new entry when not sideway.
    no_entry_ctx = {
        "features": {
            "RSI_DECISION_BTCUSDT": 44.0,
            "RSI-MEAN_DECISION_BTCUSDT": 50.0,
            "ADX_DECISION_BTCUSDT": 30.0,
            "OPTION-MARK-IV_DECISION_BTCUSDT": 10.0,
        },
        "portfolio": {"positions": {"BTCUSDT": {"qty": 0.0}}},
    }
    assert decision.decide(no_entry_ctx) == 0.0


def test_decision_neutral_when_iv_missing() -> None:
    decision = RSIIVDynamicalADXSidewayDecision(
        symbol="BTCUSDT",
        rsi="RSI_DECISION_BTCUSDT",
        rsi_mean="RSI-MEAN_DECISION_BTCUSDT",
        adx="ADX_DECISION_BTCUSDT",
        iv="OPTION-MARK-IV_DECISION_BTCUSDT",
        alpha=0.0,
        beta=0.5,
    )
    context = {
        "features": {
            "RSI_DECISION_BTCUSDT": 40.0,
            "RSI-MEAN_DECISION_BTCUSDT": 50.0,
            "ADX_DECISION_BTCUSDT": 20.0,
            "OPTION-MARK-IV_DECISION_BTCUSDT": None,
        },
        "portfolio": {"positions": {"BTCUSDT": {"qty": 0.0}}},
    }
    assert decision.decide(context) == 0.0
