from __future__ import annotations

from typing import Any

import pytest
import pandas as pd

from apps.strategy.RSI_ranges import RSIIVDynamicalStrategy, RSIIVDynamicalStrategyFractional
from ingestion.contracts.tick import IngestionTick
from quant_engine.decision.threshold import RSIIVDynamicalBandDecision, RSIIVLinearDecision
from quant_engine.features.registry import build_feature
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler


DAY_MS = 86_400_000


class _FakeOptionChainHandler:
    bootstrap_cfg: dict[str, Any] = {}

    def __init__(self, responses: list[tuple[dict[str, Any] | None, dict[str, Any]]], *, stale_ms: int = 600_000):
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []
        self.quality_cfg = {"stale_ms": int(stale_ms)}

    def select_point(self, ts: int | None = None, **kwargs: Any) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        self.calls.append({"ts": ts, **kwargs})
        if self.responses:
            return self.responses.pop(0)
        return None, {"snapshot_data_ts": ts}


def _ctx(ts: int, handler: Any) -> dict[str, Any]:
    return {
        "timestamp": int(ts),
        "engine_interval_ms": 900_000,
        "data": {"option_chain": {"BTCUSDT": handler}},
    }


def test_rsi_iv_strategy_standardize_bindings() -> None:
    cfg = RSIIVDynamicalStrategy.standardize(
        symbols={
            "A": "BTCUSDT",
            "alpha": "-0.01",
            "beta": "-0.01",
            "rsi_window": "14",
            "rsi_mean_window": "5",
            "iv_x_target": "0.0",
            "iv_tau_days": "30",
            "iv_freshness_ms": "600000",
        }
    )

    d = cfg.to_dict()
    assert set(d["required_data"]) == {"ohlcv", "option_chain"}
    assert d["decision"]["type"] == "RSI-IV-DYNAMICAL-BAND"
    assert d["decision"]["params"]["alpha"] == "-0.01"
    assert d["decision"]["params"]["beta"] == "-0.01"
    names = [f["name"] for f in d["features_user"]]
    assert "RSI_DECISION_BTCUSDT" in names
    assert "RSI-MEAN_DECISION_BTCUSDT" in names
    assert "OPTION-MARK-IV_DECISION_BTCUSDT" in names


def test_rsi_iv_fractional_strategy_standardize_bindings() -> None:
    cfg = RSIIVDynamicalStrategyFractional.standardize(
        symbols={
            "A": "BTCUSDT",
            "alpha": "-0.01",
            "beta": "-0.01",
            "rsi_window": "14",
            "rsi_mean_window": "5",
            "iv_x_target": "0.0",
            "iv_tau_days": "30",
            "iv_freshness_ms": "600000",
        }
    )

    d = cfg.to_dict()
    assert d["decision"]["type"] == "RSI-IV-DYNAMICAL-BAND"
    assert d["risk"]["rules"].get("FRACTIONAL-CASH-CONSTRAINT") is not None
    assert d["portfolio"]["type"] == "FRACTIONAL"


def test_option_mark_iv_feature_default_selects_atm_30d() -> None:
    handler = _FakeOptionChainHandler(
        responses=[
            (
                {"value_fields": {"mark_iv": 0.42}},
                {"snapshot_data_ts": 1_000},
            )
        ]
    )
    feature = build_feature("OPTION-MARK-IV", name="OPTION-MARK-IV_DECISION_BTCUSDT", symbol="BTCUSDT")
    feature.update(_ctx(1_000, handler))

    assert handler.calls
    call = handler.calls[0]
    assert int(call["tau_ms"]) == 30 * DAY_MS
    assert float(call["x"]) == pytest.approx(0.0)
    assert call["price_field"] == "mark_iv"
    assert float(feature.output()) == pytest.approx(0.42)


def test_option_mark_iv_feature_respects_configured_x_and_tau() -> None:
    handler = _FakeOptionChainHandler(
        responses=[
            (
                {"value_fields": {"mark_iv": 0.37}},
                {"snapshot_data_ts": 2_000},
            )
        ]
    )
    feature = build_feature(
        "OPTION-MARK-IV",
        name="OPTION-MARK-IV_DECISION_BTCUSDT",
        symbol="BTCUSDT",
        x=0.02,
        tau_days=7,
        freshness_ms=300_000,
    )
    feature.update(_ctx(2_000, handler))

    call = handler.calls[0]
    assert float(call["x"]) == pytest.approx(0.02)
    assert int(call["tau_ms"]) == 7 * DAY_MS
    assert float(feature.output()) == pytest.approx(0.37)


def test_option_mark_iv_feature_strict_neutral_on_missing_or_stale() -> None:
    handler = _FakeOptionChainHandler(
        responses=[
            ({"value_fields": {"mark_iv": 0.33}}, {"snapshot_data_ts": 1_000}),
            (None, {"snapshot_data_ts": 2_000}),
            (None, {"snapshot_data_ts": 7_001}),
        ]
    )
    feature = build_feature(
        "OPTION-MARK-IV",
        name="OPTION-MARK-IV_DECISION_BTCUSDT",
        symbol="BTCUSDT",
        freshness_ms=5_000,
    )

    feature.update(_ctx(1_000, handler))
    assert float(feature.output()) == pytest.approx(0.33)

    feature.update(_ctx(2_000, handler))
    assert feature.output() is None

    feature.update(_ctx(7_001, handler))
    assert feature.output() is None


def test_option_mark_iv_feature_rejects_lookahead() -> None:
    handler = _FakeOptionChainHandler(
        responses=[
            ({"value_fields": {"mark_iv": 0.9}}, {"snapshot_data_ts": 1_001}),
        ]
    )
    feature = build_feature(
        "OPTION-MARK-IV",
        name="OPTION-MARK-IV_DECISION_BTCUSDT",
        symbol="BTCUSDT",
        freshness_ms=5_000,
    )
    feature.update(_ctx(1_000, handler))
    assert feature.output() is None


def test_rsi_iv_linear_decision_score_formula() -> None:
    decision = RSIIVLinearDecision(
        symbol="BTCUSDT",
        rsi="RSI_DECISION_BTCUSDT",
        iv="OPTION-MARK-IV_DECISION_BTCUSDT",
        alpha=0.5,
    )
    context = {
        "features": {
            "RSI_DECISION_BTCUSDT": 60.0,
            "OPTION-MARK-IV_DECISION_BTCUSDT": 0.2,
        }
    }
    out = decision.decide(context)
    expected = ((60.0 - 50.0) / 50.0) + 0.5 * 0.2
    assert out == pytest.approx(expected)


def test_rsi_iv_linear_decision_is_neutral_when_iv_missing() -> None:
    decision = RSIIVLinearDecision(
        symbol="BTCUSDT",
        rsi="RSI_DECISION_BTCUSDT",
        iv="OPTION-MARK-IV_DECISION_BTCUSDT",
        alpha=0.5,
    )
    context = {
        "features": {
            "RSI_DECISION_BTCUSDT": 60.0,
            "OPTION-MARK-IV_DECISION_BTCUSDT": None,
        }
    }
    assert decision.decide(context) == 0.0


def test_rsi_iv_dynamical_band_decision_entry_exit() -> None:
    decision = RSIIVDynamicalBandDecision(
        symbol="BTCUSDT",
        rsi="RSI_DECISION_BTCUSDT",
        rsi_mean="RSI-MEAN_DECISION_BTCUSDT",
        iv="OPTION-MARK-IV_DECISION_BTCUSDT",
        alpha=0.0,
        beta=0.1,
        mae=0.0,
    )

    # width = 0.1 * 40 = 4 => band [46, 54]
    enter_ctx = {
        "features": {
            "RSI_DECISION_BTCUSDT": 45.0,
            "RSI-MEAN_DECISION_BTCUSDT": 50.0,
            "OPTION-MARK-IV_DECISION_BTCUSDT": 40.0,
        },
        "portfolio": {"positions": {"BTCUSDT": {"qty": 0.0}}},
    }
    assert decision.decide(enter_ctx) == pytest.approx(1.0)

    exit_ctx = {
        "features": {
            "RSI_DECISION_BTCUSDT": 54.0,
            "RSI-MEAN_DECISION_BTCUSDT": 50.0,
            "OPTION-MARK-IV_DECISION_BTCUSDT": 40.0,
        },
        "portfolio": {"positions": {"BTCUSDT": {"qty": 1.0}}},
    }
    assert decision.decide(exit_ctx) == pytest.approx(-1.0)


def test_rsi_iv_dynamical_band_uses_rsi_mean_center() -> None:
    decision = RSIIVDynamicalBandDecision(
        symbol="BTCUSDT",
        rsi="RSI_DECISION_BTCUSDT",
        rsi_mean="RSI-MEAN_DECISION_BTCUSDT",
        iv="OPTION-MARK-IV_DECISION_BTCUSDT",
        alpha=0.0,
        beta=0.1,
    )
    # width=4, center=60 => lower=56; should enter (would not if center were 50)
    context = {
        "features": {
            "RSI_DECISION_BTCUSDT": 55.0,
            "RSI-MEAN_DECISION_BTCUSDT": 60.0,
            "OPTION-MARK-IV_DECISION_BTCUSDT": 40.0,
        },
        "portfolio": {"positions": {"BTCUSDT": {"qty": 0.0}}},
    }
    assert decision.decide(context) == pytest.approx(1.0)


def test_rsi_iv_dynamical_band_decision_neutral_when_iv_missing() -> None:
    decision = RSIIVDynamicalBandDecision(
        symbol="BTCUSDT",
        rsi="RSI_DECISION_BTCUSDT",
        rsi_mean="RSI-MEAN_DECISION_BTCUSDT",
        iv="OPTION-MARK-IV_DECISION_BTCUSDT",
        alpha=0.0,
        beta=0.1,
    )
    context = {
        "features": {
            "RSI_DECISION_BTCUSDT": 40.0,
            "RSI-MEAN_DECISION_BTCUSDT": 50.0,
            "OPTION-MARK-IV_DECISION_BTCUSDT": None,
        },
        "portfolio": {"positions": {"BTCUSDT": {"qty": 0.0}}},
    }
    assert decision.decide(context) == 0.0


def test_rsi_iv_linear_decision_clamps_to_risk_contract_range() -> None:
    decision = RSIIVLinearDecision(
        symbol="BTCUSDT",
        rsi="RSI_DECISION_BTCUSDT",
        iv="OPTION-MARK-IV_DECISION_BTCUSDT",
        alpha=0.1,
    )
    high_context = {
        "features": {
            "RSI_DECISION_BTCUSDT": 90.0,
            "OPTION-MARK-IV_DECISION_BTCUSDT": 60.0,
        }
    }
    low_context = {
        "features": {
            "RSI_DECISION_BTCUSDT": 10.0,
            "OPTION-MARK-IV_DECISION_BTCUSDT": -60.0,
        }
    }
    assert decision.decide(high_context) == pytest.approx(1.0)
    assert decision.decide(low_context) == pytest.approx(-1.0)


def test_option_mark_iv_uses_real_option_chain_path() -> None:
    data_ts = 1_700_000_000_000
    frame = pd.DataFrame(
        [
            {
                "instrument_name": "BTC-1-EXP-C",
                "expiration_timestamp": data_ts + (2 * DAY_MS),
                "strike": 30_000.0,
                "option_type": "call",
                "bid_price": 1.0,
                "ask_price": 1.1,
                "mark_price": 1.05,
                "mark_iv": 0.44,
                "market_ts": data_ts - 1_000,
                "underlying_price": 30_000.0,
                "open_interest": 10.0,
            },
            {
                "instrument_name": "BTC-1-EXP-P",
                "expiration_timestamp": data_ts + (2 * DAY_MS),
                "strike": 30_000.0,
                "option_type": "put",
                "bid_price": 1.0,
                "ask_price": 1.1,
                "mark_price": 1.05,
                "mark_iv": 0.44,
                "market_ts": data_ts - 1_000,
                "underlying_price": 30_000.0,
                "open_interest": 10.0,
            },
        ]
    )

    handler = OptionChainDataHandler(symbol="BTCUSDT", interval="1m", preset="option_chain")
    tick = IngestionTick(
        timestamp=data_ts,
        data_ts=data_ts,
        domain="option_chain",
        symbol="BTCUSDT",
        payload={"data_ts": data_ts, "frame": frame},
        source_id=getattr(handler, "source_id", None),
    )
    handler.on_new_tick(tick)

    feature = build_feature(
        "OPTION-MARK-IV",
        name="OPTION-MARK-IV_DECISION_BTCUSDT",
        symbol="BTCUSDT",
        x=0.0,
        tau_days=2,
        freshness_ms=600_000,
    )
    feature.update(_ctx(data_ts, handler))
    assert feature.output() == pytest.approx(0.44)
