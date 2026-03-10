from __future__ import annotations

from quant_engine.runtime.modes import EngineMode
from quant_engine.strategy.loader import StrategyLoader
from quant_engine.strategy.registry import get_strategy


class _CloseSnapshot:
    def __init__(self, close: float, data_ts: int = 1_700_000_000_000) -> None:
        self._close = float(close)
        self.data_ts = int(data_ts)

    def get_attr(self, name: str):
        if name == "close":
            return self._close
        return None


def _build_engine():
    StrategyCls = get_strategy("LIVE-TEST-FLIP")
    strategy = StrategyCls().bind(A="BTCUSDT")
    return StrategyLoader.from_config(strategy=strategy, mode=EngineMode.BACKTEST, overrides={})


def _make_context(*, cash: float, position_qty: float, price: float) -> dict:
    return {
        "timestamp": 1_700_000_000_000,
        "portfolio": {"cash": cash, "position": position_qty, "position_qty": position_qty},
        "primary_snapshots": {"ohlcv": _CloseSnapshot(price)},
        "features": {},
        "models": {},
    }


def test_live_test_flip_strategy_standardize_snapshot() -> None:
    StrategyCls = get_strategy("LIVE-TEST-FLIP")
    cfg = StrategyCls.standardize(overrides={}, symbols={"A": "BTCUSDT"}).to_dict()

    assert cfg == {
        "data": {
            "primary": {
                "ohlcv": {
                    "bootstrap": {"lookback": "180d"},
                    "cache": {"max_bars": 10000},
                    "columns": ["open", "high", "low", "close", "volume"],
                    "interval": "15m",
                    "interval_ms": 900000,
                }
            }
        },
        "decision": {
            "params": {"position_epsilon": 1e-09},
            "type": "LIVE-TEST-FLIP-DECISION",
        },
        "execution": {
            "matching": {"type": "SIMULATED"},
            "policy": {"type": "IMMEDIATE"},
            "router": {"type": "SIMPLE"},
            "slippage": {"type": "LINEAR"},
        },
        "features_user": [
            {
                "name": "LIVE-TEST-ANCHOR_MODEL_BTCUSDT",
                "symbol": "BTCUSDT",
                "type": "LIVE-TEST-ANCHOR",
                "params": {},
            }
        ],
        "interval": "15m",
        "interval_ms": 900000,
        "model": None,
        "portfolio": {
            "params": {"initial_capital": 1000000, "step_size": 0.001},
            "type": "FRACTIONAL",
        },
        "required_data": ["ohlcv"],
        "risk": {
            "mapping": {"name": "identity"},
            "rules": {
                "FRACTIONAL-CASH-CONSTRAINT": {
                    "params": {"fee_rate": 0.001, "min_notional": 10.0, "slippage_bound_bps": 10}
                }
            },
            "shortable": False,
        },
        "symbol": "BTCUSDT",
        "universe": {
            "primary": "BTCUSDT",
            "soft_readiness": {
                "domains": ["orderbook", "option_chain", "iv_surface", "sentiment"],
                "enabled": False,
                "max_staleness_ms": 300000,
            },
        },
    }


def test_live_test_flip_strategy_loader_builds_without_model_or_features() -> None:
    engine = _build_engine()

    assert engine.ohlcv_handlers
    assert engine.models == {}
    assert engine.decision is not None
    assert engine.risk_manager is not None
    assert engine.execution_engine is not None
    assert engine.portfolio is not None
    channels = getattr(engine.feature_extractor, "channels", [])
    assert len(channels) == 1
    assert channels[0].name == "LIVE-TEST-ANCHOR_MODEL_BTCUSDT"
    assert getattr(engine.risk_manager, "mapping_name", None) == "identity"


def test_live_test_flip_strategy_alternates_target_intent_by_position_state() -> None:
    engine = _build_engine()

    flat_context = _make_context(cash=10_000.0, position_qty=0.0, price=100.0)
    score_flat = engine.decision.decide(flat_context)
    target_flat = engine.risk_manager.adjust(score_flat, flat_context)

    held_context = _make_context(cash=0.0, position_qty=50.0, price=100.0)
    score_held = engine.decision.decide(held_context)
    target_held = engine.risk_manager.adjust(score_held, held_context)

    assert score_flat == 1.0
    assert target_flat >= 0.99
    assert target_flat <= 1.0
    assert score_held == 0.0
    assert target_held == 0.0
