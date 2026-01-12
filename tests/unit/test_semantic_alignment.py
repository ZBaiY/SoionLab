# tests/unit/test_semantic_alignment.py
from quant_engine.decision.threshold import ThresholdDecision
from quant_engine.execution.policy.immediate import ImmediatePolicy
from quant_engine.portfolio.manager import PortfolioManager, EPS
from quant_engine.risk.engine import RiskEngine
from quant_engine.risk.rule_full import FullAllocation
from quant_engine.risk.rules_constraints import CashPositionConstraintRule


class MockSnapshot:
    def __init__(self, close: float, data_ts: int = 0):
        self._close = close
        self.data_ts = data_ts

    def get_attr(self, name):
        if name == "close":
            return self._close
        return None


def _make_context(cash: float, position_qty: float, price: float):
    return {
        "timestamp": 1234567890000,
        "portfolio": {"cash": cash, "position": position_qty, "position_qty": position_qty},
        "primary_snapshots": {"ohlcv": MockSnapshot(price, data_ts=1234567889000)},
    }


def test_decision_score_only():
    decision = ThresholdDecision(threshold=0.5)
    score = decision.decide({"models": {"main": 1.0}})
    assert isinstance(score, float)
    assert not hasattr(decision, "position")


def test_score_to_target_position_mapping():
    rule = CashPositionConstraintRule(
        symbol="BTCUSDT",
        fee_rate=0.0,
        slippage_bound_bps=0.0,
        min_qty=1,
        min_notional=10.0,
        integer_only=True,
    )
    engine = RiskEngine(
        [rule],
        symbol="BTCUSDT",
        risk_config={"shortable": False, "mapping": {"name": "score_to_target"}},
    )

    context = _make_context(cash=1000.0, position_qty=2.0, price=100.0)
    target_buy = engine.adjust(1.0, context)
    target_sell = engine.adjust(-1.0, context)
    target_hold = engine.adjust(0.0, context)

    equity = 1000.0 + 2.0 * 100.0
    current_frac = (2.0 * 100.0) / equity

    assert target_buy == 1.0
    assert target_sell == 0.0
    assert abs(target_hold - current_frac) < 1e-9


def test_rulefull_last():
    rule = CashPositionConstraintRule(
        symbol="BTCUSDT",
        fee_rate=0.0,
        slippage_bound_bps=0.0,
        min_qty=1,
        min_notional=10.0,
        integer_only=True,
    )
    full = FullAllocation(symbol="BTCUSDT")
    engine = RiskEngine(
        [full, rule],
        symbol="BTCUSDT",
        risk_config={"debug_full_position": True},
    )

    assert type(engine.rules[-1]).__name__ == "FullAllocation"


def test_risk_output_is_target_position_fraction():
    rule = CashPositionConstraintRule(
        symbol="BTCUSDT",
        fee_rate=0.0,
        slippage_bound_bps=0.0,
        min_qty=1,
        min_notional=10.0,
        integer_only=True,
    )
    engine = RiskEngine([rule], symbol="BTCUSDT", risk_config={"shortable": False})
    context = _make_context(cash=1_000_000.0, position_qty=0.0, price=100_000.0)
    target_position = engine.adjust(1.0, context)

    assert target_position <= 1.0 + EPS


def test_end_to_end_score_to_fill_flow():
    rule = CashPositionConstraintRule(
        symbol="BTCUSDT",
        fee_rate=0.0,
        slippage_bound_bps=0.0,
        min_qty=1,
        min_notional=10.0,
        integer_only=True,
    )
    engine = RiskEngine(
        [rule, FullAllocation(symbol="BTCUSDT")],
        symbol="BTCUSDT",
        risk_config={"shortable": False, "debug_full_position": True, "mapping": {"name": "score_to_target"}},
    )
    policy = ImmediatePolicy(symbol="BTCUSDT")
    portfolio = PortfolioManager(symbol="BTCUSDT", initial_capital=1_000_000.0, min_qty=1, min_notional=10.0)

    context = _make_context(cash=1_000_000.0, position_qty=0.0, price=100_000.0)
    target_position = engine.adjust(1.0, context)
    orders = policy.generate(
        target_position,
        portfolio.state().to_dict(),
        context["primary_snapshots"],
    )

    assert len(orders) == 1
    buy_qty = orders[0].qty
    assert buy_qty == 10.0

    portfolio.apply_fill(
        {"symbol": "BTCUSDT", "fill_price": 100_000.0, "filled_qty": buy_qty, "fee": 0.0, "side": "BUY"}
    )

    context = _make_context(cash=portfolio.cash, position_qty=portfolio.get_position("BTCUSDT"), price=110_000.0)
    target_position = engine.adjust(-1.0, context)
    orders = policy.generate(
        target_position,
        portfolio.state().to_dict(),
        context["primary_snapshots"],
    )
    assert len(orders) == 1
    assert orders[0].side.value == "SELL"
    assert orders[0].qty == 10.0

    portfolio.apply_fill(
        {"symbol": "BTCUSDT", "fill_price": 110_000.0, "filled_qty": 10.0, "fee": 0.0, "side": "SELL"}
    )
    assert abs(portfolio.realized_pnl - 100_000.0) < EPS
