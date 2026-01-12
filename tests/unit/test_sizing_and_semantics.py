# tests/unit/test_sizing_and_semantics.py
from quant_engine.execution.policy.immediate import ImmediatePolicy
from quant_engine.portfolio.manager import PortfolioManager, EPS
from quant_engine.risk.engine import RiskEngine
from quant_engine.risk.rules_constraints import CashPositionConstraintRule, FractionalCashConstraintRule


class MockSnapshot:
    def __init__(self, close: float, data_ts: int = 0):
        self._close = close
        self.data_ts = data_ts

    def get_attr(self, name):
        if name == "close":
            return self._close
        return None


def _make_context(cash: float, position: float, price: float):
    return {
        "timestamp": 1234567890000,
        "portfolio": {"cash": cash, "position": position, "position_qty": position},
        "primary_snapshots": {"ohlcv": MockSnapshot(price, data_ts=1234567889000)},
    }


def test_score_zero_hold_no_order():
    rule = CashPositionConstraintRule(
        symbol="BTCUSDT",
        fee_rate=0.0,
        slippage_bound_bps=0.0,
        min_qty=1,
        min_notional=10.0,
        integer_only=True,
    )
    risk_engine = RiskEngine(
        [rule],
        symbol="BTCUSDT",
        risk_config={"shortable": False, "mapping": {"name": "score_to_target"}},
    )
    context = _make_context(cash=1000.0, position=2.0, price=100.0)
    target_position = risk_engine.adjust(0.0, context)

    policy = ImmediatePolicy(symbol="BTCUSDT")
    orders = policy.generate(
        target_position,
        {"position": 2.0, "position_qty": 2.0, "cash": 1000.0},
        context["primary_snapshots"],
    )
    assert orders == []


def test_position_target_full_alloc_sizing():
    rule = CashPositionConstraintRule(
        symbol="BTCUSDT",
        fee_rate=0.0,
        slippage_bound_bps=0.0,
        min_qty=0.0,
        min_notional=0.0,
        integer_only=False,
    )
    risk_engine = RiskEngine(
        [rule],
        symbol="BTCUSDT",
        risk_config={"shortable": False, "mapping": {"name": "score_to_target"}},
    )
    context = _make_context(cash=1_000_000.0, position=0.0, price=100_000.0)
    target_position = risk_engine.adjust(1.0, context)

    policy = ImmediatePolicy(symbol="BTCUSDT")
    orders = policy.generate(
        target_position,
        {"position": 0.0, "position_qty": 0.0, "cash": 1_000_000.0},
        context["primary_snapshots"],
    )
    assert len(orders) == 1
    assert abs(orders[0].qty - 10.0) < 1e-6


def test_sell_all_converges_one_step():
    rule = CashPositionConstraintRule(
        symbol="BTCUSDT",
        fee_rate=0.0,
        slippage_bound_bps=0.0,
        min_qty=1,
        min_notional=10.0,
        integer_only=True,
    )
    risk_engine = RiskEngine(
        [rule],
        symbol="BTCUSDT",
        risk_config={"shortable": False, "mapping": {"name": "score_to_target"}},
    )
    context = _make_context(cash=0.0, position=3.0, price=100.0)
    target_position = risk_engine.adjust(-1.0, context)
    assert target_position == 0.0

    policy = ImmediatePolicy(symbol="BTCUSDT")
    orders = policy.generate(
        target_position,
        {"position": 3.0, "position_qty": 3.0, "cash": 0.0},
        context["primary_snapshots"],
    )
    assert len(orders) == 1
    assert orders[0].side.value == "SELL"
    assert orders[0].qty == 3.0


def test_dust_sell_dropped_no_loop():
    rule = FractionalCashConstraintRule(
        symbol="BTCUSDT",
        fee_rate=0.0,
        slippage_bound_bps=0.0,
        min_qty=0.01,
        min_notional=1.0,
    )
    risk_engine = RiskEngine(
        [rule],
        symbol="BTCUSDT",
        risk_config={"shortable": False, "mapping": {"name": "score_to_target"}},
    )
    context = _make_context(cash=0.0, position=0.005, price=10000.0)
    target_position = risk_engine.adjust(-1.0, context)
    assert target_position == 1.0

    policy = ImmediatePolicy(symbol="BTCUSDT")
    orders = policy.generate(
        target_position,
        {"position": 0.005, "position_qty": 0.005, "cash": 0.0},
        context["primary_snapshots"],
    )
    assert orders == []


def test_realized_pnl_on_sell():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=1000.0, min_qty=1, min_notional=10.0)
    pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 1, "fee": 0.0, "side": "BUY"})
    pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 110.0, "filled_qty": 1, "fee": 0.0, "side": "SELL"})
    assert abs(pm.realized_pnl - 10.0) < EPS


def test_no_leverage_exposure_leq_equity():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=1000.0, min_qty=1, min_notional=10.0)
    pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 5, "fee": 0.0, "side": "BUY"})
    state = pm.state().to_dict()

    assert state["exposure"] <= state["total_equity"] + EPS
    assert state["leverage"] <= 1.0 + EPS


def test_slippage_overbudget_reclip():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=1000.0, min_qty=1, min_notional=10.0)
    pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 600.0, "filled_qty": 3, "fee": 0.0, "side": "BUY"})
    assert pm.get_position("BTCUSDT") == 1.0
    assert pm.cash >= 0.0
