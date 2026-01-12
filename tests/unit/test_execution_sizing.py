from quant_engine.data.ohlcv.snapshot import OHLCVSnapshot
from quant_engine.execution.engine import ExecutionEngine
from quant_engine.execution.matching.simulated import SimulatedMatchingEngine
from quant_engine.execution.policy.immediate import ImmediatePolicy
from quant_engine.execution.router.simple import SimpleRouter
from quant_engine.execution.slippage.linear import LinearSlippage
from quant_engine.portfolio.fractional import FractionalPortfolioManager
from quant_engine.portfolio.manager import PortfolioManager, EPS


def _make_market_data(price: float) -> dict:
    snap = OHLCVSnapshot.from_bar_aligned(
        timestamp=0,
        bar={
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": 0.0,
            "data_ts": 0,
        },
        symbol="BTCUSDT",
    )
    return {"ohlcv": snap}


class MockOrderbook:
    def __init__(self, bid: float, ask: float, data_ts: int = 0):
        self._bid = bid
        self._ask = ask
        self.data_ts = data_ts

    def get_attr(self, name):
        if name == "best_bid":
            return self._bid
        if name == "best_ask":
            return self._ask
        if name == "mid":
            return 0.5 * (self._bid + self._ask)
        return None


def test_standard_sizing_uses_qty_step():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=1000.0, min_qty=1, min_notional=1.0, step_size="1")
    state = pm.state().to_dict()
    policy = ImmediatePolicy(symbol="BTCUSDT")
    market_data = _make_market_data(100.0)

    orders = policy.generate(0.5, state, market_data)

    assert len(orders) == 1
    assert abs(orders[0].qty - 5.0) < EPS


def test_fractional_sizing_floors_to_step():
    pm = FractionalPortfolioManager(
        symbol="BTCUSDT",
        initial_capital=1000.0,
        min_qty=0.0,
        min_notional=0.0,
        step_size="0.0001",
    )
    state = pm.state().to_dict()
    policy = ImmediatePolicy(symbol="BTCUSDT")
    market_data = _make_market_data(1000.0)

    orders = policy.generate(0.00015, state, market_data)

    assert len(orders) == 1
    assert abs(orders[0].qty - 0.0001) < EPS


def test_full_position_affordability_cap_and_no_repeat():
    pm = FractionalPortfolioManager(
        symbol="BTCUSDT",
        initial_capital=1_000_000.0,
        min_qty=0.0,
        min_notional=10.0,
        step_size="0.001",
    )
    state = pm.state().to_dict()
    state["slippage_bps"] = 0.0
    policy = ImmediatePolicy(symbol="BTCUSDT")
    market_data = {
        "orderbook": MockOrderbook(bid=90_690.0, ask=90_710.0),
        "ohlcv": _make_market_data(90_700.0)["ohlcv"],
    }

    orders = policy.generate(1.0, state, market_data)
    assert len(orders) == 1
    buy_qty = orders[0].qty

    per_lot_cost = 90_710.0 * 0.001
    max_lots = int((1_000_000.0) // per_lot_cost)
    assert abs(buy_qty - (max_lots * 0.001)) < EPS

    cash_left = 1_000_000.0 - (max_lots * per_lot_cost)
    state["position_lots"] = max_lots
    state["position_qty"] = max_lots * 0.001
    state["cash"] = cash_left
    state["total_equity"] = cash_left + state["position_qty"] * 90_700.0
    orders = policy.generate(1.0, state, market_data)
    assert orders == []


def test_execution_engine_determinism():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=1000.0, min_qty=1, min_notional=1.0)
    state = pm.state().to_dict()
    market_data = _make_market_data(100.0)

    engine = ExecutionEngine(
        ImmediatePolicy(symbol="BTCUSDT"),
        SimpleRouter(symbol="BTCUSDT"),
        LinearSlippage(symbol="BTCUSDT", impact=0.0005),
        SimulatedMatchingEngine(symbol="BTCUSDT"),
    )

    fills_a = engine.execute(0, 0.5, state, market_data)
    fills_b = engine.execute(0, 0.5, state, market_data)

    assert fills_a == fills_b
