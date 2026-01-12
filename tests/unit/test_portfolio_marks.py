import math

from quant_engine.portfolio.manager import PortfolioManager, EPS
from quant_engine.portfolio.fractional import FractionalPortfolioManager


def _make_market_snap(price: float) -> dict:
    return {"ohlcv": {"BTCUSDT": {"numeric": {"close": price}}}}


def test_unrealized_updates_with_mark():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=1000.0, min_qty=1, min_notional=1.0)
    pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 1, "fee": 0.0, "side": "BUY"})
    pm.update_marks(_make_market_snap(110.0))
    state = pm.state().to_dict()

    assert abs(state["unrealized_pnl"] - 10.0) < EPS
    assert abs(state["positions"]["BTCUSDT"]["unrealized"] - 10.0) < EPS


def test_realized_only_on_sell():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=1000.0, min_qty=1, min_notional=1.0)
    pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 1, "fee": 0.0, "side": "BUY"})
    pm.update_marks(_make_market_snap(110.0))
    _ = pm.state().to_dict()
    assert abs(pm.realized_pnl - 0.0) < EPS

    pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 110.0, "filled_qty": 1, "fee": 0.0, "side": "SELL"})
    assert abs(pm.realized_pnl - 10.0) < EPS

    pm.update_marks(_make_market_snap(120.0))
    state = pm.state().to_dict()
    assert abs(state["realized_pnl"] - 10.0) < EPS


def test_dust_resets_entry():
    pm = FractionalPortfolioManager(
        symbol="BTCUSDT",
        initial_capital=1000.0,
        min_qty=0.0,
        min_notional=0.0,
    )
    pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 1e-10, "fee": 0.0, "side": "BUY"})
    state = pm.state().to_dict()

    assert state["positions"]["BTCUSDT"]["qty"] == 0.0
    assert state["positions"]["BTCUSDT"]["entry"] == 0.0
    assert state["positions"]["BTCUSDT"]["unrealized"] == 0.0


def test_exposure_frac_uses_mark():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=1000.0, min_qty=1, min_notional=1.0)
    pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 1, "fee": 0.0, "side": "BUY"})
    pm.update_marks(_make_market_snap(200.0))
    state = pm.state().to_dict()

    expected_exposure = 200.0
    expected_equity = 900.0 + 200.0
    expected_frac = expected_exposure / expected_equity

    assert abs(state["exposure"] - expected_exposure) < EPS
    assert abs(state["position_frac"] - expected_frac) < EPS
    assert abs(state["leverage"] - state["position_frac"]) < EPS
    assert state["leverage"] <= 1.0 + EPS


def test_portfolio_equity_identity():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=1000.0, min_qty=1, min_notional=1.0)
    pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 2, "fee": 0.0, "side": "BUY"})
    pm.update_marks(_make_market_snap(125.0))
    state = pm.state().to_dict()

    mark = 125.0
    position_qty = state["positions"]["BTCUSDT"]["qty"]
    expected_equity = state["cash"] + position_qty * mark

    assert math.isfinite(state["total_equity"])
    assert abs(state["total_equity"] - expected_equity) < EPS
