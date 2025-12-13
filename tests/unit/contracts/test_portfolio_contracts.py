import pytest

from quant_engine.portfolio.manager import PortfolioManager
from quant_engine.contracts.portfolio import PortfolioState


def make_fill(
    symbol: str,
    price: float,
    qty: float,
    fee: float = 0.0,
):
    return {
        "symbol": symbol,
        "fill_price": price,
        "filled_qty": qty,
        "fee": fee,
    }


# ---------------------------------------------------------------------
# Basic portfolio lifecycle
# ---------------------------------------------------------------------
def test_portfolio_apply_single_fill_and_state_snapshot():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=10_000.0)

    fill = make_fill(
        symbol="BTCUSDT",
        price=10_000.0,
        qty=0.5,
        fee=1.0,
    )

    pm.apply_fill(fill)

    state = pm.state()
    assert isinstance(state, PortfolioState)

    snap = state.to_dict()

    assert snap["cash"] == pytest.approx(10_000.0 - 10_000.0 * 0.5 - 1.0)
    assert "BTCUSDT" in snap["positions"]
    assert snap["positions"]["BTCUSDT"]["qty"] == pytest.approx(0.5)
    assert snap["positions"]["BTCUSDT"]["entry"] == pytest.approx(10_000.0)
    assert snap["realized_pnl"] == pytest.approx(0.0)


# ---------------------------------------------------------------------
# Position scaling & VWAP logic
# ---------------------------------------------------------------------
def test_portfolio_vwap_updates_on_additional_fill():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=20_000.0)

    pm.apply_fill(make_fill("BTCUSDT", price=10_000.0, qty=0.5))
    pm.apply_fill(make_fill("BTCUSDT", price=12_000.0, qty=0.5))

    pos = pm.positions["BTCUSDT"]

    # VWAP = (0.5*10000 + 0.5*12000) / 1.0
    assert pos.qty == pytest.approx(1.0)
    assert pos.entry_price == pytest.approx(11_000.0)


# ---------------------------------------------------------------------
# Partial close & realized PnL
# ---------------------------------------------------------------------
def test_portfolio_realized_pnl_on_partial_close():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=20_000.0)

    # Open long
    pm.apply_fill(make_fill("BTCUSDT", price=10_000.0, qty=1.0))
    # Partial close
    pm.apply_fill(make_fill("BTCUSDT", price=12_000.0, qty=-0.4))

    snap = pm.state().to_dict()

    # Realized PnL: (12000 - 10000) * 0.4
    assert snap["realized_pnl"] == pytest.approx(800.0)
    assert snap["positions"]["BTCUSDT"]["qty"] == pytest.approx(0.6)


# ---------------------------------------------------------------------
# Full close & position reset
# ---------------------------------------------------------------------
def test_portfolio_full_close_resets_position_qty():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=20_000.0)

    pm.apply_fill(make_fill("BTCUSDT", price=10_000.0, qty=1.0))
    pm.apply_fill(make_fill("BTCUSDT", price=11_000.0, qty=-1.0))

    snap = pm.state().to_dict()

    assert snap["positions"]["BTCUSDT"]["qty"] == pytest.approx(0.0)
    assert snap["realized_pnl"] == pytest.approx(1_000.0)


# ---------------------------------------------------------------------
# Fees accounting
# ---------------------------------------------------------------------
def test_portfolio_fees_accumulate_and_reduce_cash():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=10_000.0)

    pm.apply_fill(make_fill("BTCUSDT", price=10_000.0, qty=0.2, fee=5.0))
    pm.apply_fill(make_fill("BTCUSDT", price=11_000.0, qty=-0.1, fee=3.0))

    snap = pm.state().to_dict()

    assert pm.fees == pytest.approx(8.0)
    assert snap["cash"] == pytest.approx(
        10_000.0
        - 10_000.0 * 0.2
        - 5.0
        + 11_000.0 * 0.1
        - 3.0
    )


# ---------------------------------------------------------------------
# Snapshot invariants
# ---------------------------------------------------------------------
def test_portfolio_state_snapshot_is_immutable():
    pm = PortfolioManager(symbol="BTCUSDT", initial_capital=10_000.0)

    pm.apply_fill(make_fill("BTCUSDT", price=10_000.0, qty=0.1))

    state1 = pm.state().to_dict()
    state2 = pm.state().to_dict()

    assert state1 == state2
