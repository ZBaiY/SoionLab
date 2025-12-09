from quant_engine.contracts.portfolio import (
    PortfolioManagerProto,
    PortfolioState,
    PositionRecord
)


class DummyPortfolio(PortfolioManagerProto):
    """
    Minimal contract-compliant portfolio manager.
    Does NOT implement real accounting — only verifies interface behavior.
    """

    def __init__(self):
        self._cash = 1000.0
        self._positions = {}
        self._realized = 0.0
        self._unrealized = 0.0

    def apply_fill(self, fill: dict):
        # Minimal behavior: register the fill but do not compute real pnl.
        symbol = "BTCUSDT"
        qty = fill.get("filled_qty", 0.0)
        price = fill.get("fill_price", 0.0)

        self._positions[symbol] = PositionRecord(
            symbol=symbol,
            qty=qty,
            entry_price=price,
            unrealized_pnl=0.0,
        )

    def state(self) -> PortfolioState:
        snapshot = {
            "cash": self._cash,
            "positions": {
                symbol: {
                    "qty": rec.qty,
                    "entry": rec.entry_price,
                    "unrealized": rec.unrealized_pnl,
                }
                for symbol, rec in self._positions.items()
            },
            "realized_pnl": self._realized,
            "unrealized_pnl": self._unrealized,
            "total_equity": self._cash + self._unrealized,
            "exposure": sum(abs(rec.qty) for rec in self._positions.values()),
            "leverage": 1.0,
        }
        return PortfolioState(snapshot)
    

def test_portfolio_contract_accepts_fill_and_returns_state():
    pm = DummyPortfolio()

    # Simulated fill passed from matching engine
    fill = {
        "fill_price": 50000.0,
        "filled_qty": 1.0,
        "fee": 0.0,
        "slippage": 0.0,
        "side": None,
        "order_type": None,
        "timestamp": 123456.0,
    }
    

    pm.apply_fill(fill)

    state = pm.state()
    assert isinstance(state, PortfolioState)

    snap = state.snapshot()
    
    assert isinstance(snap, dict)
    assert "cash" in snap
    assert "positions" in snap
    assert "total_equity" in snap
    assert isinstance(snap["positions"], dict)


def test_portfolio_contract_positions_structure():
    pm = DummyPortfolio()
    pm.apply_fill({"fill_price": 100.0, "filled_qty": 2.0})

    state = pm.state().snapshot()
    positions = state["positions"]

    assert isinstance(positions, dict)
    assert len(positions) == 1

    symbol, rec_dict = next(iter(positions.items()))
    assert isinstance(rec_dict, dict)
    assert "qty" in rec_dict
    assert "entry" in rec_dict
    assert "unrealized" in rec_dict

    assert isinstance(rec_dict["qty"], (float, int))
    assert isinstance(rec_dict["entry"], (float, int))


def test_portfolio_contract_snapshot_is_readonly_like():
    pm = DummyPortfolio()
    pm.apply_fill({"fill_price": 200.0, "filled_qty": 1.0})

    snap1 = pm.state().snapshot()
    snap2 = pm.state().snapshot()

    # Snapshot must be stable (read-only from manager perspective)
    assert snap1 == snap2