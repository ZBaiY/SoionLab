# tests/unit/test_portfolio_invariants.py
"""
Tests for Portfolio layer invariants and semantics.

Test coverage:
1. Standard BUY: floor+drop policy (qty=1.9 -> 1, qty=0.9 -> dropped)
2. Standard SELL: clip to position
3. Fractional BUY: clip policy
4. Fractional SELL: clip to position
5. Minimum trade filter (min_qty, min_notional)
6. Post-slippage over-budget: re-clip/re-floor
7. Entry price correctness across buy/buy/sell
8. Risk constraint rule with floor+drop/clip policies
9. Risk constraint with slippage_bound_bps
"""

import pytest
from quant_engine.portfolio.manager import PortfolioManager, EPS
from quant_engine.portfolio.fractional import FractionalPortfolioManager
from quant_engine.risk.rules_constraints import CashPositionConstraintRule, FractionalCashConstraintRule


# ============================================================
# Standard Portfolio Tests (floor+drop policy)
# ============================================================

class TestStandardPortfolioFloorDrop:
    """Test STANDARD portfolio floor+drop policy for BUY."""

    def test_buy_floor_qty_1_9_to_1(self):
        """BUY qty=1.9, cash=150, price=100 -> floor to 1, execute, cash=50, pos=1."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=150.0, min_qty=1, min_notional=10.0)

        fill = {
            "symbol": "BTCUSDT",
            "fill_price": 100.0,
            "filled_qty": 1.9,  # Will be floored to 1
            "fee": 0.0,
            "side": "BUY"
        }
        pm.apply_fill(fill)

        assert pm.get_position("BTCUSDT") == 1.0, "Position should be 1 after floor"
        assert pm.cash == 50.0, "Cash should be 50 (150 - 100*1)"

    def test_buy_floor_qty_0_9_dropped(self):
        """BUY qty=0.9 -> floor to 0 -> dropped."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=1000.0, min_qty=1, min_notional=10.0)

        fill = {
            "symbol": "BTCUSDT",
            "fill_price": 100.0,
            "filled_qty": 0.9,  # Will be floored to 0
            "fee": 0.0,
            "side": "BUY"
        }
        pm.apply_fill(fill)

        assert pm.get_position("BTCUSDT") == 0.0, "Position should remain 0 (order dropped)"
        assert pm.cash == 1000.0, "Cash should remain unchanged"

    def test_buy_integer_accepted(self):
        """BUY with integer qty should be accepted."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=1000.0, min_qty=1, min_notional=10.0)

        fill = {
            "symbol": "BTCUSDT",
            "fill_price": 100.0,
            "filled_qty": 5,
            "fee": 0.0,
            "side": "BUY"
        }
        pm.apply_fill(fill)

        assert pm.get_position("BTCUSDT") == 5.0
        assert pm.cash == 500.0


class TestStandardPortfolioSellClip:
    """Test STANDARD portfolio clip policy for SELL."""

    def test_sell_clip_to_position(self):
        """SELL: pos=2, sell=5 -> clipped to 2."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=10000.0, min_qty=1, min_notional=10.0)

        # Buy 2
        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 2, "fee": 0.0})
        assert pm.get_position("BTCUSDT") == 2.0

        # Try to sell 5 (clipped to 2)
        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 110.0, "filled_qty": -5, "fee": 0.0})

        assert pm.get_position("BTCUSDT") == 0.0, "Position should be 0 after clipped sell"
        assert pm.cash > 10000.0 - 200.0 + 220.0 - 1, "Cash should increase from sell"

    def test_sell_exact_position(self):
        """SELL exactly what we have."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=10000.0, min_qty=1, min_notional=10.0)

        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 3, "fee": 0.0})
        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 110.0, "filled_qty": -3, "fee": 0.0})

        assert pm.get_position("BTCUSDT") == 0.0

    def test_sell_floor_fractional(self):
        """SELL with fractional qty should be floored."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=10000.0, min_qty=1, min_notional=10.0)

        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 5, "fee": 0.0})
        # Try to sell 2.7 (floored to 2)
        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 110.0, "filled_qty": -2.7, "fee": 0.0})

        assert pm.get_position("BTCUSDT") == 3.0, "Position should be 3 after floored sell of 2"


# ============================================================
# Fractional Portfolio Tests (clip policy)
# ============================================================

class TestFractionalPortfolioClip:
    """Test FRACTIONAL portfolio clip policy."""

    def test_buy_clip_by_cash(self):
        """Fractional BUY: cash=100, price=300 -> clip qty to ~0.333."""
        pm = FractionalPortfolioManager(symbol="BTCUSDT", initial_capital=100.0, min_qty=0.0001, min_notional=1.0)

        fill = {
            "symbol": "BTCUSDT",
            "fill_price": 300.0,
            "filled_qty": 1.0,  # Would cost 300, but only have 100
            "fee": 0.0,
            "side": "BUY"
        }
        pm.apply_fill(fill)

        # Should be clipped to ~0.333 (100/300)
        assert abs(pm.get_position("BTCUSDT") - 100.0 / 300.0) < 0.001
        assert pm.cash < 1.0, "Cash should be nearly depleted"

    def test_buy_fractional_accepted(self):
        """Fractional BUY with small qty should be accepted."""
        pm = FractionalPortfolioManager(symbol="BTCUSDT", initial_capital=1000.0, min_qty=0.0001, min_notional=1.0)

        fill = {
            "symbol": "BTCUSDT",
            "fill_price": 100.0,
            "filled_qty": 0.25,
            "fee": 0.0,
            "side": "BUY"
        }
        pm.apply_fill(fill)

        assert abs(pm.get_position("BTCUSDT") - 0.25) < EPS

    def test_sell_clip_fractional(self):
        """Fractional SELL clips to available position."""
        pm = FractionalPortfolioManager(symbol="BTCUSDT", initial_capital=10000.0, min_qty=0.0001, min_notional=1.0)

        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 0.5, "fee": 0.0})
        # Try to sell 1.0 (clipped to 0.5)
        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 110.0, "filled_qty": -1.0, "fee": 0.0})

        assert pm.get_position("BTCUSDT") == 0.0


# ============================================================
# Minimum Trade Filter Tests
# ============================================================

class TestMinimumTradeFilter:
    """Test minimum trade size filter (min_qty, min_notional)."""

    def test_fractional_dust_dropped_by_min_notional(self):
        """Fractional clipped qty produces notional below min_notional -> dropped."""
        pm = FractionalPortfolioManager(symbol="BTCUSDT", initial_capital=5.0, min_qty=0.0001, min_notional=10.0)

        # With cash=5 and price=100, max qty = 0.05, notional = 5 < min_notional=10
        fill = {
            "symbol": "BTCUSDT",
            "fill_price": 100.0,
            "filled_qty": 1.0,
            "fee": 0.0,
            "side": "BUY"
        }
        pm.apply_fill(fill)

        # Should be dropped because notional (5) < min_notional (10)
        assert pm.get_position("BTCUSDT") == 0.0, "Order should be dropped due to min_notional"
        assert pm.cash == 5.0

    def test_standard_below_min_qty_dropped(self):
        """Standard portfolio: order below min_qty after floor is dropped."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=50.0, min_qty=1, min_notional=10.0)

        # With cash=50 and price=100, can't afford 1 share
        fill = {
            "symbol": "BTCUSDT",
            "fill_price": 100.0,
            "filled_qty": 1,
            "fee": 0.0,
            "side": "BUY"
        }
        pm.apply_fill(fill)

        # Dropped due to insufficient cash
        assert pm.get_position("BTCUSDT") == 0.0

    def test_fractional_passes_min_thresholds(self):
        """Fractional: order passes min thresholds."""
        pm = FractionalPortfolioManager(symbol="BTCUSDT", initial_capital=100.0, min_qty=0.0001, min_notional=10.0)

        fill = {
            "symbol": "BTCUSDT",
            "fill_price": 100.0,
            "filled_qty": 0.5,  # notional = 50 >= 10
            "fee": 0.0,
            "side": "BUY"
        }
        pm.apply_fill(fill)

        assert pm.get_position("BTCUSDT") == 0.5


# ============================================================
# Post-Slippage Over-Budget Tests
# ============================================================

class TestPostSlippageOverBudget:
    """Test post-slippage over-budget handling."""

    def test_standard_reclip_after_slippage(self):
        """Standard: risk approves qty using p_eff, but execution produces worse fill_price."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=100.0, min_qty=1, min_notional=10.0)

        # Risk approved qty=1 at price~100, but execution fills at price=150 (worse slippage)
        # total_cost = 150 > cash=100, so re-floor to floor(100/150) = 0 -> dropped
        fill = {
            "symbol": "BTCUSDT",
            "fill_price": 150.0,  # Worse than expected
            "filled_qty": 1,
            "fee": 0.0,
            "side": "BUY"
        }
        pm.apply_fill(fill)

        # Re-floored qty = floor(100/150) = 0 -> dropped
        assert pm.get_position("BTCUSDT") == 0.0
        assert pm.cash == 100.0

    def test_fractional_reclip_after_slippage(self):
        """Fractional: re-clip after slippage to prevent negative cash."""
        pm = FractionalPortfolioManager(symbol="BTCUSDT", initial_capital=100.0, min_qty=0.0001, min_notional=1.0)

        # Try to buy 1.0 at price=150 (cost=150 > cash=100)
        # Should be clipped to 100/150 = 0.666...
        fill = {
            "symbol": "BTCUSDT",
            "fill_price": 150.0,
            "filled_qty": 1.0,
            "fee": 0.0,
            "side": "BUY"
        }
        pm.apply_fill(fill)

        # Clipped to ~0.666
        assert abs(pm.get_position("BTCUSDT") - 100.0 / 150.0) < 0.001
        assert pm.cash >= 0, "Cash must never be negative"

    def test_slippage_with_fee_over_budget(self):
        """Slippage + fee causes over-budget, must re-clip."""
        pm = FractionalPortfolioManager(symbol="BTCUSDT", initial_capital=100.0, min_qty=0.0001, min_notional=1.0)

        # Try to buy qty=1 at price=90, fee=15 -> total = 105 > 100
        # After re-clip: max_qty = (100-15)/90 = 0.944...
        fill = {
            "symbol": "BTCUSDT",
            "fill_price": 90.0,
            "filled_qty": 1.0,
            "fee": 15.0,
            "side": "BUY"
        }
        pm.apply_fill(fill)

        # Should be clipped
        expected_qty = (100.0 - 15.0) / 90.0
        assert abs(pm.get_position("BTCUSDT") - expected_qty) < 0.001
        assert pm.cash >= 0

    def test_cash_never_negative_after_slippage(self):
        """Cash must never go negative regardless of slippage."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=100.0, min_qty=1, min_notional=10.0)

        # Try multiple fills with bad slippage
        for i in range(10):
            fill = {
                "symbol": "BTCUSDT",
                "fill_price": 100.0 + i * 50,  # Increasingly worse price
                "filled_qty": 5,
                "fee": 10.0,
                "side": "BUY"
            }
            pm.apply_fill(fill)
            assert pm.cash >= 0, f"Cash must be non-negative after iteration {i}"


# ============================================================
# Entry Price Correctness Tests
# ============================================================

class TestEntryPriceCorrectness:
    """Test entry price / average cost basis calculation."""

    def test_weighted_average_entry_price(self):
        """buy 1 @100, buy 1 @200 => avg=150."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=100000.0, min_qty=1, min_notional=10.0)

        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 1, "fee": 0.0})
        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 200.0, "filled_qty": 1, "fee": 0.0})

        assert pm.get_position("BTCUSDT") == 2.0
        assert abs(pm.get_avg_entry_price("BTCUSDT") - 150.0) < EPS

    def test_sell_preserves_avg_entry(self):
        """sell 1 @160 => avg remains 150 on remaining 1."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=100000.0, min_qty=1, min_notional=10.0)

        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 1, "fee": 0.0})
        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 200.0, "filled_qty": 1, "fee": 0.0})
        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 160.0, "filled_qty": -1, "fee": 0.0})

        assert pm.get_position("BTCUSDT") == 1.0
        assert abs(pm.get_avg_entry_price("BTCUSDT") - 150.0) < EPS, "Avg entry should be preserved on partial sell"

    def test_realized_pnl_calculation(self):
        """sell 1 @160 with avg=150 => realized pnl = 10."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=100000.0, min_qty=1, min_notional=10.0)

        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 1, "fee": 0.0})
        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 200.0, "filled_qty": 1, "fee": 0.0})
        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 160.0, "filled_qty": -1, "fee": 0.0})

        assert abs(pm.realized_pnl - 10.0) < EPS

    def test_sell_all_resets_entry_price(self):
        """Selling all shares resets entry price to 0."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=100000.0, min_qty=1, min_notional=10.0)

        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 2, "fee": 0.0})
        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 120.0, "filled_qty": -2, "fee": 0.0})

        assert pm.get_position("BTCUSDT") == 0.0
        assert pm.get_avg_entry_price("BTCUSDT") == 0.0


# ============================================================
# Risk Constraint Rule Tests
# ============================================================

class TestRiskConstraintRule:
    """Test risk layer cash/position constraint rules."""

    def _make_context(self, cash, position, price):
        """Helper to create context for risk rule."""

        class MockSnapshot:
            def __init__(self, close):
                self._close = close

            def get_attr(self, name):
                if name == "close":
                    return self._close
                return None

        return {
            "portfolio": {
                "cash": cash,
                "position": position,
                "position_qty": position,
            },
            "primary_snapshots": {
                "ohlcv": MockSnapshot(price),
            },
        }

    def test_standard_floor_drop_buy(self):
        """Standard: full allocation keeps target position fraction at 1.0."""
        rule = CashPositionConstraintRule(
            symbol="BTCUSDT",
            fee_rate=0.0,
            slippage_bound_bps=0.0,
            min_qty=1,
            min_notional=10.0,
            integer_only=True
        )

        # Target=1.0 => 100% allocation, cash=1000, price=100 => qty=10 => fraction=1.0
        context = self._make_context(cash=1000.0, position=0.0, price=100.0)
        result = rule.adjust(1.0, context)
        assert result == 1.0

    def test_standard_floor_drop_to_zero(self):
        """Standard: target fraction equal to current fraction returns unchanged."""
        rule = CashPositionConstraintRule(
            symbol="BTCUSDT",
            fee_rate=0.0,
            slippage_bound_bps=0.0,
            min_qty=1,
            min_notional=10.0,
            integer_only=True
        )

        context = self._make_context(cash=1000.0, position=2.0, price=100.0)
        equity = 1000.0 + 2.0 * 100.0
        current_frac = (2.0 * 100.0) / equity
        result = rule.adjust(current_frac, context)
        assert abs(result - current_frac) < 1e-9

    def test_standard_clip_by_cash(self):
        """Standard: BUY clipped by cash constraint."""
        rule = CashPositionConstraintRule(
            symbol="BTCUSDT",
            fee_rate=0.0,
            slippage_bound_bps=0.0,
            min_qty=1,
            min_notional=10.0,
            integer_only=True
        )

        # Cash=150, price=100, target=1.0 => qty=1.5 -> floor to 1 => fraction=100/150
        context = self._make_context(cash=150.0, position=0.0, price=100.0)
        result = rule.adjust(1.0, context)
        assert abs(result - (100.0 / 150.0)) < 1e-6

    def test_standard_sell_clip_to_position(self):
        """Standard: SELL clips to available position."""
        rule = CashPositionConstraintRule(
            symbol="BTCUSDT",
            fee_rate=0.0,
            slippage_bound_bps=0.0,
            min_qty=1,
            min_notional=10.0,
            integer_only=True
        )

        # Target=0, should sell all
        context = self._make_context(cash=10000.0, position=2.0, price=100.0)
        result = rule.adjust(0.0, context)
        assert result == 0.0, "Should sell all (clip to 0)"

    def test_fractional_clip_buy(self):
        """Fractional: BUY clips to affordable amount."""
        rule = FractionalCashConstraintRule(
            symbol="BTCUSDT",
            fee_rate=0.0,
            slippage_bound_bps=0.0,
            min_qty=0.0001,
            min_notional=1.0
        )

        # Cash=100, price=300, target=1.0 => qty ~0.333 => fraction=1.0
        context = self._make_context(cash=100.0, position=0.0, price=300.0)
        result = rule.adjust(1.0, context)
        assert abs(result - 1.0) < 1e-9

    def test_min_trade_filter_drops_dust(self):
        """Risk rule drops orders below min_notional."""
        rule = CashPositionConstraintRule(
            symbol="BTCUSDT",
            fee_rate=0.0,
            slippage_bound_bps=0.0,
            min_qty=1,
            min_notional=50.0,  # Higher threshold
            integer_only=True
        )

        # Cash=40, price=100 -> qty=0.4 -> floor to 0 => drop to current fraction
        context = self._make_context(cash=40.0, position=0.0, price=100.0)
        result = rule.adjust(1.0, context)
        assert result == 0.0, "Order should be dropped due to insufficient cash"

    def test_slippage_bound_conservative(self):
        """Risk rule uses conservative slippage bound for affordability."""
        rule = CashPositionConstraintRule(
            symbol="BTCUSDT",
            fee_rate=0.0,
            slippage_bound_bps=100,  # 1% slippage bound
            min_qty=1,
            min_notional=0.0,
            integer_only=True
        )

        # Cash=100, price=100, slippage_bound=1%
        # p_eff = 100 * 1.01 = 101
        # max_affordable = 100 / 101 = 0.99 -> floor to 0 -> dropped
        context = self._make_context(cash=100.0, position=0.0, price=100.0)
        result = rule.adjust(1.0, context)
        assert result == 0.0, "Conservative slippage bound should prevent order"


# ============================================================
# Portfolio Invariant Tests
# ============================================================

class TestPortfolioInvariants:
    """Test portfolio invariants are never violated."""

    def test_cash_never_negative_standard(self):
        """Cash must never go negative in STANDARD portfolio."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=100.0, min_qty=1, min_notional=10.0)

        for i in range(20):
            fill = {
                "symbol": "BTCUSDT",
                "fill_price": 50.0 + i * 10,
                "filled_qty": 10,
                "fee": 5.0,
                "side": "BUY"
            }
            pm.apply_fill(fill)
            assert pm.cash >= 0, f"Cash must be non-negative after BUY {i}"

    def test_cash_never_negative_fractional(self):
        """Cash must never go negative in FRACTIONAL portfolio."""
        pm = FractionalPortfolioManager(symbol="BTCUSDT", initial_capital=100.0, min_qty=0.0001, min_notional=1.0)

        for i in range(20):
            fill = {
                "symbol": "BTCUSDT",
                "fill_price": 50.0 + i * 10,
                "filled_qty": 0.5,
                "fee": 1.0,
                "side": "BUY"
            }
            pm.apply_fill(fill)
            assert pm.cash >= 0, f"Cash must be non-negative after BUY {i}"

    def test_position_never_negative_standard(self):
        """Position must never go negative in STANDARD portfolio."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=10000.0, min_qty=1, min_notional=10.0)

        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 3, "fee": 0.0})

        for i in range(10):
            fill = {
                "symbol": "BTCUSDT",
                "fill_price": 100.0,
                "filled_qty": -5,  # Try to oversell
                "fee": 0.0,
                "side": "SELL"
            }
            pm.apply_fill(fill)
            assert pm.get_position("BTCUSDT") >= 0, f"Position must be non-negative after SELL {i}"

    def test_position_never_negative_fractional(self):
        """Position must never go negative in FRACTIONAL portfolio."""
        pm = FractionalPortfolioManager(symbol="BTCUSDT", initial_capital=10000.0, min_qty=0.0001, min_notional=1.0)

        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 1.5, "fee": 0.0})

        for i in range(10):
            fill = {
                "symbol": "BTCUSDT",
                "fill_price": 100.0,
                "filled_qty": -2.0,  # Try to oversell
                "fee": 0.0,
                "side": "SELL"
            }
            pm.apply_fill(fill)
            assert pm.get_position("BTCUSDT") >= 0, f"Position must be non-negative after SELL {i}"


# ============================================================
# Portfolio State Snapshot Tests
# ============================================================

class TestPortfolioStateSnapshot:
    """Test portfolio state snapshot contains required fields."""

    def test_state_contains_position_field(self):
        """State snapshot should contain 'position' field."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=10000.0)

        pm.apply_fill({"symbol": "BTCUSDT", "fill_price": 100.0, "filled_qty": 5, "fee": 0.0})

        state = pm.state()
        state_dict = state.to_dict()

        assert "position" in state_dict
        assert state_dict["position"] == 5.0

    def test_state_contains_cash(self):
        """State snapshot should contain 'cash' field."""
        pm = PortfolioManager(symbol="BTCUSDT", initial_capital=10000.0)

        state = pm.state()
        state_dict = state.to_dict()

        assert "cash" in state_dict
        assert state_dict["cash"] == 10000.0
