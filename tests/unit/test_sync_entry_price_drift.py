"""Tests for entry price drift fix during sync_from_exchange."""

from __future__ import annotations

from quant_engine.contracts.exchange_account import AccountState, AssetBalance
from quant_engine.portfolio.fractional import FractionalPortfolioManager
from quant_engine.portfolio.manager import PortfolioManager


def _account_state(*, btc_free: float = 0.0, usdt_free: float = 1000.0) -> AccountState:
    return AccountState(
        balances={
            "BTC": AssetBalance(free=btc_free, locked=0.0),
            "USDT": AssetBalance(free=usdt_free, locked=0.0),
        },
        positions={"BTCUSDT": btc_free},
        timestamp=1700000000000,
    )


class TestEntryPriceDriftFix:
    """Verify that sync_from_exchange uses mark price as proxy when entry_price=0."""

    def test_nonzero_position_gets_mark_as_entry(self) -> None:
        pm = FractionalPortfolioManager(
            symbol="BTCUSDT", initial_capital=1000.0,
            min_qty=0.0001, min_notional=1.0,
        )
        # Simulate marks being populated (as happens after first engine.step)
        pm.update_marks({"ohlcv": {"BTCUSDT": {"numeric": {"close": 65000.0}}}})

        # Sync a non-zero position from exchange — entry_price starts at 0
        pm.sync_from_exchange(
            _account_state(btc_free=0.5, usdt_free=500.0),
            symbol="BTCUSDT",
            quote_asset="USDT",
        )

        pos = pm.positions["BTCUSDT"]
        assert pos.lots > 0
        assert pos.entry_price == 65000.0  # mark price used as proxy

    def test_flat_position_keeps_zero_entry(self) -> None:
        pm = FractionalPortfolioManager(
            symbol="BTCUSDT", initial_capital=1000.0,
            min_qty=0.0001, min_notional=1.0,
        )
        pm.update_marks({"ohlcv": {"BTCUSDT": {"numeric": {"close": 65000.0}}}})

        pm.sync_from_exchange(
            _account_state(btc_free=0.0, usdt_free=500.0),
            symbol="BTCUSDT",
            quote_asset="USDT",
        )

        pos = pm.positions["BTCUSDT"]
        assert pos.lots == 0
        assert pos.entry_price == 0.0  # flat → stays zero

    def test_existing_entry_price_not_overwritten(self) -> None:
        pm = FractionalPortfolioManager(
            symbol="BTCUSDT", initial_capital=1000.0,
            min_qty=0.0001, min_notional=1.0,
        )
        # Buy first to set a real entry price
        pm.apply_fill({
            "symbol": "BTCUSDT", "fill_price": 60000.0,
            "filled_qty": 0.5, "fee": 0.0, "side": "BUY",
        })
        assert pm.positions["BTCUSDT"].entry_price == 60000.0

        pm.update_marks({"ohlcv": {"BTCUSDT": {"numeric": {"close": 65000.0}}}})

        # Sync same qty — entry_price should stay at 60000, not become mark
        pm.sync_from_exchange(
            _account_state(btc_free=0.5, usdt_free=500.0),
            symbol="BTCUSDT",
            quote_asset="USDT",
        )

        assert pm.positions["BTCUSDT"].entry_price == 60000.0

    def test_no_mark_available_entry_stays_zero(self) -> None:
        pm = FractionalPortfolioManager(
            symbol="BTCUSDT", initial_capital=1000.0,
            min_qty=0.0001, min_notional=1.0,
        )
        # No update_marks called — simulates startup before first step

        pm.sync_from_exchange(
            _account_state(btc_free=0.5, usdt_free=500.0),
            symbol="BTCUSDT",
            quote_asset="USDT",
        )

        pos = pm.positions["BTCUSDT"]
        assert pos.lots > 0
        assert pos.entry_price == 0.0  # no mark → stays zero until first step

    def test_standard_portfolio_mark_proxy(self) -> None:
        pm = PortfolioManager(
            symbol="BTCUSDT", initial_capital=10000.0,
            min_qty=1, min_notional=1.0,
        )
        pm.update_marks({"ohlcv": {"BTCUSDT": {"numeric": {"close": 100.0}}}})

        # Standard portfolio with step_size=1, so btc_free=3.0 → 3 lots
        pm.sync_from_exchange(
            _account_state(btc_free=3.0, usdt_free=5000.0),
            symbol="BTCUSDT",
            quote_asset="USDT",
        )

        pos = pm.positions["BTCUSDT"]
        assert pos.lots == 3
        assert pos.entry_price == 100.0

    def test_second_sync_preserves_mark_entry_when_still_nonzero(self) -> None:
        """After mark-proxy entry is set, next sync should keep it (entry != 0 now)."""
        pm = FractionalPortfolioManager(
            symbol="BTCUSDT", initial_capital=1000.0,
            min_qty=0.0001, min_notional=1.0,
        )
        pm.update_marks({"ohlcv": {"BTCUSDT": {"numeric": {"close": 65000.0}}}})
        pm.sync_from_exchange(
            _account_state(btc_free=0.5, usdt_free=500.0),
            symbol="BTCUSDT", quote_asset="USDT",
        )
        assert pm.positions["BTCUSDT"].entry_price == 65000.0

        # Update mark to new price, sync again with same qty
        pm.update_marks({"ohlcv": {"BTCUSDT": {"numeric": {"close": 70000.0}}}})
        pm.sync_from_exchange(
            _account_state(btc_free=0.5, usdt_free=500.0),
            symbol="BTCUSDT", quote_asset="USDT",
        )
        # Entry should stay at 65000 (not overwritten to 70000 because entry != 0)
        assert pm.positions["BTCUSDT"].entry_price == 65000.0

    def test_full_close_then_reopen_uses_new_mark(self) -> None:
        """Position goes to zero (entry reset), then reopens — should use current mark."""
        pm = FractionalPortfolioManager(
            symbol="BTCUSDT", initial_capital=1000.0,
            min_qty=0.0001, min_notional=1.0,
        )
        pm.update_marks({"ohlcv": {"BTCUSDT": {"numeric": {"close": 65000.0}}}})
        pm.sync_from_exchange(
            _account_state(btc_free=0.5, usdt_free=500.0),
            symbol="BTCUSDT", quote_asset="USDT",
        )
        assert pm.positions["BTCUSDT"].entry_price == 65000.0

        # Close position
        pm.sync_from_exchange(
            _account_state(btc_free=0.0, usdt_free=1000.0),
            symbol="BTCUSDT", quote_asset="USDT",
        )
        assert pm.positions["BTCUSDT"].entry_price == 0.0

        # Reopen with new mark
        pm.update_marks({"ohlcv": {"BTCUSDT": {"numeric": {"close": 70000.0}}}})
        pm.sync_from_exchange(
            _account_state(btc_free=0.3, usdt_free=800.0),
            symbol="BTCUSDT", quote_asset="USDT",
        )
        assert pm.positions["BTCUSDT"].entry_price == 70000.0
