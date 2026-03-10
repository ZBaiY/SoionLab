"""Tests for startup readiness contract, adapter normalization, and evaluator."""

from __future__ import annotations

import pytest

from quant_engine.contracts.exchange_account import (
    AccountState,
    AssetBalance,
    StartupReadiness,
    SymbolConstraints,
)
from quant_engine.execution.exchange.account_adapter import BinanceAccountAdapter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_readiness(
    *,
    open_order_count: int = 0,
    quote_locked: float = 0.0,
    base_position_qty: float = 0.0,
) -> StartupReadiness:
    return StartupReadiness(
        timestamp=1700000000000,
        symbol="BTCUSDT",
        open_order_count=open_order_count,
        quote_asset="USDT",
        quote_free=1000.0,
        quote_locked=quote_locked,
        base_asset="BTC",
        base_position_qty=base_position_qty,
    )


class _StubClient:
    """Minimal stub matching the BinanceSpotClient interface used by adapter."""

    def __init__(
        self,
        *,
        balances: list[dict] | None = None,
        open_orders: list[dict] | None = None,
        exchange_info: dict | None = None,
        symbol_filters: dict | None = None,
    ) -> None:
        self._balances = balances or []
        self._open_orders = open_orders or []
        self._exchange_info = exchange_info or {
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
        }
        self._symbol_filters = symbol_filters or {
            "LOT_SIZE": {"stepSize": "0.00001", "minQty": "0.00001"},
            "MIN_NOTIONAL": {"minNotional": "10.0"},
        }

    def api(self, method: str, path: str, *, signed: bool = False, params: dict | None = None) -> list | dict:
        if path == "/api/v3/account":
            return {"balances": self._balances}
        if path == "/api/v3/openOrders":
            return self._open_orders
        return {}

    def get_exchange_info(self, symbol: str, *, refresh: bool = False) -> dict:
        return self._exchange_info

    def get_symbol_filters(self, symbol: str, *, refresh: bool = False) -> dict:
        return self._symbol_filters


def _make_adapter(
    *,
    balances: list[dict] | None = None,
    open_orders: list[dict] | None = None,
) -> BinanceAccountAdapter:
    default_balances = [
        {"asset": "USDT", "free": "5000.0", "locked": "0.0"},
        {"asset": "BTC", "free": "0.0", "locked": "0.0"},
    ]
    client = _StubClient(
        balances=balances or default_balances,
        open_orders=open_orders or [],
    )
    adapter = BinanceAccountAdapter(client)  # type: ignore[arg-type]
    # Populate _symbol_assets by calling get_symbol_constraints first
    adapter.get_symbol_constraints("BTCUSDT")
    return adapter


# ---------------------------------------------------------------------------
# Adapter normalization tests
# ---------------------------------------------------------------------------

class TestAdapterStartupReadiness:
    def test_clean_flat_account(self) -> None:
        adapter = _make_adapter()
        r = adapter.get_startup_readiness("BTCUSDT")
        assert r.symbol == "BTCUSDT"
        assert r.open_order_count == 0
        assert r.quote_locked == 0.0
        assert r.base_position_qty == 0.0
        assert r.quote_asset == "USDT"
        assert r.base_asset == "BTC"

    def test_open_orders_counted(self) -> None:
        adapter = _make_adapter(open_orders=[{"orderId": 1}, {"orderId": 2}])
        r = adapter.get_startup_readiness("BTCUSDT")
        assert r.open_order_count == 2

    def test_locked_quote_reported(self) -> None:
        adapter = _make_adapter(
            balances=[
                {"asset": "USDT", "free": "4000.0", "locked": "1000.0"},
                {"asset": "BTC", "free": "0.0", "locked": "0.0"},
            ],
        )
        r = adapter.get_startup_readiness("BTCUSDT")
        assert r.quote_free == 4000.0
        assert r.quote_locked == 1000.0

    def test_nonflat_base_reported(self) -> None:
        adapter = _make_adapter(
            balances=[
                {"asset": "USDT", "free": "5000.0", "locked": "0.0"},
                {"asset": "BTC", "free": "1.5", "locked": "0.0"},
            ],
        )
        r = adapter.get_startup_readiness("BTCUSDT")
        assert r.base_position_qty == 1.5

    def test_requires_symbol_constraints_first(self) -> None:
        client = _StubClient()
        adapter = BinanceAccountAdapter(client)  # type: ignore[arg-type]
        with pytest.raises(Exception, match="not resolved"):
            adapter.get_startup_readiness("BTCUSDT")


# ---------------------------------------------------------------------------
# Evaluator tests
# ---------------------------------------------------------------------------

# Import after adapter tests to keep test file self-contained on import failure
from apps.run_code.realtime_app import evaluate_startup_readiness


class TestEvaluateStartupReadiness:
    def test_clean_flat_passes(self) -> None:
        r = _make_readiness()
        assert evaluate_startup_readiness(r, is_mainnet=True) is None

    def test_open_orders_blocked(self) -> None:
        r = _make_readiness(open_order_count=3)
        failure = evaluate_startup_readiness(r, is_mainnet=False)
        assert failure is not None
        assert "open orders" in failure

    def test_locked_quote_blocked(self) -> None:
        r = _make_readiness(quote_locked=50.0)
        failure = evaluate_startup_readiness(r, is_mainnet=False)
        assert failure is not None
        assert "locked quote" in failure

    def test_nonflat_mainnet_blocked_by_default(self) -> None:
        r = _make_readiness(base_position_qty=0.5)
        failure = evaluate_startup_readiness(r, is_mainnet=True)
        assert failure is not None
        assert "non-flat" in failure

    def test_nonflat_mainnet_allowed_with_override(self) -> None:
        r = _make_readiness(base_position_qty=0.5)
        assert evaluate_startup_readiness(
            r, is_mainnet=True, allow_nonflat_start=True,
        ) is None

    def test_nonflat_testnet_allowed(self) -> None:
        r = _make_readiness(base_position_qty=0.5)
        assert evaluate_startup_readiness(r, is_mainnet=False) is None

    def test_open_orders_checked_before_locked(self) -> None:
        """Open orders fail even if locked is also bad — first check wins."""
        r = _make_readiness(open_order_count=1, quote_locked=100.0)
        failure = evaluate_startup_readiness(r, is_mainnet=False)
        assert failure is not None
        assert "open orders" in failure
