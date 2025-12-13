import pytest
from typing import Any, Dict, List

from quant_engine.contracts.feature import FeatureChannelBase
from quant_engine.data.ohlcv.snapshot import OHLCVSnapshot
from quant_engine.data.derivatives.option_chain.snapshot import OptionChainSnapshot
from quant_engine.data.derivatives.iv.snapshot import IVSurfaceSnapshot
from quant_engine.data.orderbook.snapshot import OrderbookSnapshot

from quant_engine.data.ohlcv.realtime import RealTimeDataHandler
from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler
from quant_engine.data.derivatives.iv.iv_handler import IVSurfaceDataHandler
from quant_engine.data.orderbook.realtime import RealTimeOrderbookHandler


class DummyChannel(FeatureChannelBase):
    """
    Minimal concrete FeatureChannel for contract testing.

    It only relies on FeatureChannelBase helpers; initialize/update/output
    are not relevant for these tests.
    """

    def __init__(self, symbol: str | None = None, name: str = "Name", **params: Any) -> None:
        self.name = name
        assert symbol is not None, "DummyChannel requires a symbol"
        self.symbol = symbol
        self.params: Dict[str, Any] = params or {}

        self.required_window_value: int = 1
        self.initialize_calls: List[Dict[str, Any]] = []
        self.update_calls: List[Dict[str, Any]] = []
        self.output_value: Dict[str, Any] = {name: 1.0}

    def required_window(self) -> int:
        return self.required_window_value

    def initialize(self, context: Dict[str, Any], warmup_window: int) -> None:
        self.initialize_calls.append(
            {"context": context, "warmup_window": warmup_window}
        )

    def update(self, context: Dict[str, Any]) -> None:
        self.update_calls.append({"context": context})

    def output(self) -> Dict[str, Any]:
        return self.output_value



# ---------------------------------------------------------------------------
# Dummy handlers using real snapshot classes
# ---------------------------------------------------------------------------


class DummyOHLCVHandler(RealTimeDataHandler):
    """
    Test-only subclass of RealTimeDataHandler.

    We intentionally do NOT call the real __init__ to avoid DataCache setup.
    Only snapshot/window behaviour relevant for FeatureChannelBase is provided.
    """

    def __init__(self) -> None:  # type: ignore[override]
        # do not call super().__init__
        self.snapshot_calls: list[float] = []
        self.window_calls: list[tuple[float, int]] = []

    def get_snapshot(self, ts: float) -> OHLCVSnapshot:  # type: ignore[override]
        self.snapshot_calls.append(ts)
        return OHLCVSnapshot(
            timestamp=ts,
            open=1.0,
            high=2.0,
            low=0.5,
            close=1.5,
            volume=100.0,
            latency=0.0,
        )

    def window(self, ts: float, n: int) -> list[OHLCVSnapshot]:  # type: ignore[override]
        self.window_calls.append((ts, n))
        return [
            OHLCVSnapshot(
                timestamp=ts - i,
                open=1.0,
                high=1.0,
                low=1.0,
                close=1.0,
                volume=1.0,
                latency=0.0,
            )
            for i in range(n)
        ]


class DummyOptionChainHandler(OptionChainDataHandler):
    """
    Test-only subclass of OptionChainDataHandler.

    We bypass the real __init__ and only implement get_snapshot(ts)
    used by FeatureChannelBase.snapshot.
    """

    def __init__(self) -> None:  # type: ignore[override]
        # do not call super().__init__
        self.snapshot_calls: list[float] = []

    def get_snapshot(self, ts: float) -> OptionChainSnapshot:  # type: ignore[override]
        self.snapshot_calls.append(ts)
        # minimal chain with one call and one put
        chain = [
            {"strike": 30000, "iv": 0.20, "type": "call", "moneyness": 0.0},
            {"strike": 30000, "iv": 0.22, "type": "put", "moneyness": 0.0},
        ]
        return OptionChainSnapshot.from_chain(timestamp=ts, chain=chain)


class DummyIVSurfaceHandler(IVSurfaceDataHandler):
    """
    Test-only subclass of IVSurfaceDataHandler.

    We override __init__ and get_snapshot(ts) so no real OptionChainDataHandler
    wiring is required for these contract tests.
    """

    def __init__(self) -> None:  # type: ignore[override]
        # do not call super().__init__
        self.snapshot_calls: list[float] = []
        # minimal identity attributes to satisfy dataclass fields when needed
        self.symbol = "BTCUSDT"
        self.chain_handler = None  # type: ignore[assignment]
        self.expiry = "2025-06-27"
        self.model_name = "TEST_MODEL"

    def get_snapshot(self, ts: float) -> IVSurfaceSnapshot:  # type: ignore[override]
        self.snapshot_calls.append(ts)
        curve = {"0.0": 0.25}
        params: Dict[str, Any] = {"alpha": 0.1, "beta": 0.5}
        return IVSurfaceSnapshot.from_surface(
            ts=ts,
            surface_ts=ts,
            atm_iv=0.25,
            skew=-0.01,
            curve=curve,
            surface_params=params,
            symbol=self.symbol,
            expiry=self.expiry,
            model=self.model_name,
        )


class DummyOrderbookHandler(RealTimeOrderbookHandler):
    """
    Test-only subclass of RealTimeOrderbookHandler.

    We bypass the real __init__ (and OrderbookCache) and only
    implement get_snapshot() / window() behaviour needed for tests.
    """

    def __init__(self, symbol: str) -> None:  # type: ignore[override]
        # do not call super().__init__
        self.symbol = symbol
        self.snapshot_calls: list[float] = []
        self.window_calls: list[tuple[float, int]] = []

    def get_snapshot(self, ts: float) -> OrderbookSnapshot:  # type: ignore[override]
        self.snapshot_calls.append(ts)
        tick = {
            "timestamp": ts,
            "best_bid": 99.0,
            "best_bid_size": 1.0,
            "best_ask": 101.0,
            "best_ask_size": 2.0,
        }
        return OrderbookSnapshot.from_tick(ts=ts, tick=tick, symbol=self.symbol)

    def window(self, ts: float, n: int) -> list[OrderbookSnapshot]:  # type: ignore[override]
        self.window_calls.append((ts, n))
        out: list[OrderbookSnapshot] = []
        for i in range(n):
            tick_ts = ts - i
            tick = {
                "timestamp": tick_ts,
                "best_bid": 99.0,
                "best_bid_size": 1.0,
                "best_ask": 101.0,
                "best_ask_size": 2.0,
            }
            out.append(OrderbookSnapshot.from_tick(ts=tick_ts, tick=tick, symbol=self.symbol))
        return out


# ---------------------------------------------------------------------------
# Helper to build context
# ---------------------------------------------------------------------------


def make_context(
    ts: float,
    data: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Build the context dict expected by FeatureChannelBase:

        {
            "ts": float,
            "data": {
                "ohlcv": {symbol -> handler},
                "orderbook": {...},
                "options": {...},
                "iv_surface": {...},
                "sentiment": {...},
            }
        }
    """
    return {"timestamp": ts, "data": data}


# ---------------------------------------------------------------------------
# _get_handler contract (accessed indirectly through snapshot/window_any)
# ---------------------------------------------------------------------------


def test_snapshot_uses_correct_handler_for_primary_symbol():
    ts = 100.0
    ohlcv_handler = DummyOHLCVHandler()
    ctx = make_context(
        ts,
        data={"ohlcv": {"BTCUSDT": ohlcv_handler}},
    )

    ch = DummyChannel(symbol="BTCUSDT")
    result = ch.snapshot_dict(ctx, "ohlcv")

    assert isinstance(result, dict)
    assert result["timestamp"] == ts
    assert ohlcv_handler.snapshot_calls == [ts]


def test_snapshot_uses_symbol_override_and_ignores_self_symbol():
    ts = 200.0
    h_btc = DummyOHLCVHandler()
    h_eth = DummyOHLCVHandler()
    ctx = make_context(
        ts,
        data={"ohlcv": {"BTCUSDT": h_btc, "ETHUSDT": h_eth}},
    )

    ch = DummyChannel(symbol="BTCUSDT")
    result = ch.snapshot_dict(ctx, "ohlcv", symbol="ETHUSDT")

    assert isinstance(result, dict)
    assert h_btc.snapshot_calls == []
    assert h_eth.snapshot_calls == [ts]


def test_snapshot_missing_symbol_raises_key_error():
    ts = 300.0
    h_btc = DummyOHLCVHandler()
    ctx = make_context(
        ts,
        data={"ohlcv": {"BTCUSDT": h_btc}},
    )

    ch = DummyChannel(symbol="ETHUSDT")  # not present in handlers

    with pytest.raises(KeyError):
        _ = ch.snapshot_dict(ctx, "ohlcv")


def test_snapshot_missing_data_type_raises_key_error():
    ts = 400.0
    h_btc = DummyOHLCVHandler()
    # context has only "ohlcv", no "orderbook"
    ctx = make_context(
        ts,
        data={"ohlcv": {"BTCUSDT": h_btc}},
    )

    ch = DummyChannel(symbol="BTCUSDT")

    with pytest.raises(KeyError):
        _ = ch.snapshot_dict(ctx, "orderbook")


def test_snapshot_raises_attribute_error_if_handler_has_no_get_snapshot():
    class NoSnapshotHandler:
        pass

    ts = 500.0
    ctx = make_context(
        ts,
        data={"ohlcv": {"BTCUSDT": NoSnapshotHandler()}},
    )

    ch = DummyChannel(symbol="BTCUSDT")

    with pytest.raises(AttributeError):
        _ = ch.snapshot_dict(ctx, "ohlcv")


# ---------------------------------------------------------------------------
# window_any() contract
# ---------------------------------------------------------------------------


def test_window_any_calls_handler_window_with_ts_and_n_and_returns_sequence():
    ts = 600.0
    n = 3
    ob_handler = DummyOrderbookHandler(symbol="BTCUSDT")
    ctx = make_context(
        ts,
        data={"orderbook": {"BTCUSDT": ob_handler}},
    )

    ch = DummyChannel(symbol="BTCUSDT")
    result = ch.window_any(ctx, "orderbook", n)

    assert ob_handler.window_calls == [(ts, n)]
    assert len(result) == n
    assert all(isinstance(r, OrderbookSnapshot) for r in result)


def test_window_any_raises_attribute_error_if_handler_has_no_window():
    class NoWindowHandler:
        def get_snapshot(self, ts: float) -> OHLCVSnapshot:
            return OHLCVSnapshot(
                timestamp=ts,
                open=0.0,
                high=0.0,
                low=0.0,
                close=0.0,
                volume=0.0,
                latency=0.0,
            )

    ts = 700.0
    ctx = make_context(
        ts,
        data={"ohlcv": {"BTCUSDT": NoWindowHandler()}},
    )

    ch = DummyChannel(symbol="BTCUSDT")

    with pytest.raises(AttributeError):
        _ = ch.window_any(ctx, "ohlcv", 10)


# ---------------------------------------------------------------------------
# Multi data-type snapshot contract
# ---------------------------------------------------------------------------


def test_snapshot_supports_multiple_data_types_and_snapshot_classes():
    ts = 800.0
    ohlcv_handler = DummyOHLCVHandler()
    opt_handler = DummyOptionChainHandler()
    iv_handler = DummyIVSurfaceHandler()
    ob_handler = DummyOrderbookHandler(symbol="BTCUSDT")

    ctx = make_context(
        ts,
        data={
            "ohlcv": {"BTCUSDT": ohlcv_handler},
            "options": {"BTCUSDT": opt_handler},
            "iv_surface": {"BTCUSDT": iv_handler},
            "orderbook": {"BTCUSDT": ob_handler},
        },
    )

    ch = DummyChannel(symbol="BTCUSDT")

    snap_ohlcv = ch.snapshot_dict(ctx, "ohlcv")
    snap_opt = ch.snapshot_dict(ctx, "options")
    snap_iv = ch.snapshot_dict(ctx, "iv_surface")
    snap_ob = ch.snapshot_dict(ctx, "orderbook")

    assert isinstance(snap_ohlcv, dict)
    assert isinstance(snap_opt, dict)
    assert isinstance(snap_iv, dict)
    assert isinstance(snap_ob, dict)


def test_context_without_ts_raises_key_error_for_snapshot():
    # This checks that FeatureChannelBase expects a 'ts' key in context.
    data = {"ohlcv": {"BTCUSDT": DummyOHLCVHandler()}}
    ctx = {"data": data}  # missing "ts"

    ch = DummyChannel(symbol="BTCUSDT")

    with pytest.raises(KeyError):
        _ = ch.snapshot_dict(ctx, "ohlcv")
