from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast

from quant_engine.contracts.execution.order import Order, OrderSide, OrderType
from quant_engine.execution.exchange.binance_client import (
    BinanceAPIError,
    BinanceSpotClient,
    BinanceTransportError,
)
from quant_engine.execution.matching.live import LiveBinanceMatchingEngine
from quant_engine.health.events import Action, ActionKind, ExecutionPermit
from quant_engine.health.manager import HealthManager


@dataclass
class _SpyHealth:
    permit: ExecutionPermit = ExecutionPermit.FULL

    def __post_init__(self):
        self.events = []

    def execution_permit(self) -> ExecutionPermit:
        return self.permit

    def report(self, event):
        self.events.append(event)
        return Action(kind=ActionKind.CONTINUE, execution_permit=self.permit)


class _FakeClient:
    def __init__(self, script: list[tuple[str, str, Any]], filters: dict[str, dict[str, Any]] | None = None):
        self.script = list(script)
        self.calls: list[tuple[str, str, dict[str, Any], bool]] = []
        self.filters = filters or {
            "LOT_SIZE": {"stepSize": "0.000001", "minQty": "0.000001"},
            "MARKET_LOT_SIZE": {"stepSize": "0.000001", "minQty": "0.000001"},
            "MIN_NOTIONAL": {"minNotional": "0"},
        }

    def get_symbol_filters(self, symbol, refresh=False):
        return dict(self.filters)

    def quantize_qty(self, symbol, qty, mode="down", lot_filter="LOT_SIZE", refresh=False):
        return Decimal(str(qty))

    def quantize_price(self, symbol, price, side, refresh=False):
        return Decimal(str(price))

    def safe_limit_price(self, symbol, side, ref_price, rel_offset, refresh=False):
        return Decimal(str(ref_price))

    def api(self, method, path, params=None, signed=False, retry_5xx=1):
        params = dict(params or {})
        self.calls.append((method, path, params, signed))
        if not self.script:
            raise AssertionError(f"unexpected call {method} {path}")
        exp_method, exp_path, ret = self.script.pop(0)
        assert method == exp_method
        assert path == exp_path
        if isinstance(ret, Exception):
            raise ret
        return ret


class _MiniOrderbook:
    def __init__(self, bid: float, ask: float) -> None:
        self.best_bid = float(bid)
        self.best_ask = float(ask)

    def get_attr(self, name: str):
        return getattr(self, name, None)


def test_matcher_generates_client_order_id_and_places_then_cancels_limit():
    script = [
        (
            "POST",
            "/api/v3/order",
            {
                "orderId": 123,
                "clientOrderId": "will-be-overwritten-by-matcher",
                "status": "NEW",
                "executedQty": "0",
                "cummulativeQuoteQty": "0",
            },
        ),
        ("DELETE", "/api/v3/order", {"status": "CANCELED", "orderId": 123}),
        (
            "GET",
            "/api/v3/order",
            {
                "orderId": 123,
                "clientOrderId": "abc",
                "status": "CANCELED",
                "executedQty": "0",
                "cummulativeQuoteQty": "0",
                "updateTime": 1_700_000_000_000,
            },
        ),
    ]
    client = _FakeClient(script=script)
    matcher = LiveBinanceMatchingEngine(symbol="BTCUSDT", client=cast(BinanceSpotClient, client))

    order = Order(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        qty=0.001,
        order_type=OrderType.LIMIT,
        price=68000.0,
        timestamp=1_700_000_000_000,
        tag="unit",
    )
    fills = matcher.match([order], market_data=None)
    assert fills == []
    post_params = client.calls[0][2]
    assert "newClientOrderId" in post_params
    assert post_params["newClientOrderId"].startswith("qe-")
    assert len(post_params["newClientOrderId"]) <= 36
    assert client.calls[1][0] == "DELETE"


def test_uncertain_transport_path_emits_fault_with_uncertain_true():
    health = _SpyHealth()
    script = [
        (
            "POST",
            "/api/v3/order",
            BinanceTransportError(
                message="timeout",
                path="/api/v3/order",
                method="POST",
                params={"symbol": "BTCUSDT"},
                uncertain=True,
            ),
        ),
        (
            "GET",
            "/api/v3/order",
            BinanceTransportError(
                message="timeout",
                path="/api/v3/order",
                method="GET",
                params={"symbol": "BTCUSDT"},
                uncertain=True,
            ),
        ),
    ]
    client = _FakeClient(script=script)
    matcher = LiveBinanceMatchingEngine(
        symbol="BTCUSDT",
        client=cast(BinanceSpotClient, client),
        health=cast(HealthManager, health),
    )
    order = Order(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        qty=0.001,
        order_type=OrderType.MARKET,
        price=None,
        timestamp=1_700_000_000_000,
        tag="unit",
    )
    fills = matcher.match([order], market_data=None)
    assert fills == []
    assert health.events, "expected FaultEvent emission"
    evt = health.events[-1]
    assert evt.domain == "execution/binance"
    assert evt.context is not None
    assert evt.context.get("uncertain") is True


def test_block_permit_skips_placement():
    health = _SpyHealth(permit=ExecutionPermit.BLOCK)
    client = _FakeClient(script=[])
    matcher = LiveBinanceMatchingEngine(
        symbol="BTCUSDT",
        client=cast(BinanceSpotClient, client),
        health=cast(HealthManager, health),
    )
    order = Order(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        qty=0.001,
        order_type=OrderType.MARKET,
        price=None,
        timestamp=1_700_000_000_000,
    )
    fills = matcher.match([order], market_data=None)
    assert fills == []
    assert client.calls == []


def test_rate_limit_context_carries_total_event_count_across_attempts():
    health = _SpyHealth()
    script = [
        (
            "POST",
            "/api/v3/order",
            BinanceAPIError(
                status_code=429,
                path="/api/v3/order",
                method="POST",
                params={"symbol": "BTCUSDT"},
                payload={"code": -1003, "msg": "Too many requests"},
            ),
        ),
        (
            "POST",
            "/api/v3/order",
            BinanceAPIError(
                status_code=429,
                path="/api/v3/order",
                method="POST",
                params={"symbol": "BTCUSDT"},
                payload={"code": -1003, "msg": "Too many requests"},
            ),
        ),
    ]
    client = _FakeClient(script=script)
    matcher = LiveBinanceMatchingEngine(
        symbol="BTCUSDT",
        client=cast(BinanceSpotClient, client),
        health=cast(HealthManager, health),
    )
    order = Order(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        qty=0.001,
        order_type=OrderType.MARKET,
        price=None,
        timestamp=1_700_000_000_000,
        tag="unit",
    )

    assert matcher.match([order], market_data=None) == []
    assert matcher.match([order], market_data=None) == []

    rate_limit_events = [e for e in health.events if e.source == "execution.binance.api_error"]
    assert len(rate_limit_events) == 2
    assert rate_limit_events[0].context is not None
    assert rate_limit_events[0].context.get("rate_limit_streak") == 1
    assert rate_limit_events[0].context.get("rate_limit_events_total") == 1
    assert rate_limit_events[1].context is not None
    assert rate_limit_events[1].context.get("rate_limit_streak") == 1
    assert rate_limit_events[1].context.get("rate_limit_events_total") == 2


def test_market_min_notional_validation_blocks_non_close_all_before_submit():
    health = _SpyHealth()
    client = _FakeClient(
        script=[],
        filters={
            "LOT_SIZE": {"stepSize": "0.001", "minQty": "0.001"},
            "MARKET_LOT_SIZE": {"stepSize": "0.001", "minQty": "0.001"},
            "MIN_NOTIONAL": {"minNotional": "10"},
        },
    )
    matcher = LiveBinanceMatchingEngine(
        symbol="BTCUSDT",
        client=cast(BinanceSpotClient, client),
        health=cast(HealthManager, health),
    )
    order = Order(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        qty=0.01,
        order_type=OrderType.MARKET,
        price=None,
        timestamp=1_700_000_000_000,
        tag="unit",
    )

    market_data = {"orderbook": _MiniOrderbook(bid=100.0, ask=100.0)}
    assert matcher.match([order], market_data=market_data) == []
    assert client.calls == []
    assert health.events
    evt = health.events[-1]
    assert evt.source == "execution.binance.filter_validation"
    assert evt.context is not None
    assert evt.context.get("close_all") is False


def test_market_min_notional_validation_skips_close_all_submit_path():
    health = _SpyHealth()
    client = _FakeClient(
        script=[
            (
                "POST",
                "/api/v3/order",
                {
                    "orderId": 7,
                    "clientOrderId": "close-all",
                    "status": "REJECTED",
                    "executedQty": "0",
                    "cummulativeQuoteQty": "0",
                },
            ),
            (
                "GET",
                "/api/v3/order",
                {
                    "orderId": 7,
                    "clientOrderId": "close-all",
                    "status": "REJECTED",
                    "executedQty": "0",
                    "cummulativeQuoteQty": "0",
                },
            ),
        ],
        filters={
            "LOT_SIZE": {"stepSize": "0.001", "minQty": "0.001"},
            "MARKET_LOT_SIZE": {"stepSize": "0.001", "minQty": "0.001"},
            "MIN_NOTIONAL": {"minNotional": "10"},
        },
    )
    matcher = LiveBinanceMatchingEngine(
        symbol="BTCUSDT",
        client=cast(BinanceSpotClient, client),
        health=cast(HealthManager, health),
    )
    order = Order(
        symbol="BTCUSDT",
        side=OrderSide.SELL,
        qty=0.01,
        order_type=OrderType.MARKET,
        price=None,
        timestamp=1_700_000_000_000,
        tag="unit",
        extra={"close_all": True},
    )

    assert matcher.match([order], market_data=None) == []
    assert client.calls
    assert client.calls[0][0] == "POST"


def test_api_error_context_marks_close_all_attempt():
    health = _SpyHealth()
    client = _FakeClient(
        script=[
            (
                "POST",
                "/api/v3/order",
                BinanceAPIError(
                    status_code=400,
                    path="/api/v3/order",
                    method="POST",
                    params={"symbol": "BTCUSDT"},
                    payload={"code": -1013, "msg": "Filter failure"},
                ),
            )
        ],
    )
    matcher = LiveBinanceMatchingEngine(
        symbol="BTCUSDT",
        client=cast(BinanceSpotClient, client),
        health=cast(HealthManager, health),
    )
    order = Order(
        symbol="BTCUSDT",
        side=OrderSide.SELL,
        qty=0.01,
        order_type=OrderType.MARKET,
        price=None,
        timestamp=1_700_000_000_000,
        tag="unit",
        extra={"close_all": True},
    )

    assert matcher.match([order], market_data=None) == []
    api_events = [e for e in health.events if e.source == "execution.binance.api_error"]
    assert api_events
    evt = api_events[-1]
    assert evt.context is not None
    assert evt.context.get("close_all") is True
