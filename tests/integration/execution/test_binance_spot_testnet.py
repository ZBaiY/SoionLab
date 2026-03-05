from __future__ import annotations

import os
import time
from decimal import Decimal

import pytest

from quant_engine.execution.exchange.binance_client import (
    BinanceSpotClient,
    BinanceTransportError,
    resolve_binance_profile,
)


pytestmark = [pytest.mark.integration, pytest.mark.binance_testnet]


def _should_run() -> tuple[bool, str]:
    env = str(os.environ.get("BINANCE_ENV", "testnet")).strip().lower()
    if env != "testnet":
        return False, "BINANCE_ENV must be testnet"
    if not os.environ.get("BINANCE_TESTNET_API_KEY"):
        return False, "BINANCE_TESTNET_API_KEY not set"
    if not os.environ.get("BINANCE_TESTNET_API_SECRET"):
        return False, "BINANCE_TESTNET_API_SECRET not set"
    return True, ""


@pytest.mark.timeout(45)
def test_spot_testnet_place_query_cancel_cycle() -> None:
    ok, reason = _should_run()
    if not ok:
        pytest.skip(reason)

    cfg = resolve_binance_profile(env="testnet")
    client = BinanceSpotClient(
        api_key=cfg.api_key,
        api_secret=cfg.api_secret,
        base_url=cfg.base_url,
    )
    try:
        _ = client.sync_time()
    except BinanceTransportError as exc:
        pytest.skip(f"testnet endpoint unreachable: {type(exc).__name__}")

    symbol = "BTCUSDT"
    filters = client.get_symbol_filters(symbol)
    tick = Decimal(str(filters["PRICE_FILTER"]["tickSize"]))
    step = Decimal(str(filters["LOT_SIZE"]["stepSize"]))
    min_qty = Decimal(str(filters["LOT_SIZE"]["minQty"]))

    ticker = client.api("GET", "/api/v3/ticker/price", params={"symbol": symbol}, signed=False)
    last = Decimal(str(ticker["price"]))
    px = client.safe_limit_price(symbol, side="BUY", ref_price=last, rel_offset=Decimal("0.003"))
    px = client.quantize(px, tick, mode="down")
    qty = client.quantize(Decimal("0.001"), step, mode="down")
    if qty < min_qty:
        qty = min_qty

    cid = f"itest-{int(time.time() * 1000)}"[:36]
    placed = client.api(
        "POST",
        "/api/v3/order",
        signed=True,
        params={
            "symbol": symbol,
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": format(qty, "f"),
            "price": format(px, "f"),
            "newClientOrderId": cid,
        },
    )
    order_id = int(placed["orderId"])
    status = client.api(
        "GET",
        "/api/v3/order",
        signed=True,
        params={"symbol": symbol, "orderId": order_id},
    )
    if str(status.get("status")) in {"NEW", "PARTIALLY_FILLED", "PENDING_CANCEL"}:
        _ = client.api(
            "DELETE",
            "/api/v3/order",
            signed=True,
            params={"symbol": symbol, "orderId": order_id},
        )
        status = client.api(
            "GET",
            "/api/v3/order",
            signed=True,
            params={"symbol": symbol, "orderId": order_id},
        )

    assert status.get("orderId") == order_id
    assert str(status.get("status")) in {
        "NEW",
        "PARTIALLY_FILLED",
        "FILLED",
        "CANCELED",
        "PENDING_CANCEL",
        "REJECTED",
        "EXPIRED",
    }
