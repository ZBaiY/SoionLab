from __future__ import annotations

import hashlib
import hmac
from decimal import Decimal
from urllib.parse import urlencode

import pytest

from quant_engine.execution.exchange.binance_client import BinanceAPIError, BinanceSpotClient


class _FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        json_data=None,
        text: str = "",
        headers: dict[str, str] | None = None,
        url: str = "https://example.test",
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data
        self.text = text
        self.headers = headers or {}
        self.url = url

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self):
        if self._json_data is None:
            raise ValueError("no json")
        return self._json_data


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []
        self.headers = {}

    def request(self, method, url, params=None, timeout=None):
        self.calls.append({"method": method, "url": url, "params": dict(params or {}), "timeout": timeout})
        if not self._responses:
            raise AssertionError("unexpected request")
        nxt = self._responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


def _mk_client(session: _FakeSession) -> BinanceSpotClient:
    return BinanceSpotClient(
        api_key="k",
        api_secret="s",
        base_url="https://testnet.binance.vision",
        session=session,
        time_sync_interval_s=999.0,
    )


def test_api_signs_and_adds_timestamp_and_recvwindow(monkeypatch):
    session = _FakeSession([_FakeResponse(status_code=200, json_data={"ok": True})])
    client = _mk_client(session)
    client._time_offset_ms = 0
    client._last_sync_mono = 10_000.0
    monkeypatch.setattr("quant_engine.execution.exchange.binance_client.time.time", lambda: 1000.0)
    monkeypatch.setattr("quant_engine.execution.exchange.binance_client.time.monotonic", lambda: 10_000.0)

    out = client.api("GET", "/api/v3/account", signed=True, params={"symbol": "BTCUSDT"})
    assert out["ok"] is True
    call = session.calls[0]
    params = dict(call["params"])
    assert "timestamp" in params
    assert "recvWindow" in params
    assert "signature" in params
    sig = params.pop("signature")
    qs = urlencode(sorted(params.items(), key=lambda kv: kv[0]), doseq=True)
    expected = hmac.new(b"s", qs.encode(), hashlib.sha256).hexdigest()
    assert sig == expected


def test_sync_time_applies_offset(monkeypatch):
    session = _FakeSession([_FakeResponse(status_code=200, json_data={"serverTime": 1_500})])
    client = _mk_client(session)

    times = iter([1.000, 1.004])
    monkeypatch.setattr("quant_engine.execution.exchange.binance_client.time.time", lambda: next(times))
    info = client.sync_time()
    assert info["server_ms"] == 1500
    assert info["local_mid_ms"] == 1002
    assert info["skew_ms"] == 498
    assert client.time_offset_ms == 498


def test_api_error_payload_extraction():
    session = _FakeSession(
        [
            _FakeResponse(
                status_code=400,
                json_data={"code": -1013, "msg": "Filter failure: LOT_SIZE"},
                url="https://testnet.binance.vision/api/v3/order",
            )
        ]
    )
    client = _mk_client(session)
    with pytest.raises(BinanceAPIError) as ei:
        client.api("POST", "/api/v3/order", signed=True, params={"symbol": "BTCUSDT"})
    exc = ei.value
    assert exc.status_code == 400
    assert exc.code == -1013
    assert "LOT_SIZE" in str(exc.msg)


def test_safe_limit_price_clamps_to_percent_band():
    exchange_info = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.00001", "minQty": "0.00001"},
                    {
                        "filterType": "PERCENT_PRICE_BY_SIDE",
                        "bidMultiplierDown": "0.90",
                        "bidMultiplierUp": "1.10",
                        "askMultiplierDown": "0.90",
                        "askMultiplierUp": "1.10",
                    },
                ],
            }
        ]
    }
    session = _FakeSession([_FakeResponse(status_code=200, json_data=exchange_info)])
    client = _mk_client(session)

    px = client.safe_limit_price(
        "BTCUSDT",
        side="BUY",
        ref_price=Decimal("100"),
        rel_offset=Decimal("0.50"),
    )
    # Raw target = 50, clamped to bidMultiplierDown * ref = 90.
    assert px == Decimal("90")
