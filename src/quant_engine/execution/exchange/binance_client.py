from __future__ import annotations

import hashlib
import hmac
import os
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any
from urllib.parse import urlencode, urlparse

import requests


BINANCE_PROFILE_MAP: dict[str, dict[str, str]] = {
    "testnet": {
        "api_key_env": "BINANCE_TESTNET_API_KEY",
        "api_secret_env": "BINANCE_TESTNET_API_SECRET",
        "base_url": "https://testnet.binance.vision",
    },
    "mainnet": {
        "api_key_env": "BINANCE_MAINNET_API_KEY",
        "api_secret_env": "BINANCE_MAINNET_API_SECRET",
        "base_url": "https://api.binance.com",
    },
}


class BinanceClientError(RuntimeError):
    pass


class BinanceTransportError(BinanceClientError):
    def __init__(
        self,
        *,
        message: str,
        path: str,
        method: str,
        params: dict[str, Any],
        uncertain: bool = True,
    ) -> None:
        super().__init__(message)
        self.path = path
        self.method = method
        self.params = dict(params)
        self.uncertain = bool(uncertain)


class BinanceAPIError(BinanceClientError):
    def __init__(
        self,
        *,
        status_code: int,
        path: str,
        method: str,
        params: dict[str, Any],
        payload: Any,
    ) -> None:
        self.status_code = int(status_code)
        self.path = path
        self.method = method
        self.params = dict(params)
        self.payload = payload
        code = None
        msg = None
        if isinstance(payload, dict):
            code = payload.get("code")
            msg = payload.get("msg")
        self.code = code
        self.msg = msg
        super().__init__(
            f"Binance API error status={self.status_code} code={self.code} path={self.path} msg={self.msg}"
        )


@dataclass(frozen=True)
class BinanceClientConfig:
    api_key: str
    api_secret: str
    base_url: str
    env: str
    recv_window: int = 5_000
    time_sync_interval_s: float = 30.0
    timeout_s: float = 10.0


def _base_url_host(url: str) -> str:
    parsed = urlparse(str(url))
    return str(parsed.netloc or parsed.path).strip().lower()


def _looks_testnet_host(host: str) -> bool:
    return "testnet" in host or host.endswith("binance.vision")


def _assert_profile_base_url_guard(*, profile: str, base_url: str, explicit_override: bool) -> None:
    if not explicit_override:
        return
    override_confirm = str(os.environ.get("BINANCE_BASE_URL_CONFIRM", "")).strip()
    proxy_mode = str(os.environ.get("BINANCE_PROXY_MODE", "")).strip()
    if override_confirm != "YES" and proxy_mode != "1":
        raise BinanceClientError(
            "BINANCE_BASE_URL override requires explicit opt-in: set BINANCE_BASE_URL_CONFIRM=YES or BINANCE_PROXY_MODE=1"
        )
    host = _base_url_host(base_url)
    if not host:
        raise BinanceClientError(
            "BINANCE_BASE_URL override is empty/invalid; provide a full URL like https://testnet.binance.vision"
        )
    if profile == "testnet" and "binance" in host and not _looks_testnet_host(host):
        raise BinanceClientError(
            f"BINANCE_BASE_URL={base_url!r} looks mainnet while BINANCE_ENV=testnet; use a testnet URL"
        )
    if profile == "mainnet" and _looks_testnet_host(host):
        raise BinanceClientError(
            f"BINANCE_BASE_URL={base_url!r} looks testnet while BINANCE_ENV=mainnet; use a mainnet URL"
        )


def resolve_binance_profile(
    *,
    env: str | None = None,
    api_key_env: str | None = None,
    api_secret_env: str | None = None,
    base_url: str | None = None,
) -> BinanceClientConfig:
    profile = str(env or os.environ.get("BINANCE_ENV", "testnet")).strip().lower()
    if profile not in BINANCE_PROFILE_MAP:
        raise BinanceClientError(
            f"BINANCE_ENV={profile!r} invalid; expected one of {sorted(BINANCE_PROFILE_MAP)}"
        )
    p = BINANCE_PROFILE_MAP[profile]
    # Role: these hold the env var names (not values) so profile wiring stays explicit and auditable.
    key_name = str(api_key_env or p["api_key_env"]).strip()
    sec_name = str(api_secret_env or p["api_secret_env"]).strip()
    base_url_override = base_url if base_url is not None else os.environ.get("BINANCE_BASE_URL")
    resolved_base_url = str(base_url_override or p["base_url"]).strip()
    _assert_profile_base_url_guard(
        profile=profile,
        base_url=resolved_base_url,
        explicit_override=base_url_override is not None,
    )
    api_key = str(os.environ.get(key_name, "")).strip()
    api_secret = str(os.environ.get(sec_name, "")).strip()
    if not api_key:
        raise BinanceClientError(
            f"env var {key_name} is required for BINANCE_ENV={profile} but is not set"
        )
    if not api_secret:
        raise BinanceClientError(
            f"env var {sec_name} is required for BINANCE_ENV={profile} but is not set"
        )
    return BinanceClientConfig(
        api_key=api_key,
        api_secret=api_secret,
        base_url=resolved_base_url,
        env=profile,
    )


class BinanceSpotClient:
    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        base_url: str,
        recv_window: int = 5_000,
        time_sync_interval_s: float = 30.0,
        timeout_s: float = 10.0,
        session: requests.Session | None = None,
    ) -> None:
        self._api_key = str(api_key)
        self._api_secret = str(api_secret)
        self.base_url = str(base_url).rstrip("/")
        self.recv_window = int(recv_window)
        self.time_sync_interval_s = float(time_sync_interval_s)
        self.timeout_s = float(timeout_s)
        self.session = session or requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": self._api_key})
        self._time_offset_ms = 0
        self._last_sync_mono = 0.0
        self._exchange_info_cache: dict[str, dict[str, Any]] = {}

    @property
    def time_offset_ms(self) -> int:
        return int(self._time_offset_ms)

    def _now_ms(self) -> int:
        return int(time.time() * 1000) + int(self._time_offset_ms)

    @staticmethod
    def _safe_json(resp: requests.Response) -> Any:
        try:
            return resp.json()
        except Exception:
            # Why: preserve a bounded raw payload for fault reports when Binance returns non-JSON bodies.
            return {"raw": resp.text[:500]}

    def _sign(self, params: dict[str, Any]) -> str:
        items = sorted(params.items(), key=lambda kv: kv[0])
        qs = urlencode(items, doseq=True)
        return hmac.new(self._api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

    def sync_time(self) -> dict[str, int]:
        local_before = int(time.time() * 1000)
        data = self.api("GET", "/api/v3/time", signed=False, params={})
        local_after = int(time.time() * 1000)
        server_ms = int(data["serverTime"])
        local_mid = (local_before + local_after) // 2
        self._time_offset_ms = server_ms - local_mid
        self._last_sync_mono = time.monotonic()
        return {
            "server_ms": server_ms,
            "local_mid_ms": local_mid,
            "skew_ms": int(self._time_offset_ms),
        }

    def _maybe_sync_time(self) -> None:
        now = time.monotonic()
        if (now - float(self._last_sync_mono)) >= float(self.time_sync_interval_s):
            self.sync_time()

    def api(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        signed: bool = False,
        retry_5xx: int = 1,
    ) -> Any:
        attempts = 0
        max_attempts = max(1, int(retry_5xx) + 1)
        base_params = dict(params or {})
        # Invariant: timestamp resync should happen at most once per request loop to avoid infinite retry churn.
        timestamp_resync_attempted = False
        while attempts < max_attempts:
            attempts += 1
            if signed:
                self._maybe_sync_time()
            req_params = dict(base_params)
            req_query_params: dict[str, Any] | list[tuple[str, Any]] = req_params
            if signed:
                req_params.setdefault("timestamp", self._now_ms())
                req_params.setdefault("recvWindow", self.recv_window)
                signature = self._sign(req_params)
                # Invariant: signed query ordering must match the signed payload exactly.
                sorted_items = sorted(req_params.items(), key=lambda kv: kv[0])
                req_query_params = sorted_items + [("signature", signature)]
                req_params["signature"] = signature
            url = f"{self.base_url}{path}"
            try:
                resp = self.session.request(
                    method=method,
                    url=url,
                    params=req_query_params,
                    timeout=self.timeout_s,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempts < max_attempts:
                    time.sleep(0.25 * attempts)
                    continue
                raise BinanceTransportError(
                    message=str(exc),
                    path=path,
                    method=method,
                    params=req_params,
                    uncertain=True,
                ) from exc
            except requests.RequestException as exc:
                raise BinanceTransportError(
                    message=str(exc),
                    path=path,
                    method=method,
                    params=req_params,
                    uncertain=True,
                ) from exc

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = 0.5
                if retry_after is not None:
                    try:
                        sleep_s = max(0.1, float(retry_after))
                    except Exception:
                        sleep_s = 0.5
                if attempts < max_attempts:
                    # Scenario: respect exchange backpressure before retrying this same request payload.
                    time.sleep(sleep_s)
                    continue

            payload = self._safe_json(resp)
            if resp.ok:
                return payload

            code = payload.get("code") if isinstance(payload, dict) else None
            if signed and code == -1021 and not timestamp_resync_attempted:
                # Scenario: server rejected timestamp window; force one clock resync and replay.
                self.sync_time()
                timestamp_resync_attempted = True
                continue
            if resp.status_code >= 500 and attempts < max_attempts:
                time.sleep(0.25 * attempts)
                continue
            raise BinanceAPIError(
                status_code=int(resp.status_code),
                path=path,
                method=method,
                params=req_params,
                payload=payload,
            )
        raise BinanceClientError(f"unreachable retry loop for {method} {path}")

    def get_exchange_info(self, symbol: str, *, refresh: bool = False) -> dict[str, Any]:
        key = str(symbol).upper()
        if not refresh and key in self._exchange_info_cache:
            return self._exchange_info_cache[key]
        payload = self.api("GET", "/api/v3/exchangeInfo", params={"symbol": key}, signed=False)
        symbols = payload.get("symbols") if isinstance(payload, dict) else None
        if not symbols:
            raise BinanceClientError(f"symbol not found in exchangeInfo: {key}")
        info = symbols[0]
        self._exchange_info_cache[key] = info
        return info

    def get_symbol_filters(self, symbol: str, *, refresh: bool = False) -> dict[str, dict[str, Any]]:
        info = self.get_exchange_info(symbol, refresh=refresh)
        filters = info.get("filters") if isinstance(info, dict) else None
        if not isinstance(filters, list):
            raise BinanceClientError(f"filters missing in exchangeInfo for {symbol}")
        out: dict[str, dict[str, Any]] = {}
        for f in filters:
            if isinstance(f, dict) and "filterType" in f:
                out[str(f["filterType"])] = f
        return out

    def get_cached_symbol_filters(self, symbol: str) -> dict[str, dict[str, Any]] | None:
        key = str(symbol).upper()
        info = self._exchange_info_cache.get(key)
        if not isinstance(info, dict):
            return None
        filters = info.get("filters")
        if not isinstance(filters, list):
            return None
        out: dict[str, dict[str, Any]] = {}
        for f in filters:
            if isinstance(f, dict) and "filterType" in f:
                out[str(f["filterType"])] = f
        return out

    @staticmethod
    def _to_decimal(x: Any) -> Decimal:
        return Decimal(str(x))

    @staticmethod
    def quantize(x: Decimal, step: Decimal, *, mode: str = "down") -> Decimal:
        if step <= 0:
            return x
        rounding = ROUND_DOWN if mode == "down" else ROUND_UP
        n = (x / step).to_integral_value(rounding=rounding)
        return n * step

    def quantize_qty(
        self,
        symbol: str,
        qty: Decimal,
        *,
        mode: str = "down",
        lot_filter: str = "LOT_SIZE",
        refresh: bool = False,
    ) -> Decimal:
        filters = self.get_symbol_filters(symbol, refresh=refresh)
        lot = filters.get(lot_filter) or filters.get("LOT_SIZE", {})
        step = self._to_decimal(lot.get("stepSize", "0"))
        min_qty = self._to_decimal(lot.get("minQty", "0"))
        q = self.quantize(qty, step, mode=mode)
        if q < min_qty:
            q = min_qty
        return q

    def quantize_price(
        self,
        symbol: str,
        price: Decimal,
        *,
        side: str,
        refresh: bool = False,
    ) -> Decimal:
        filters = self.get_symbol_filters(symbol, refresh=refresh)
        pfilter = filters.get("PRICE_FILTER", {})
        tick = self._to_decimal(pfilter.get("tickSize", "0"))
        mode = "down" if str(side).upper() == "BUY" else "up"
        return self.quantize(price, tick, mode=mode)

    def safe_limit_price(
        self,
        symbol: str,
        *,
        side: str,
        ref_price: Decimal,
        rel_offset: Decimal = Decimal("0.003"),
        refresh: bool = False,
    ) -> Decimal:
        side_u = str(side).upper()
        filters = self.get_symbol_filters(symbol, refresh=refresh)
        if side_u == "BUY":
            px = ref_price * (Decimal("1") - rel_offset)
        else:
            px = ref_price * (Decimal("1") + rel_offset)
        px = self.quantize_price(symbol, px, side=side_u, refresh=refresh)
        percent = filters.get("PERCENT_PRICE_BY_SIDE")
        if not isinstance(percent, dict):
            return px
        if side_u == "BUY":
            lo = self._to_decimal(percent.get("bidMultiplierDown", "0")) * ref_price
            hi = self._to_decimal(percent.get("bidMultiplierUp", "999")) * ref_price
            lo_q = self.quantize_price(symbol, lo, side="BUY", refresh=refresh)
            hi_q = self.quantize_price(symbol, hi, side="BUY", refresh=refresh)
            # Invariant: limit price must stay inside Binance side-specific percent bands.
            px = min(max(px, lo_q), hi_q)
            return px
        lo = self._to_decimal(percent.get("askMultiplierDown", "0")) * ref_price
        hi = self._to_decimal(percent.get("askMultiplierUp", "999")) * ref_price
        lo_q = self.quantize_price(symbol, lo, side="SELL", refresh=refresh)
        hi_q = self.quantize_price(symbol, hi, side="SELL", refresh=refresh)
        # Invariant: limit price must stay inside Binance side-specific percent bands.
        px = min(max(px, lo_q), hi_q)
        return px
