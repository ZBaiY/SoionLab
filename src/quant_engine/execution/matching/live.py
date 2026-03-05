from __future__ import annotations

import os
import time
from decimal import Decimal
from typing import Any

from quant_engine.contracts.execution.matching import MatchingBase
from quant_engine.contracts.execution.order import Order, OrderType
from quant_engine.health.events import ExecutionPermit, FaultEvent, FaultKind
from quant_engine.health.manager import HealthManager
from quant_engine.execution.exchange.binance_client import (
    BinanceAPIError,
    BinanceClientError,
    BinanceSpotClient,
    BinanceTransportError,
    resolve_binance_profile,
)
from quant_engine.utils.logger import get_logger, log_debug, log_info, log_warn

from .registry import register_matching

# Role: statuses that can still gain fills until we explicitly cancel/refetch.
_OPEN_ORDER_STATUSES = {"NEW", "PARTIALLY_FILLED", "PENDING_CANCEL"}
# Role: statuses after which Binance will not execute additional quantity for that order.
_TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}
# Role: Binance API codes that mean key/secret/signature auth failure.
_AUTH_ERROR_CODES = {-2014, -2015}
# Role: deterministic order-parameter/filter failures (tick/step/percent/min-notional).
_FILTER_ERROR_CODES = {-1013}
# Role: balance/margin-side rejection codes where retrying immediately is non-productive.
_INSUFFICIENT_BALANCE_CODES = {-2010}


def _d(x: Any) -> Decimal:
    return Decimal(str(x))


def _fmt_decimal(x: Decimal) -> str:
    return format(x, "f")


@register_matching("LIVE-BINANCE")
class LiveBinanceMatchingEngine(MatchingBase):
    """
    Binance Spot matching adapter.

    Reduce-only rule (minimal, long-only portfolio assumption):
      - only SELL orders are allowed while ExecutionPermit.REDUCE_ONLY is active.
    """

    def __init__(
        self,
        symbol: str,
        *,
        client: BinanceSpotClient | None = None,
        health: HealthManager | None = None,
        env: str | None = None,
        api_key_env: str | None = None,
        api_secret_env: str | None = None,
        base_url: str | None = None,
        recv_window: int = 5_000,
        time_sync_interval_s: float = 30.0,
        timeout_s: float = 10.0,
        run_tag: str = "qe",
        limit_rel_offset: float = 0.003,
    ):
        """Build the live Binance matcher.

        Args:
            symbol: Trading symbol sent to Binance (e.g., `BTCUSDT`).
            client: Optional pre-built client; when provided profile/env resolution is bypassed.
            health: Optional health manager used for permit gating and fault emission.
            env: Binance profile selector (`testnet`/`mainnet`) when `client` is not provided.
            api_key_env: Env-var name override for API key lookup.
            api_secret_env: Env-var name override for API secret lookup.
            base_url: Optional REST base URL override.
            recv_window: Binance signed-request receive window in milliseconds.
            time_sync_interval_s: Max age of local/server clock sync before refresh.
            timeout_s: Per-request HTTP timeout in seconds.
            run_tag: Stable prefix used when generating deterministic client order ids.
            limit_rel_offset: Relative offset from reference price when synthesizing limit prices.
        """
        self.symbol = symbol
        self._health = health
        self._logger = get_logger(__name__)
        self._run_tag = str(run_tag or "qe")
        self._limit_rel_offset = max(0.0, float(limit_rel_offset))
        self._client_order_seq = 0
        self._consecutive_filter_failures = 0
        self._consecutive_rate_limits = 0
        self._rate_limit_events_total = 0
        # Role: report the mainnet guard fault once to avoid log storms on every step.
        self._mainnet_guard_reported = False

        if client is not None:
            self.client = client
            self._env = "custom"
            self._mainnet_block_reason: str | None = None
            return

        resolved = resolve_binance_profile(
            env=env,
            api_key_env=api_key_env,
            api_secret_env=api_secret_env,
            base_url=base_url,
        )
        self._env = resolved.env
        self._mainnet_block_reason = None
        if resolved.env == "mainnet" and str(os.environ.get("BINANCE_MAINNET_CONFIRM", "")).strip() != "YES":
            # Invariant: live mainnet order flow is hard-blocked unless explicit confirm is set.
            self._mainnet_block_reason = (
                "BINANCE_MAINNET_CONFIRM=YES is required to place orders on mainnet"
            )
            log_warn(
                self._logger,
                "binance.mainnet.guard.blocked",
                symbol=symbol,
                reason=self._mainnet_block_reason,
            )
        self.client = BinanceSpotClient(
            api_key=resolved.api_key,
            api_secret=resolved.api_secret,
            base_url=resolved.base_url,
            recv_window=int(recv_window),
            time_sync_interval_s=float(time_sync_interval_s),
            timeout_s=float(timeout_s),
        )
        try:
            self.client.sync_time()
        except Exception as exc:
            self._report_fault(
                kind=FaultKind.EXECUTION_EXCEPTION,
                source="execution.binance.sync_time_init",
                severity_hint="fatal",
                exc=exc,
                context={"uncertain": False},
            )
            raise

    def _execution_permit(self) -> ExecutionPermit:
        if self._health is None:
            return ExecutionPermit.FULL
        return self._health.execution_permit()

    def _report_fault(
        self,
        *,
        kind: FaultKind,
        source: str,
        severity_hint: str | None,
        exc: Exception | None,
        context: dict[str, Any] | None = None,
    ):
        if self._health is None:
            return None
        return self._health.report(
            FaultEvent(
                ts=int(time.time() * 1000),
                source=source,
                kind=kind,
                domain="execution/binance",
                symbol=self.symbol,
                severity_hint=severity_hint,  # type: ignore[arg-type]
                exc_type=type(exc).__name__ if exc is not None else None,
                exc_msg=(str(exc)[:200] if exc is not None else None),
                context=context,
            )
        )

    def _build_client_order_id(self, order: Order) -> str:
        self._client_order_seq += 1
        ts_ms = int(time.time() * 1000)
        tag = str(order.tag or "ord")
        raw = f"{self._run_tag}-{tag}-{ts_ms}-{self._client_order_seq}"
        safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in raw)
        # Invariant: Binance `newClientOrderId` max length is 36.
        return safe[:36]

    def _get_price_ref(self, market_data: dict[str, Any] | None) -> Decimal | None:
        if market_data is None:
            return None
        orderbook = market_data.get("orderbook")
        ohlcv = market_data.get("ohlcv")
        bid = orderbook.get_attr("best_bid") if orderbook is not None else None
        ask = orderbook.get_attr("best_ask") if orderbook is not None else None
        if bid is not None and ask is not None:
            return (_d(bid) + _d(ask)) / Decimal("2")
        close = ohlcv.get_attr("close") if ohlcv is not None else None
        if close is not None:
            return _d(close)
        return None

    def _validate_and_build_params(self, order: Order, market_data: dict[str, Any] | None, client_order_id: str) -> dict[str, Any]:
        side = order.side.value
        qty = self.client.quantize_qty(order.symbol, _d(abs(float(order.qty))), mode="down")
        if qty <= 0:
            raise ValueError("quantized qty <= 0")

        params: dict[str, Any] = {
            "symbol": order.symbol,
            "side": side,
            "newClientOrderId": client_order_id,
            "quantity": _fmt_decimal(qty),
        }

        if order.order_type == OrderType.MARKET:
            params["type"] = "MARKET"
            return params

        if order.order_type != OrderType.LIMIT:
            raise ValueError(f"unsupported live order_type={order.order_type.value}")

        ref_price = self._get_price_ref(market_data)
        if order.price is not None:
            limit_px = self.client.quantize_price(order.symbol, _d(order.price), side=side)
        elif ref_price is not None:
            limit_px = self.client.safe_limit_price(
                order.symbol,
                side=side,
                ref_price=ref_price,
                rel_offset=_d(self._limit_rel_offset),
            )
        else:
            raise ValueError("limit order requires price or market price reference")
        if limit_px <= 0:
            raise ValueError("limit price <= 0")
        params.update(
            {
                "type": "LIMIT",
                "timeInForce": str(order.extra.get("time_in_force", "GTC")),
                "price": _fmt_decimal(limit_px),
            }
        )
        return params

    def _avg_price_from_order_payload(self, payload: dict[str, Any]) -> Decimal | None:
        fills = payload.get("fills")
        if isinstance(fills, list) and fills:
            qty = Decimal("0")
            quote = Decimal("0")
            for f in fills:
                try:
                    p = _d(f.get("price"))
                    q = _d(f.get("qty"))
                except Exception:
                    continue
                qty += q
                quote += p * q
            if qty > 0:
                return quote / qty
        try:
            ex_qty = _d(payload.get("executedQty", "0"))
            cum_quote = _d(payload.get("cummulativeQuoteQty", "0"))
        except Exception:
            return None
        if ex_qty > 0 and cum_quote > 0:
            return cum_quote / ex_qty
        return None

    def _build_fill_from_payload(self, order: Order, payload: dict[str, Any], fallback_payload: dict[str, Any] | None = None) -> dict[str, Any] | None:
        chosen = payload
        ex_qty = _d(chosen.get("executedQty", "0"))
        if ex_qty <= 0 and fallback_payload is not None:
            chosen = fallback_payload
            ex_qty = _d(chosen.get("executedQty", "0"))
        if ex_qty <= 0:
            return None

        px = self._avg_price_from_order_payload(payload)
        if px is None and fallback_payload is not None:
            px = self._avg_price_from_order_payload(fallback_payload)
        if px is None and order.price is not None:
            px = _d(order.price)
        if px is None:
            return None

        signed_qty = float(ex_qty)
        if order.side.value == "SELL":
            signed_qty = -signed_qty

        commission = Decimal("0")
        commission_asset = None
        fills = payload.get("fills")
        if isinstance(fills, list) and fills:
            for f in fills:
                try:
                    commission += _d(f.get("commission", "0"))
                except Exception:
                    continue
            if isinstance(fills[0], dict):
                commission_asset = fills[0].get("commissionAsset")

        ts = chosen.get("transactTime") or chosen.get("updateTime") or order.timestamp
        return {
            "fill_price": float(px),
            "filled_qty": signed_qty,
            "fee": float(commission),
            "slippage": float(order.extra.get("slippage", 0.0)),
            "side": order.side.value,
            "order_type": order.order_type.value,
            "timestamp": int(ts) if ts is not None else int(time.time() * 1000),
            "symbol": order.symbol,
            "exchange_order_id": chosen.get("orderId"),
            "client_order_id": chosen.get("clientOrderId"),
            "exchange_status": chosen.get("status"),
            "commission_asset": commission_asset,
            "exchange": "binance",
        }

    def _handle_api_error(self, exc: BinanceAPIError, order: Order) -> None:
        severity = "transient"
        context: dict[str, Any] = {"uncertain": False, "status_code": exc.status_code, "code": exc.code}
        if exc.status_code == 401 or exc.code in _AUTH_ERROR_CODES:
            severity = "fatal"
        elif exc.code == -1021:
            severity = "degraded"
        elif exc.status_code == 429:
            self._consecutive_rate_limits += 1
            self._rate_limit_events_total += 1
            severity = "degraded" if self._consecutive_rate_limits >= 3 else "transient"
            context["rate_limit_streak"] = self._consecutive_rate_limits
            context["rate_limit_events_total"] = self._rate_limit_events_total
        elif exc.code in _INSUFFICIENT_BALANCE_CODES:
            severity = "degraded"
        elif exc.code in _FILTER_ERROR_CODES:
            self._consecutive_filter_failures += 1
            severity = "degraded" if self._consecutive_filter_failures >= 3 else "transient"
            context["filter_failure_streak"] = self._consecutive_filter_failures
        self._report_fault(
            kind=FaultKind.EXECUTION_EXCEPTION,
            source="execution.binance.api_error",
            severity_hint=severity,
            exc=exc,
            context=context,
        )
        log_warn(
            self._logger,
            "binance.order.rejected",
            symbol=order.symbol,
            side=order.side.value,
            code=exc.code,
            status=exc.status_code,
            # Invariant: API-error logging must not raise — enforced here to prevent error-path crash
            api_msg=exc.msg,
        )

    def match(self, orders, market_data):
        log_debug(self._logger, "LiveBinanceMatchingEngine.received", count=len(orders))
        if not orders:
            return []
        permit = self._execution_permit()
        if permit == ExecutionPermit.BLOCK:
            # Scenario: health requested hard block, so matcher must emit no exchange side effects.
            log_warn(self._logger, "binance.execution.blocked.permit", symbol=self.symbol)
            return []

        if self._mainnet_block_reason is not None:
            if not self._mainnet_guard_reported:
                self._report_fault(
                    kind=FaultKind.EXECUTION_EXCEPTION,
                    source="execution.binance.mainnet_guard",
                    severity_hint="fatal",
                    exc=RuntimeError(self._mainnet_block_reason),
                    context={"uncertain": False, "guard": "mainnet_confirm"},
                )
                self._mainnet_guard_reported = True
            log_warn(self._logger, "binance.mainnet.guard.active", reason=self._mainnet_block_reason)
            return []

        fills: list[dict[str, Any]] = []
        for order in orders:
            if permit == ExecutionPermit.REDUCE_ONLY and order.side.value != "SELL":
                # Invariant: reduce-only mode cannot increase long exposure in this matcher.
                log_warn(
                    self._logger,
                    "binance.execution.reduce_only.skip",
                    symbol=order.symbol,
                    side=order.side.value,
                )
                continue
            client_order_id = self._build_client_order_id(order)
            try:
                params = self._validate_and_build_params(order, market_data, client_order_id)
            except Exception as exc:
                self._consecutive_filter_failures += 1
                severity = "degraded" if self._consecutive_filter_failures >= 3 else "transient"
                self._report_fault(
                    kind=FaultKind.EXECUTION_EXCEPTION,
                    source="execution.binance.filter_validation",
                    severity_hint=severity,
                    exc=exc,
                    context={"uncertain": False, "filter_failure_streak": self._consecutive_filter_failures},
                )
                log_warn(self._logger, "binance.order.validation.failed", err=str(exc), symbol=order.symbol)
                continue

            # Role: streak resets mark a clean submission window before attempting exchange placement.
            self._consecutive_filter_failures = 0
            self._consecutive_rate_limits = 0
            try:
                placed = self.client.api(
                    "POST",
                    "/api/v3/order",
                    params=params,
                    signed=True,
                    retry_5xx=1,
                )
            except BinanceAPIError as exc:
                self._handle_api_error(exc, order)
                continue
            except BinanceTransportError as exc:
                queried = None
                try:
                    # Scenario: transport uncertainty requires read-after-write lookup by client order id.
                    queried = self.client.api(
                        "GET",
                        "/api/v3/order",
                        params={"symbol": order.symbol, "origClientOrderId": client_order_id},
                        signed=True,
                        retry_5xx=1,
                    )
                except BinanceClientError:
                    queried = None
                self._report_fault(
                    kind=FaultKind.EXECUTION_EXCEPTION,
                    source="execution.binance.transport",
                    severity_hint="degraded",
                    exc=exc,
                    context={"uncertain": True, "client_order_id": client_order_id, "query_result": queried},
                )
                if isinstance(queried, dict):
                    maybe_fill = self._build_fill_from_payload(order, queried)
                    if maybe_fill is not None:
                        fills.append(maybe_fill)
                continue

            status = str(placed.get("status", ""))
            fallback = None
            if status in _OPEN_ORDER_STATUSES:
                if order.order_type == OrderType.LIMIT and placed.get("orderId") is not None:
                    try:
                        _ = self.client.api(
                            "DELETE",
                            "/api/v3/order",
                            params={"symbol": order.symbol, "orderId": placed.get("orderId")},
                            signed=True,
                            retry_5xx=1,
                        )
                    except BinanceClientError:
                        # Why: cancel is best-effort here; recovery continues via explicit status refetch below.
                        pass
                    try:
                        fallback = self.client.api(
                            "GET",
                            "/api/v3/order",
                            params={"symbol": order.symbol, "orderId": placed.get("orderId")},
                            signed=True,
                            retry_5xx=1,
                        )
                    except BinanceClientError:
                        fallback = None
                # Invariant: any observed executedQty in OPEN->CANCEL flow must propagate — enforced here to prevent fill-loss
                if _d(placed.get("executedQty", "0")) > 0 or (
                    isinstance(fallback, dict) and _d(fallback.get("executedQty", "0")) > 0
                ):
                    fill = self._build_fill_from_payload(order, placed, fallback_payload=fallback)
                    if fill is not None:
                        fills.append(fill)
            elif status in _TERMINAL_ORDER_STATUSES or _d(placed.get("executedQty", "0")) > 0:
                if self._avg_price_from_order_payload(placed) is None:
                    try:
                        fallback = self.client.api(
                            "GET",
                            "/api/v3/order",
                            params={"symbol": order.symbol, "orderId": placed.get("orderId")},
                            signed=True,
                            retry_5xx=1,
                        )
                    except BinanceClientError:
                        fallback = None
                fill = self._build_fill_from_payload(order, placed, fallback_payload=fallback)
                if fill is not None:
                    fills.append(fill)

            log_info(
                self._logger,
                "binance.order.placed",
                symbol=order.symbol,
                side=order.side.value,
                order_type=order.order_type.value,
                client_order_id=client_order_id,
                exchange_order_id=placed.get("orderId"),
                status=placed.get("status"),
            )

        log_info(self._logger, "LiveBinanceMatchingEngine.completed", fills=len(fills))
        return fills
