from __future__ import annotations

import time
from typing import Any, Mapping, get_args, cast

from ingestion.contracts.tick import IngestionTick, Domain, normalize_tick
from ingestion.contracts.normalize import Normalizer
from ingestion.contracts.market import annotate_payload_market


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000.0)


def _to_int_ms(x: Any) -> int:
    """Coerce seconds-or-ms epoch into epoch-ms int."""
    if x is None:
        raise TypeError("timestamp is None")
    if isinstance(x, bool):
        raise TypeError("timestamp is bool")
    if isinstance(x, int):
        # heuristic: seconds ~ 1e9, ms ~ 1e12
        return x * 1000 if x < 10_000_000_000 else x
    if isinstance(x, float):
        v = x * 1000.0 if x < 10_000_000_000 else x
        return int(round(v))
    if isinstance(x, str):
        return _to_int_ms(float(x))
    # last resort: try int()
    return _to_int_ms(int(x))


def _to_float(x: Any) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(str(x))
    except Exception:
        return 0.0


# Binance WS aggTrade keys -> canonical keys
# https://binance-docs.github.io/apidocs/spot/en/#aggregate-trade-streams
_WS_KEYMAP: dict[str, str] = {
    "a": "trade_id",
    "p": "price",
    "q": "quantity",
    "f": "first_trade_id",
    "l": "last_trade_id",
    "T": "data_ts",      # trade time (event time)
    "m": "is_buyer_maker",
    "M": "is_best_match",
    "E": "event_ts",     # stream event time (observe/arrival proxy)
}


_ALLOWED_DOMAINS: set[str] = set(get_args(Domain))


def _coerce_domain(x: Domain | str) -> Domain:
    if x in _ALLOWED_DOMAINS:
        return cast(Domain, x)
    raise ValueError(f"Invalid domain: {x!r}. Expected one of: {sorted(_ALLOWED_DOMAINS)}")


def _coerce_aggtrade_mapping(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Return a dict using canonical field names.

    Accepts:
      - already-normalized dict with `data_ts` and `trade_id`
      - Binance REST/WS aggTrade dict with keys a/p/q/f/l/T/m/M (and optional E)
    """
    if "data_ts" in raw and "trade_id" in raw:
        return dict(raw)

    # Binance REST `/api/v3/aggTrades` uses a/p/q/f/l/T/m/M (no E)
    # Binance WS `@aggTrade` includes e/E/s and the same core fields.
    out: dict[str, Any] = {}
    for k, v in raw.items():
        mapped = _WS_KEYMAP.get(k)
        if mapped is None:
            continue
        out[mapped] = v

    # keep raw for audit if needed
    out.setdefault("_raw", dict(raw))
    return out


# ---------------------------------------------------------------------------
# Normalizer
# ---------------------------------------------------------------------------


class BinanceAggTradesNormalizer(Normalizer):
    """Normalize Binance aggTrades (REST/WS) into `IngestionTick`.

    Time semantics (project convention):
      - IngestionTick.data_ts   : event/logical time (trade time, epoch ms)
      - IngestionTick.timestamp : arrival/observe time (epoch ms)

    Output payload keeps full schema (core + aux). It is still *raw-like* and
    should be converted into snapshot objects inside runtime handlers.
    """

    venue: str
    asset_class: str
    currency: str | None
    calendar: str | None
    session: str | None
    timezone_name: str | None

    def __init__(
        self,
        *,
        symbol: str,
        domain: Domain | str = "trades",
        venue: str = "binance",
        asset_class: str = "crypto",
        currency: str | None = None,
        calendar: str | None = None,
        session: str | None = None,
        timezone_name: str | None = None,
    ):
        self.symbol = symbol
        self.domain: Domain = _coerce_domain(domain)
        self.venue = venue
        self.asset_class = asset_class
        self.currency = currency
        self.calendar = calendar
        self.session = session
        self.timezone_name = timezone_name

    def normalize(self, *, raw: Mapping[str, Any]) -> IngestionTick:
        payload = _coerce_aggtrade_mapping(raw)

        # --- event time (trade time) ---
        # canonical: data_ts
        if "data_ts" not in payload:
            # allow legacy names
            if "timestamp" in payload:
                payload["data_ts"] = payload["timestamp"]
            elif "T" in payload:
                payload["data_ts"] = payload["T"]

        event_ts = _to_int_ms(payload.get("data_ts"))

        # --- arrival/observe time ---
        # prefer explicit event_ts (WS); else try raw['E']; else wall clock.
        arrival_any = payload.get("event_ts")
        if arrival_any is None and isinstance(raw, Mapping):
            arrival_any = raw.get("E")
        arrival_ts = _to_int_ms(arrival_any) if arrival_any is not None else _now_ms()

        # --- canonical payload (full schema) ---
        # Note: we do NOT include `timestamp` as a canonical name; event time is `data_ts`.
        out_payload: dict[str, Any] = {
            "trade_id": int(payload["trade_id"]) if payload.get("trade_id") is not None else None,
            "price": _to_float(payload.get("price")),
            "quantity": _to_float(payload.get("quantity")),
            "first_trade_id": int(payload["first_trade_id"]) if payload.get("first_trade_id") is not None else None,
            "last_trade_id": int(payload["last_trade_id"]) if payload.get("last_trade_id") is not None else None,
            "is_buyer_maker": bool(payload.get("is_buyer_maker")) if payload.get("is_buyer_maker") is not None else None,
            "is_best_match": bool(payload.get("is_best_match")) if payload.get("is_best_match") is not None else None,
        }

        # preserve optional fields if present
        if "event_ts" in payload:
            out_payload["event_ts"] = _to_int_ms(payload["event_ts"]) if payload["event_ts"] is not None else None
        if "_raw" in payload:
            out_payload["_raw"] = payload["_raw"]

        out_payload = annotate_payload_market(
            out_payload,
            symbol=self.symbol,
            venue=self.venue,
            asset_class=self.asset_class,
            currency=self.currency,
            event_ts=event_ts,
            calendar=self.calendar,
            session=self.session,
            timezone_name=self.timezone_name,
        )

        return normalize_tick(
            timestamp=arrival_ts,
            data_ts=event_ts,
            domain=self.domain,
            symbol=self.symbol,
            payload=out_payload,
            source_id=getattr(self, "source_id", None),
        )

    __call__ = normalize
