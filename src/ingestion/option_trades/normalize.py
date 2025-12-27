

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Any, Mapping, cast

from ingestion.contracts.tick import Domain, IngestionTick, normalize_tick


def _to_ms_int(x: Any) -> int:
    """Coerce seconds-or-ms epoch into epoch milliseconds int."""
    if x is None:
        raise ValueError("timestamp cannot be None")
    if isinstance(x, bool):
        raise ValueError("invalid timestamp type: bool")
    try:
        v = float(x)
    except Exception as e:
        raise ValueError(f"invalid timestamp: {x!r}") from e
    # heuristic: seconds ~1e9, ms ~1e12
    if v < 10_000_000_000:
        return int(round(v * 1000.0))
    return int(round(v))


def _to_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


def _to_int(x: Any) -> int:
    try:
        return int(float(x))
    except Exception:
        return 0


_MONTH = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def _parse_deribit_instrument(name: str) -> dict[str, Any]:
    """Parse Deribit option instrument name.

    Format (typical): BTC-30NOV25-91000-C
      - <currency>-<DDMMMYY>-<strike>-<C|P>

    Returns keys:
      - currency (str)
      - expiry_date (datetime.date)
      - expiry_ts (int, ms)  # Deribit expiry assumed 08:00 UTC
      - strike (float)
      - option_type ('call'|'put')
    """
    parts = str(name).split("-")
    if len(parts) < 4:
        return {"currency": None, "expiry_date": None, "expiry_ts": None, "strike": None, "option_type": None}

    currency = parts[0]
    expiry_raw = parts[1].upper()
    strike_raw = parts[2]
    cp = parts[3].upper()

    # expiry_raw: 30NOV25
    try:
        dd = int(expiry_raw[:2])
        mmm = expiry_raw[2:5]
        yy = int(expiry_raw[5:7])
        mm = _MONTH.get(mmm)
        if mm is None:
            raise ValueError(f"unknown month: {mmm}")
        year = 2000 + yy
        expiry_date = _dt.date(year, mm, dd)

        # Deribit expiry time is exchange-defined; use 08:00 UTC as a sane default.
        expiry_dt = _dt.datetime(year, mm, dd, 8, 0, 0, tzinfo=_dt.timezone.utc)
        expiry_ts = int(expiry_dt.timestamp() * 1000)
    except Exception:
        expiry_date = None
        expiry_ts = None

    try:
        strike = float(strike_raw)
    except Exception:
        strike = None

    option_type = "call" if cp == "C" else ("put" if cp == "P" else None)

    return {
        "currency": currency,
        "expiry_date": expiry_date,
        "expiry_ts": expiry_ts,
        "strike": strike,
        "option_type": option_type,
    }


@dataclass(frozen=True)
class DeribitOptionTradesNormalizer:
    """Normalize Deribit option trades into `IngestionTick`.

    Input: a single trade mapping (as returned by Deribit HTTP APIs or your parquet).

    Output tick:
      - domain: "option_trade" (preferred)
      - data_ts: event time from trade['timestamp'] (epoch ms)
      - timestamp: arrival/observe time.
          * If `arrival_ts` passed to `normalize`, uses it.
          * Else tries raw['arrival_ts'] / raw['ingest_ts'].
          * Else falls back to data_ts.

    Note:
      - This normalizer does NOT aggregate.
      - It only enriches payload with parsed instrument fields (expiry_ts/strike/option_type) to
        make downstream term-structure queries possible without extra joins.
    """

    symbol: str
    domain: Domain = cast(Domain, "option_trade")

    def normalize(self, raw: Mapping[str, Any], *, arrival_ts: Any | None = None) -> IngestionTick:
        r: dict[str, Any] = {str(k): v for k, v in raw.items()}

        # --- event time (required) ---
        if "timestamp" in r:
            data_ts = _to_ms_int(r.get("timestamp"))
        elif "data_ts" in r:
            data_ts = _to_ms_int(r.get("data_ts"))
        elif "event_ts" in r:
            data_ts = _to_ms_int(r.get("event_ts"))
        else:
            raise KeyError("Deribit option trade missing event timestamp field: expected 'timestamp'")

        # --- arrival time (optional but preferred at ingestion boundary) ---
        if arrival_ts is not None:
            ts = _to_ms_int(arrival_ts)
        elif "arrival_ts" in r:
            ts = _to_ms_int(r.get("arrival_ts"))
        elif "ingest_ts" in r:
            ts = _to_ms_int(r.get("ingest_ts"))
        else:
            ts = data_ts

        # instrument parsing
        inst = str(r.get("instrument_name", ""))
        parsed = _parse_deribit_instrument(inst) if inst else {}

        expiry_ts = parsed.get("expiry_ts")
        dte_ms = int(expiry_ts - data_ts) if isinstance(expiry_ts, int) else None

        # payload: keep raw fields (pure python) + parsed enrichments
        payload: dict[str, Any] = dict(r)
        payload.update(
            {
                "exchange": "DERIBIT",
                "instrument_name": inst or None,
                "expiry_ts": expiry_ts,
                "strike": parsed.get("strike"),
                "option_type": parsed.get("option_type"),
                "dte_ms": dte_ms,
            }
        )

        # light numeric coercions (optional but helps downstream)
        for k in ("price", "mark_price", "index_price", "iv", "amount", "contracts"):
            if k in payload and payload[k] is not None:
                payload[k] = _to_float(payload[k])
        for k in ("trade_seq", "tick_direction"):
            if k in payload and payload[k] is not None:
                payload[k] = _to_int(payload[k])

        return normalize_tick(
            timestamp=ts,
            data_ts=data_ts,
            domain=self.domain,
            symbol=self.symbol,
            payload=payload,
        )