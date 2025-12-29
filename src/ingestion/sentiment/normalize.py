from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, cast

from ingestion.contracts.normalize import Normalizer
from ingestion.contracts.tick import Domain, IngestionTick, _coerce_epoch_ms, normalize_tick
from ingestion.contracts.market import annotate_payload_market


@dataclass(frozen=True)
class SentimentNormalizer(Normalizer):
    """Normalize raw sentiment records into `IngestionTick`.

    What we can realistically fetch today (your notebook):
      - timestamp: publish time / event time (epoch ms int, or coercible)
      - text: headline / snippet (str)
      - source: publisher/vendor name (str, e.g. 'decrypt')

    Rules:
      - `data_ts` is the event/publish timestamp.
      - `timestamp` is the ingestion arrival timestamp.
        If not provided by the caller, we best-effort read raw['arrival_ts'/'ingest_ts'],
        else fall back to `data_ts`.
      - No scoring here (VADER/FinBERT is downstream feature/model).
    """

    symbol: str
    provider: str | None = None  # e.g. 'news' | 'twitter' (IO-side grouping)
    venue: str = "news"
    asset_class: str = "news"
    currency: str | None = None
    calendar: str | None = None
    session: str | None = None
    timezone_name: str | None = None
    venue: str
    asset_class: str
    currency: str | None
    calendar: str | None
    session: str | None
    timezone_name: str | None

    # Domain is a Literal type; cast keeps pylance happy.
    domain: Domain = cast(Domain, "sentiment")

    def normalize(self, raw: Mapping[str, Any], *, arrival_ts: Any | None = None) -> IngestionTick:
        r: dict[str, Any] = {str(k): v for k, v in raw.items()}

        # --- event-time (publish) ---
        if "timestamp" in r:
            event_ts = _coerce_epoch_ms(r["timestamp"])
        elif "published_at" in r:
            event_ts = _coerce_epoch_ms(r["published_at"])
        elif "ts" in r:
            event_ts = _coerce_epoch_ms(r["ts"])
        else:
            raise ValueError("Sentiment payload missing event timestamp field")

        # --- arrival-time (ingestion) ---
        if arrival_ts is not None:
            ingest_ts = _coerce_epoch_ms(arrival_ts)
        elif "arrival_ts" in r:
            ingest_ts = _coerce_epoch_ms(r["arrival_ts"])
        elif "ingest_ts" in r:
            ingest_ts = _coerce_epoch_ms(r["ingest_ts"])
        else:
            ingest_ts = event_ts

        # --- symbol association ---
        sym = r.get("symbol") or r.get("asset") or r.get("ticker") or self.symbol
        sym = str(sym) if sym is not None else str(self.symbol)

        # --- minimal schema hygiene ---
        # Keep publisher field name stable.
        if "source" in r and "publisher" not in r:
            r["publisher"] = r.get("source")
        if self.provider is not None:
            r.setdefault("provider", self.provider)

        # (Optional) enforce text presence as empty string rather than None
        if "text" in r and r["text"] is None:
            r["text"] = ""

        r = annotate_payload_market(
            r,
            symbol=sym,
            venue=self.venue or self.provider or "unknown",
            asset_class=self.asset_class,
            currency=self.currency,
            event_ts=event_ts,
            calendar=self.calendar,
            session=self.session,
            timezone_name=self.timezone_name,
        )

        return normalize_tick(
            timestamp=ingest_ts,
            data_ts=event_ts,
            domain=self.domain,
            symbol=sym,
            payload=r,
        )
