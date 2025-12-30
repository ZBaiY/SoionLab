

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

from ingestion.contracts.tick import IngestionTick
from ingestion.contracts.worker import IngestWorker
from ingestion.option_trades.normalize import DeribitOptionTradesNormalizer
from ingestion.option_trades.source import (
    DeribitOptionTradesParquetSource,
    DeribitOptionTradesRESTSource,
)

_LOG_SAMPLE_EVERY = 100
_DOMAIN = "option_trades"

def _log(logger: logging.Logger, level: int, event: str, **ctx: Any) -> None:
    # Best-effort JSON safety without importing quant_engine.safe_jsonable
    safe: dict[str, Any] = {}
    for k, v in ctx.items():
        try:
            key = str(k)
        except Exception:
            key = repr(k)
        if v is None or isinstance(v, (str, int, float, bool)):
            safe[key] = v
        else:
            try:
                safe[key] = repr(v)
            except Exception:
                safe[key] = "<unrepr>"
    logger.log(level, event, extra={"context": safe})

EmitFn = Callable[[IngestionTick], None] | Callable[[IngestionTick], Awaitable[None]]


class _FetchLike(Protocol):
    def fetch(self) -> Any:  # may return iterable, or awaitable of iterable
        ...


@dataclass
class OptionTradesWorker(IngestWorker):
    """Option trades ingestion worker.

    Responsibility: raw -> normalize -> emit tick

    Notes:
      - Source may be:
          * (async) iterator yielding raw dict-like objects
          * an object exposing `fetch()` returning (or awaiting) an iterable of raw dict-like objects
      - This worker is IO-agnostic; polling cadence is optional and only used for `fetch()` sources.
      - poll_interval is engineering-only and must not affect runtime observation semantics.
    """

    normalizer: DeribitOptionTradesNormalizer
    source: DeribitOptionTradesRESTSource | DeribitOptionTradesParquetSource | _FetchLike | Any

    symbol: str
    poll_interval_s: float | None = None

    def __init__(
        self,
        *,
        normalizer: DeribitOptionTradesNormalizer,
        source: DeribitOptionTradesRESTSource | DeribitOptionTradesParquetSource | Any,
        symbol: str,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.normalizer = normalizer
        self.source = source
        self.symbol = str(symbol)
        self._logger = logger or logging.getLogger(f"ingestion.{_DOMAIN}.{self.__class__.__name__}")
        self._poll_seq = 0
        self._error_logged = False

        if poll_interval_ms is not None:
            self.poll_interval_s = float(poll_interval_ms) / 1000.0
        else:
            self.poll_interval_s = float(poll_interval) if poll_interval is not None else None

    def _normalize(self, raw: Any) -> IngestionTick:
        # Normalizer is the only place allowed to interpret source schema.
        try:
            return self.normalizer.normalize(raw)
        except Exception as exc:
            self._error_logged = True
            raw_ts = _extract_raw_ts(raw)
            _log(
                self._logger,
                logging.WARNING,
                "ingestion.normalize_drop",
                worker=self.__class__.__name__,
                symbol=self.symbol,
                domain=_DOMAIN,
                poll_seq=self._poll_seq,
                err_type=type(exc).__name__,
                err=str(exc),
                raw_type=type(raw).__name__,
                raw_ts=raw_ts,
            )
            raise

    async def _emit(self, emit: EmitFn, tick: IngestionTick) -> None:
        try:
            res = emit(tick)  # type: ignore[misc]
            if inspect.isawaitable(res):
                await res  # type: ignore[func-returns-value]
        except Exception as exc:
            self._error_logged = True
            _log(
                self._logger,
                logging.WARNING,
                "ingestion.emit_error",
                worker=self.__class__.__name__,
                symbol=self.symbol,
                domain=_DOMAIN,
                poll_seq=self._poll_seq,
                err_type=type(exc).__name__,
                err=str(exc),
            )
            raise

    async def run(self, emit: EmitFn) -> None:
        _log(
            self._logger,
            logging.INFO,
            "ingestion.worker_start",
            worker=self.__class__.__name__,
            source_type=type(self.source).__name__,
            symbol=self.symbol,
            poll_interval_ms=int(self.poll_interval_s * 1000) if self.poll_interval_s is not None else None,
            domain=_DOMAIN,
        )
        self._error_logged = False
        stop_reason = "exit"
        src = self.source

        try:
            # --- async iterator source ---
            if hasattr(src, "__aiter__"):
                last_fetch = time.monotonic()
                async for raw in src:  # type: ignore[assignment]
                    now = time.monotonic()
                    self._poll_seq += 1
                    if self._poll_seq % _LOG_SAMPLE_EVERY == 0:
                        _log(
                            self._logger,
                            logging.DEBUG,
                            "ingestion.source_fetch_success",
                            worker=self.__class__.__name__,
                            symbol=self.symbol,
                            domain=_DOMAIN,
                            latency_ms=int((now - last_fetch) * 1000),
                            n_items=1,
                            poll_seq=self._poll_seq,
                        )
                    last_fetch = time.monotonic()
                    tick = self._normalize(raw)
                    await self._emit(emit, tick)
                    # cooperative yield
                    await asyncio.sleep(0)
                return

            # --- fetch()-style source (optionally polled) ---
            if hasattr(src, "fetch") and callable(getattr(src, "fetch")):
                while True:
                    t0 = time.monotonic()
                    try:
                        batch = src.fetch()  # type: ignore[attr-defined]
                        if inspect.isawaitable(batch):
                            batch = await batch
                    except Exception as exc:
                        _log(
                            self._logger,
                            logging.WARNING,
                            "ingestion.source_fetch_error",
                            worker=self.__class__.__name__,
                            symbol=self.symbol,
                            domain=_DOMAIN,
                            poll_seq=self._poll_seq,
                            err_type=type(exc).__name__,
                            err=str(exc),
                            retry_count=0,
                            backoff_ms=0,
                        )
                        self._error_logged = True
                        stop_reason = "error"
                        raise

                    rows = len(batch or [])
                    self._poll_seq += 1
                    if self._poll_seq % _LOG_SAMPLE_EVERY == 0:
                        _log(
                            self._logger,
                            logging.DEBUG,
                            "ingestion.source_fetch_success",
                            worker=self.__class__.__name__,
                            symbol=self.symbol,
                            domain=_DOMAIN,
                            latency_ms=int((time.monotonic() - t0) * 1000),
                            n_items=rows,
                            poll_seq=self._poll_seq,
                        )

                    for raw in (batch or []):
                        tick = self._normalize(raw)
                        await self._emit(emit, tick)

                    if self.poll_interval_s is None:
                        # one-shot fetch
                        return

                    # if the fetch returned nothing, still sleep (avoid hot loop)
                    await asyncio.sleep(max(0.0, float(self.poll_interval_s)))
                
            # --- sync iterator source ---
            last_fetch = time.monotonic()
            for raw in src:  # type: ignore[assignment]
                now = time.monotonic()
                self._poll_seq += 1
                if self._poll_seq % _LOG_SAMPLE_EVERY == 0:
                    _log(
                        self._logger,
                        logging.DEBUG,
                        "ingestion.source_fetch_success",
                        worker=self.__class__.__name__,
                        symbol=self.symbol,
                        domain=_DOMAIN,
                        latency_ms=int((now - last_fetch) * 1000),
                        n_items=1,
                        poll_seq=self._poll_seq,
                    )
                last_fetch = time.monotonic()
                tick = self._normalize(raw)
                await self._emit(emit, tick)
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            stop_reason = "cancelled"
            raise
        except Exception as exc:
            if not self._error_logged:
                _log(
                    self._logger,
                    logging.WARNING,
                    "ingestion.source_fetch_error",
                    worker=self.__class__.__name__,
                    symbol=self.symbol,
                    domain=_DOMAIN,
                    poll_seq=self._poll_seq,
                    err_type=type(exc).__name__,
                    err=str(exc),
                    retry_count=0,
                    backoff_ms=0,
                )
            stop_reason = "error"
            raise
        finally:
            _log(
                self._logger,
                logging.INFO,
                "ingestion.worker_stop",
                worker=self.__class__.__name__,
                symbol=self.symbol,
                domain=_DOMAIN,
                reason=stop_reason,
            )


def _extract_raw_ts(raw: Any) -> str | int | float | None:
    if isinstance(raw, dict):
        for key in ("data_ts", "timestamp", "ts", "time", "T", "E"):
            if key in raw:
                return raw.get(key)
    return None
