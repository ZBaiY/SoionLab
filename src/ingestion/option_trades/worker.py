

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol, Iterable

from ingestion.contracts.tick import IngestionTick
from ingestion.contracts.source import Raw
from ingestion.contracts.worker import IngestWorker
from ingestion.option_trades.normalize import DeribitOptionTradesNormalizer
from ingestion.option_trades.source import (
    DeribitOptionTradesParquetSource,
    DeribitOptionTradesRESTSource,
)
from quant_engine.utils.asyncio import iter_source, source_kind
from quant_engine.utils.logger import get_logger, log_info, log_debug, log_exception

_LOG_SAMPLE_EVERY = 100
_DOMAIN = "option_trades"

def _as_primitive(x: Any) -> str | int | float | bool | None:
    if x is None or isinstance(x, (str, int, float, bool)):
        return x
    try:
        return str(x)
    except Exception:
        return "<unrepr>"

EmitFn = Callable[[IngestionTick], Awaitable[None] | None]


class _FetchLike(Protocol):
    def fetch(self) -> Iterable[Raw] | Awaitable[Iterable[Raw]]:
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
    source: DeribitOptionTradesRESTSource | DeribitOptionTradesParquetSource | _FetchLike

    symbol: str
    poll_interval_s: float | None = None

    def __init__(
        self,
        *,
        normalizer: DeribitOptionTradesNormalizer,
        source: DeribitOptionTradesRESTSource | DeribitOptionTradesParquetSource | _FetchLike,
        symbol: str,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.normalizer = normalizer
        self.source = source
        self.symbol = str(symbol)
        self._logger = logger or get_logger(f"ingestion.{_DOMAIN}.{self.__class__.__name__}")
        self._poll_seq = 0
        self._error_logged = False

        if poll_interval_ms is not None:
            self.poll_interval_s = float(poll_interval_ms) / 1000.0
        else:
            self.poll_interval_s = float(poll_interval) if poll_interval is not None else None

    def _normalize(self, raw: Any) -> IngestionTick:
        # Normalizer is the only place allowed to interpret source schema.
        try:
            return self.normalizer.normalize(raw=raw)
        except Exception as exc:
            self._error_logged = True
            raw_ts = _extract_raw_ts(raw)
            log_exception(
                self._logger,
                "ingestion.normalize_drop",
                worker=self.__class__.__name__,
                symbol=self.symbol,
                domain=_DOMAIN,
                poll_seq=self._poll_seq,
                err_type=type(exc).__name__,
                err=str(exc),
                raw_type=type(raw).__name__,
                raw_ts=_as_primitive(raw_ts),
            )
            raise

    async def _emit(self, emit: EmitFn, tick: IngestionTick) -> None:
        try:
            res = emit(tick)  # type: ignore[misc]
            if inspect.isawaitable(res):
                await res  # type: ignore[func-returns-value]
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._error_logged = True
            log_exception(
                self._logger,
                "ingestion.emit_error",
                worker=self.__class__.__name__,
                symbol=self.symbol,
                domain=_DOMAIN,
                data_ts=int(tick.data_ts),
                timestamp=int(tick.timestamp),
                poll_seq=self._poll_seq,
                err_type=type(exc).__name__,
                err=str(exc),
            )
            raise

    async def run(self, emit: EmitFn) -> None:
        log_info(
            self._logger,
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
            kind = source_kind(src)
            sync_context = {
                "worker": self.__class__.__name__,
                "symbol": self.symbol,
                "domain": _DOMAIN,
            }
            poll_interval_s = self.poll_interval_s if self.poll_interval_s and self.poll_interval_s > 0 else None
            last_fetch = time.monotonic()
            async for raw in iter_source(
                src,
                logger=self._logger,
                context=sync_context,
                poll_interval_s=poll_interval_s if kind == "fetch" else None,
            ):
                now = time.monotonic()
                self._poll_seq += 1
                sample = (self._poll_seq % _LOG_SAMPLE_EVERY) == 0
                if sample:
                    log_debug(
                        self._logger,
                        "ingestion.source_fetch_success",
                        worker=self.__class__.__name__,
                        symbol=self.symbol,
                        domain=_DOMAIN,
                        latency_ms=int((now - last_fetch) * 1000),
                        n_items=1,
                        normalize_ms=None,
                        emit_ms=None,
                        poll_seq=self._poll_seq,
                    )
                last_fetch = time.monotonic()
                norm_start = time.monotonic()
                tick = self._normalize(raw)
                normalize_ms = int((time.monotonic() - norm_start) * 1000)
                emit_start = time.monotonic()
                await self._emit(emit, tick)
                emit_ms = int((time.monotonic() - emit_start) * 1000)
                if sample:
                    log_debug(
                        self._logger,
                        "ingestion.sample_timing",
                        worker=self.__class__.__name__,
                        symbol=self.symbol,
                        domain=_DOMAIN,
                        normalize_ms=normalize_ms,
                        emit_ms=emit_ms,
                        poll_seq=self._poll_seq,
                    )
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            stop_reason = "cancelled"
            raise
        except Exception as exc:
            if not self._error_logged:
                log_exception(
                    self._logger,
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
            log_info(
                self._logger,
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
