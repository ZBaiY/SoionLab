from __future__ import annotations

import asyncio
import inspect
import logging
import time
from typing import Callable, Awaitable, Any

from ingestion.contracts.tick import IngestionTick, _to_interval_ms, _guard_interval_ms
from ingestion.contracts.worker import IngestWorker
from ingestion.ohlcv.normalize import BinanceOHLCVNormalizer
from ingestion.ohlcv.source import OHLCVFileSource, OHLCVRESTSource, OHLCVWebSocketSource
from quant_engine.utils.asyncio import iter_source, source_kind
from quant_engine.utils.logger import get_logger, log_info, log_debug, log_exception

_LOG_SAMPLE_EVERY = 100
_DOMAIN = "ohlcv"

def _as_primitive(x: Any) -> str | int | float | bool | None:
    if x is None or isinstance(x, (str, int, float, bool)):
        return x
    try:
        return str(x)
    except Exception:
        return "<unrepr>"

class OHLCVWorker(IngestWorker):
    """
    Generic OHLCV ingestion worker.
    The only responsibility is:
        raw -> normalize -> emit tick
    Time, alignment, caching, and backpressure are NOT handled here.
    """

    def __init__(
        self,
        *,
        normalizer: BinanceOHLCVNormalizer,
        source: OHLCVFileSource | OHLCVRESTSource | OHLCVWebSocketSource,
        symbol: str,
        interval: str | None = None,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        logger: logging.Logger | None = None,
    ):
        self._normalizer = normalizer
        self._source = source
        self._symbol = symbol
        self._interval = interval
        self._logger = logger or get_logger(f"ingestion.{_DOMAIN}.{self.__class__.__name__}")
        self._poll_seq = 0
        self._error_logged = False
        # Semantic bar interval length (ms-int). Used for metadata / validation.
        self.interval_ms: int | None = None
        if self._interval is not None:
            ms = _to_interval_ms(self._interval)
            if ms is None:
                raise ValueError(f"Invalid interval format: {self._interval!r}")
            self.interval_ms = int(ms)
            _guard_interval_ms(self._interval, self.interval_ms)
        # Worker-level pacing for *sync* sources (file replay / REST wrappers).
        # Internal convention: ms-int.
        if poll_interval_ms is not None:
            self._poll_interval_ms: int | None = int(poll_interval_ms)
        elif poll_interval is not None:
            self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        elif self.interval_ms is not None:
            # Default pacing for sync sources: 1 bar per interval.
            self._poll_interval_ms = int(self.interval_ms)
        else:
            self._poll_interval_ms = None

        if self._poll_interval_ms is not None and self._poll_interval_ms <= 0:
            raise ValueError(f"poll interval must be > 0ms, got {self._poll_interval_ms}")

    async def run(self, emit: Callable[[IngestionTick], Awaitable[None] | None]) -> None:
        log_info(
            self._logger,
            "ingestion.worker_start",
            worker=self.__class__.__name__,
            source_type=type(self._source).__name__,
            symbol=self._symbol,
            interval=self._interval,
            poll_interval_ms=self._poll_interval_ms,
            domain=_DOMAIN,
        )
        self._error_logged = False
        stop_reason = "exit"

        async def _emit(tick: IngestionTick) -> None:
            try:
                r = emit(tick)
                if inspect.isawaitable(r):
                    await r  
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._error_logged = True
                log_exception(
                    self._logger,
                    "ingestion.emit_error",
                    worker=self.__class__.__name__,
                    symbol=self._symbol,
                    domain=_DOMAIN,
                    interval=self._interval,
                    data_ts=int(tick.data_ts),
                    timestamp=int(tick.timestamp),
                    poll_seq=self._poll_seq,
                    err_type=type(exc).__name__,
                    err=str(exc),
                )
                raise
                
        try:
            kind = source_kind(self._source)
            sync_context = {
                "worker": self.__class__.__name__,
                "symbol": self._symbol,
                "domain": _DOMAIN,
                "interval": self._interval,
            }
            poll_interval_s = (
                float(self._poll_interval_ms) / 1000.0
                if self._poll_interval_ms is not None and self._poll_interval_ms > 0
                else None
            )
            last_fetch = time.monotonic()
            async for raw in iter_source(
                self._source,
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
                        worker=type(self).__name__,
                        symbol=self._symbol,
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
                emit_ms = None
                if tick is not None:
                    emit_start = time.monotonic()
                    await _emit(tick)
                    emit_ms = int((time.monotonic() - emit_start) * 1000)
                if sample:
                    log_debug(
                        self._logger,
                        "ingestion.sample_timing",
                        worker=type(self).__name__,
                        symbol=self._symbol,
                        domain=_DOMAIN,
                        normalize_ms=normalize_ms,
                        emit_ms=emit_ms,
                        poll_seq=self._poll_seq,
                    )
                if kind == "iter" and self._poll_interval_ms is not None:
                    await asyncio.sleep(self._poll_interval_ms / 1000.0)
                else:
                    # cooperative yield for fast iterators / file replay
                    await asyncio.sleep(0)
        except asyncio.CancelledError:
            stop_reason = "cancelled"
            raise
        except Exception as exc:
            if not self._error_logged:
                log_exception(
                    self._logger,
                    "ingestion.source_fetch_error",
                    worker=type(self).__name__,
                    symbol=self._symbol,
                    domain=_DOMAIN,
                    interval=self._interval,
                    err_type=type(exc).__name__,
                    err=str(exc),
                    poll_seq=self._poll_seq,
                    retry_count=0,
                    backoff_ms=0,
                )
            stop_reason = "error"
            raise
        finally:
            log_info(
                self._logger,
                "ingestion.worker_stop",
                worker=type(self).__name__,
                symbol=self._symbol,
                domain=_DOMAIN,
                stop_reason=stop_reason,
                poll_seq=self._poll_seq,
            )

    def _normalize(self, raw: dict) -> IngestionTick | None:
        # symbol/domain knowledge lives in normalizer
        try:
            return self._normalizer.normalize(
                raw=raw,
            )
        except ValueError as exc:
            self._error_logged = True
            raw_ts = _extract_raw_ts(raw)
            log_exception(
                self._logger,
                "ingestion.normalize_drop",
                worker=type(self).__name__,
                symbol=self._symbol,
                domain=_DOMAIN,
                interval=self._interval,
                raw_ts=_as_primitive(raw_ts),
                raw_type=type(raw).__name__,
                err_type=type(exc).__name__,
                err=str(exc),
                poll_seq=self._poll_seq,
            )
            return None


def _extract_raw_ts(raw: Any) -> str | int | float | None:
    if isinstance(raw, dict):
        for key in ("data_ts", "timestamp", "ts", "T", "E", "open_time", "close_time"):
            if key in raw:
                return raw.get(key)
    return None
