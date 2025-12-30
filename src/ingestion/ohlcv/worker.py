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

_LOG_SAMPLE_EVERY = 100
_DOMAIN = "ohlcv"

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
        self._logger = logger or logging.getLogger(f"ingestion.{_DOMAIN}.{self.__class__.__name__}")
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
        _log(
            self._logger,
            logging.INFO,
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
                    await r  # type: ignore[misc]
            except Exception as exc:
                self._error_logged = True
                _log(
                    self._logger,
                    logging.WARNING,
                    "ingestion.emit_error",
                    worker=self.__class__.__name__,
                    symbol=self._symbol,
                    domain=_DOMAIN,
                    poll_seq=self._poll_seq,
                    err_type=type(exc).__name__,
                    err=str(exc),
                )
                raise
                
        try:
            # --- async source ---
            if hasattr(self._source, "__aiter__"):
                last_fetch = time.monotonic()
                async for raw in self._source:  # type: ignore
                    now = time.monotonic()
                    self._poll_seq += 1
                    if self._poll_seq % _LOG_SAMPLE_EVERY == 0:
                        _log(
                            self._logger,
                            logging.INFO,
                            "ingestion.source_fetch_success",
                            worker=type(self).__name__,
                            symbol=self._symbol,
                            domain=_DOMAIN,
                            latency_ms=int((now - last_fetch) * 1000),
                            n_items=1,
                            poll_seq=self._poll_seq,
                        )
                    last_fetch = time.monotonic()
                    tick = self._normalize(raw)
                    if tick is not None:
                        await _emit(tick)
                    # cooperative yield: avoid starving other tasks (e.g., driver loop)
                    await asyncio.sleep(0) 
            # --- sync source (e.g. backtest iterator) ---
            else:
                last_fetch = time.monotonic()
                for raw in self._source:  # type: ignore
                    now = time.monotonic()
                    self._poll_seq += 1
                    if self._poll_seq % _LOG_SAMPLE_EVERY == 0:
                        _log(
                            self._logger,
                            logging.INFO,
                            "ingestion.source_fetch_success",
                            worker=type(self).__name__,
                            symbol=self._symbol,
                            domain=_DOMAIN,
                            latency_ms=int((now - last_fetch) * 1000),
                            n_items=1,
                            poll_seq=self._poll_seq,
                        )
                    last_fetch = time.monotonic()
                    tick = self._normalize(raw)
                    if tick is not None:
                        await _emit(tick)
                    if self._poll_interval_ms is not None:
                        await asyncio.sleep(self._poll_interval_ms / 1000.0)
                    else:
                        # cooperative yield for fast iterators / file replay
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
                    worker=type(self).__name__,
                    symbol=self._symbol,
                    domain=_DOMAIN,
                    err_type=type(exc).__name__,
                    err=str(exc),
                    poll_seq=self._poll_seq,
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
            _log(
                self._logger,
                logging.WARNING,
                "ingestion.normalize_drop",
                worker=type(self).__name__,
                symbol=self._symbol,
                domain=_DOMAIN,
                raw_ts=raw_ts,
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
