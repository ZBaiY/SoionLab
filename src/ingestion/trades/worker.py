

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from typing import Any, Callable, Awaitable
from ingestion.contracts.source import Source, AsyncSource

from ingestion.contracts.tick import IngestionTick
from ingestion.contracts.worker import IngestWorker

from ingestion.trades.normalize import BinanceAggTradesNormalizer
from quant_engine.utils.asyncio import iter_source, source_kind
from quant_engine.utils.logger import get_logger, log_info, log_debug, log_exception

_LOG_SAMPLE_EVERY = 100
_DOMAIN = "trades"

def _as_primitive(x: Any) -> str | int | float | bool | None:
    if x is None or isinstance(x, (str, int, float, bool)):
        return x
    try:
        return str(x)
    except Exception:
        return "<unrepr>"

EmitFn = Callable[[IngestionTick], Awaitable[None] | None]


class TradesWorker(IngestWorker):
    """Generic trades ingestion worker.

    Responsibility:
        raw -> normalize -> emit tick

    Non-responsibilities:
        - time alignment / resampling
        - caching
        - backpressure policy (beyond cooperative yielding)
        - IO policy (delegated to `source`)
        - observation interval semantics (poll interval is IO-only)

    Source compatibility:
        - async sources: must implement `__aiter__` yielding raw payloads
        - sync sources : must implement `__iter__` yielding raw payloads

    Emit compatibility:
        - `emit(tick)` may be sync or async; if it returns an awaitable, we await it.
    """

    def __init__(
        self,
        *,
        normalizer: BinanceAggTradesNormalizer,
        source: Source | AsyncSource,
        symbol: str,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._normalizer = normalizer
        self._source = source
        self._symbol = str(symbol)
        self._logger = logger or get_logger(f"ingestion.{_DOMAIN}.{self.__class__.__name__}")
        self._poll_seq = 0
        self._error_logged = False

        if poll_interval_ms is not None:
            self._poll_interval_ms = int(poll_interval_ms)
        elif poll_interval is not None:
            self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            self._poll_interval_ms = 0
        if self._poll_interval_ms < 0:
            raise ValueError("poll_interval_ms must be >= 0")

    async def _emit(self, emit: EmitFn, tick: IngestionTick) -> None:
        try:
            out = emit(tick)
            if inspect.isawaitable(out):
                await out  # type: ignore[func-returns-value]
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
                data_ts=int(tick.data_ts),
                timestamp=int(tick.timestamp),
                poll_seq=self._poll_seq,
                err_type=type(exc).__name__,
                err=str(exc),
            )
            # Worker is intentionally dumb; let driver/logging decide policy.
            raise

    def _normalize(self, raw: Any) -> IngestionTick:
        # Normalizer already enforces ms-int time semantics.
        try:
            if isinstance(raw, dict):
                return self._normalizer.normalize(raw=raw)
            # tolerate Mapping-like / pydantic-like objects
            try:
                return self._normalizer.normalize(raw=dict(raw))  # type: ignore[arg-type]
            except Exception:
                # last resort: pass through
                return self._normalizer.normalize(raw=raw)  # type: ignore[arg-type]
        except Exception as exc:
            self._error_logged = True
            raw_ts = _extract_raw_ts(raw)
            log_exception(
                self._logger,
                "ingestion.normalize_drop",
                worker=self.__class__.__name__,
                symbol=self._symbol,
                domain=_DOMAIN,
                poll_seq=self._poll_seq,
                err_type=type(exc).__name__,
                err=str(exc),
                raw_type=type(raw).__name__,
                raw_ts=_as_primitive(raw_ts),
            )
            raise

    async def run(self, emit: EmitFn) -> None:
        log_info(
            self._logger,
            "ingestion.worker_start",
            worker=self.__class__.__name__,
            source_type=type(self._source).__name__,
            symbol=self._symbol,
            poll_interval_ms=self._poll_interval_ms,
            domain=_DOMAIN,
        )
        self._error_logged = False
        stop_reason = "exit"
        # --- async source ---
        try:
            kind = source_kind(self._source)
            sync_context = {
                "worker": self.__class__.__name__,
                "symbol": self._symbol,
                "domain": _DOMAIN,
            }
            poll_interval_s = (
                float(self._poll_interval_ms) / 1000.0
                if self._poll_interval_ms and self._poll_interval_ms > 0
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
                        worker=self.__class__.__name__,
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
                emit_start = time.monotonic()
                await self._emit(emit, tick)
                emit_ms = int((time.monotonic() - emit_start) * 1000)
                if sample:
                    log_debug(
                        self._logger,
                        "ingestion.sample_timing",
                        worker=self.__class__.__name__,
                        symbol=self._symbol,
                        domain=_DOMAIN,
                        normalize_ms=normalize_ms,
                        emit_ms=emit_ms,
                        poll_seq=self._poll_seq,
                    )
                if kind == "iter" and self._poll_interval_ms > 0:
                    await asyncio.sleep(self._poll_interval_ms / 1000.0)
                else:
                    await asyncio.sleep(0)
            return
        except asyncio.CancelledError:
            stop_reason = "cancelled"
            raise
        except Exception as exc:
            if not self._error_logged:
                log_exception(
                    self._logger,
                    "ingestion.source_fetch_error",
                    worker=self.__class__.__name__,
                    symbol=self._symbol,
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
                symbol=self._symbol,
                domain=_DOMAIN,
                reason=stop_reason,
            )


def _extract_raw_ts(raw: Any) -> str | int | float | None:
    if isinstance(raw, dict):
        for key in ("data_ts", "timestamp", "ts", "time", "T", "E"):
            if key in raw:
                return raw.get(key)
    return None
