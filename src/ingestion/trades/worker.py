

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from typing import Any, Callable, Awaitable

from ingestion.contracts.tick import IngestionTick
from ingestion.contracts.worker import IngestWorker

from ingestion.trades.normalize import BinanceAggTradesNormalizer

_LOG_SAMPLE_EVERY = 100
_DOMAIN = "trades"

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

EmitFn = Callable[[IngestionTick], Any]


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
        source: Any,
        symbol: str,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._normalizer = normalizer
        self._source = source
        self._symbol = str(symbol)
        self._logger = logger or logging.getLogger(f"ingestion.{_DOMAIN}.{self.__class__.__name__}")
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
            # Worker is intentionally dumb; let driver/logging decide policy.
            raise

    def _normalize(self, raw: Any) -> IngestionTick:
        # Normalizer already enforces ms-int time semantics.
        try:
            if isinstance(raw, dict):
                return self._normalizer.normalize(raw)
            # tolerate Mapping-like / pydantic-like objects
            try:
                return self._normalizer.normalize(dict(raw))  # type: ignore[arg-type]
            except Exception:
                # last resort: pass through
                return self._normalizer.normalize(raw)  # type: ignore[arg-type]
        except Exception as exc:
            self._error_logged = True
            raw_ts = _extract_raw_ts(raw)
            _log(
                self._logger,
                logging.WARNING,
                "ingestion.normalize_drop",
                worker=self.__class__.__name__,
                symbol=self._symbol,
                domain=_DOMAIN,
                poll_seq=self._poll_seq,
                err_type=type(exc).__name__,
                err=str(exc),
                raw_type=type(raw).__name__,
                raw_ts=raw_ts,
            )
            raise

    async def run(self, emit: EmitFn) -> None:
        _log(
            self._logger,
            logging.INFO,
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
            if hasattr(self._source, "__aiter__"):
                last_fetch = time.monotonic()
                async for raw in self._source:  # type: ignore
                    now = time.monotonic()
                    self._poll_seq += 1
                    if self._poll_seq % _LOG_SAMPLE_EVERY == 0:
                        _log(
                            self._logger,
                            logging.DEBUG,
                            "ingestion.source_fetch_success",
                            worker=self.__class__.__name__,
                            symbol=self._symbol,
                            domain=_DOMAIN,
                            latency_ms=int((now - last_fetch) * 1000),
                            n_items=1,
                            poll_seq=self._poll_seq,
                        )
                    last_fetch = time.monotonic()
                    tick = self._normalize(raw)
                    await self._emit(emit, tick)
                    # cooperative yield: avoid starving other tasks (e.g., driver loop)
                    await asyncio.sleep(0)
                return

            # --- sync source ---
            if hasattr(self._source, "__iter__"):
                last_fetch = time.monotonic()
                for raw in self._source:  # type: ignore
                    now = time.monotonic()
                    self._poll_seq += 1
                    if self._poll_seq % _LOG_SAMPLE_EVERY == 0:
                        _log(
                            self._logger,
                            logging.DEBUG,
                            "ingestion.source_fetch_success",
                            worker=self.__class__.__name__,
                            symbol=self._symbol,
                            domain=_DOMAIN,
                            latency_ms=int((now - last_fetch) * 1000),
                            n_items=1,
                            poll_seq=self._poll_seq,
                        )
                    last_fetch = time.monotonic()
                    tick = self._normalize(raw)
                    await self._emit(emit, tick)
                    if self._poll_interval_ms > 0:
                        await asyncio.sleep(self._poll_interval_ms / 1000.0)
                    else:
                        await asyncio.sleep(0)
                return

            raise TypeError(
                "TradesWorker source must be an async-iterable or iterable; "
                f"got {type(self._source)!r}"
            )
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
            _log(
                self._logger,
                logging.INFO,
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
