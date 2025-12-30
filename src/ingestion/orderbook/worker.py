from __future__ import annotations

import asyncio
import inspect
import logging
import time
from typing import Callable, Awaitable, Any

from ingestion.contracts.tick import IngestionTick, _to_interval_ms, _guard_interval_ms
from ingestion.contracts.worker import IngestWorker
from ingestion.orderbook.normalize import BinanceOrderbookNormalizer
from ingestion.orderbook.source import OrderbookFileSource, OrderbookRESTSource, OrderbookWebSocketSource

_LOG_SAMPLE_EVERY = 100
_DOMAIN = "orderbook"

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

class OrderbookWorker(IngestWorker):
    """
    Orderbook ingestion worker.
    This worker is source-agnostic and supports:
        - WebSocket streaming (AsyncSource)
        - REST polling / replay (Source)

    Responsibilities:
        raw -> normalize -> emit tick
    """

    def __init__(
        self,
        *,
        normalizer: BinanceOrderbookNormalizer,
        source: OrderbookFileSource | OrderbookRESTSource | OrderbookWebSocketSource,
        symbol: str,
        interval: str | None = None,
        interval_ms: int | None = None,
        poll_interval: float | None = None,
        logger: logging.Logger | None = None,
    ):
        self._normalizer = normalizer
        self._source = source
        self._symbol = symbol
        self._interval = interval
        self._logger = logger or logging.getLogger(f"ingestion.{_DOMAIN}.{self.__class__.__name__}")
        self._poll_seq = 0
        self._error_logged = False
        if interval_ms is not None:
            self._interval_ms = interval_ms
        elif interval is not None:
            self._interval_ms = _to_interval_ms(interval)
            if self._interval_ms is None:
                raise ValueError(f"Invalid interval format: {interval!r}")
            _guard_interval_ms(interval, self._interval_ms)
        elif poll_interval is not None:
            self._interval_ms = int(round(poll_interval * 1000))
        else:
            self._interval_ms = None

    async def run(self, emit: Callable[[IngestionTick], Awaitable[None] | None]) -> None:
        _log(
            self._logger,
            logging.INFO,
            "ingestion.worker_start",
            worker=self.__class__.__name__,
            source_type=type(self._source).__name__,
            symbol=self._symbol,
            interval=self._interval,
            poll_interval_ms=self._interval_ms,
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
            # --- async source (e.g. WebSocket depth stream) ---
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
                    await _emit(tick)
                    await asyncio.sleep(0)

            # --- sync source (e.g. REST snapshot / backtest replay) ---
            else:
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
                    await _emit(tick)
                    if self._interval_ms is not None:
                        await asyncio.sleep(self._interval_ms / 1000.0)
                    else:
                        # cooperative yield to avoid starving other asyncio tasks
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

    def _normalize(self, raw: dict) -> IngestionTick:
        try:
            return self._normalizer.normalize(raw=raw)
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


def _extract_raw_ts(raw: Any) -> str | int | float | None:
    if isinstance(raw, dict):
        for key in ("data_ts", "timestamp", "ts", "time", "T", "E"):
            if key in raw:
                return raw.get(key)
    return None
