

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
from pathlib import Path
from typing import Any, Callable, Awaitable, Iterable, Mapping, cast
from ingestion.contracts.source import Source, AsyncSource

from ingestion.contracts.tick import IngestionTick, resolve_source_id
from ingestion.contracts.worker import IngestWorker

from ingestion.trades.normalize import BinanceAggTradesNormalizer
import ingestion.trades.source as trades_source
from quant_engine.utils.asyncio import iter_source, source_kind
from quant_engine.utils.logger import get_logger, log_info, log_debug, log_exception, log_warn

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
        fetch_source: Source | None = None,
        symbol: str,
        source_id: str | None = None,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        """Configure trades ingestion plumbing.

        Args:
            normalizer: Raw trades payload normalizer.
            source: Primary runtime source (sync iterator or async stream).
            fetch_source: Optional sync source used for bounded backfill windows.
            symbol: Canonical symbol used in emitted ticks and raw persistence paths.
            source_id: Stable source identity override for downstream lineage checks.
            poll_interval: Poll cadence in seconds for fetch sources only (IO cadence).
            poll_interval_ms: Poll cadence in milliseconds; takes precedence over `poll_interval`.
            logger: Structured logger override.
        """
        self._normalizer = normalizer
        self._source = source
        self._fetch_source = fetch_source
        self._symbol = str(symbol)
        self._logger = logger or get_logger(f"ingestion.{_DOMAIN}.{self.__class__.__name__}")
        self._poll_seq = 0
        self._error_logged = False
        self._raw_root: Path = trades_source.DATA_ROOT / "raw" / "trades"
        self._raw_used_paths: set[Path] = set()
        self._raw_write_count = 0
        self._raw_write_dispatcher: trades_source._ProcessWriteDispatcher | None = None
        self._source_id = resolve_source_id(self._source, override=source_id)
        setattr(self._normalizer, "source_id", self._source_id)

        if poll_interval_ms is not None:
            self._poll_interval_ms = int(poll_interval_ms)
        elif poll_interval is not None:
            self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            self._poll_interval_ms = None
        if self._poll_interval_ms is not None and self._poll_interval_ms <= 0:
            raise ValueError("poll_interval_ms must be > 0")
        use_writer_process = str(os.getenv("TRADES_WRITE_PROCESS", "0")).strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
            "on",
        }
        if use_writer_process:
            queue_size = int(str(os.getenv("TRADES_WRITE_QUEUE_SIZE", str(trades_source._WRITER_QUEUE_SIZE))).strip() or str(trades_source._WRITER_QUEUE_SIZE))
            put_timeout = float(str(os.getenv("TRADES_WRITE_PUT_TIMEOUT_S", str(trades_source._WRITER_QUEUE_PUT_TIMEOUT_S))).strip() or str(trades_source._WRITER_QUEUE_PUT_TIMEOUT_S))
            self._raw_write_dispatcher = trades_source._ProcessWriteDispatcher(
                root=self._raw_root,
                symbol=self._symbol,
                queue_size=queue_size,
                put_timeout_s=put_timeout,
            )
            log_info(
                self._logger,
                "ingestion.writer_process_enabled",
                component="trades",
                symbol=self._symbol,
                queue_size=queue_size,
            )

    def backfill(
        self,
        *,
        start_ts: int,
        end_ts: int,
        anchor_ts: int,
        emit: EmitFn | None = None,
    ) -> int:
        fetch_source = self._fetch_source
        if fetch_source is None:
            log_debug(
                self._logger,
                "ingestion.backfill.no_fetch_source",
                worker=self.__class__.__name__,
                symbol=self._symbol,
                domain=_DOMAIN,
            )
            return 0
        fetch = getattr(fetch_source, "backfill", None)
        if not callable(fetch):
            log_debug(
                self._logger,
                "ingestion.backfill.no_backfill_method",
                worker=self.__class__.__name__,
                symbol=self._symbol,
                domain=_DOMAIN,
                source_type=type(fetch_source).__name__,
            )
            return 0

        def _emit_tick(tick: IngestionTick) -> None:
            if emit is None:
                return
            try:
                res = emit(tick)
                if inspect.isawaitable(res):
                    raise RuntimeError("backfill emit must be synchronous")
            except Exception as exc:
                log_exception(
                    self._logger,
                    "ingestion.backfill.emit_error",
                    worker=self.__class__.__name__,
                    symbol=self._symbol,
                    domain=_DOMAIN,
                    err_type=type(exc).__name__,
                    err=str(exc),
                )
                raise

        count = 0
        raw_convert_errors = 0
        for raw in cast(Iterable[Mapping[str, Any]], fetch(start_ts=int(start_ts), end_ts=int(end_ts))):
            try:
                raw_map = dict(raw)
            except Exception as exc:
                raw_convert_errors += 1
                if raw_convert_errors <= 3 or raw_convert_errors % 100 == 0:
                    log_warn(
                        self._logger,
                        "ingestion.backfill.raw_convert_error",
                        worker=self.__class__.__name__,
                        symbol=self._symbol,
                        domain=_DOMAIN,
                        err_type=type(exc).__name__,
                        err=str(exc),
                        error_count=raw_convert_errors,
                    )
                continue
            ts_any = raw_map.get("data_ts") or raw_map.get("timestamp") or raw_map.get("T")
            if "E" not in raw_map and ts_any is not None:
                raw_map["E"] = ts_any
            tick = self._normalize(raw_map)
            if int(tick.data_ts) > int(anchor_ts):
                continue
            if int(tick.data_ts) < int(start_ts) or int(tick.data_ts) > int(end_ts):
                continue
            # Process-boundary handoff: enqueue a dedicated object and avoid mutating it afterward.
            row_for_write = dict(raw_map)
            write_counter = [self._raw_write_count]
            trades_source._write_raw_snapshot(
                root=self._raw_root,
                symbol=self._symbol,
                row=row_for_write,
                used_paths=self._raw_used_paths,
                write_counter=write_counter,
                dispatcher=self._raw_write_dispatcher,
            )
            self._raw_write_count = write_counter[0]
            _emit_tick(tick)
            count += 1
        return count

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
        self._error_logged = False
        stop_reason = "exit"
        # --- async source ---
        try:
            kind = source_kind(self._source)
            poll_interval_ms = self._poll_interval_ms
            if kind == "fetch":
                if poll_interval_ms is None:
                    raise ValueError(f"Trades fetch source requires poll_interval_ms; symbol={self._symbol}")
            else:
                poll_interval_ms = None

            log_info(
                self._logger,
                "ingestion.worker_start",
                worker=self.__class__.__name__,
                source_type=type(self._source).__name__,
                symbol=self._symbol,
                poll_interval_ms=poll_interval_ms,
                domain=_DOMAIN,
            )
            sync_context = {
                "worker": self.__class__.__name__,
                "symbol": self._symbol,
                "domain": _DOMAIN,
            }
            poll_interval_s = (
                float(poll_interval_ms) / 1000.0
                if poll_interval_ms is not None and poll_interval_ms > 0
                else None
            )
            async for raw in iter_source(
                self._source,
                logger=self._logger,
                context=sync_context,
                poll_interval_s=poll_interval_s if kind == "fetch" else None,
            ):
                self._poll_seq += 1
                tick = self._normalize(raw)
                await self._emit(emit, tick)
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
            if self._raw_write_dispatcher is not None:
                self._raw_write_dispatcher.close()
                self._raw_write_dispatcher = None
            trades_source._close_used_paths(self._raw_used_paths)
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
