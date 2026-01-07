

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Protocol, Iterable, cast

from ingestion.contracts.tick import IngestionTick
from ingestion.contracts.source import Raw
from ingestion.contracts.worker import IngestWorker
from ingestion.option_trades.normalize import DeribitOptionTradesNormalizer
from ingestion.option_trades.source import (
    DeribitOptionTradesParquetSource,
    DeribitOptionTradesRESTSource,
)
import ingestion.option_trades.source as option_trades_source
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
        fetch_source: DeribitOptionTradesRESTSource | None = None,
        symbol: str,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.normalizer = normalizer
        self.source = source
        self._fetch_source = fetch_source
        self.symbol = str(symbol)
        self._logger = logger or get_logger(f"ingestion.{_DOMAIN}.{self.__class__.__name__}")
        self._poll_seq = 0
        self._error_logged = False
        self._raw_root: Path = option_trades_source.DATA_ROOT / "raw"
        self._raw_used_paths: set[Path] = set()
        self._raw_write_count = 0
        self._venue = getattr(fetch_source, "exchange", None) or "DERIBIT"

        if poll_interval_ms is not None:
            self.poll_interval_s = float(poll_interval_ms) / 1000.0
        else:
            self.poll_interval_s = float(poll_interval) if poll_interval is not None else None

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
                symbol=self.symbol,
                domain=_DOMAIN,
            )
            return 0
        fetch = getattr(fetch_source, "backfill", None)
        if not callable(fetch):
            log_debug(
                self._logger,
                "ingestion.backfill.no_backfill_method",
                worker=self.__class__.__name__,
                symbol=self.symbol,
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
                    symbol=self.symbol,
                    domain=_DOMAIN,
                    err_type=type(exc).__name__,
                    err=str(exc),
                )
                raise

        count = 0
        for raw in cast(Iterable[Mapping[str, Any]], fetch(start_ts=int(start_ts), end_ts=int(end_ts))):
            try:
                raw_map = dict(raw)
            except Exception:
                continue
            ts_any = raw_map.get("timestamp") or raw_map.get("data_ts") or raw_map.get("event_ts")
            tick = self.normalizer.normalize(raw=raw_map, arrival_ts=ts_any or anchor_ts)
            if int(tick.data_ts) > int(anchor_ts):
                continue
            if int(tick.data_ts) < int(start_ts) or int(tick.data_ts) > int(end_ts):
                continue
            write_counter = [self._raw_write_count]
            venue = getattr(fetch_source, "exchange", None) or self._venue
            option_trades_source._write_raw_snapshot(
                root=self._raw_root,
                venue=venue,
                asset=self.symbol,
                row=raw_map,
                used_paths=self._raw_used_paths,
                write_counter=write_counter,
            )
            self._raw_write_count = write_counter[0]
            _emit_tick(tick)
            count += 1
        return count

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
