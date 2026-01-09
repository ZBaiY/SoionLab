from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any, Iterable, cast

from ingestion.contracts.tick import IngestionTick, _to_interval_ms, _guard_interval_ms, resolve_source_id
from ingestion.contracts.worker import IngestWorker
from ingestion.sentiment.normalize import SentimentNormalizer
from ingestion.sentiment.source import (
    SentimentFileSource,
    SentimentRESTSource,
    SentimentStreamSource,
)
import ingestion.sentiment.source as sentiment_source
from quant_engine.utils.asyncio import iter_source, source_kind
from quant_engine.utils.logger import get_logger, log_info, log_debug, log_exception, log_warn

_LOG_SAMPLE_EVERY = 100
_DOMAIN = "sentiment"

def _as_primitive(x: Any) -> str | int | float | bool | None:
    if x is None or isinstance(x, (str, int, float, bool)):
        return x
    try:
        return str(x)
    except Exception:
        return "<unrepr>"

class SentimentWorker(IngestWorker):
    """Sentiment ingestion worker.

    Responsibilities:
      - raw -> normalize -> emit tick
      - provide arrival_ts (ingestion timestamp) to the normalizer

    Non-responsibilities:
      - scoring (VADER/FinBERT)
      - alignment/backpressure
    """

    def __init__(
        self,
        *,
        normalizer: SentimentNormalizer,
        source: SentimentFileSource | SentimentRESTSource | SentimentStreamSource,
        fetch_source: SentimentRESTSource | None = None,
        interval: str | None = None,
        interval_ms: int | None = None,
        source_id: str | None = None,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._normalizer = normalizer
        self._source = source
        self._fetch_source = fetch_source
        self._interval = interval
        self._logger = logger or get_logger(f"ingestion.{_DOMAIN}.{self.__class__.__name__}")
        self._poll_seq = 0
        self._error_logged = False
        self._raw_root: Path = sentiment_source.DATA_ROOT / "raw" / "sentiment"
        self._provider = str(normalizer.provider or normalizer.symbol)
        self._source_id = resolve_source_id(self._source, override=source_id)
        setattr(self._normalizer, "source_id", self._source_id)

        # Canonical interval (ms) for alignment/semantics.
        if interval_ms is not None:
            self._interval_ms: int | None = int(interval_ms)
        elif interval is not None:
            ms = _to_interval_ms(interval)
            if ms is None:
                raise ValueError(f"Invalid interval format: {interval!r}")
            self._interval_ms = int(ms)
            _guard_interval_ms(interval, self._interval_ms)
        else:
            self._interval_ms = None

        if self._interval_ms is not None and self._interval_ms <= 0:
            raise ValueError("interval_ms must be > 0")

        if poll_interval_ms is not None:
            self._poll_interval_ms = int(poll_interval_ms)
        elif poll_interval is not None:
            self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            self._poll_interval_ms = None
        if self._poll_interval_ms is not None and self._poll_interval_ms <= 0:
            raise ValueError("poll_interval_ms must be > 0")

    def backfill(
        self,
        *,
        start_ts: int,
        end_ts: int,
        anchor_ts: int,
        emit: Callable[[IngestionTick], Awaitable[None] | None] | None = None,
    ) -> int:
        fetch_source = self._fetch_source
        if fetch_source is None:
            log_debug(
                self._logger,
                "ingestion.backfill.no_fetch_source",
                worker=self.__class__.__name__,
                domain=_DOMAIN,
            )
            return 0
        fetch = getattr(fetch_source, "backfill", None)
        if not callable(fetch):
            log_debug(
                self._logger,
                "ingestion.backfill.no_backfill_method",
                worker=self.__class__.__name__,
                domain=_DOMAIN,
                source_type=type(fetch_source).__name__,
            )
            return 0

        def _emit_tick(tick: IngestionTick) -> None:
            if emit is None:
                return
            try:
                res = emit(tick)
                if asyncio.iscoroutine(res) or isinstance(res, asyncio.Future):
                    raise RuntimeError("backfill emit must be synchronous")
            except Exception as exc:
                log_exception(
                    self._logger,
                    "ingestion.backfill.emit_error",
                    worker=self.__class__.__name__,
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
            ts_any = raw_map.get("timestamp") or raw_map.get("published_at") or raw_map.get("ts")
            tick = self._normalizer.normalize(raw=raw_map, arrival_ts=ts_any or anchor_ts)
            if int(tick.data_ts) > int(anchor_ts):
                continue
            if int(tick.data_ts) < int(start_ts) or int(tick.data_ts) > int(end_ts):
                continue
            sentiment_source._write_raw_snapshot(root=self._raw_root, provider=self._provider, row=raw_map)
            _emit_tick(tick)
            count += 1
        return count

    async def run(
        self,
        emit: Callable[[IngestionTick], Awaitable[None] | None],
    ) -> None:
        self._error_logged = False
        stop_reason = "exit"

        async def _emit(tick: IngestionTick) -> None:
            try:
                res = emit(tick)
                if asyncio.iscoroutine(res) or isinstance(res, asyncio.Future):
                    await res
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._error_logged = True
                log_exception(
                    self._logger,
                    "ingestion.emit_error",
                    worker=self.__class__.__name__,
                    domain=_DOMAIN,
                    interval=self._interval,
                    data_ts=int(tick.data_ts),
                    timestamp=int(tick.timestamp),
                    poll_seq=self._poll_seq,
                    err_type=type(exc).__name__,
                    err=str(exc),
                )
                raise

        def _now_ms() -> int:
            return int(time.time() * 1000)

        try:
            kind = source_kind(self._source)
            poll_interval_ms = self._poll_interval_ms
            if kind == "fetch":
                if poll_interval_ms is None:
                    if self._interval_ms is None:
                        raise ValueError("Sentiment fetch source requires poll_interval_ms or interval")
                    poll_interval_ms = int(self._interval_ms)
                elif self._interval_ms is not None and poll_interval_ms != self._interval_ms:
                    log_warn(
                        self._logger,
                        "ingestion.poll_interval_override",
                        worker=self.__class__.__name__,
                        domain=_DOMAIN,
                        interval=self._interval,
                        interval_ms=int(self._interval_ms),
                        poll_interval_ms=int(poll_interval_ms),
                    )
                    poll_interval_ms = int(self._interval_ms)
                self._poll_interval_ms = poll_interval_ms
            else:
                poll_interval_ms = None

            log_info(
                self._logger,
                "ingestion.worker_start",
                worker=self.__class__.__name__,
                source_type=type(self._source).__name__,
                interval=self._interval,
                interval_ms=self._interval_ms,
                poll_interval_ms=poll_interval_ms,
                domain=_DOMAIN,
            )
            sync_context = {
                "worker": self.__class__.__name__,
                "domain": _DOMAIN,
                "interval": self._interval,
            }
            poll_interval_s = (
                float(poll_interval_ms) / 1000.0
                if poll_interval_ms is not None and poll_interval_ms > 0
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
                        domain=_DOMAIN,
                        latency_ms=int((now - last_fetch) * 1000),
                        n_items=1,
                        normalize_ms=None,
                        emit_ms=None,
                        poll_seq=self._poll_seq,
                    )
                last_fetch = time.monotonic()
                norm_start = time.monotonic()
                tick = self._normalize(raw, arrival_ts=_now_ms())
                normalize_ms = int((time.monotonic() - norm_start) * 1000)
                emit_start = time.monotonic()
                await _emit(tick)
                emit_ms = int((time.monotonic() - emit_start) * 1000)
                if sample:
                    log_debug(
                        self._logger,
                        "ingestion.sample_timing",
                        worker=self.__class__.__name__,
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
                    domain=_DOMAIN,
                    interval=self._interval,
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
                domain=_DOMAIN,
                reason=stop_reason,
            )

    def _normalize(self, raw: Any, *, arrival_ts: int) -> IngestionTick:
        try:
            return self._normalizer.normalize(raw=raw, arrival_ts=arrival_ts)
        except Exception as exc:
            self._error_logged = True
            raw_ts = _extract_raw_ts(raw)
            log_exception(
                self._logger,
                "ingestion.normalize_drop",
                worker=self.__class__.__name__,
                domain=_DOMAIN,
                interval=self._interval,
                poll_seq=self._poll_seq,
                err_type=type(exc).__name__,
                err=str(exc),
                raw_type=type(raw).__name__,
                raw_ts=_as_primitive(raw_ts),
            )
            raise


def _extract_raw_ts(raw: Any) -> str | int | float | None:
    if isinstance(raw, dict):
        for key in ("data_ts", "timestamp", "ts", "time", "T", "E"):
            if key in raw:
                return raw.get(key)
    return None
