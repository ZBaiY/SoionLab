from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
from pathlib import Path
from typing import Callable, Awaitable, Any, Iterable, Mapping, cast

from ingestion.contracts.tick import IngestionTick, _to_interval_ms, _guard_interval_ms, resolve_source_id
from ingestion.contracts.worker import IngestWorker
from ingestion.ohlcv.normalize import BinanceOHLCVNormalizer
from ingestion.ohlcv.source import OHLCVFileSource, OHLCVRESTSource, OHLCVWebSocketSource
import ingestion.ohlcv.source as ohlcv_source
from quant_engine.utils.asyncio import iter_source, source_kind
from quant_engine.utils.logger import get_logger, log_info, log_debug, log_exception, log_warn
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
        fetch_source: OHLCVRESTSource | None = None,
        symbol: str,
        interval: str | None = None,
        interval_ms: int | None = None,
        source_id: str | None = None,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        logger: logging.Logger | None = None,
    ):
        """Configure OHLCV ingestion plumbing.

        Args:
            normalizer: Raw-to-`IngestionTick` converter for OHLCV payloads.
            source: Primary runtime source (file/replay, REST polling, or websocket stream).
            fetch_source: Optional REST source used only for bounded backfill windows.
            symbol: Canonical instrument identifier used in emitted ticks and raw persistence paths.
            interval: Semantic bar interval label (`1m`, `5m`, ...) used for validation/metadata.
            interval_ms: Optional explicit interval in ms; overrides `interval` parsing when supplied.
            source_id: Stable source identity override for downstream provenance checks.
            poll_interval: Poll cadence in seconds for fetch sources only (IO cadence, not bar semantics).
            poll_interval_ms: Poll cadence in milliseconds; takes precedence over `poll_interval`.
            logger: Structured logger override.
        """
        self._normalizer = normalizer
        self._source = source
        self._fetch_source = fetch_source
        self._symbol = symbol
        self._interval = interval
        self._logger = logger or get_logger(f"ingestion.{_DOMAIN}.{self.__class__.__name__}")
        self._poll_seq = 0
        self._error_logged = False
        self._raw_root: Path = ohlcv_source.DATA_ROOT / "raw" / "ohlcv"
        self._raw_used_paths: set[Path] = set()
        self._raw_write_count = 0
        self._raw_write_dispatcher: ohlcv_source._ProcessWriteDispatcher | None = None
        self._source_id = resolve_source_id(self._source, override=source_id)
        setattr(self._normalizer, "source_id", self._source_id)
        # Semantic bar interval length (ms-int). Used for metadata / validation.
        self.interval_ms: int | None = None
        if interval_ms is not None:
            self.interval_ms = int(interval_ms)
        elif self._interval is not None:
            ms = _to_interval_ms(self._interval)
            if ms is None:
                raise ValueError(f"Invalid interval format: {self._interval!r}")
            self.interval_ms = int(ms)
            _guard_interval_ms(self._interval, self.interval_ms)
        # REST-only poll cadence (ms-int). Interval remains canonical.
        if poll_interval_ms is not None:
            self._poll_interval_ms: int | None = int(poll_interval_ms)
        elif poll_interval is not None:
            self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            self._poll_interval_ms = None

        if self._poll_interval_ms is not None and self._poll_interval_ms <= 0:
            raise ValueError(f"poll interval must be > 0ms, got {self._poll_interval_ms}")
        use_writer_process = str(os.getenv("OHLCV_WRITE_PROCESS", "0")).strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
            "on",
        }
        if use_writer_process:
            queue_size = int(str(os.getenv("OHLCV_WRITE_QUEUE_SIZE", str(ohlcv_source._WRITER_QUEUE_SIZE))).strip() or str(ohlcv_source._WRITER_QUEUE_SIZE))
            put_timeout = float(str(os.getenv("OHLCV_WRITE_PUT_TIMEOUT_S", str(ohlcv_source._WRITER_QUEUE_PUT_TIMEOUT_S))).strip() or str(ohlcv_source._WRITER_QUEUE_PUT_TIMEOUT_S))
            self._raw_write_dispatcher = ohlcv_source._ProcessWriteDispatcher(
                root=self._raw_root,
                queue_size=queue_size,
                put_timeout_s=put_timeout,
            )
            log_info(
                self._logger,
                "ingestion.writer_process_enabled",
                component="ohlcv",
                symbol=self._symbol,
                interval=self._interval,
                queue_size=queue_size,
            )

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
                symbol=self._symbol,
                domain=_DOMAIN,
            )
            return 0
        if self._interval is None:
            log_debug(
                self._logger,
                "ingestion.backfill.no_interval",
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
        for raw in cast(Iterable[Mapping[str, Any]], fetch(start_ts=int(start_ts), end_ts=int(end_ts))):
            try:
                raw_map = dict(raw)
            except Exception as exc:
                log_debug(
                    self._logger,
                    "ingestion.backfill.raw_convert_error",
                    worker=self.__class__.__name__,
                    symbol=self._symbol,
                    domain=_DOMAIN,
                    err_type=type(exc).__name__,
                    err=str(exc),
                )
                continue
            ts_any = raw_map.get("data_ts") or raw_map.get("close_time")
            if ts_any is None and "open_time" in raw_map and self.interval_ms is not None:
                ts_any = int(raw_map.get("open_time", 0)) + int(self.interval_ms)
            if "E" not in raw_map and ts_any is not None:
                raw_map["E"] = ts_any
            tick = self._normalize(raw_map)
            if tick is None:
                continue
            if int(tick.data_ts) > int(anchor_ts):
                continue
            if int(tick.data_ts) < int(start_ts) or int(tick.data_ts) > int(end_ts):
                continue
            # Process-boundary handoff: enqueue a dedicated object and avoid mutating it afterward.
            bar_for_write = dict(raw_map)
            # Keep "E" available for tick normalization, but never persist it into OHLCV raw parquet.
            bar_for_write.pop("E", None)
            write_counter = [self._raw_write_count]
            ohlcv_source._write_raw_snapshot(
                root=self._raw_root,
                symbol=self._symbol,
                interval=self._interval,
                bar=bar_for_write,
                used_paths=self._raw_used_paths,
                write_counter=write_counter,
                dispatcher=self._raw_write_dispatcher,
            )
            self._raw_write_count = write_counter[0]
            _emit_tick(tick)
            count += 1
        return count

    async def run(self, emit: Callable[[IngestionTick], Awaitable[None] | None]) -> None:
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
                if type(exc).__name__ == "_StopReplay":
                    log_debug(
                        self._logger,
                        "ingestion.replay.stopped",
                        worker=self.__class__.__name__,
                        symbol=self._symbol,
                        domain=_DOMAIN,
                        reason="stop_replay",
                    )
                    raise  # let outer except handle uniformly
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
            poll_interval_ms = self._poll_interval_ms
            if kind == "fetch":
                if poll_interval_ms is None:
                    if self.interval_ms is None:
                        raise ValueError(
                            f"OHLCV fetch source requires poll_interval_ms or interval; symbol={self._symbol}"
                        )
                    poll_interval_ms = int(self.interval_ms)
                self._poll_interval_ms = poll_interval_ms
            else:
                poll_interval_ms = None

            log_info(
                self._logger,
                "ingestion.worker_start",
                worker=self.__class__.__name__,
                source_type=type(self._source).__name__,
                symbol=self._symbol,
                interval=self._interval,
                interval_ms=self.interval_ms,
                poll_interval_ms=poll_interval_ms,
                domain=_DOMAIN,
            )
            sync_context = {
                "worker": self.__class__.__name__,
                "symbol": self._symbol,
                "domain": _DOMAIN,
                "interval": self._interval,
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
                if tick is not None:
                    await _emit(tick)
                # cooperative yield for fast iterators / file replay
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            stop_reason = "cancelled"
            raise
        except Exception as exc:
            if type(exc).__name__ == "_StopReplay":
                stop_reason = "replay_done"
                return
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
                )
            stop_reason = "error"
            raise
        finally:
            if self._raw_write_dispatcher is not None:
                self._raw_write_dispatcher.close()
                self._raw_write_dispatcher = None
            ohlcv_source._close_used_paths(self._raw_used_paths)
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
