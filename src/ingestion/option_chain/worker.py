from __future__ import annotations

import asyncio
import inspect
import logging
import time
from pathlib import Path
from typing import Callable, Awaitable, Any, Iterable, Mapping, cast

from ingestion.contracts.tick import IngestionTick
from ingestion.contracts.worker import IngestWorker
from ingestion.option_chain.normalize import DeribitOptionChainNormalizer
from ingestion.option_chain.source import (
    DeribitOptionChainRESTSource,
    OptionChainFileSource,
    OptionChainStreamSource,
)
import ingestion.option_chain.source as option_chain_source
from quant_engine.utils.asyncio import iter_source, source_kind
from quant_engine.utils.logger import get_logger, log_info, log_debug, log_exception

_LOG_SAMPLE_EVERY = 100
_DOMAIN = "option_chain"

def _as_primitive(x: Any) -> str | int | float | bool | None:
    if x is None or isinstance(x, (str, int, float, bool)):
        return x
    try:
        return str(x)
    except Exception:
        return "<unrepr>"

class OptionChainWorker(IngestWorker):
    """
    Option chain ingestion worker.
    Responsibilities:
        raw -> normalize -> emit tick
    """

    def __init__(
        self,
        *,
        normalizer: DeribitOptionChainNormalizer,
        source: OptionChainFileSource | DeribitOptionChainRESTSource | OptionChainStreamSource,
        fetch_source: DeribitOptionChainRESTSource | None = None,
        symbol: str,
        interval: str | None = None,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        logger: logging.Logger | None = None,
    ):
        self._normalizer = normalizer
        self._source = source
        self._fetch_source = fetch_source
        self._symbol = symbol
        self._interval = interval
        self._logger = logger or get_logger(f"ingestion.{_DOMAIN}.{self.__class__.__name__}")
        self._poll_seq = 0
        self._error_logged = False
        self._raw_root: Path = option_chain_source.DATA_ROOT / "raw" / "option_chain"
        self._raw_used_paths: set[Path] = set()
        self._raw_write_count = 0
        if poll_interval_ms is not None:
            self._poll_interval_ms = int(poll_interval_ms)
        elif poll_interval is not None:
            self._poll_interval_ms = int(round(float(poll_interval) * 1000.0))
        else:
            self._poll_interval_ms = None
        if self._poll_interval_ms is not None and self._poll_interval_ms < 0:
            raise ValueError("poll_interval_ms must be >= 0")

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
        interval = self._interval or getattr(fetch_source, "interval", None)
        if interval is None:
            log_debug(
                self._logger,
                "ingestion.backfill.no_interval",
                worker=self.__class__.__name__,
                symbol=self._symbol,
                domain=_DOMAIN,
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

        def _persist_payload(payload: Any, *, data_ts: int | None = None) -> None:
            if isinstance(payload, Mapping):
                ts_any = payload.get("data_ts") or payload.get("timestamp") or data_ts
                ts = option_chain_source._coerce_epoch_ms(ts_any) if ts_any is not None else option_chain_source._now_ms()
                records = payload.get("records") or payload.get("chain") or payload.get("frame") or []
                if isinstance(records, option_chain_source.pd.DataFrame):
                    df = records
                else:
                    df = option_chain_source.pd.DataFrame(records)
            elif isinstance(payload, option_chain_source.pd.DataFrame):
                ts = int(data_ts) if data_ts is not None else option_chain_source._now_ms()
                df = payload
            else:
                ts = int(data_ts) if data_ts is not None else option_chain_source._now_ms()
                try:
                    df = option_chain_source.pd.DataFrame(payload)
                except Exception:
                    df = option_chain_source.pd.DataFrame([])
            write_counter = [self._raw_write_count]
            option_chain_source._write_raw_snapshot(
                root=self._raw_root,
                asset=self._symbol,
                interval=str(interval),
                df=df,
                data_ts=int(ts),
                used_paths=self._raw_used_paths,
                write_counter=write_counter,
            )
            self._raw_write_count = write_counter[0]

        count = 0
        for raw in cast(Iterable[Mapping[str, Any]], fetch(start_ts=int(start_ts), end_ts=int(end_ts))):
            raw_for_norm: Any = raw
            if isinstance(raw, Mapping):
                raw_map = dict(raw)
                if "data_ts" not in raw_map and "timestamp" not in raw_map:
                    raw_map["data_ts"] = int(anchor_ts)
                raw_for_norm = raw_map
            tick = self._normalize(raw_for_norm)
            if tick is None:
                continue
            if int(tick.data_ts) > int(anchor_ts):
                continue
            if int(tick.data_ts) < int(start_ts) or int(tick.data_ts) > int(end_ts):
                continue
            _persist_payload(raw, data_ts=int(tick.data_ts))
            _emit_tick(tick)
            count += 1
        return count

    async def run(self, emit: Callable[[IngestionTick], Awaitable[None] | None]) -> None:
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

        async def _emit(tick: IngestionTick) -> None:
            try:
                r = emit(tick)
                if inspect.isawaitable(r):
                    await r  # type: ignore[misc]
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
                raise

        try:
            kind = source_kind(self._source)
            sync_context = {
                "worker": self.__class__.__name__,
                "symbol": self._symbol,
                "domain": _DOMAIN,
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
                await _emit(tick)
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
                if kind == "iter" and self._poll_interval_ms is not None:
                    await asyncio.sleep(self._poll_interval_ms / 1000.0)
                else:
                    # Cooperative yield: prevent starvation when the stream is bursty
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

    def _normalize(self, raw: dict) -> IngestionTick:
        try:
            return self._normalizer.normalize(raw=raw)
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


def _extract_raw_ts(raw: Any) -> str | int | float | None:
    if isinstance(raw, dict):
        for key in ("data_ts", "timestamp", "ts", "time", "T", "E"):
            if key in raw:
                return raw.get(key)
    return None
