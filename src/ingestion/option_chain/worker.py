from __future__ import annotations

import asyncio
import inspect
import logging
import time
from pathlib import Path
from typing import Callable, Awaitable, Any, Iterable, Mapping, cast

from ingestion.contracts.tick import IngestionTick, _to_interval_ms, _guard_interval_ms, resolve_source_id
from ingestion.contracts.worker import IngestWorker
from ingestion.option_chain.normalize import DeribitOptionChainNormalizer
from ingestion.option_chain.source import (
    DeribitOptionChainRESTSource,
    OptionChainFileSource,
    OptionChainStreamSource,
)
import ingestion.option_chain.source as option_chain_source
from quant_engine.utils.asyncio import iter_source, source_kind
from quant_engine.utils.logger import get_logger, log_info, log_debug, log_exception, log_warn, log_throttle, throttle_key
from ingestion.utils import resolve_poll_interval_ms

_DOMAIN = "option_chain"

def _as_primitive(x: Any) -> str | int | float | bool | None:
    if x is None or isinstance(x, (str, int, float, bool)):
        return x
    try:
        return str(x)
    except Exception:
        return "<unrepr>"

_EMPTY_PAYLOAD_LOG_EVERY_S = 5.0
_EMPTY_BOOKKEEPING_COLS = {"arrival_ts", "data_ts", "fetch_step_ts"}

def _select_payload_value(payload: Mapping[str, Any], *, keys: tuple[str, ...]) -> tuple[Any, bool]:
    selected = None
    saw_key = False
    for key in keys:
        if key in payload:
            saw_key = True
            selected = payload.get(key)
            if selected is not None:
                break
    return selected, saw_key

def _is_empty_payload(raw: Any) -> tuple[bool, dict[str, Any]]:
    info: dict[str, Any] = {
        "reason": "empty_payload",
        "kind": None,
        "shape": None,
        "columns_sample": None,
    }
    if isinstance(raw, Mapping):
        # Prefer frame (FileSource) then chain, then records (legacy REST).
        selected, saw_key = _select_payload_value(raw, keys=("frame", "chain", "records"))
        info["data_ts"] = _as_primitive(raw.get("data_ts"))
        if not saw_key:
            return False, info
        if selected is None:
            info["kind"] = "none"
            return True, info
        if isinstance(selected, list):
            info["kind"] = "list"
            info["shape"] = [len(selected)]
            return len(selected) == 0, info
        if isinstance(selected, option_chain_source.pd.DataFrame):
            info["kind"] = "df"
            info["shape"] = [int(selected.shape[0]), int(selected.shape[1])]
            cols = list(selected.columns)
            info["columns_sample"] = [str(c) for c in cols[:6]]
            if selected.empty:
                return True, info
            if set(cols).issubset(_EMPTY_BOOKKEEPING_COLS):
                has_data = bool(selected.notna().any().any())
                return not has_data, info
            return False, info
        info["kind"] = type(selected).__name__
        return False, info
    if isinstance(raw, option_chain_source.pd.DataFrame):
        info["kind"] = "df"
        info["shape"] = [int(raw.shape[0]), int(raw.shape[1])]
        cols = list(raw.columns)
        info["columns_sample"] = [str(c) for c in cols[:6]]
        if raw.empty:
            return True, info
        if set(cols).issubset(_EMPTY_BOOKKEEPING_COLS):
            has_data = bool(raw.notna().any().any())
            return not has_data, info
        return False, info
    if isinstance(raw, list):
        info["kind"] = "list"
        info["shape"] = [len(raw)]
        return len(raw) == 0, info
    return False, info

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
        interval_ms: int | None = None,
        source_id: str | None = None,
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
        self._source_id = resolve_source_id(self._source, override=source_id)
        setattr(self._normalizer, "source_id", self._source_id)
        if interval_ms is not None:
            self._interval_ms = int(interval_ms)
        elif interval is not None:
            ms = _to_interval_ms(interval)
            if ms is None:
                raise ValueError(f"Invalid interval format: {interval!r}")
            self._interval_ms = int(ms)
            _guard_interval_ms(interval, self._interval_ms)
        else:
            self._interval_ms = None
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
                ts_any = payload.get("data_ts") or data_ts
                ts = option_chain_source._coerce_epoch_ms(ts_any) if ts_any is not None else option_chain_source._now_ms()
                # Prefer frame (FileSource) then chain, then records (legacy REST) while avoiding DataFrame truthiness.
                selected = None
                saw_key = False
                for key in ("frame", "chain", "records"):
                    if key in payload:
                        saw_key = True
                        selected = payload.get(key)
                        if selected is not None:
                            break
                if not saw_key or selected is None:
                    return
                if isinstance(selected, list):
                    if len(selected) == 0:
                        return
                    df = option_chain_source.pd.DataFrame(selected)
                elif isinstance(selected, option_chain_source.pd.DataFrame):
                    if selected.empty:
                        return
                    cols = list(selected.columns)
                    if set(cols).issubset(_EMPTY_BOOKKEEPING_COLS) and not bool(selected.notna().any().any()):
                        return
                    df = selected
                else:
                    df = option_chain_source.pd.DataFrame(selected)
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
            is_empty, info = _is_empty_payload(raw)
            if is_empty:
                throttle_id = throttle_key("option_chain.empty_payload", self._symbol, "backfill")
                if log_throttle(throttle_id, _EMPTY_PAYLOAD_LOG_EVERY_S):
                    log_debug(
                        self._logger,
                        "ingestion.empty_payload",
                        worker=self.__class__.__name__,
                        symbol=self._symbol,
                        domain=_DOMAIN,
                        source=getattr(self, "_source_id", None),
                        **info,
                    )
                continue
            raw_for_norm: Any = raw
            if isinstance(raw, Mapping):
                raw_for_norm = dict(raw)
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
                    if self._interval_ms is None:
                        raise ValueError(
                            f"Option chain fetch source requires poll_interval_ms or interval; symbol={self._symbol}"
                        )
                    poll_interval_ms = int(self._interval_ms)
                poll_interval_ms = resolve_poll_interval_ms(
                    self._logger,
                    poll_interval_ms=poll_interval_ms,
                    interval_ms=self._interval_ms,
                    log_context={
                        "worker": self.__class__.__name__,
                        "symbol": self._symbol,
                        "domain": _DOMAIN,
                        "interval": self._interval,
                    },
                )
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
                interval_ms=self._interval_ms,
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
                is_empty, info = _is_empty_payload(raw)
                if is_empty:
                    throttle_id = throttle_key("option_chain.empty_payload", self._symbol, "run")
                    if log_throttle(throttle_id, _EMPTY_PAYLOAD_LOG_EVERY_S):
                        log_debug(
                            self._logger,
                            "ingestion.empty_payload",
                            worker=self.__class__.__name__,
                            symbol=self._symbol,
                            domain=_DOMAIN,
                            source=getattr(self, "_source_id", None),
                            **info,
                        )
                    continue
                self._poll_seq += 1
                tick = self._normalize(raw)
                await _emit(tick)
                # Cooperative yield: prevent starvation when the stream is bursty
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
                    worker=self.__class__.__name__,
                    symbol=self._symbol,
                    domain=_DOMAIN,
                    poll_seq=self._poll_seq,
                    err_type=type(exc).__name__,
                    err=str(exc),
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
