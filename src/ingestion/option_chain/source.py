from __future__ import annotations

import itertools
import json
import multiprocessing as mp
import queue as queue_mod
import time
from datetime import datetime, timezone, date
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Any, AsyncIterable, AsyncIterator, Iterator, Mapping, Iterable
import pandas as pd

import os
import threading
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from ingestion.contracts.source import Source, AsyncSource, Raw
from ingestion.contracts.tick import _coerce_epoch_ms, _guard_interval_ms, _to_interval_ms

from quant_engine.utils.paths import data_root_from_file, resolve_under_root
from quant_engine.utils.logger import get_logger, log_warn, log_debug
from quant_engine.utils.memory import PeriodicMemoryTrim

DATA_ROOT = data_root_from_file(__file__, levels_up=3)
_LOG = get_logger(__name__)
_LOCK_WARN_S = 0.2
_WRITE_LOG_EVERY = 100
_WRITER_PERIODIC_CLOSE_EVERY = 100
_WRITER_PERIODIC_CLOSE_SECONDS = 5.0
_WRITER_QUEUE_SIZE = 64
_WRITER_QUEUE_PUT_TIMEOUT_S = 5.0
_WRITER_RECYCLE_SECONDS = 900.0
_WRITER_RECYCLE_TASKS = 2000
# Example: 6 dead children in 60s => escalate instead of endless restart loop.
_WRITER_RESTART_STORM_WINDOW_S = 60.0
_WRITER_RESTART_STORM_MAX = 5
_RPC_URL = "https://www.deribit.com/api/v2"
_RPC_ID = itertools.count(1)

records: list[Mapping[str, Any]] = []
_UNIVERSE_LOCKS: dict[Path, threading.Lock] = {}

_RAW_OPTION_CHAIN_COLUMNS: list[str] = [
    "instrument_name",
    "expiration_timestamp",
    "strike",
    "option_type",
    "state",
    "is_active",
    "instrument_id",
    "settlement_currency",
    "base_currency",
    "quote_currency",
    "contract_size",
    "tick_size",
    "min_trade_amount",
    "kind",
    "instrument_type",
    "price_index",
    "counter_currency",
    "settlement_period",
    "tick_size_steps",
    "bid_price",
    "ask_price",
    "mid_price",
    "last",
    "mark_price",
    "open_interest",
    "volume_24h",
    "volume_usd_24h",
    "mark_iv",
    "high",
    "low",
    "price_change",
    "market_ts",
    "underlying_price",
    "underlying_index",
    "estimated_delivery_price",
    "interest_rate",
    "arrival_ts",
    "aux",
]
_RAW_OPTION_CHAIN_COL_SET: set[str] = set(_RAW_OPTION_CHAIN_COLUMNS)

class OptionChainWriteError(RuntimeError):
    """Raised for non-recoverable option-chain parquet write failures."""

def _now_ms() -> int:
    return int(time.time() * 1000.0)


def deribit_rpc(
    method: str,
    params: Mapping[str, Any] | None,
    *,
    session: requests.Session,
    timeout: float,
    url: str = _RPC_URL,
) -> Any:
    payload = {
        "jsonrpc": "2.0",
        "id": next(_RPC_ID),
        "method": method,
        "params": params or {},
    }
    r = session.post(url, json=payload, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    if j.get("error"):
        raise RuntimeError(f"Deribit error: {j['error']}")
    return j.get("result")


def _flatten_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        if out[col].dtype != "object":
            continue
        if not out[col].map(lambda v: isinstance(v, (dict, list))).any():
            continue
        out[col] = out[col].map(
            lambda v: json.dumps(v, sort_keys=True) if isinstance(v, (dict, list)) else v
        )
    return out


def _pack_aux(df: pd.DataFrame, cols_to_aux: set[str]) -> pd.DataFrame:
    present = sorted(c for c in cols_to_aux if c in df.columns)  # + stabilize aux key order for deterministic serialization/tests  # +
    if not present:
        return df
    out = df.copy()
    has_aux = "aux" in out.columns
    if not has_aux:
        out["aux"] = pd.Series(([{} for _ in range(len(out))]), index=out.index, dtype="object")
    else:
        out["aux"] = out["aux"].map(lambda v: dict(v) if isinstance(v, dict) else {})
    # + avoid DataFrame.apply for ingestion hot path  # +
    base_aux = out["aux"].tolist()  # +
    present_vals = [out[c].tolist() for c in present]  # +
    out["aux"] = pd.Series(
        [{**aux, **dict(zip(present, vals))} for aux, *vals in zip(base_aux, *present_vals)],
        index=out.index,
        dtype="object",
    )
    return out.drop(columns=present)

def _date_path(root: Path, *, interval: str, asset: str, data_ts: int) -> Path:
    dt = datetime.fromtimestamp(int(data_ts) / 1000.0, tz=timezone.utc)
    year = dt.strftime("%Y")
    ymd = dt.strftime("%Y_%m_%d")
    return root / asset / interval / year / f"{ymd}.parquet"


def _quote_date_path(root: Path, *, interval: str, asset: str, step_ts: int) -> Path:
    dt = datetime.fromtimestamp(int(step_ts) / 1000.0, tz=timezone.utc)
    year = dt.strftime("%Y")
    ymd = dt.strftime("%Y_%m_%d")
    return root / asset / interval / year / f"{ymd}.parquet"


def _get_lock(path: Path) -> threading.Lock:
    with DeribitOptionChainRESTSource._locks_guard:
        lock = DeribitOptionChainRESTSource._global_locks.get(path)
        if lock is None:
            lock = threading.Lock()
            DeribitOptionChainRESTSource._global_locks[path] = lock
        return lock


def _get_universe_lock(path: Path) -> threading.Lock:
    with DeribitOptionChainRESTSource._global_lock:
        lock = _UNIVERSE_LOCKS.get(path)
        if lock is None:
            lock = threading.Lock()
            _UNIVERSE_LOCKS[path] = lock
        return lock


def _used_paths_has(used_paths: set[Path], path: Path, used_paths_lock: threading.Lock | None) -> bool:
    if used_paths_lock is None:
        return path in used_paths
    with used_paths_lock:
        return path in used_paths


def _used_paths_add(used_paths: set[Path], path: Path, used_paths_lock: threading.Lock | None) -> None:
    if used_paths_lock is None:
        used_paths.add(path)
        return
    with used_paths_lock:
        used_paths.add(path)


def _used_paths_discard(used_paths: set[Path], path: Path, used_paths_lock: threading.Lock | None) -> None:
    if used_paths_lock is None:
        used_paths.discard(path)
        return
    with used_paths_lock:
        used_paths.discard(path)


def _used_paths_snapshot(used_paths: set[Path], used_paths_lock: threading.Lock | None) -> list[Path]:
    if used_paths_lock is None:
        return list(used_paths)
    with used_paths_lock:
        return list(used_paths)


def _used_paths_clear(used_paths: set[Path], used_paths_lock: threading.Lock | None) -> None:
    if used_paths_lock is None:
        used_paths.clear()
        return
    with used_paths_lock:
        used_paths.clear()


def _close_used_paths(used_paths: set[Path]) -> None:
    # Invariant: worker-owned raw writer refs must be decref/closed on shutdown — enforced here to prevent writer-handle retention
    paths = sorted(list(used_paths), key=lambda p: str(p))
    for path in paths:
        lock = _get_lock(path)
        lock.acquire()
        try:
            writer = None
            with DeribitOptionChainRESTSource._global_lock:
                ref = DeribitOptionChainRESTSource._global_refs.get(path, 0) - 1
                if ref <= 0:
                    writer = DeribitOptionChainRESTSource._global_writers.pop(path, None)
                    DeribitOptionChainRESTSource._global_refs.pop(path, None)
                    DeribitOptionChainRESTSource._global_schemas.pop(path, None)
                    DeribitOptionChainRESTSource._current_dates.pop(path, None)
                    DeribitOptionChainRESTSource._path_write_counts.pop(path, None)
                    DeribitOptionChainRESTSource._writer_last_close_monotonic.pop(path, None)
                else:
                    DeribitOptionChainRESTSource._global_refs[path] = ref
            used_paths.discard(path)
            if writer is not None:
                try:
                    writer.close()
                except Exception as close_exc:
                    log_debug(
                        _LOG,
                        "ingestion.close_error",
                        path=str(path),
                        err_type=type(close_exc).__name__,
                        err=str(close_exc),
                    )
        finally:
            lock.release()
    used_paths.clear()


class _PathLockGuard:
    __slots__ = ("path", "lock")

    def __init__(self, path: Path, lock: threading.Lock):
        self.path = path
        self.lock = lock


def _validate_path_guard(path: Path, guard: _PathLockGuard) -> None:
    if guard.path != path:
        raise RuntimeError("path guard mismatch")


def _get_writer_and_schema(
    path: Path,
    df: pd.DataFrame,
    used_paths: set[Path],
    guard: _PathLockGuard,
    used_paths_lock: threading.Lock | None = None,
    bad_writer_ids: dict[Path, int] | None = None,
) -> tuple[pq.ParquetWriter, pa.Schema, bool]:
    _validate_path_guard(path, guard)
    path.parent.mkdir(parents=True, exist_ok=True)
    with DeribitOptionChainRESTSource._global_lock:
        writer = DeribitOptionChainRESTSource._global_writers.get(path)
        if writer is not None:
            if bad_writer_ids is not None:
                bad_id = bad_writer_ids.get(path)
                if bad_id is not None and bad_id == id(writer):
                    raise OptionChainWriteError(f"Writer handle marked bad for path={path}")
                if bad_id is not None and bad_id != id(writer):
                    bad_writer_ids.pop(path, None)
            did_incref = False
            if not _used_paths_has(used_paths, path, used_paths_lock):
                DeribitOptionChainRESTSource._global_refs[path] = DeribitOptionChainRESTSource._global_refs.get(path, 0) + 1
                _used_paths_add(used_paths, path, used_paths_lock)
                did_incref = True
            return writer, DeribitOptionChainRESTSource._global_schemas[path], did_incref

    if bad_writer_ids is not None:
        bad_writer_ids.pop(path, None)
    if path.exists():
        result = _bootstrap_existing(
            path,
            df,
            used_paths,
            guard,
            used_paths_lock=used_paths_lock,
        )
        if result is not None:
            log_debug(_LOG, "ingestion.writer_open", path=str(path))
            return result
        # Example: unreadable parquet => move to *.corrupt.* and continue with fresh writer.
        corrupt_path = path.with_suffix(f".parquet.corrupt.{_now_ms()}.{os.getpid()}")
        if path.exists():
            try:
                os.replace(path, corrupt_path)
                log_warn(
                    _LOG,
                    "option_chain.bootstrap.corrupt_quarantined",
                    path=str(path),
                    corrupt_path=str(corrupt_path),
                )
            except Exception as quarantine_exc:
                raise OptionChainWriteError(
                    f"Failed to quarantine corrupt parquet path={path}: {quarantine_exc}"
                ) from quarantine_exc

    table = pa.Table.from_pandas(df, preserve_index=False)
    writer = pq.ParquetWriter(path, table.schema)
    did_incref = _register_writer(path, writer, table.schema, used_paths, guard, used_paths_lock=used_paths_lock)
    return writer, table.schema, did_incref


def _bootstrap_existing(
    path: Path,
    df: pd.DataFrame,
    used_paths: set[Path],
    guard: _PathLockGuard,
    used_paths_lock: threading.Lock | None = None,
) -> tuple[pq.ParquetWriter, pa.Schema, bool] | None:
    _validate_path_guard(path, guard)
    path.parent.mkdir(parents=True, exist_ok=True)
    bak_path = path.with_suffix(".parquet.bak")
    os.replace(path, bak_path)
    try:
        table = pq.read_table(bak_path)
        schema = table.schema
        with DeribitOptionChainRESTSource._global_lock:
            cached_schema = DeribitOptionChainRESTSource._global_schemas.get(path)
        if cached_schema is not None:
            schema = cached_schema
            table = _align_table_to_schema(table, schema, bak_path)
        writer = pq.ParquetWriter(path, schema)
        writer.write_table(table)
        did_incref = _register_writer(path, writer, schema, used_paths, guard, used_paths_lock=used_paths_lock)
        os.remove(bak_path)
        return writer, schema, did_incref
    except Exception as exc:
        log_warn(
            _LOG,
            "option_chain.bootstrap.corrupt_recovered",
            path=str(path),
            bak_path=str(bak_path),
            err_type=type(exc).__name__,
            err=str(exc),
        )
        rollback_ok = False
        if bak_path.exists():
            try:
                os.replace(bak_path, path)
                rollback_ok = True
            except Exception as rollback_exc:
                log_warn(
                    _LOG,
                    "option_chain.bootstrap.rollback_failed",
                    path=str(path),
                    bak_path=str(bak_path),
                    err_type=type(rollback_exc).__name__,
                    err=str(rollback_exc),
                )
        if not rollback_ok:
            log_warn(
                _LOG,
                "option_chain.bootstrap.rollback_missing",
                path=str(path),
                bak_path=str(bak_path),
            )
        return None


def _align_raw(df: pd.DataFrame, *, allowed_cols: list[str], path: Path) -> pd.DataFrame:
    if list(df.columns) == allowed_cols:
        return df
    allowed_col_set = set(allowed_cols)
    out = df
    if "instrument_name" not in out.columns:
        out = out.copy()
        out["instrument_name"] = None
    extra = sorted(c for c in out.columns if c not in allowed_col_set)
    if extra:
        # Pack unknown columns into aux; extras win on key collisions.
        out = _pack_aux(out, set(extra))
        log_warn(
            _LOG,
            "option_chain.raw.schema_drift.packed_to_aux",
            extra_cols=extra,
            path=str(path),
            n_rows=int(len(out)),
        )
    missing = [c for c in allowed_cols if c not in out.columns]
    if missing:
        if out is df:
            out = out.copy()
        for c in missing:
            out[c] = None
    if list(out.columns) == allowed_cols:
        return out
    return out[allowed_cols]


def _prepare_raw_snapshot_df(df: pd.DataFrame) -> pd.DataFrame:
    if list(df.columns) == _RAW_OPTION_CHAIN_COLUMNS:
        return df
    out = df
    if "instrument_name" not in out.columns:
        out = out.copy()
        out["instrument_name"] = None
    extra = sorted(c for c in out.columns if c not in _RAW_OPTION_CHAIN_COL_SET)
    if extra:
        out = _pack_aux(out, set(extra))
    missing = [c for c in _RAW_OPTION_CHAIN_COLUMNS if c not in out.columns]
    if missing:
        if out is df:
            out = out.copy()
        for c in missing:
            out[c] = None
    if list(out.columns) == _RAW_OPTION_CHAIN_COLUMNS:
        return out
    aligned = out[_RAW_OPTION_CHAIN_COLUMNS]
    extras_after = [c for c in aligned.columns if c not in _RAW_OPTION_CHAIN_COL_SET]
    if extras_after:
        raise OptionChainWriteError(f"unexpected raw columns after alignment: {extras_after}")
    return aligned


def _register_writer(
    path: Path,
    writer: pq.ParquetWriter,
    schema: pa.Schema,
    used_paths: set[Path],
    guard: _PathLockGuard,
    used_paths_lock: threading.Lock | None = None,
) -> bool:
    _validate_path_guard(path, guard)
    did_incref = False
    with DeribitOptionChainRESTSource._global_lock:
        DeribitOptionChainRESTSource._global_writers[path] = writer
        DeribitOptionChainRESTSource._global_schemas[path] = schema
        DeribitOptionChainRESTSource._writer_last_close_monotonic.setdefault(path, time.monotonic())
        if not _used_paths_has(used_paths, path, used_paths_lock):
            DeribitOptionChainRESTSource._global_refs[path] = DeribitOptionChainRESTSource._global_refs.get(path, 0) + 1
            did_incref = True
    if not _used_paths_has(used_paths, path, used_paths_lock):
        _used_paths_add(used_paths, path, used_paths_lock)
    log_debug(_LOG, "ingestion.writer_open", path=str(path))
    return did_incref


def _get_raw_writer_and_schema(
    path: Path,
    df: pd.DataFrame,
    used_paths: set[Path],
    guard: _PathLockGuard,
    used_paths_lock: threading.Lock | None = None,
    bad_writer_ids: dict[Path, int] | None = None,
) -> tuple[pq.ParquetWriter, pa.Schema, bool]:
    _validate_path_guard(path, guard)
    path.parent.mkdir(parents=True, exist_ok=True)
    with DeribitOptionChainRESTSource._global_lock:
        writer = DeribitOptionChainRESTSource._global_writers.get(path)
        if writer is not None:
            if bad_writer_ids is not None:
                bad_id = bad_writer_ids.get(path)
                if bad_id is not None and bad_id == id(writer):
                    raise OptionChainWriteError(f"Writer handle marked bad for path={path}")
                if bad_id is not None and bad_id != id(writer):
                    bad_writer_ids.pop(path, None)
            did_incref = False
            if not _used_paths_has(used_paths, path, used_paths_lock):
                DeribitOptionChainRESTSource._global_refs[path] = DeribitOptionChainRESTSource._global_refs.get(path, 0) + 1
                _used_paths_add(used_paths, path, used_paths_lock)
                did_incref = True
            return writer, DeribitOptionChainRESTSource._global_schemas[path], did_incref

    if bad_writer_ids is not None:
        bad_writer_ids.pop(path, None)
    if path.exists():
        result = _bootstrap_existing_raw(path, df, used_paths, guard, used_paths_lock=used_paths_lock)
        if result is not None:
            return result
        # Example: unreadable raw parquet => quarantine and resume append on a new file.
        corrupt_path = path.with_suffix(f".parquet.corrupt.{_now_ms()}.{os.getpid()}")
        if path.exists():
            try:
                os.replace(path, corrupt_path)
                log_warn(
                    _LOG,
                    "option_chain.bootstrap.corrupt_quarantined",
                    path=str(path),
                    corrupt_path=str(corrupt_path),
                )
            except Exception as quarantine_exc:
                raise OptionChainWriteError(
                    f"Failed to quarantine corrupt raw parquet path={path}: {quarantine_exc}"
                ) from quarantine_exc

    aligned = _align_raw(df, allowed_cols=_RAW_OPTION_CHAIN_COLUMNS, path=path)
    table = pa.Table.from_pandas(aligned, preserve_index=False)
    writer = pq.ParquetWriter(path, table.schema)
    did_incref = _register_writer(path, writer, table.schema, used_paths, guard, used_paths_lock=used_paths_lock)
    return writer, table.schema, did_incref


def _bootstrap_existing_raw(
    path: Path,
    df: pd.DataFrame,
    used_paths: set[Path],
    guard: _PathLockGuard,
    used_paths_lock: threading.Lock | None = None,
) -> tuple[pq.ParquetWriter, pa.Schema, bool] | None:
    _validate_path_guard(path, guard)
    path.parent.mkdir(parents=True, exist_ok=True)
    bak_path = path.with_suffix(".parquet.bak")
    os.replace(path, bak_path)
    try:
        aligned_new = _align_raw(df, allowed_cols=_RAW_OPTION_CHAIN_COLUMNS, path=path)
        schema = pa.Table.from_pandas(aligned_new, preserve_index=False).schema
        table = pq.read_table(bak_path)
        try:
            table = _align_table_to_schema(table, schema, path)
        except ValueError:
            # Fallback for legacy files that may contain extra raw columns requiring aux packing.
            table_df = table.to_pandas()
            table_df = _align_raw(table_df, allowed_cols=_RAW_OPTION_CHAIN_COLUMNS, path=path)
            schema = pa.Table.from_pandas(table_df, preserve_index=False).schema
            table = pa.Table.from_pandas(table_df, schema=schema, preserve_index=False)
        writer = pq.ParquetWriter(path, schema)
        writer.write_table(table)
        did_incref = _register_writer(path, writer, schema, used_paths, guard, used_paths_lock=used_paths_lock)
        os.remove(bak_path)
        return writer, schema, did_incref
    except Exception as exc:
        log_warn(
            _LOG,
            "option_chain.bootstrap.corrupt_recovered",
            path=str(path),
            bak_path=str(bak_path),
            err_type=type(exc).__name__,
            err=str(exc),
        )
        rollback_ok = False
        if bak_path.exists():
            try:
                os.replace(bak_path, path)
                rollback_ok = True
            except Exception as rollback_exc:
                log_warn(
                    _LOG,
                    "option_chain.bootstrap.rollback_failed",
                    path=str(path),
                    bak_path=str(bak_path),
                    err_type=type(rollback_exc).__name__,
                    err=str(rollback_exc),
                )
        if not rollback_ok:
            log_warn(
                _LOG,
                "option_chain.bootstrap.rollback_missing",
                path=str(path),
                bak_path=str(bak_path),
            )
        return None


def _align_to_schema(df: pd.DataFrame, schema: pa.Schema, path: Path) -> pd.DataFrame:
    cols = list(schema.names)
    # Only map fetch_step_ts -> step_ts when the stored schema strictly requires it.
    if "fetch_step_ts" in df.columns and "fetch_step_ts" not in cols and "step_ts" in cols:
        df = df.copy()
        df["step_ts"] = df["fetch_step_ts"]
        df = df.drop(columns=["fetch_step_ts"])
    extra = [c for c in df.columns if c not in cols]
    if extra:
        raise ValueError(
            f"Option chain schema drift for {path}: unexpected columns {extra}"
        )
    missing = [c for c in cols if c not in df.columns]
    if missing:
        df = df.copy()
        for c in missing:
            df[c] = None
    return df[cols]


def _align_table_to_schema(table: pa.Table, schema: pa.Schema, path: Path) -> pa.Table:
    cols = list(schema.names)
    extra = [c for c in table.column_names if c not in cols]
    if extra:
        raise ValueError(
            f"Option chain schema drift for {path}: unexpected columns {extra}"
        )
    missing = [c for c in cols if c not in table.column_names]
    if missing:
        for c in missing:
            table = table.append_column(c, pa.nulls(table.num_rows))
    return table.select(cols)


def _rotate_if_date_changed_locked(path: Path, lock: threading.Lock, current_date: str) -> None:
    if not lock.locked():
        raise RuntimeError("path lock must be held before rotate")
    writer_to_close: pq.ParquetWriter | None = None
    prev_date: str | None = None
    with DeribitOptionChainRESTSource._global_lock:
        prev_date = DeribitOptionChainRESTSource._current_dates.get(path)
        if prev_date is None:
            DeribitOptionChainRESTSource._current_dates[path] = current_date
            return
        if prev_date == current_date:
            return
        writer_to_close = DeribitOptionChainRESTSource._global_writers.pop(path, None)
        DeribitOptionChainRESTSource._global_schemas.pop(path, None)
        DeribitOptionChainRESTSource._current_dates[path] = current_date
    if writer_to_close is not None:
        try:
            writer_to_close.close()
            log_debug(
                _LOG,
                "ingestion.writer_rotated",
                path=str(path),
                prev_date=prev_date,
                new_date=current_date,
            )
            log_debug(_LOG, "ingestion.writer_close", path=str(path))
        except Exception as close_exc:
            log_debug(
                _LOG,
                "ingestion.close_error",
                path=str(path),
                err_type=type(close_exc).__name__,
                err=str(close_exc),
            )
            raise


def _maybe_periodic_close_locked(path: Path, lock: threading.Lock) -> None:
    if not lock.locked():
        raise RuntimeError("path lock must be held before periodic close")
    writer_to_close: pq.ParquetWriter | None = None
    now_mono = time.monotonic()
    trigger_count = False
    trigger_time = False
    with DeribitOptionChainRESTSource._global_lock:
        n = DeribitOptionChainRESTSource._path_write_counts.get(path, 0) + 1
        DeribitOptionChainRESTSource._path_write_counts[path] = n
        last_close = DeribitOptionChainRESTSource._writer_last_close_monotonic.get(path, now_mono)
        trigger_count = (n % _WRITER_PERIODIC_CLOSE_EVERY) == 0
        trigger_time = (now_mono - float(last_close)) >= float(_WRITER_PERIODIC_CLOSE_SECONDS)
        if not trigger_count and not trigger_time:
            return
        writer_to_close = DeribitOptionChainRESTSource._global_writers.pop(path, None)
        DeribitOptionChainRESTSource._global_schemas.pop(path, None)
        DeribitOptionChainRESTSource._writer_last_close_monotonic[path] = now_mono
    if trigger_count:
        log_debug(_LOG, "ingestion.writer_periodic_close", path=str(path))
    if trigger_time:
        log_debug(_LOG, "ingestion.writer_periodic_close_time", path=str(path))
    if writer_to_close is None:
        return
    try:
        writer_to_close.close()
        log_debug(_LOG, "ingestion.writer_close", path=str(path))
    except Exception as close_exc:
        log_debug(
            _LOG,
            "ingestion.close_error",
            path=str(path),
            err_type=type(close_exc).__name__,
            err=str(close_exc),
        )


def _handle_write_exception_locked(
    *,
    path: Path,
    lock: threading.Lock,
    used_paths: set[Path],
    used_paths_lock: threading.Lock | None,
    bad_writer_ids: dict[Path, int] | None,
    did_incref: bool,
    writer_id: int | None,
) -> None:
    if not lock.locked():
        raise RuntimeError("path lock must be held before exception cleanup")
    writer_to_close: pq.ParquetWriter | None = None
    if bad_writer_ids is not None:
        if writer_id is not None:
            bad_writer_ids[path] = writer_id
        else:
            bad_writer_ids.pop(path, None)
    if not did_incref:
        return
    with DeribitOptionChainRESTSource._global_lock:
        ref = DeribitOptionChainRESTSource._global_refs.get(path, 0)
        if ref > 1:
            DeribitOptionChainRESTSource._global_refs[path] = ref - 1
        else:
            writer_to_close = DeribitOptionChainRESTSource._global_writers.pop(path, None)
            DeribitOptionChainRESTSource._global_schemas.pop(path, None)
            DeribitOptionChainRESTSource._global_refs.pop(path, None)
            DeribitOptionChainRESTSource._current_dates.pop(path, None)
            DeribitOptionChainRESTSource._path_write_counts.pop(path, None)
            DeribitOptionChainRESTSource._writer_last_close_monotonic.pop(path, None)
    _used_paths_discard(used_paths, path, used_paths_lock)
    try:
        if writer_to_close is not None:
            writer_to_close.close()
            log_debug(_LOG, "ingestion.writer_close", path=str(path))
    except Exception as close_exc:
        log_debug(
            _LOG,
            "ingestion.close_error",
            path=str(path),
            err_type=type(close_exc).__name__,
            err=str(close_exc),
        )


def _write_raw_snapshot(
    *,
    root: Path,
    asset: str,
    interval: str,
    df: pd.DataFrame,
    data_ts: int,
    used_paths: set[Path],
    used_paths_lock: threading.Lock | None = None,
    bad_writer_ids: dict[Path, int] | None = None,
    write_counter: list[int] | None = None,
) -> None:
    path = _date_path(root, interval=interval, asset=asset, data_ts=data_ts)
    current_date = datetime.fromtimestamp(int(data_ts) / 1000.0, tz=timezone.utc).strftime("%Y_%m_%d")
    path.parent.mkdir(parents=True, exist_ok=True)
    if "arrival_ts" not in df.columns:
        df = df.copy()
        df["arrival_ts"] = int(data_ts)

    lock = _get_lock(path)
    write_start = time.monotonic()
    start = time.monotonic()
    lock.acquire()
    waited_s = time.monotonic() - start
    if waited_s > _LOCK_WARN_S:
        log_debug(
            _LOG,
            "ingestion.lock_wait",
            component="option_chain",
            path=str(path),
            wait_ms=int(waited_s * 1000.0),
        )
    try:
        guard = _PathLockGuard(path, lock)
        did_incref = False
        writer_id: int | None = None
        _rotate_if_date_changed_locked(path, lock, current_date)
        writer, schema, did_incref = _get_raw_writer_and_schema(
            path,
            df,
            used_paths,
            guard,
            used_paths_lock=used_paths_lock,
            bad_writer_ids=bad_writer_ids,
        )
        writer_id = id(writer)
        aligned = _align_raw(df, allowed_cols=_RAW_OPTION_CHAIN_COLUMNS, path=path)
        table = pa.Table.from_pandas(aligned, schema=schema, preserve_index=False)
        writer.write_table(table)
        _maybe_periodic_close_locked(path, lock)
        if write_counter is not None:
            write_counter[0] += 1
            write_count = write_counter[0]
        else:
            write_count = None
        if write_count is not None and write_count % _WRITE_LOG_EVERY == 0:
            log_debug(
                _LOG,
                "ingestion.write_sample",
                component="option_chain",
                path=str(path),
                rows=int(len(df)),
                write_ms=int((time.monotonic() - write_start) * 1000),
                write_seq=write_count,
            )
    except Exception as exc:
        _handle_write_exception_locked(
            path=path,
            lock=lock,
            used_paths=used_paths,
            used_paths_lock=used_paths_lock,
            bad_writer_ids=bad_writer_ids,
            did_incref=did_incref,
            writer_id=writer_id,
        )
        raise OptionChainWriteError(str(exc)) from exc
    finally:
        lock.release()

def _universe_date_path(root: Path, *, asset: str, interval: str, data_ts: int) -> Path:
    dt = datetime.fromtimestamp(int(data_ts) / 1000.0, tz=timezone.utc)
    year = dt.strftime("%Y")
    ymd = dt.strftime("%Y_%m_%d")
    # Example: ETH/1m and ETH/5m must not append into the same universe parquet file.
    return root / asset / interval / year / f"{ymd}.parquet"

def _write_universe_snapshot(
    *,
    root: Path,
    asset: str,
    interval: str,
    df: pd.DataFrame,
    data_ts: int,
    used_paths: set[Path],
    used_paths_lock: threading.Lock | None = None,
    bad_writer_ids: dict[Path, int] | None = None,
    write_counter: list[int] | None = None,
) -> None:
    path = _universe_date_path(root, asset=asset, interval=interval, data_ts=data_ts)
    path.parent.mkdir(parents=True, exist_ok=True)
    if "data_ts" not in df.columns:
        df = df.copy()
        df["data_ts"] = int(data_ts)

    lock = _get_lock(path)
    write_start = time.monotonic()
    start = time.monotonic()
    lock.acquire()
    waited_s = time.monotonic() - start
    if waited_s > _LOCK_WARN_S:
        log_debug(
            _LOG,
            "ingestion.lock_wait",
            component="option_universe",
            path=str(path),
            wait_ms=int(waited_s * 1000.0),
        )
    try:
        guard = _PathLockGuard(path, lock)
        did_incref = False
        writer_id: int | None = None
        writer, schema, did_incref = _get_writer_and_schema(
            path,
            df,
            used_paths,
            guard,
            used_paths_lock=used_paths_lock,
            bad_writer_ids=bad_writer_ids,
        )
        writer_id = id(writer)
        aligned = _align_to_schema(df, schema, path)
        table = pa.Table.from_pandas(aligned, schema=schema, preserve_index=False)
        writer.write_table(table)
        _maybe_periodic_close_locked(path, lock)
        if write_counter is not None:
            write_counter[0] += 1
            write_count = write_counter[0]
        else:
            write_count = None
        if write_count is not None and write_count % _WRITE_LOG_EVERY == 0:
            log_debug(
                _LOG,
                "ingestion.write_sample",
                component="option_universe",
                path=str(path),
                rows=int(len(df)),
                write_ms=int((time.monotonic() - write_start) * 1000),
                write_seq=write_count,
            )
    except Exception as exc:
        _handle_write_exception_locked(
            path=path,
            lock=lock,
            used_paths=used_paths,
            used_paths_lock=used_paths_lock,
            bad_writer_ids=bad_writer_ids,
            did_incref=did_incref,
            writer_id=writer_id,
        )
        raise OptionChainWriteError(str(exc)) from exc
    finally:
        lock.release()


def _writer_process_main(
    *,
    task_queue: mp.Queue,
    root: Path,
    quote_root: Path,
    universe_root: Path,
    asset: str,
    interval: str,
) -> None:
    used_paths: set[Path] = set()
    used_paths_lock = threading.Lock()
    bad_writer_ids: dict[Path, int] = {}
    raw_counter = [0]
    quote_counter = [0]
    universe_counter = [0]
    try:
        while True:
            try:
                task = task_queue.get(timeout=0.5)
            except queue_mod.Empty:
                continue
            except (EOFError, OSError, ValueError, KeyboardInterrupt) as exc:
                log_debug(
                    _LOG,
                    "ingestion.writer_process_queue_closed",
                    err_type=type(exc).__name__,
                    err=str(exc),
                )
                break
            if task is None:
                break
            kind = str(task.get("kind", ""))
            try:
                if kind == "raw":
                    _write_raw_snapshot(
                        root=root,
                        asset=asset,
                        interval=interval,
                        df=task["df"],
                        data_ts=int(task["data_ts"]),
                        used_paths=used_paths,
                        used_paths_lock=used_paths_lock,
                        bad_writer_ids=bad_writer_ids,
                        write_counter=raw_counter,
                    )
                elif kind == "quote":
                    _write_quote_snapshot(
                        root=quote_root,
                        asset=asset,
                        interval=interval,
                        df=task["df"],
                        step_ts=int(task["step_ts"]),
                        used_paths=used_paths,
                        used_paths_lock=used_paths_lock,
                        bad_writer_ids=bad_writer_ids,
                        write_counter=quote_counter,
                    )
                elif kind == "universe":
                    _write_universe_snapshot(
                        root=universe_root,
                        asset=asset,
                        interval=interval,
                        df=task["df"],
                        data_ts=int(task["data_ts"]),
                        used_paths=used_paths,
                        used_paths_lock=used_paths_lock,
                        bad_writer_ids=bad_writer_ids,
                        write_counter=universe_counter,
                    )
            except Exception as task_exc:
                log_warn(
                    _LOG,
                    "option_chain.writer_task_failed",
                    kind=kind,
                    err_type=type(task_exc).__name__,
                    err=str(task_exc),
                )
    finally:
        paths = sorted(_used_paths_snapshot(used_paths, used_paths_lock), key=lambda p: str(p))
        for path in paths:
            lock = _get_lock(path)
            lock.acquire()
            try:
                writer = None
                with DeribitOptionChainRESTSource._global_lock:
                    ref = DeribitOptionChainRESTSource._global_refs.get(path, 0) - 1
                    if ref <= 0:
                        writer = DeribitOptionChainRESTSource._global_writers.pop(path, None)
                        DeribitOptionChainRESTSource._global_refs.pop(path, None)
                        DeribitOptionChainRESTSource._global_schemas.pop(path, None)
                        DeribitOptionChainRESTSource._current_dates.pop(path, None)
                        DeribitOptionChainRESTSource._path_write_counts.pop(path, None)
                        DeribitOptionChainRESTSource._writer_last_close_monotonic.pop(path, None)
                    else:
                        DeribitOptionChainRESTSource._global_refs[path] = ref
                _used_paths_discard(used_paths, path, used_paths_lock)
                if writer is not None:
                    try:
                        writer.close()
                    except Exception as close_exc:
                        log_debug(
                            _LOG,
                            "ingestion.close_error",
                            path=str(path),
                            err_type=type(close_exc).__name__,
                            err=str(close_exc),
                        )
            finally:
                lock.release()
        _used_paths_clear(used_paths, used_paths_lock)


class _ProcessWriteDispatcher:
    def __init__(
        self,
        *,
        root: Path,
        quote_root: Path,
        universe_root: Path,
        asset: str,
        interval: str,
        queue_size: int,
        put_timeout_s: float,
        recycle_seconds: float,
        recycle_tasks: int,
    ) -> None:
        self._put_timeout_s = max(0.1, float(put_timeout_s))
        self._queue_size = max(1, int(queue_size))
        self._recycle_seconds = max(0.0, float(recycle_seconds))
        self._recycle_tasks = max(0, int(recycle_tasks))
        self._root = root
        self._quote_root = quote_root
        self._universe_root = universe_root
        self._asset = asset
        self._interval = interval
        self._proc: BaseProcess | None = None
        self._queue: mp.Queue | None = None
        self._started_monotonic = 0.0
        self._enqueued = 0
        self._restart_window_start = time.monotonic()
        self._restart_count = 0
        self._start_proc()

    def _start_proc(self) -> None:
        ctx = mp.get_context("spawn")
        self._queue = ctx.Queue(self._queue_size)
        proc = ctx.Process(
            target=_writer_process_main,
            kwargs={
                "task_queue": self._queue,
                "root": self._root,
                "quote_root": self._quote_root,
                "universe_root": self._universe_root,
                "asset": self._asset,
                "interval": self._interval,
            },
            daemon=True,
        )
        proc.start()
        self._proc = proc
        self._started_monotonic = time.monotonic()
        self._enqueued = 0

    def _signal_stop(self) -> None:
        if self._queue is None:
            return
        try:
            self._queue.put(None, timeout=self._put_timeout_s)
        except Exception as exc:
            log_debug(
                _LOG,
                "ingestion.writer_process_signal_error",
                err_type=type(exc).__name__,
                err=str(exc),
            )

    def _stop_proc(self) -> None:
        proc = self._proc
        queue = self._queue
        if proc is None:
            if queue is not None:
                try:
                    queue.close()
                    queue.join_thread()
                except Exception:
                    pass
                self._queue = None
            return
        proc.join(timeout=5.0)
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=1.0)
        try:
            proc.close()
        except Exception:
            pass
        if queue is not None:
            try:
                queue.close()
                queue.join_thread()
            except Exception:
                pass
        self._proc = None
        self._queue = None

    def _maybe_recycle(self) -> None:
        proc = self._proc
        if proc is None:
            self._start_proc()
            return
        if not proc.is_alive():
            now_mono = time.monotonic()
            if (now_mono - self._restart_window_start) > _WRITER_RESTART_STORM_WINDOW_S:
                self._restart_window_start = now_mono
                self._restart_count = 0
            self._restart_count += 1
            if self._restart_count > _WRITER_RESTART_STORM_MAX:
                # Example: keep running on one bad file, but stop on systemic child-process churn.
                raise OptionChainWriteError(
                    f"writer process restart storm: {_WRITER_RESTART_STORM_MAX} in {_WRITER_RESTART_STORM_WINDOW_S:.0f}s"
                )
            self._stop_proc()
            self._start_proc()
            log_warn(
                _LOG,
                "ingestion.writer_process_restarted",
                currency=self._asset,
                interval=self._interval,
                reason="dead_process",
                pid=int(self._proc.pid) if self._proc is not None and self._proc.pid is not None else None,
            )
            return
        by_time = self._recycle_seconds > 0.0 and (time.monotonic() - self._started_monotonic) >= self._recycle_seconds
        by_tasks = self._recycle_tasks > 0 and self._enqueued >= self._recycle_tasks
        if not by_time and not by_tasks:
            return
        old_pid = int(proc.pid) if proc.pid is not None else None
        self._signal_stop()
        self._stop_proc()
        self._start_proc()
        log_debug(
            _LOG,
            "ingestion.writer_process_recycled",
            currency=self._asset,
            interval=self._interval,
            reason="time" if by_time else "tasks",
            old_pid=old_pid,
            new_pid=int(self._proc.pid) if self._proc is not None and self._proc.pid is not None else None,
        )

    def enqueue(self, payload: Mapping[str, Any]) -> None:
        self._maybe_recycle()
        proc = self._proc
        queue = self._queue
        if proc is None or queue is None or not proc.is_alive():
            raise OptionChainWriteError("writer process is not alive")
        try:
            queue.put(dict(payload), timeout=self._put_timeout_s)
            self._enqueued += 1
        except queue_mod.Full as exc:
            raise OptionChainWriteError("writer process queue full") from exc
        except Exception as exc:
            raise OptionChainWriteError(f"writer process enqueue failed: {exc}") from exc

    def close(self) -> None:
        self._signal_stop()
        self._stop_proc()


def _write_quote_snapshot(
    *,
    root: Path,
    asset: str,
    interval: str,
    df: pd.DataFrame,
    step_ts: int,
    used_paths: set[Path],
    used_paths_lock: threading.Lock | None = None,
    bad_writer_ids: dict[Path, int] | None = None,
    write_counter: list[int] | None = None,
) -> None:
    path = _quote_date_path(root, interval=interval, asset=asset, step_ts=step_ts)
    current_date = datetime.fromtimestamp(int(step_ts) / 1000.0, tz=timezone.utc).strftime("%Y_%m_%d")
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = _get_lock(path)
    write_start = time.monotonic()
    start = time.monotonic()
    lock.acquire()
    waited_s = time.monotonic() - start
    if waited_s > _LOCK_WARN_S:
        log_debug(
            _LOG,
            "ingestion.lock_wait",
            component="option_quote",
            path=str(path),
            wait_ms=int(waited_s * 1000.0),
        )
    try:
        guard = _PathLockGuard(path, lock)
        did_incref = False
        writer_id: int | None = None
        _rotate_if_date_changed_locked(path, lock, current_date)
        writer, schema, did_incref = _get_writer_and_schema(
            path,
            df,
            used_paths,
            guard,
            used_paths_lock=used_paths_lock,
            bad_writer_ids=bad_writer_ids,
        )
        writer_id = id(writer)
        aligned = _align_to_schema(df, schema, path)
        table = pa.Table.from_pandas(aligned, schema=schema, preserve_index=False)
        writer.write_table(table)
        _maybe_periodic_close_locked(path, lock)
        if write_counter is not None:
            write_counter[0] += 1
            write_count = write_counter[0]
        else:
            write_count = None
        if write_count is not None and write_count % _WRITE_LOG_EVERY == 0:
            log_debug(
                _LOG,
                "ingestion.write_sample",
                component="option_quote",
                path=str(path),
                rows=int(len(df)),
                write_ms=int((time.monotonic() - write_start) * 1000),
                write_seq=write_count,
            )
    except Exception as exc:
        _handle_write_exception_locked(
            path=path,
            lock=lock,
            used_paths=used_paths,
            used_paths_lock=used_paths_lock,
            bad_writer_ids=bad_writer_ids,
            did_incref=did_incref,
            writer_id=writer_id,
        )
        raise OptionChainWriteError(str(exc)) from exc
    finally:
        lock.release()

class OptionChainFileSource(Source):
    """
    Option chain source backed by local parquet snapshots.

    Layout:
        data/raw/option_chain/<ASSET>/<INTERVAL>/<YYYY>/<YYYY>_<MM>_<DD>.parquet.
    """

    def __init__(
        self,
        *,
        root: str | Path,
        asset: str,
        interval: str | None = None,
        start_ts: int | None = None,
        end_ts: int | None = None,
        paths: Iterable[Path] | None = None,
    ):
        self._root = resolve_under_root(DATA_ROOT, root, strip_prefix="data")
        self._asset = str(asset)
        self._interval = str(interval) if interval is not None else None
        self._start_ts = int(start_ts) if start_ts is not None else None
        self._end_ts = int(end_ts) if end_ts is not None else None
        self._path = self._root / self._asset
        if self._interval is not None:
            self._path = self._path / self._interval
        if paths is not None:
            resolved_paths: list[Path] = []
            for p in paths:
                path = Path(p)
                if not path.is_absolute():
                    path = self._root / path
                resolved_paths.append(path)
            self._paths: list[Path] | None = resolved_paths
        else:
            self._paths = None

    def __iter__(self) -> Iterator[Raw]:
        try:
            import pandas as pd
        except ImportError as e:
            raise RuntimeError("pandas is required for OptionChainFileSource parquet loading") from e

        if self._paths is not None:
            files: list[Path] = [p for p in self._paths if p.exists()]
        elif self._start_ts is not None or self._end_ts is not None:
            start_date = datetime.fromtimestamp((self._start_ts or 0) / 1000.0, tz=timezone.utc).date()
            end_date = datetime.fromtimestamp((self._end_ts or _now_ms()) / 1000.0, tz=timezone.utc).date()

            dated_files: list[tuple[date, Path]] = []
            for year_dir in sorted(self._path.glob("[0-9][0-9][0-9][0-9]")):
                if not year_dir.is_dir():
                    continue
                year_name = year_dir.name
                if not year_name.isdigit():
                    continue
                year = int(year_name)
                if year < start_date.year or year > end_date.year:
                    continue

                for fp in year_dir.glob("*.parquet"):
                    stem = fp.stem
                    parts = stem.split("_")
                    if len(parts) != 3 or not all(p.isdigit() for p in parts):
                        continue
                    try:
                        d = datetime(int(parts[0]), int(parts[1]), int(parts[2]), tzinfo=timezone.utc).date()
                    except Exception:
                        continue
                    if start_date <= d <= end_date:
                        dated_files.append((d, fp))

            dated_files.sort(key=lambda t: (t[0], str(t[1])))
            files: list[Path] = [fp for _, fp in dated_files]
        else:
            files: list[Path] = sorted(self._path.rglob("*.parquet"))
        if not files:
            return

        for fp in files:
            df = pd.read_parquet(fp)
            if df is None or df.empty:
                continue
            if "arrival_ts" not in df.columns:
                if "data_ts" in df.columns:
                    df["arrival_ts"] = df["data_ts"]
                else:
                    raise ValueError(f"Parquet file {fp} missing arrival_ts column")

            df["arrival_ts"] = df["arrival_ts"].map(_coerce_epoch_ms)
            if self._start_ts is not None:
                df = df[df["arrival_ts"] >= int(self._start_ts)]
            if self._end_ts is not None:
                df = df[df["arrival_ts"] <= int(self._end_ts)]
            if df.empty:
                continue
            df = df.sort_values(["arrival_ts"], kind="stable")
            df["arrival_ts"] = df["arrival_ts"].astype("int64")
            for ts, sub in df.groupby("arrival_ts", sort=True):
                snap = sub.reset_index(drop=True)
                # Source contract: yield a Mapping[str, Any]
                assert isinstance(ts, (int, float)), f"data_ts must be int/float, got {type(ts)!r}"
                yield {"arrival_ts": int(ts), "data_ts": int(ts), "frame": snap}


class DeribitOptionChainRESTSource(Source):
    """Deribit option-chain source using Deribit JSON-RPC polling.

    Fetches instrument metadata (public/get_instruments) and quote summaries
    (public/get_book_summary_by_currency), writing:
      - data/raw/option_chain/<ASSET>/<INTERVAL>/<YYYY>/<YYYY>_<MM>_<DD>.parquet
      - data/raw/option_quote/<ASSET>/<INTERVAL>/<YYYY>/<YYYY>_<MM>_<DD>.parquet
    """
    _global_lock = threading.Lock()
    _locks_guard = threading.Lock()
    _global_writers: dict[Path, pq.ParquetWriter] = {}
    _global_schemas: dict[Path, pa.Schema] = {}
    _global_locks: dict[Path, threading.Lock] = {}
    _global_refs: dict[Path, int] = {}
    _current_dates: dict[Path, str] = {}
    _path_write_counts: dict[Path, int] = {}
    _writer_last_close_monotonic: dict[Path, float] = {}

    def __init__(
        self,
        *,
        currency: str,
        interval: str | None = None,
        poll_interval: float | None = None,
        poll_interval_ms: int | None = None,
        base_url: str = "https://www.deribit.com",
        rpc_url: str | None = None,
        timeout: float = 10.0,
        root: str | Path = DATA_ROOT / "raw" / "option_chain",
        quote_root: str | Path = DATA_ROOT / "raw" / "option_quote",
        universe_root: str | Path = DATA_ROOT / "raw" / "option_universe",
        kind: str = "option",
        expired: bool = False,
        chain_ttl_s: float | None = 3600.0,
        max_retries: int = 5,
        backoff_s: float = 1.0,
        backoff_max_s: float = 30.0,
        writer_process: bool = False,
        writer_queue_size: int = _WRITER_QUEUE_SIZE,
        writer_put_timeout_s: float = _WRITER_QUEUE_PUT_TIMEOUT_S,
        writer_recycle_seconds: float = _WRITER_RECYCLE_SECONDS,
        writer_recycle_tasks: int = _WRITER_RECYCLE_TASKS,
        stop_event: threading.Event | None = None,
        session: requests.Session | None = None,
    ):

        self._currency = str(currency)
        self._base_url = base_url.rstrip("/")
        self._rpc_url = rpc_url or f"{self._base_url}/api/v2"
        self._timeout = float(timeout)
        self.interval = interval if interval is not None else "1m"
        self._kind = str(kind)
        self._expired = bool(expired)
        self._root = resolve_under_root(DATA_ROOT, root, strip_prefix="data")
        self._quote_root = resolve_under_root(DATA_ROOT, quote_root, strip_prefix="data")
        self._universe_root = resolve_under_root(DATA_ROOT, universe_root, strip_prefix="data")
        self._root.mkdir(parents=True, exist_ok=True)
        self._quote_root.mkdir(parents=True, exist_ok=True)
        self._universe_root.mkdir(parents=True, exist_ok=True)
        self._max_retries = int(max_retries)
        self._backoff_s = float(backoff_s)
        self._backoff_max_s = float(backoff_max_s)
        self._stop_event = stop_event
        self._used_paths: set[Path] = set()
        self._used_paths_lock = threading.Lock()
        self._bad_writer_ids: dict[Path, int] = {}
        self._write_count = 0
        self._quote_write_count = 0
        self._universe_write_count = 0
        self._chain_ttl_ms = int(round(float(chain_ttl_s) * 1000.0)) if chain_ttl_s is not None else 0
        self._chain_cache: pd.DataFrame | None = None
        self._chain_cache_step_ts: int | None = None
        self._chain_cache_arrival_ts: int | None = None
        self._session = session or requests.Session()
        self._memory_trim = PeriodicMemoryTrim(component="option_chain")
        self._write_dispatcher: _ProcessWriteDispatcher | None = None

        interval_ms = _to_interval_ms(self.interval)
        if interval_ms is None:
            raise ValueError(f"Invalid interval format: {self.interval!r}")

        if poll_interval_ms is not None:
            poll_ms = int(poll_interval_ms)
        elif poll_interval is not None:
            poll_ms = int(round(float(poll_interval) * 1000.0))
        else:
            poll_ms = int(interval_ms)

        if poll_ms != int(interval_ms):
            log_warn(
                _LOG,
                "ingestion.poll_interval_override",
                domain="option_chain",
                interval=self.interval,
                interval_ms=int(interval_ms),
                poll_interval_ms=int(poll_ms),
            )
            poll_ms = int(interval_ms)

        self._poll_interval_ms = poll_ms
        _guard_interval_ms(self.interval, self._poll_interval_ms)

        if self._poll_interval_ms <= 0:
            raise ValueError(f"poll interval must be > 0ms, got {self._poll_interval_ms}")

        use_writer_process = bool(writer_process)
        if not use_writer_process:
            env_val = str(os.getenv("OPTION_CHAIN_WRITE_PROCESS", "0")).strip().lower()
            use_writer_process = env_val in {"1", "true", "yes", "y", "on"}
        if use_writer_process:
            recycle_seconds = float(os.getenv("OPTION_CHAIN_WRITER_RECYCLE_SECONDS", str(writer_recycle_seconds)))
            recycle_tasks = int(os.getenv("OPTION_CHAIN_WRITER_RECYCLE_TASKS", str(writer_recycle_tasks)))
            self._write_dispatcher = _ProcessWriteDispatcher(
                root=self._root,
                quote_root=self._quote_root,
                universe_root=self._universe_root,
                asset=self._currency,
                interval=self.interval,
                queue_size=int(writer_queue_size),
                put_timeout_s=float(writer_put_timeout_s),
                recycle_seconds=float(recycle_seconds),
                recycle_tasks=int(recycle_tasks),
            )
            pid = None
            if self._write_dispatcher is not None and self._write_dispatcher._proc is not None:
                pid = self._write_dispatcher._proc.pid
            log_warn(
                _LOG,
                "ingestion.writer_process_enabled",
                currency=self._currency,
                interval=self.interval,
                queue_size=int(writer_queue_size),
                recycle_seconds=float(recycle_seconds),
                recycle_tasks=int(recycle_tasks),
                writer_pid=int(pid) if pid is not None else None,
            )

    def __iter__(self) -> Iterator[Raw]:
        try:
            while True:
                if self._stop_event is not None and self._stop_event.is_set():
                    break
                for snap in self.fetch():
                    yield snap
                if self._sleep_or_stop(self._poll_interval_ms / 1000.0):
                    return
        finally:
            self._close_writers()

    def fetch(self, *, step_ts: int | None = None, include_records: bool = True) -> list[Raw]:  # +
        return self.fetch_step(step_ts=step_ts, include_records=include_records)  # +

    def fetch_step(self, *, step_ts: int | None = None, include_records: bool = True) -> list[Raw]:  # +
        if self._stop_event is not None and self._stop_event.is_set():
            self._memory_trim.maybe_run(logger=_LOG, currency=self._currency, interval=self.interval)
            return []
        try:
            import pandas as pd
        except ImportError as e:
            raise RuntimeError("pandas is required for DeribitOptionChainRESTSource parquet writing") from e

        # step_ts: rounded ingestion sampling anchor (cadence clock), not exchange time.
        # data_ts: local arrival time when this snapshot is received/assembled.
        # arrival_ts: receive time for each RPC response within the step.
        # market_ts: exchange-provided timestamp if available, else NaN.
        # fetch_step_ts: explicit column carrying the sampling anchor for alignment.
        anchor_ts = _coerce_epoch_ms(step_ts) if step_ts is not None else _now_ms()
        backoff = self._backoff_s
        for _ in range(self._max_retries):
            try:
                chain_df = self._get_chain_df(anchor_ts)
                quote_df, quote_arrival_ts = self._fetch_quote_df(anchor_ts)
                records: list[dict[str, Any]] | None = [] if include_records else None  # +

                if quote_df is not None and not quote_df.empty:
                    self._write_quote_snapshot(df=quote_df, step_ts=int(anchor_ts))

                merged = self._merge_chain_quote(
                    chain_df=chain_df,
                    quote_df=quote_df,
                    step_ts=int(anchor_ts),
                    quote_arrival_ts=int(quote_arrival_ts),
                )
                if merged is not None and not merged.empty:
                    merged = _flatten_object_columns(merged)
                    if "data_ts" in merged.columns:
                        merged = merged.rename(columns={"data_ts": "row_data_ts"})
                    aux_cols = {
                        "creation_timestamp",
                        "creation_timestamp_quote",
                        "base_currency_quote",
                        "quote_currency_quote",
                        "maker_commission",
                        "taker_commission",
                        "block_trade_commission",
                        "block_trade_min_trade_amount",
                        "block_trade_tick_size",
                        "row_data_ts",
                        "fetch_step_ts",
                        "arrival_ts",
                        "aux_chain_data_ts",
                        "aux_chain_arrival_ts",
                    }
                    quote_cols = {
                        "bid_price",
                        "ask_price",
                        "mid_price",
                        "last",
                        "mark_price",
                        "open_interest",
                        "volume_24h",
                        "volume_usd_24h",
                        "mark_iv",
                        "high",
                        "low",
                        "price_change",
                        "market_ts",
                    }
                    aux_cols |= {c for c in merged.columns if c.endswith("_quote") and c not in quote_cols}
                    merged = _pack_aux(merged, aux_cols)
                    merged["arrival_ts"] = int(quote_arrival_ts)
                    raw_df = _prepare_raw_snapshot_df(merged)
                    self._write_raw_snapshot(df=raw_df, data_ts=int(quote_arrival_ts))
                    if include_records:  # +
                        records = [{str(k): v for k, v in rec.items()} for rec in merged.to_dict(orient="records")]  # +

                # Source contract: return Mapping[str, Any] items
                self._memory_trim.maybe_run(logger=_LOG, currency=self._currency, interval=self.interval)
                return [{"data_ts": int(quote_arrival_ts), "records": records}]  # +
            except Exception as exc:
                log_warn(
                    _LOG,
                    "option_chain.fetch_or_write_error",
                    err_type=type(exc).__name__,
                    err=str(exc),
                )
                if isinstance(exc, OptionChainWriteError):
                    raise
                import random  # + add jitter to prevent synchronized retries  # +
                sleep_s = min(backoff, self._backoff_max_s)  # +
                jittered_sleep_s = min(self._backoff_max_s, max(0.0, sleep_s * random.uniform(0.8, 1.2)))  # + add jitter to prevent synchronized retries  # +
                if self._sleep_or_stop(jittered_sleep_s):  # +
                    self._memory_trim.maybe_run(logger=_LOG, currency=self._currency, interval=self.interval)
                    return []
                backoff = min(backoff * 2.0, self._backoff_max_s)
        self._memory_trim.maybe_run(logger=_LOG, currency=self._currency, interval=self.interval)
        return []

    def _write_raw_snapshot(self, *, df, data_ts: int) -> None:
        if self._write_dispatcher is not None:
            self._write_dispatcher.enqueue(
                {
                    "kind": "raw",
                    "df": df,
                    "data_ts": int(data_ts),
                }
            )
            self._write_count += 1
            return
        write_counter = [self._write_count]
        _write_raw_snapshot(
            root=self._root,
            asset=self._currency,
            interval=self.interval,
            df=df,
            data_ts=int(data_ts),
            used_paths=self._used_paths,
            used_paths_lock=self._used_paths_lock,
            bad_writer_ids=self._bad_writer_ids,
            write_counter=write_counter,
        )
        self._write_count = write_counter[0]

    def _write_quote_snapshot(self, *, df: pd.DataFrame, step_ts: int) -> None:
        if self._write_dispatcher is not None:
            self._write_dispatcher.enqueue(
                {
                    "kind": "quote",
                    "df": df,
                    "step_ts": int(step_ts),
                }
            )
            self._quote_write_count += 1
            return
        write_counter = [self._quote_write_count]
        _write_quote_snapshot(
            root=self._quote_root,
            asset=self._currency,
            interval=self.interval,
            df=df,
            step_ts=int(step_ts),
            used_paths=self._used_paths,
            used_paths_lock=self._used_paths_lock,
            bad_writer_ids=self._bad_writer_ids,
            write_counter=write_counter,
        )
        self._quote_write_count = write_counter[0]

    def _write_universe_snapshot(self, *, df: pd.DataFrame, data_ts: int) -> None:
        if self._write_dispatcher is not None:
            self._write_dispatcher.enqueue(
                {
                    "kind": "universe",
                    "df": df,
                    "data_ts": int(data_ts),
                }
            )
            self._universe_write_count += 1
            return
        write_counter = [self._universe_write_count]
        _write_universe_snapshot(
            root=self._universe_root,
            asset=self._currency,
            interval=self.interval,
            df=df,
            data_ts=int(data_ts),
            used_paths=self._used_paths,
            used_paths_lock=self._used_paths_lock,
            bad_writer_ids=self._bad_writer_ids,
            write_counter=write_counter,
        )
        self._universe_write_count = write_counter[0]

    def _should_refresh_chain(self, step_ts: int) -> bool:
        if self._chain_ttl_ms <= 0:
            return True
        if self._chain_cache is None or self._chain_cache_step_ts is None:
            return True
        return int(step_ts) - int(self._chain_cache_step_ts) >= int(self._chain_ttl_ms)

    def _get_chain_df(self, step_ts: int) -> pd.DataFrame:
        if self._should_refresh_chain(step_ts):
            chain_df, arrival_ts = self._fetch_chain_df(step_ts)
            self._chain_cache = chain_df
            self._chain_cache_step_ts = int(step_ts)
            self._chain_cache_arrival_ts = int(arrival_ts)
        if self._chain_cache is None:
            return pd.DataFrame()
        return self._chain_cache.copy()

    def _fetch_chain_df(self, step_ts: int) -> tuple[pd.DataFrame, int]:
        params = {
            "currency": self._currency,
            "kind": self._kind,
            "expired": self._expired,
        }
        result = deribit_rpc(
            "public/get_instruments",
            params,
            session=self._session,
            timeout=self._timeout,
            url=self._rpc_url,
        )
        if not isinstance(result, list):
            raise RuntimeError(f"Unexpected Deribit response: {type(result)!r}")
        arrival_ts = _now_ms()
        df = pd.DataFrame(result or [])
        if df.empty:
            return df, arrival_ts
        df = _flatten_object_columns(df)
        df["fetch_step_ts"] = int(step_ts)
        self._write_universe_snapshot(df=df, data_ts=int(arrival_ts))
        # After process-boundary handoff of a mutable object, do not mutate that same object.
        # Create a new object for subsequent mutations.
        df = df.assign(aux_chain_data_ts=int(step_ts), aux_chain_arrival_ts=int(arrival_ts))
        return df, arrival_ts

    def _fetch_quote_df(self, step_ts: int) -> tuple[pd.DataFrame, int]:
        params = {
            "currency": self._currency,
            "kind": self._kind,
        }
        result = deribit_rpc(
            "public/get_book_summary_by_currency",
            params,
            session=self._session,
            timeout=self._timeout,
            url=self._rpc_url,
        )
        if not isinstance(result, list):
            raise RuntimeError(f"Unexpected Deribit response: {type(result)!r}")
        arrival_ts = _now_ms()
        df = pd.DataFrame(result or [])
        if df.empty:
            return df, arrival_ts
        df = _flatten_object_columns(df)
        df = df.rename(
            columns={
                "volume": "volume_24h",
                "volume_usd": "volume_usd_24h",
            }
        )
        def _safe_market_ts(x: Any) -> int | None:
            if x is None:
                return None
            try:
                return int(_coerce_epoch_ms(x))
            except Exception:
                return None

        market_ts_source = None
        if "timestamp" in df.columns:
            market_ts_source = "timestamp"
            market_ts_list = df["timestamp"].map(_safe_market_ts).tolist()
        elif "creation_timestamp" in df.columns:
            market_ts_source = "creation_timestamp"
            market_ts_list = df["creation_timestamp"].map(_safe_market_ts).tolist()
        elif "creation_timestamp_quote" in df.columns:
            market_ts_source = "creation_timestamp_quote"
            market_ts_list = df["creation_timestamp_quote"].map(_safe_market_ts).tolist()
        else:
            market_ts_list = [pd.NA] * len(df)

        df["market_ts"] = pd.Series(market_ts_list, dtype="Int64")
        if market_ts_source is not None:
            log_debug(
                _LOG,
                "option_quote.market_ts_source",
                source=market_ts_source,
            )
        df["fetch_step_ts"] = int(step_ts)
        df["arrival_ts"] = int(arrival_ts)
        return df, arrival_ts

    def _merge_chain_quote(
        self,
        *,
        chain_df: pd.DataFrame,
        quote_df: pd.DataFrame | None,
        step_ts: int,
        quote_arrival_ts: int,
    ) -> pd.DataFrame:
        if chain_df is None or chain_df.empty:
            return pd.DataFrame()
        chain_frame = chain_df.copy()
        chain_frame["fetch_step_ts"] = int(step_ts)
        if "aux_chain_data_ts" not in chain_frame.columns:
            chain_frame["aux_chain_data_ts"] = int(step_ts)
        if "aux_chain_arrival_ts" not in chain_frame.columns:
            chain_frame["aux_chain_arrival_ts"] = (
                int(self._chain_cache_arrival_ts) if self._chain_cache_arrival_ts is not None else int(quote_arrival_ts)
            )
        if quote_df is None or quote_df.empty or "instrument_name" not in quote_df.columns:
            return chain_frame
        quote_merge = quote_df.drop(columns=["fetch_step_ts", "arrival_ts"], errors="ignore")
        return chain_frame.merge(
            quote_merge,
            on="instrument_name",
            how="left",
            suffixes=("", "_quote"),
        )

    def _sleep_or_stop(self, seconds: float) -> bool:
        if self._stop_event is None:
            time.sleep(seconds)
            return False
        return self._stop_event.wait(seconds)

    def _get_lock(self, path: Path) -> threading.Lock:
        with self._locks_guard:
            lock = self._global_locks.get(path)
            if lock is None:
                lock = threading.Lock()
                self._global_locks[path] = lock
            return lock

    def _get_writer_and_schema(
        self,
        path: Path,
        df: pd.DataFrame,
    ) -> tuple[pq.ParquetWriter, pa.Schema]:
        lock = _get_lock(path)
        lock.acquire()
        try:
            guard = _PathLockGuard(path, lock)
            writer, schema, _ = _get_writer_and_schema(
                path,
                df,
                self._used_paths,
                guard,
                used_paths_lock=self._used_paths_lock,
                bad_writer_ids=self._bad_writer_ids,
            )
            return writer, schema
        finally:
            lock.release()

    def _bootstrap_existing(
        self,
        path: Path,
        df: pd.DataFrame,
    ) -> tuple[pq.ParquetWriter, pa.Schema] | None:
        lock = _get_lock(path)
        lock.acquire()
        try:
            guard = _PathLockGuard(path, lock)
            result = _bootstrap_existing(
                path,
                df,
                self._used_paths,
                guard,
                used_paths_lock=self._used_paths_lock,
            )
            if result is None:
                return None
            writer, schema, _ = result
            return writer, schema
        finally:
            lock.release()

    def _align_to_schema(self, df: pd.DataFrame, schema: pa.Schema, path: Path) -> pd.DataFrame:
        return _align_to_schema(df, schema, path)  # + delegate to module-level helper to avoid drift  # +

    def _align_table_to_schema(self, table: pa.Table, schema: pa.Schema, path: Path) -> pa.Table:
        return _align_table_to_schema(table, schema, path)  # + delegate to module-level helper to avoid drift  # +

    def _close_writers(self) -> None:
        if self._write_dispatcher is not None:
            self._write_dispatcher.close()
            self._write_dispatcher = None
        paths = sorted(_used_paths_snapshot(self._used_paths, self._used_paths_lock), key=lambda p: str(p))
        for path in paths:
            lock = _get_lock(path)
            lock.acquire()
            try:
                writer = None
                with self._global_lock:
                    ref = self._global_refs.get(path, 0) - 1
                    if ref <= 0:
                        writer = self._global_writers.pop(path, None)
                        self._global_refs.pop(path, None)
                        self._global_schemas.pop(path, None)
                        self._current_dates.pop(path, None)
                        self._path_write_counts.pop(path, None)
                        self._writer_last_close_monotonic.pop(path, None)
                    else:
                        self._global_refs[path] = ref
                _used_paths_discard(self._used_paths, path, self._used_paths_lock)
                if writer is not None:
                    try:
                        writer.close()
                        log_debug(_LOG, "ingestion.writer_close", path=str(path))
                    except Exception as exc:
                        log_debug(
                            _LOG,
                            "ingestion.close_error",
                            path=str(path),
                            err_type=type(exc).__name__,
                            err=str(exc),
                        )
            finally:
                lock.release()
        _used_paths_clear(self._used_paths, self._used_paths_lock)


class OptionChainStreamSource(AsyncSource):
    """
    Option chain source backed by an async stream.
    """

    def __init__(self, stream: AsyncIterable[Raw] | None = None):
        self._stream = stream

    def __aiter__(self) -> AsyncIterator[Raw]:
        async def _gen():
            assert self._stream is not None, "stream must be provided for OptionChainStreamSource"
            async for msg in self._stream:
                yield msg

        return _gen()
