from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ingestion.option_chain.normalize import DeribitOptionChainNormalizer
from ingestion.option_chain.source import DeribitOptionChainRESTSource, OptionChainFileSource
from quant_engine.data.contracts.protocol_realtime import to_interval_ms
from quant_engine.data.derivatives.option_chain.helpers import _infer_data_ts, _tick_from_payload
from quant_engine.strategy.base import GLOBAL_PRESETS
from quant_engine.utils.logger import get_logger, log_debug, log_info, log_warn

_MODE_MAP = {
    "sample": "sample",
    "cleaned": "backtest",
    "raw": "backtest",
    "realtime": "realtime",
}

_ROOT_MAP = {
    "sample": "sample/option_chain",
    "cleaned": "cleaned/option_chain",
    "raw": "raw/option_chain",
}


def setup_logging(verbose: bool = False) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    return get_logger(__name__)


def now_ms() -> int:
    return int(time.time() * 1000)


def _to_utc_timestamp(ts: str | int | float | pd.Timestamp) -> pd.Timestamp:
    out = pd.Timestamp(ts)
    if out.tzinfo is None:
        return out.tz_localize("UTC")
    return out.tz_convert("UTC")


def _to_epoch_ms(ts: pd.Timestamp) -> int:
    return int(ts.value // 1_000_000)


def parse_date_ms(s: str | None) -> int | None:
    if not s:
        return None
    return int(datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)


def _guess_project_root() -> Path:
    here = Path.cwd().resolve()
    if (here / "src").exists() and (here / "data").exists():
        return here
    if here.name == "notebooks" and (here.parent / "src").exists() and (here.parent / "data").exists():
        return here.parent
    return here


def _resolve_interval(handler_kwargs: dict[str, Any]) -> str:
    interval = handler_kwargs.get("interval")
    if isinstance(interval, str) and interval:
        return interval

    preset_name = handler_kwargs.get("preset")
    if isinstance(preset_name, str) and preset_name:
        preset = GLOBAL_PRESETS.get(preset_name)
        if isinstance(preset, dict):
            preset_interval = preset.get("interval")
            if isinstance(preset_interval, str) and preset_interval:
                return preset_interval

    default_preset = GLOBAL_PRESETS.get("OPTION_CHAIN_5M", {})
    default_interval = default_preset.get("interval") if isinstance(default_preset, dict) else None
    if isinstance(default_interval, str) and default_interval:
        return default_interval

    raise RuntimeError("Unable to resolve option_chain interval from handler kwargs/preset")


def _resolve_bootstrap_lookback(handler_kwargs: dict[str, Any], *, maxlen: int) -> Any | None:
    bootstrap_cfg = handler_kwargs.get("bootstrap")
    if isinstance(bootstrap_cfg, dict) and "lookback" in bootstrap_cfg:
        return bootstrap_cfg.get("lookback")

    preset_name = handler_kwargs.get("preset")
    if isinstance(preset_name, str) and preset_name:
        preset = GLOBAL_PRESETS.get(preset_name)
        if isinstance(preset, dict):
            preset_bootstrap = preset.get("bootstrap")
            if isinstance(preset_bootstrap, dict) and "lookback" in preset_bootstrap:
                return preset_bootstrap.get("lookback")

    return {"bars": min(100, maxlen)}


def _find_latest_data_ts(project_root: Path, *, symbol: str, interval: str, mode: str) -> int | None:
    stage = _ROOT_MAP.get(mode, "raw/option_chain")
    data_dir = project_root / "data" / stage / symbol / interval
    if not data_dir.exists():
        return None

    latest: datetime | None = None
    for fp in data_dir.rglob("*.parquet"):
        parts = fp.stem.split("_")
        if len(parts) != 3 or not all(p.isdigit() for p in parts):
            continue
        try:
            d = datetime(int(parts[0]), int(parts[1]), int(parts[2]), 23, 59, 59, tzinfo=timezone.utc)
        except ValueError:
            continue
        if latest is None or d > latest:
            latest = d

    return int(latest.timestamp() * 1000) if latest is not None else None


def create_file_source(
    *,
    project_root: Path,
    asset: str,
    interval: str,
    mode: str,
    start_ts: int | None,
    end_ts: int | None,
) -> OptionChainFileSource:
    stage = _ROOT_MAP.get(mode, "raw/option_chain")
    root_path = project_root / "data" / stage
    return OptionChainFileSource(root=root_path, asset=asset, interval=interval, start_ts=start_ts, end_ts=end_ts)


def create_rest_source(*, currency: str, interval: str, stop_event: threading.Event) -> DeribitOptionChainRESTSource:
    return DeribitOptionChainRESTSource(
        currency=currency,
        interval=interval,
        kind="option",
        expired=False,
        stop_event=stop_event,
    )


def _payload_nrows(raw_payload: dict) -> int:
    v = None
    for key in ("frame", "chain", "records"):
        if key in raw_payload:
            v = raw_payload.get(key)
            if v is not None:
                break
    if v is None:
        return 0
    if isinstance(v, pd.DataFrame):
        return int(len(v))
    if isinstance(v, list):
        return int(len(v))
    return 0


def load_from_files(
    *,
    handler: Any,
    source: OptionChainFileSource,
    logger: logging.Logger,
) -> int:
    count = 0
    display_symbol = str(getattr(handler, "display_symbol", getattr(handler, "symbol", "")))
    source_id = getattr(handler, "source_id", None)
    last_ts = None
    last_snap = handler.cache.last() if hasattr(handler, "cache") else None
    if last_snap is not None:
        last_ts = int(last_snap.data_ts)

    for raw_payload in source:
        try:
            ts = int(_infer_data_ts(raw_payload))
            if last_ts is not None and int(ts) <= int(last_ts):
                continue
            tick = _tick_from_payload(raw_payload, symbol=display_symbol, source_id=source_id)
            if hasattr(handler, "align_to"):
                handler.align_to(int(ts))
            handler.on_new_tick(tick)
            last_ts = int(ts)
            count += 1
            if count % 100 == 0:
                log_debug(logger, "option_chain.load.progress", count=count, data_ts=int(ts))
        except Exception as exc:
            log_warn(logger, "option_chain.load.payload_error", error=str(exc))
    return count


def load_from_rest(
    *,
    handler: Any,
    source: DeribitOptionChainRESTSource,
    normalizer: DeribitOptionChainNormalizer,
    logger: logging.Logger,
    max_polls: int,
    stop_event: threading.Event | None = None,
) -> int:
    count = 0
    try:
        for raw_payload in source:
            try:
                tick = normalizer.normalize(raw=raw_payload)
                if hasattr(handler, "align_to"):
                    handler.align_to(int(tick.data_ts))
                handler.on_new_tick(tick)
                count += 1
                log_info(
                    logger,
                    "option_chain.rest.snapshot",
                    count=count,
                    data_ts=int(tick.data_ts),
                    nrows=_payload_nrows(dict(raw_payload) if isinstance(raw_payload, dict) else {}),
                )
                if count >= int(max_polls):
                    if stop_event is not None:
                        stop_event.set()
                    break
            except Exception as exc:
                log_warn(logger, "option_chain.rest.payload_error", error=str(exc))
    finally:
        if stop_event is not None:
            stop_event.set()
    return count


def build_and_bootstrap_option_chain_handler(
    *,
    handler_cls,
    handler_kwargs: dict,
    start_ts: str | None,
    end_ts: str | None,
):
    """
    Minimal notebook bootstrap helper.

    - Instantiate handler
    - Bootstrap at start_ts
    - Replay ingestion payloads step-by-step via on_new_tick (sync)
    - Return handler
    """
    logger = setup_logging(bool(handler_kwargs.get("verbose", False)))
    project_root = _guess_project_root()

    kwargs = dict(handler_kwargs)
    mode = str(kwargs.get("mode", "cleaned")).lower()
    interval = _resolve_interval(kwargs)
    kwargs.setdefault("interval", interval)

    if mode in _MODE_MAP:
        kwargs["mode"] = _MODE_MAP[mode]
    else:
        mode = "cleaned"
        kwargs["mode"] = _MODE_MAP[mode]

    handler = handler_cls(**kwargs)

    maxlen = getattr(getattr(handler, "cache", None), "maxlen", kwargs.get("maxlen", 512))
    maxlen = int(maxlen) if isinstance(maxlen, int) else 512

    start_ms = _to_epoch_ms(_to_utc_timestamp(start_ts)) if start_ts else None
    end_ms = _to_epoch_ms(_to_utc_timestamp(end_ts)) if end_ts else None

    if start_ms is None and mode != "realtime":
        interval_ms = to_interval_ms(interval)
        if interval_ms is None or interval_ms <= 0:
            raise RuntimeError(f"Invalid interval: {interval!r}")
        anchor = end_ms
        if anchor is None:
            symbol_for_scan = str(kwargs.get("symbol", getattr(handler, "asset", "BTC")))
            anchor = _find_latest_data_ts(project_root, symbol=symbol_for_scan, interval=interval, mode=mode)
        if anchor is None:
            anchor = now_ms()
        lookback_ms = int(maxlen * 1.5) * int(interval_ms)
        start_ms = int(anchor) - lookback_ms
        log_info(
            logger,
            "option_chain.auto_start_ts",
            start_ts=int(start_ms),
            anchor_ts=int(anchor),
            lookback_bars=int(maxlen * 1.5),
            interval_ms=int(interval_ms),
        )

    if end_ms is None:
        end_ms = now_ms()
    if start_ms is None:
        start_ms = int(end_ms)
    if int(end_ms) < int(start_ms):
        raise ValueError("end_ts must be >= start_ts")

    bootstrap = getattr(handler, "bootstrap", None)
    if not callable(bootstrap):
        raise RuntimeError("Handler does not expose bootstrap(anchor_ts=..., lookback=...)")

    bootstrap_lookback = _resolve_bootstrap_lookback(kwargs, maxlen=maxlen)
    bootstrap(anchor_ts=int(start_ms), lookback=bootstrap_lookback)

    if hasattr(handler, "align_to"):
        handler.align_to(int(start_ms))
    if hasattr(handler, "warmup_to"):
        handler.warmup_to(int(start_ms))

    symbol = str(kwargs.get("symbol", getattr(handler, "symbol", "BTC")))
    asset = str(getattr(handler, "asset", kwargs.get("asset", symbol)))
    normalizer = DeribitOptionChainNormalizer(symbol=symbol)
    setattr(normalizer, "source_id", getattr(handler, "source_id", None))

    if mode == "realtime":
        stop_event = threading.Event()
        source = create_rest_source(currency=asset, interval=interval, stop_event=stop_event)
        loaded = load_from_rest(
            handler=handler,
            source=source,
            normalizer=normalizer,
            logger=logger,
            max_polls=int(kwargs.get("poll_count", 5)),
            stop_event=stop_event,
        )
    else:
        source = create_file_source(
            project_root=project_root,
            asset=asset,
            interval=interval,
            mode=mode,
            start_ts=int(start_ms),
            end_ts=int(end_ms),
        )
        loaded = load_from_files(
            handler=handler,
            source=source,
            logger=logger,
        )

    log_info(
        logger,
        "option_chain.notebook_bootstrap.done",
        symbol=symbol,
        interval=interval,
        mode=mode,
        snapshots_loaded=int(loaded),
        start_ts=int(start_ms),
        end_ts=int(end_ms),
    )

    return handler


if __name__ == "__main__":
    from quant_engine.data.derivatives.option_chain.chain_handler import OptionChainDataHandler

    handler = build_and_bootstrap_option_chain_handler(
        handler_cls=OptionChainDataHandler,
        handler_kwargs={
            "symbol": "BTC",
            "preset": "OPTION_CHAIN_5M",
            "source": "DERIBIT",
            "cache": {
                "kind": "term",
                "maxlen": 512,
                "default_expiry_window": 5,
                "default_term_window": 5,
                "term_bucket_ms": 86_400_000,
            },
            "bootstrap": {"lookback": {"bars": 100}},
            "mode": "cleaned",
        },
        start_ts=None,
        end_ts=None,
    )
    print(type(handler).__name__)
    print("last_ts:", handler.last_timestamp() if hasattr(handler, "last_timestamp") else None)
