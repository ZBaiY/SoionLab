from __future__ import annotations

import argparse
import signal
import threading
import time
from pathlib import Path

from ingestion.contracts.tick import _to_interval_ms, _guard_interval_ms
from ingestion.option_chain.source import DeribitOptionChainRESTSource
from quant_engine.utils.paths import data_root_from_file, resolve_under_root

DATA_ROOT = data_root_from_file(__file__, levels_up=2)


def _parse_bool(value: str) -> bool:
    v = str(value).strip().lower()
    if v in {"1", "true", "yes", "y", "t"}:
        return True
    if v in {"0", "false", "no", "n", "f"}:
        return False
    raise ValueError(f"invalid bool: {value!r}")


def _parse_csv(value: str, *, upper: bool = False) -> list[str]:
    items = [p.strip() for p in str(value).split(",") if p.strip()]
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        v = item.upper() if upper else item
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _run_poll(
    *,
    currency: str,
    interval: str,
    root: Path,
    quote_root: Path,
    timeout_s: float,
    expired: bool,
    chain_ttl_s: float,
    stop_event: threading.Event,
) -> None:
    poll_ms = _to_interval_ms(interval)
    if poll_ms is None or poll_ms <= 0:
        raise ValueError(f"Invalid interval: {interval!r}")
    _guard_interval_ms(interval, int(poll_ms))

    source = DeribitOptionChainRESTSource(
        currency=currency,
        interval=interval,
        poll_interval_ms=int(poll_ms),
        root=root,
        quote_root=quote_root,
        timeout=timeout_s,
        expired=expired,
        chain_ttl_s=chain_ttl_s,
        stop_event=stop_event,
    )

    last_step_ts: int | None = None
    while not stop_event.is_set():
        now_ms = int(time.time() * 1000.0)
        step_ts = (now_ms // int(poll_ms)) * int(poll_ms)
        if last_step_ts is not None and step_ts <= last_step_ts:
            stop_event.wait(0.25)
            continue
        source.fetch_step(step_ts=step_ts)
        # print(f"[option_chain] currency={currency} interval={interval} step_ts={step_ts}")
        last_step_ts = int(step_ts)
        sleep_ms = max(0, (step_ts + int(poll_ms)) - int(time.time() * 1000.0))
        stop_event.wait(sleep_ms / 1000.0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Deribit option-chain + quote JSON-RPC scraper")
    parser.add_argument("--currencies", default="BTC,ETH", help="comma-separated, e.g. BTC,ETH")
    parser.add_argument("--intervals", default="1m,5m,1h", help="comma-separated, e.g. 1m,5m,1h")
    parser.add_argument("--expired", default="false", help="true|false")
    parser.add_argument("--chain-ttl-s", type=float, default=3600.0, help="chain cache TTL in seconds")
    parser.add_argument("--root", default=str(DATA_ROOT / "raw" / "option_chain"), help="option_chain output root")
    parser.add_argument("--quote-root", default=str(DATA_ROOT / "raw" / "option_quote"), help="option_quote output root")
    parser.add_argument("--timeout_s", type=float, default=10.0)
    args = parser.parse_args()

    currencies = _parse_csv(args.currencies, upper=True)
    intervals = _parse_csv(args.intervals)
    if not currencies:
        raise ValueError("currencies must be non-empty")
    if not intervals:
        raise ValueError("intervals must be non-empty")
    expired = _parse_bool(args.expired)
    chain_ttl_s = float(args.chain_ttl_s)

    root = resolve_under_root(DATA_ROOT, args.root, strip_prefix="data")
    root.mkdir(parents=True, exist_ok=True)
    quote_root = resolve_under_root(DATA_ROOT, args.quote_root, strip_prefix="data")
    quote_root.mkdir(parents=True, exist_ok=True)

    stop_event = threading.Event()

    def _handle_stop(signum, _frame):
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    threads: list[threading.Thread] = []
    for currency in currencies:
        for interval in intervals:
            t = threading.Thread(
                target=_run_poll,
                kwargs={
                    "currency": currency,
                    "interval": interval,
                    "root": root,
                    "quote_root": quote_root,
                    "timeout_s": float(args.timeout_s),
                    "expired": expired,
                    "chain_ttl_s": chain_ttl_s,
                    "stop_event": stop_event,
                },
                daemon=False,
            )
            threads.append(t)
            t.start()

    while not stop_event.is_set():
        time.sleep(1)

    print("Stopping option-chain scraper...")
    for t in threads:
        t.join(timeout=5.0)


if __name__ == "__main__":
    main()
