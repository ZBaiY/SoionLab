from __future__ import annotations

import argparse
import threading
import time
from pathlib import Path

from ingestion.contracts.tick import _to_interval_ms
from ingestion.option_chain.source import DeribitOptionChainRESTSource


def _run_poll(*, asset: str, interval: str, root: Path, timeout_s: float) -> None:
    poll_ms = _to_interval_ms(interval)
    if poll_ms is None or poll_ms <= 0:
        raise ValueError(f"Invalid interval: {interval!r}")

    source = DeribitOptionChainRESTSource(
        currency=asset,
        interval=interval,
        poll_interval_ms=int(poll_ms),
        root=root,
        timeout=timeout_s,
    )

    for df in source:
        rows = 0 if df is None else len(df)
        print(f"[option_chain] asset={asset} interval={interval} rows={rows}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Deribit option-chain live snapshot scraper")
    parser.add_argument("--assets", default="BTC", help="comma-separated, e.g. BTC,ETH")
    parser.add_argument("--intervals", default="1m,5m,1h", help="comma-separated, e.g. 1m,5m,1h")
    parser.add_argument("--root", default="data/raw/option_chain", help="output root")
    parser.add_argument("--timeout_s", type=float, default=10.0)
    args = parser.parse_args()

    assets = [a.strip().upper() for a in str(args.assets).split(",") if a.strip()]
    intervals = [i.strip() for i in str(args.intervals).split(",") if i.strip()]

    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)

    threads: list[threading.Thread] = []
    for asset in assets:
        for interval in intervals:
            t = threading.Thread(
                target=_run_poll,
                kwargs={
                    "asset": asset,
                    "interval": interval,
                    "root": root,
                    "timeout_s": float(args.timeout_s),
                },
                daemon=True,
            )
            threads.append(t)
            t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping option-chain scraper...")


if __name__ == "__main__":
    main()
