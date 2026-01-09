from __future__ import annotations

import asyncio
from logging import Logger
import time
from typing import Any, Awaitable, Callable, Iterator

from ingestion.contracts.source import AsyncSource, Source
from ingestion.contracts.tick import IngestionTick, normalize_tick
from ingestion.contracts.worker import IngestWorker
from ingestion.trades.normalize import BinanceAggTradesNormalizer
from ingestion.trades.worker import TradesWorker
from quant_engine.utils.asyncio import iter_source
from quant_engine.utils.logger import get_logger, init_logging, log_exception, log_info


class _DummySource:
    def __init__(self, *, n: int) -> None:
        self._n = int(n)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        for i in range(self._n):
            now_ms = int(time.time() * 1000)
            yield {"timestamp": now_ms, "data_ts": now_ms, "price": i}
        raise RuntimeError("dummy source failure")


class _DummyNormalizer:
    def __init__(self, *, symbol: str) -> None:
        self._symbol = symbol

    def normalize(self, raw: dict[str, Any]) -> IngestionTick:
        return normalize_tick(
            timestamp=raw["timestamp"],
            data_ts=raw["data_ts"],
            domain="trades",
            symbol=self._symbol,
            payload=raw,
            source_id=getattr(self, "source_id", None),
        )

class DummyWorker(IngestWorker):
    def __init__(self, *, normalizer: _DummyNormalizer, source: _DummySource,
                    symbol: str, poll_interval: float | None = None, 
                    poll_interval_ms: int | None = None, logger: Logger | None = None) -> None:
        super().__init__()
        self._normalizer = normalizer
        self._source = source
        self._symbol = symbol
        self._logger = logger or get_logger(f"ingestion.dummy.{self.__class__.__name__}")
        self._poll_seq = 0
        self._error_logged = False
        self._poll_interval = poll_interval
        self._poll_interval_ms = poll_interval_ms
        
    async def run(self, emit: Callable[[IngestionTick], Awaitable[None]]) -> None:
        log_info(self._logger, "worker.start", symbol=self._symbol)
        try:
            async for raw in iter_source(source= self._source,
                                            logger = self._logger,
                                            context = {},
                                            poll_interval_s = self._poll_interval_ms,
                                            log_exceptions = True):
                tick = self._normalizer.normalize(raw)
                try:
                    r = emit(tick)
                    if asyncio.iscoroutine(r):
                        await r
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log_exception(self._logger, "worker.emit.error", exc=exc, symbol=self._symbol)
        except Exception as exc:
            log_exception(self._logger, "worker.run.error", exc=exc, symbol=self._symbol)
            raise
        finally:
            log_info(self._logger, "worker.stop", symbol=self._symbol)
    


async def main() -> None:
    init_logging()
    logger = get_logger("ingestion.asyncio_smoke")
    worker = DummyWorker(
        normalizer=_DummyNormalizer(symbol="SMOKE"),
        source=_DummySource(n=2),
        symbol="SMOKE",
    )

    async def emit(_: IngestionTick) -> None:
        return None

    try:
        await worker.run(emit)
    except Exception as exc:
        logger.info("smoke caught exception: %s", exc)


if __name__ == "__main__":
    asyncio.run(main())
