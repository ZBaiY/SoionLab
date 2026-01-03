from __future__ import annotations

from typing import Iterable, Protocol
from collections.abc import Mapping

from ingestion.contracts.tick import IngestionTick
from quant_engine.data.contracts.protocol_realtime import OHLCVHandlerProto, RealTimeDataHandler
from quant_engine.runtime.modes import EngineSpec
from quant_engine.runtime.snapshot import EngineSnapshot


class StrategyEngineProto(Protocol):
    spec: EngineSpec
    ohlcv_handlers: Mapping[str, OHLCVHandlerProto]
    orderbook_handlers: Mapping[str, RealTimeDataHandler]
    option_chain_handlers: Mapping[str, RealTimeDataHandler]
    iv_surface_handlers: Mapping[str, RealTimeDataHandler]
    sentiment_handlers: Mapping[str, RealTimeDataHandler]
    trades_handlers: Mapping[str, RealTimeDataHandler]
    option_trades_handlers: Mapping[str, RealTimeDataHandler]

    def preload_data(self, *, anchor_ts: int | None = None) -> None:
        ...

    def bootstrap(self, *, anchor_ts: int | None = None) -> None:
        ...

    def load_history(
        self,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> None:
        ...

    def warmup_features(self, *, anchor_ts: int | None = None) -> None:
        ...

    def align_to(self, ts: int) -> None:
        ...

    def ingest_tick(self, tick: IngestionTick) -> None:
        ...

    def step(self, *, ts: int) -> EngineSnapshot:
        ...

    def get_snapshot(self) -> EngineSnapshot | None:
        ...

    def last_timestamp(self) -> int | None:
        ...

    def iter_shutdown_objects(self) -> Iterable[object]:
        ...
