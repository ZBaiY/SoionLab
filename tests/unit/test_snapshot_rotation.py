from __future__ import annotations

from quant_engine.runtime.driver import BaseDriver, _SNAPSHOT_MAXLEN
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.runtime.snapshot import EngineSnapshot
from quant_engine.contracts.portfolio import PortfolioState


class _DummyDriver(BaseDriver):
    async def iter_timestamps(self):
        if False:
            yield 0

    async def run(self) -> None:
        return None


class _DummyEngine:
    def iter_shutdown_objects(self):
        return []


def test_driver_snapshots_are_bounded() -> None:
    spec = EngineSpec(mode=EngineMode.MOCK, symbol="BTCUSDT", interval="1m", interval_ms=60_000)
    driver = _DummyDriver(engine=_DummyEngine(), spec=spec)
    for i in range(_SNAPSHOT_MAXLEN + 5):
        driver._snapshots.append(  # type: ignore[attr-defined]
            EngineSnapshot(
                timestamp=i,
                mode=EngineMode.MOCK,
                features={},
                model_outputs={},
                decision_score=None,
                target_position=None,
                fills=[],
                portfolio=PortfolioState({"symbol": "BTCUSDT"}),
            )
        )

    assert len(driver.snapshots) == _SNAPSHOT_MAXLEN
