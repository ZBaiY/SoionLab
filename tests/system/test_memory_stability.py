from __future__ import annotations

from quant_engine.contracts.portfolio import PortfolioState
from quant_engine.runtime.driver import BaseDriver, _SNAPSHOT_MAXLEN
from quant_engine.runtime.modes import EngineMode, EngineSpec
from quant_engine.runtime.snapshot import EngineSnapshot


class _DummyEngine:
    def iter_shutdown_objects(self):
        return []


class _DummyDriver(BaseDriver):
    async def iter_timestamps(self):
        if False:
            yield 0

    async def run(self) -> None:
        return None


def test_snapshot_storage_remains_bounded_over_long_run() -> None:
    driver = _DummyDriver(
        engine=_DummyEngine(),  # type: ignore[arg-type]
        spec=EngineSpec.from_interval(mode=EngineMode.MOCK, interval="1m", symbol="BTCUSDT"),
    )
    for i in range(5000):
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

