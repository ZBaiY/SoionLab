from __future__ import annotations

from typing import Any

from quant_engine.contracts.feature import FeatureChannelBase


class FakeSnapshot:
    def __init__(self, close: float) -> None:
        self.close = float(close)

    def to_dict(self) -> dict[str, Any]:
        return {"close": self.close}


class FakeHandler:
    bootstrap_cfg: dict[str, Any] = {}

    def __init__(self, *, window_snaps: list[FakeSnapshot], snapshot: FakeSnapshot) -> None:
        self._window_snaps = list(window_snaps)
        self._snapshot = snapshot

    def load_history(self, *args: Any, **kwargs: Any) -> None:
        return None

    def bootstrap(self, *args: Any, **kwargs: Any) -> None:
        return None

    def warmup_to(self, *args: Any, **kwargs: Any) -> None:
        return None

    def align_to(self, *args: Any, **kwargs: Any) -> None:
        return None

    def last_timestamp(self) -> int | None:
        return None

    def get_snapshot(self, ts: int | None = None) -> FakeSnapshot | None:
        return self._snapshot

    def window(self, ts: int | None = None, n: int = 1) -> list[FakeSnapshot]:
        return list(self._window_snaps)

    def on_new_tick(self, tick: Any) -> None:
        return None


class WindowAnyFeature(FeatureChannelBase):
    def __init__(self, *, name: str, symbol: str) -> None:
        super().__init__(name=name, symbol=symbol)
        self._values: list[float] = []

    def required_window(self) -> dict[str, int]:
        return {"ohlcv": 2}

    def initialize(self, context: dict[str, Any], warmup_window: int | None = None) -> None:
        snaps = self.window_any_dicts(context, "ohlcv", 2)
        self._values = [float(s["close"]) for s in snaps]

    def update(self, context: dict[str, Any]) -> None:
        snap = self.snapshot_dict(context, "ohlcv")
        if not snap:
            return
        self._values = [float(snap["close"])]

    def output(self) -> list[float]:
        return list(self._values)


def test_window_any_dicts_handles_list_snapshots() -> None:
    handler = FakeHandler(
        window_snaps=[FakeSnapshot(1.0), FakeSnapshot(2.0)],
        snapshot=FakeSnapshot(3.0),
    )
    feature = WindowAnyFeature(name="TEST_WINDOW_ANY", symbol="BTCUSDT")
    context = {
        "timestamp": 0,
        "data": {"ohlcv": {"BTCUSDT": handler}},
    }

    feature.initialize(context)
    assert feature.output() == [1.0, 2.0]

    feature.update(context)
    assert feature.output() == [3.0]
