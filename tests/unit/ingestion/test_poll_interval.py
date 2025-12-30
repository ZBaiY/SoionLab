from __future__ import annotations

from quant_engine.utils.paths import data_root_from_file
from ingestion.option_chain.source import DeribitOptionChainRESTSource


def test_option_chain_poll_interval_ms_from_interval() -> None:
    data_root = data_root_from_file(__file__, levels_up=3)
    src = DeribitOptionChainRESTSource(
        currency="BTC",
        interval="1m",
        root=data_root / "raw" / "option_chain",
    )
    assert src._poll_interval_ms == 60_000
